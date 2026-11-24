// Package prom 是查 Prometheus 的四个只读工具：即时查询、范围查询、当前的告警、告警规则。
//
// 直接用 net/http 调 Prometheus 的 HTTP API，不引入客户端库（设计 3d 的 3.3）。
// 裁剪规则见设计 3.4：Result.Text 是裁过的纯文本，Result.Raw 是 Prometheus 返回的完整响应体。
package prom

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

// maxBodyBytes 是读响应体的上限。超过的不读完，直接报错让模型缩小查询。
const maxBodyBytes = 8 << 20

// Config 是这组工具的配置。
type Config struct {
	// BaseURL 是 Prometheus 的地址，例如 http://kps-prometheus.monitoring:9090。
	BaseURL string
	// HTTP 为 nil 时用 http.DefaultClient。
	HTTP *http.Client
	// Now 为 nil 时用 time.Now。测试里固定它，相对时间才能断言。
	Now func() time.Time
	// Hidden 是对模型不存在的命名空间（故障注入工具），nil 时什么都不隐藏。见 tool.Hidden。
	Hidden *tool.Hidden
}

// New 返回四个工具：prom_query、prom_query_range、prom_alerts、prom_rule。
func New(cfg Config) []tool.Tool {
	c := &client{base: strings.TrimRight(cfg.BaseURL, "/"), http: cfg.HTTP, now: cfg.Now, hidden: cfg.Hidden}
	if c.http == nil {
		c.http = http.DefaultClient
	}
	if c.now == nil {
		c.now = time.Now
	}
	return []tool.Tool{
		&queryTool{c: c},
		&rangeTool{c: c},
		&alertsTool{c: c},
		&ruleTool{c: c},
	}
}

// client 建好之后只读，所以工具可以并发调用。
type client struct {
	base   string
	http   *http.Client
	now    func() time.Time
	hidden *tool.Hidden
}

// envelope 是 Prometheus API 响应的外层。
type envelope struct {
	Status    string         `json:"status"`
	Data      jsontext.Value `json:"data"`
	ErrorType string         `json:"errorType"`
	Error     string         `json:"error"`
	Warnings  []string       `json:"warnings"`
	Infos     []string       `json:"infos"`
}

// response 是一次成功调用的结果。
type response struct {
	data jsontext.Value
	// notes 是 Prometheus 附带的 warnings 和 infos，原样转给模型（比如结果被截断、查询用错了函数）。
	notes []string
	raw   []byte
}

// get 调一次 Prometheus 的 GET 接口。不重试：重试在调用链的别处，只有一层。
// 返回的错误是写给模型看的，不带内部地址。
func (c *client) get(ctx context.Context, path string, q url.Values) (response, error) {
	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return response{}, fmt.Errorf("build the Prometheus request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return response{}, transportError(ctx, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return response{}, transportError(ctx, err)
	}
	if len(raw) > maxBodyBytes {
		return response{}, fmt.Errorf("the Prometheus response is larger than %d MiB and was not read; narrow the query (fewer series, a shorter range or a larger step) or aggregate with sum by (...) / topk(...)", maxBodyBytes>>20)
	}

	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil || env.Status == "" {
		if resp.StatusCode/100 != 2 {
			return response{}, fmt.Errorf("prometheus answered HTTP %d (%s) without an error message", resp.StatusCode, http.StatusText(resp.StatusCode))
		}
		return response{}, errors.New("prometheus answered with a body that is not a Prometheus API response")
	}
	if env.Status != "success" {
		return response{}, apiError(env)
	}
	if resp.StatusCode/100 != 2 {
		return response{}, fmt.Errorf("prometheus answered HTTP %d (%s)", resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	notes := make([]string, 0, len(env.Warnings)+len(env.Infos))
	for _, w := range env.Warnings {
		notes = append(notes, "warning: "+w)
	}
	for _, i := range env.Infos {
		notes = append(notes, "info: "+i)
	}
	return response{data: env.Data, notes: notes, raw: raw}, nil
}

// apiError 把 Prometheus 的错误转成给模型看的错误，带上 Prometheus 的原文，它据此改查询。
func apiError(env envelope) error {
	msg := strings.TrimSpace(env.Error)
	if msg == "" {
		msg = "no error message"
	}
	switch env.ErrorType {
	case "bad_data":
		return fmt.Errorf("prometheus rejected the request (bad_data): %s. Fix the query or arguments and try again", msg)
	case "timeout", "canceled":
		return fmt.Errorf("prometheus could not finish the query (%s): %s. Narrow the query (fewer series, a shorter range or a larger step)", env.ErrorType, msg)
	default:
		return fmt.Errorf("prometheus returned an error (%s): %s", env.ErrorType, msg)
	}
}

// transportError 处理连不上、读到一半断开、超时。不把 url.Error 原样返回：它带着内部地址。
// 超时和取消包住 ctx.Err()，调用方可以用 errors.Is 判断。
func transportError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return fmt.Errorf("prometheus did not answer in time; narrow the query (fewer series, a shorter range or a larger step): %w", ctxErr)
		}
		return fmt.Errorf("the Prometheus request was canceled: %w", ctxErr)
	}
	if ue, ok := errors.AsType[*url.Error](err); ok {
		err = ue.Err
	}
	return fmt.Errorf("cannot reach Prometheus: %w", err)
}

// unixParam 把时间写成 Prometheus 接受的 Unix 秒（带小数）。
func unixParam(t time.Time) string {
	return strconv.FormatFloat(float64(t.UnixMilli())/1000, 'f', -1, 64)
}

// appendNotes 把 Prometheus 附带的 warnings 和 infos 接在结果后面。
func appendNotes(b *strings.Builder, notes []string) {
	if len(notes) == 0 {
		return
	}
	b.WriteString("\nPrometheus notes:\n")
	for _, n := range notes {
		b.WriteString("  " + n + "\n")
	}
}

// object 是工具参数的 Schema 外层。参数有可选的，所以不用 strict（strict 要求字段全部 required）。
func object(props map[string]any, required ...string) map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           props,
		"required":             required,
		"additionalProperties": false,
	}
}

func str(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}

const timeDesc = `RFC 3339 (2026-11-24T03:00:00Z) or relative to now (-30m, -2h, -90s, now).`

// fnDef 让四个工具的 Def 写得短一点。
func fnDef(name, desc string, schema map[string]any) llm.ToolDef {
	return llm.ToolDef{Name: name, Description: desc, Schema: schema}
}
