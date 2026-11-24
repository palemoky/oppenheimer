// Package loki 是查 Loki 的两个只读工具：loki_query（日志和日志上的指标查询）、loki_labels（标签名和标签值）。
//
// 直接用 net/http 调 Loki 的 HTTP API，不引入客户端库（设计 3.3）。裁剪规则见设计 3.4：
// 给模型的 Text 是裁过的纯文本，Raw 是 Loki 返回的原始 JSON。
package loki

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

const (
	// maxBody 是读 Loki 响应体的上限。300 行日志远到不了这个数，超了多半是 matrix 的序列太多。
	maxBody = 8 << 20
	// maxErrorBody 是读出错响应体的上限，只用来把 Loki 的错误信息转给模型。
	maxErrorBody = 4 << 10
	// maxErrorText 是错误信息里引用 Loki 原话的上限。
	maxErrorText = 500
)

// Config 是两个工具共用的配置。
type Config struct {
	// BaseURL 是 Loki 的地址，例如 http://loki.monitoring:3100。
	BaseURL string
	// HTTP 为 nil 时用 http.DefaultClient。
	HTTP *http.Client
	// Now 为 nil 时用 time.Now。相对时间（-15m）按它算。
	Now func() time.Time
}

// New 返回 loki_query 和 loki_labels。
func New(cfg Config) []tool.Tool {
	c := &client{base: strings.TrimRight(cfg.BaseURL, "/"), http: cfg.HTTP, now: cfg.Now}
	if c.http == nil {
		c.http = http.DefaultClient
	}
	if c.now == nil {
		c.now = time.Now
	}
	return []tool.Tool{&queryTool{c: c}, &labelsTool{c: c}}
}

// client 建好之后只读，可以并发使用。
type client struct {
	base string
	http *http.Client
	now  func() time.Time
}

// get 发一次 GET，返回 2xx 的响应体。错误信息是写给模型看的：不带内部地址，带 Loki 的原话。
func (c *client) get(ctx context.Context, path string, params url.Values) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path+"?"+params.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("build the request to Loki: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, transportError(ctx, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		text := oneLine(string(msg), maxErrorText)
		if text == "" {
			text = http.StatusText(resp.StatusCode)
		}
		if resp.StatusCode == http.StatusBadRequest {
			return nil, fmt.Errorf("the Loki server rejected the request (HTTP 400): %s; fix the arguments or the LogQL and try again", text)
		}
		return nil, fmt.Errorf("the Loki server returned HTTP %d: %s", resp.StatusCode, text)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, transportError(ctx, err)
	}
	if len(body) > maxBody {
		return nil, fmt.Errorf("the Loki response is larger than %d MiB and was not read; narrow the query (a more selective stream selector, a line filter, a shorter range, a smaller limit, or sum by (...) for metric queries)", maxBody>>20)
	}
	return body, nil
}

// transportError 把传输层的错误改写成给模型看的话。超时的情况保留 ctx 的错误，调用方能用 errors.Is 认出来；
// 其余的去掉带着内部地址的外层（*url.Error、*net.OpError）。
func transportError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("the Loki server did not answer in time; use a shorter range or a more selective query: %w", ctxErr)
	}
	if ue, ok := errors.AsType[*url.Error](err); ok {
		err = ue.Err
	}
	// *net.OpError 的文本里有对端的 IP 和端口，只留里面的原因（connection refused 之类）。
	if oe, ok := errors.AsType[*net.OpError](err); ok && oe.Err != nil {
		err = oe.Err
	}
	return fmt.Errorf("cannot reach the Loki server: %w", err)
}

// timeRange 解析 start、end 两个参数，返回 Loki 要的纳秒时间戳参数。
func timeRange(startArg, endArg, defStart string, now time.Time) (start, end time.Time, err error) {
	if startArg == "" {
		startArg = defStart
	}
	if start, err = tool.ParseTime(startArg, now); err != nil {
		return start, end, fmt.Errorf("start: %w", err)
	}
	if end, err = tool.ParseTime(endArg, now); err != nil {
		return start, end, fmt.Errorf("end: %w", err)
	}
	if !start.Before(end) {
		return start, end, fmt.Errorf("start (%s) must be before end (%s)", start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339))
	}
	return start, end, nil
}

func setRange(params url.Values, start, end time.Time) {
	params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
}

func fmtRange(start, end time.Time) string {
	return start.UTC().Format(time.RFC3339) + " and " + end.UTC().Format(time.RFC3339)
}

// oneLine 把多行文本压成一行并截到 maxBytes。
func oneLine(s string, maxBytes int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > maxBytes {
		lines, _ := tool.TrimLines([]string{s}, 1, maxBytes, false)
		s = lines[0]
	}
	return s
}
