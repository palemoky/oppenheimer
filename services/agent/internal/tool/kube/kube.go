// Package kube 是诊断 Agent 查看集群的只读工具：kube_get、kube_describe、kube_events、kube_logs。
//
// 边界（设计 3.3、3.4）：
//   - kind 是白名单（kinds.go），名单外的一律拒绝，特别是 Secret 和 ConfigMap。没有通用的、按名字反射的读取路径，
//     加一种资源要在白名单里加一项、写它的投影。ServiceAccount 的 RBAC 也只给这些资源的 get、list、watch 和 pods/log。
//   - 只读：没有 exec、没有写、没有 watch。
//   - 给模型的是手工挑过字段的摘要，不是整个对象；环境变量只给名字，值在 Text 和 Raw 里都不出现。
package kube

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

// 裁剪的上限。
const (
	// maxItems 是列表和事件（合并之后）最多给模型的条数。
	maxItems = 50
	// describeEvents 是 kube_describe 末尾附带的事件条数，更多的让模型去 kube_events。
	describeEvents = 20
	// listPageLimit 是一次列表最多从 apiserver 取的条数。超过时只看这一页，并在结果里写明。
	listPageLimit = 1000
	// maxAnnotationValue 是注解的值的上限，达到它的只给键。
	maxAnnotationValue = 80
	// maxMessageBytes 是事件消息、容器状态消息的截断长度。
	maxMessageBytes = 300
)

// Config 是这组工具的依赖。
type Config struct {
	Client kubernetes.Interface
	// Now 返回现在的时间，nil 时用 time.Now。相对时间参数和“多久以前”都按它算。
	Now func() time.Time
}

// New 返回四个只读工具。cfg.Client 在调用期间只读，工具可以并发调用。
func New(cfg Config) []tool.Tool {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	k := &kube{client: cfg.Client, now: cfg.Now}
	return []tool.Tool{
		&fn{def: getDef(), call: k.get},
		&fn{def: describeDef(), call: k.describe},
		&fn{def: eventsDef(), call: k.events},
		&fn{def: logsDef(), call: k.logs},
	}
}

type kube struct {
	client kubernetes.Interface
	now    func() time.Time
}

// fn 把一个函数包成 tool.Tool。
type fn struct {
	def  llm.ToolDef
	call func(ctx context.Context, input jsontext.Value) (tool.Result, error)
}

func (f *fn) Def() llm.ToolDef { return f.def }

func (f *fn) Call(ctx context.Context, input jsontext.Value) (tool.Result, error) {
	return f.call(ctx, input)
}

// schema 拼参数的 JSON Schema。不用 strict：strict 要求字段全部 required，这里大多是可选的。
func schema(props map[string]any, required ...string) map[string]any {
	s := map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func str(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}

// apiError 把 apiserver 的错误翻译成写给模型看的话。what 是“pod x in namespace y”这样的描述。
// 找不到和没权限不带原始错误（里面有 ServiceAccount 名字这类内部细节）；其余的包起来，
// 让调用方能用 errors.Is 认出 context.DeadlineExceeded。
func apiError(err error, what string) error {
	switch {
	case apierrors.IsNotFound(err):
		return fmt.Errorf("%s not found; use kube_get to list what exists", what)
	case apierrors.IsForbidden(err):
		return fmt.Errorf("the agent is not permitted to read %s", what)
	}
	return fmt.Errorf("reading %s: %w", what, err)
}

// where 描述对象的位置，用在错误信息里。
func where(kind, ns, name string) string {
	var b strings.Builder
	b.WriteString(strings.ToLower(kind))
	if name != "" {
		fmt.Fprintf(&b, " %q", name)
	} else {
		b.WriteString("s")
	}
	if ns != "" {
		fmt.Fprintf(&b, " in namespace %q", ns)
	}
	return b.String()
}

// marshalRaw 把（已经脱敏的）对象编码成落盘用的 JSON。编码失败不影响给模型的结果，只是不落 Raw。
func marshalRaw(v any) []byte {
	raw, err := json.Marshal(v, jsontext.AllowInvalidUTF8(true))
	if err != nil {
		return nil
	}
	return raw
}

// age 是 kubectl 风格的“多久以前”：90s、5m、3h、2d。
func age(t, now time.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}
	d := now.Sub(t)
	switch {
	case d < 0:
		return "0s"
	case d < 2*time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < 2*time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

// stamp 是绝对时间加“多久以前”，模型拿绝对时间去对 Prometheus 和 Loki。
func stamp(t, now time.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}
	return fmt.Sprintf("%s (%s ago)", t.UTC().Format(time.RFC3339), age(t, now))
}

// cut 把一段文字压成一行并截到 n 字节以内（不切断 UTF-8 字符）。
func cut(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	out, _ := tool.TrimLines([]string{s}, 1, n, false)
	if len(out) == 0 {
		return ""
	}
	return out[0]
}
