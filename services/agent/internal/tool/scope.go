package tool

import (
	"context"
	"sync"
	"time"
)

// 这个文件是给“有运行内状态”的工具用的两件东西：一次运行的共享状态（Scope），和工具自己声明的时限（Timeouter）。
// 大多数工具是无状态的，用不着它们。

// Scope 是一次运行里工具之间共享的状态，循环每次运行建一个，经 ctx 交给每次调用（WithCall）。
// 用来做“同一次运行里最多几次”“引用这次运行里前面某一步的结果”这类事。方法可以并发调用。
type Scope struct {
	// RunID 是运行号，写日志用；手工调工具（agent tool）时为空。
	RunID string

	mu   sync.Mutex
	vals map[any]any
}

// NewScope 建一次运行的共享状态。
func NewScope(runID string) *Scope {
	return &Scope{RunID: runID, vals: make(map[any]any)}
}

// Value 取 key 对应的值，没有时用 init 建一个存下来。key 用工具包里未导出的类型，免得不同的工具撞上。
// 返回的值由工具自己负责并发安全。
func (s *Scope) Value(key any, init func() any) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.vals[key]
	if !ok {
		v = init()
		s.vals[key] = v
	}
	return v
}

type callKey struct{}

type callInfo struct {
	scope *Scope
	step  string
}

// WithCall 把这次调用所在的运行和步骤号放进 ctx。
func WithCall(ctx context.Context, scope *Scope, step string) context.Context {
	return context.WithValue(ctx, callKey{}, callInfo{scope: scope, step: step})
}

// CallFrom 取 WithCall 放进去的运行和步骤号；不在一次运行里（手工调用、测试）时返回 nil 和空串。
func CallFrom(ctx context.Context) (scope *Scope, step string) {
	ci, _ := ctx.Value(callKey{}).(callInfo)
	return ci.scope, ci.step
}

// Timeouter 是需要比循环的默认时限更长的工具（例如要等 30 秒的 CPU 剖析）。
type Timeouter interface {
	// Timeout 返回这个工具一次调用的时限上限。工具自己仍按参数设更紧的时限。
	Timeout() time.Duration
}

// TimeoutOf 返回 t 一次调用的时限：它自己声明的和 def 里大的那个。
func TimeoutOf(t Tool, def time.Duration) time.Duration {
	if to, ok := t.(Timeouter); ok {
		return max(def, to.Timeout())
	}
	return def
}
