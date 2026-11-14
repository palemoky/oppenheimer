// Package degrade 管理一个服务里“可以降级的功能”：每个功能一个有名字的开关，
// 可以由人强制关掉，也能看到它现在是不是被自动降级了（设计文档 1d 的 6.9）。
//
// 开关和自动降级走同一条代码路径：调用点先问开关，关着就走降级的那条路，和熔断器打开时走的是同一条。
// 这样降级路径平时也能被人工触发来验证，不会到真出故障时才第一次执行。
//
// 开关只在本进程里生效，不持久化：初始值来自环境变量，运行中通过服务内部的 HTTP 端口修改，
// 重启后回到环境变量的值。它要在依赖坏掉的时候用，所以不能自己依赖 Redis 或数据库。
package degrade

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Env 是存放开关初始值的环境变量名。格式见 Set.Apply。
const Env = "DEGRADE"

// 开关的两个取值。
const (
	// ModeAuto：功能开着，是否降级由熔断器或探测自动决定。默认值。
	ModeAuto = "auto"
	// ModeOff：强制降级，不管依赖是否正常。
	ModeOff = "off"
)

// Switch 是一个功能的开关。并发安全。
type Switch struct {
	name, desc string
	off        atomic.Bool
	// status 报告自动降级的状态，可以为 nil（这个功能只有人工开关，没有自动降级）。
	status atomic.Pointer[func() Status]
}

// Status 是一个功能自动降级的状态。
type Status struct {
	// Degraded 表示它现在被自动降级了（熔断器不在关闭状态、探测不通过）。
	Degraded bool
	// Detail 是给人看的说明，比如各个熔断器的状态。
	Detail string
}

// Name 返回开关的名字。
func (s *Switch) Name() string { return s.name }

// Off 报告这个功能是否被人强制关掉了。调用点在访问依赖之前先问它。
//
// 在 nil 上调用返回 false：没有接开关的地方（比如测试里直接构造的对象）按“没有人工降级”处理。
func (s *Switch) Off() bool { return s != nil && s.off.Load() }

// Watch 登记一个报告自动降级状态的函数，展示时调用。它会被并发调用，不能阻塞。
func (s *Switch) Watch(fn func() Status) { s.status.Store(&fn) }

// Set 是一个服务的全部开关。并发安全。
type Set struct {
	service string
	logger  *slog.Logger

	mu       sync.Mutex
	switches map[string]*Switch
	order    []string
}

// New 返回一个空的 Set。logger 为 nil 时用 slog.Default()。
func New(service string, logger *slog.Logger) *Set {
	if logger == nil {
		logger = slog.Default()
	}
	return &Set{service: service, logger: logger, switches: map[string]*Switch{}}
}

// Switch 登记并返回名为 name 的开关，desc 说明关掉之后的表现。同一个名字只能登记一次。
func (s *Set) Switch(name, desc string) *Switch {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.switches[name]; dup {
		// 重复登记是写代码时的错误，不是运行时会出现的情况。
		panic("degrade: switch " + name + " registered twice")
	}
	sw := &Switch{name: name, desc: desc}
	s.switches[name] = sw
	s.order = append(s.order, name)
	s.observe(name, false)
	return sw
}

// 指标（设计文档 1d 的 8.8）：每个开关现在是不是被人关掉了。登记时报一次，之后每次改动报一次。
// 自动降级（熔断器打开）不在这里，看 breaker.state。
var switchOff, _ = otel.Meter("github.com/palemoky/oppenheimer/pkg/degrade").Int64Gauge("degrade.switch.off",
	metric.WithDescription("降级开关的状态：1 表示被人工关掉，0 表示自动"))

// metricCtx 是记指标时用的 ctx：开关的登记和改动都不带 ctx，指标也用不到它。
var metricCtx = context.Background()

//nolint:contextcheck // 见 metricCtx：这里不需要调用方的 ctx
func (s *Set) observe(name string, off bool) {
	var v int64
	if off {
		v = 1
	}
	switchOff.Record(metricCtx, v, metric.WithAttributes(attribute.String("switch", name)))
}

// ErrUnknownSwitch 表示没有这个名字的开关。
var ErrUnknownSwitch = errors.New("degrade: unknown switch")

// SetMode 把名为 name 的开关设为 mode（ModeAuto 或 ModeOff）。
func (s *Set) SetMode(name, mode string) error {
	var off bool
	switch mode {
	case ModeAuto:
	case ModeOff:
		off = true
	default:
		return fmt.Errorf("degrade: mode must be %s or %s, got %q", ModeAuto, ModeOff, mode)
	}
	s.mu.Lock()
	sw, ok := s.switches[name]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrUnknownSwitch, name)
	}
	// 改值和报指标在同一把锁里：两个并发的 SetMode 各自“先改后报”时，后报的那个可能是先改的，
	// 指标就和实际的值相反，而且一直错到下一次改动。
	changed := sw.off.Swap(off) != off
	s.observe(name, off)
	s.mu.Unlock()
	if changed {
		// 人工降级是要留痕的操作：谁在什么时候把什么关了，事后要查得到。
		s.logger.Warn("degrade switch changed", "switch", name, "mode", mode)
	}
	return nil
}

// Apply 按 spec 设置开关的初始值，在所有开关登记之后调用。
//
// spec 是逗号分隔的 name=mode，例如 "archive_read=off,cache=off"；空串什么也不做。
// 名字不认识、取值不对都返回错误：写错的开关名被悄悄忽略的话，人以为关了其实没关。
func (s *Set) Apply(spec string) error {
	for item := range strings.SplitSeq(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, mode, ok := strings.Cut(item, "=")
		if !ok {
			return fmt.Errorf("degrade: bad entry %q in %s, want name=%s or name=%s", item, Env, ModeOff, ModeAuto)
		}
		if err := s.SetMode(strings.TrimSpace(name), strings.TrimSpace(mode)); err != nil {
			return err
		}
	}
	return nil
}

// View 是一个开关对外展示的样子。
type View struct {
	Name string `json:"name"`
	// Mode 是人设的取值：auto 或 off。
	Mode string `json:"mode"`
	// Degraded 是它现在实际上有没有降级：被强制关掉，或者被自动降级。
	Degraded bool   `json:"degraded"`
	Detail   string `json:"detail,omitempty"`
	Desc     string `json:"description"`
}

// Snapshot 返回全部开关当前的样子，按登记的顺序。
func (s *Set) Snapshot() []View {
	s.mu.Lock()
	names := slices.Clone(s.order)
	sws := make([]*Switch, 0, len(names))
	for _, n := range names {
		sws = append(sws, s.switches[n])
	}
	s.mu.Unlock()

	out := make([]View, 0, len(sws))
	for _, sw := range sws {
		v := View{Name: sw.name, Mode: ModeAuto, Desc: sw.desc}
		if fn := sw.status.Load(); fn != nil {
			st := (*fn)()
			v.Degraded, v.Detail = st.Degraded, st.Detail
		}
		if sw.off.Load() {
			v.Mode, v.Degraded = ModeOff, true
		}
		out = append(out, v)
	}
	return out
}

type snapshotResponse struct {
	Service  string `json:"service"`
	Switches []View `json:"switches"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// Register 把查看和修改开关的接口挂到 mux 上：
//
//	GET /degrade                    列出全部开关
//	PUT /degrade/{name}?mode=off    强制降级
//	PUT /degrade/{name}?mode=auto   恢复自动
//
// mux 应当是服务内部的那个端口（和 /healthz 同一个），不经过 gateway。接口没有鉴权。
func (s *Set) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /degrade", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, snapshotResponse{Service: s.service, Switches: s.Snapshot()})
	})
	mux.HandleFunc("PUT /degrade/{name}", func(w http.ResponseWriter, r *http.Request) {
		err := s.SetMode(r.PathValue("name"), r.URL.Query().Get("mode"))
		switch {
		case errors.Is(err, ErrUnknownSwitch):
			writeJSON(w, http.StatusNotFound, errorResponse{Error: err.Error()})
		case err != nil:
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		default:
			writeJSON(w, http.StatusOK, snapshotResponse{Service: s.service, Switches: s.Snapshot()})
		}
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// 写响应失败说明客户端已断开，没有可做的补救。
	_ = json.MarshalWrite(w, v)
}
