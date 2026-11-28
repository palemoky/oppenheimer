//go:build faults

package fault

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Compiled 报告这个二进制里有没有故障代码。
const Compiled = true

const (
	// DefaultTTL 是打开时没有给 ttl 用的值。
	DefaultTTL = 30 * time.Minute
	// MaxTTL 是 ttl 的上限。
	MaxTTL = 6 * time.Hour
)

// ErrUnknown 表示没有这个名字的开关。
var ErrUnknown = errors.New("unknown switch")

var nameRE = regexp.MustCompile(`^f[0-9]{2}$`)

// Toggle 是一个开关。并发安全。
type Toggle struct {
	name string
	on   atomic.Bool

	mu sync.Mutex
	// until 是自动关的时刻，零值表示没有（关着，或者是从文件打开的）。
	until time.Time
	timer *time.Timer
}

// On 报告开关是不是开着。它在热路径上被调用，只读一个原子变量。
func (t *Toggle) On() bool { return t.on.Load() }

var (
	regMu   sync.Mutex
	toggles = map[string]*Toggle{}
	// fromFile 是 FileEnv 指的文件里列出的开关。业务包的包级变量在本包的 init 之后才初始化，
	// 所以先记下来，登记的时候再打开。
	fromFile = map[string]bool{}
)

func init() {
	path := os.Getenv(FileEnv)
	if path == "" {
		return
	}
	raw, err := os.ReadFile(path) //nolint:gosec // 路径来自部署清单里的环境变量，不是外部输入
	if err != nil {
		// 文件不存在是常态（那个 Secret 平时是空的）；别的错误也不挡启动，故障开关不能反过来弄坏服务。
		return
	}
	for _, name := range strings.FieldsFunc(string(raw), func(r rune) bool { return r == ',' || r == '\n' || r == ' ' || r == '\t' || r == '\r' }) {
		fromFile[name] = true
	}
}

// Switch 登记并返回名为 name 的开关。在业务包的包级变量里调用；名字不合规或者重复时 panic（启动时就暴露）。
func Switch(name string) *Toggle {
	if !nameRE.MatchString(name) {
		panic(fmt.Sprintf("fault: switch name %q must look like f01", name))
	}
	regMu.Lock()
	defer regMu.Unlock()
	if _, dup := toggles[name]; dup {
		panic(fmt.Sprintf("fault: switch %q registered twice", name))
	}
	t := &Toggle{name: name}
	t.on.Store(fromFile[name])
	toggles[name] = t
	return t
}

// Set 打开或关掉名为 name 的开关。打开时 ttl 到了自己关；ttl 为 0 用 DefaultTTL，超过 MaxTTL 报错。
func Set(name string, on bool, ttl time.Duration) error {
	regMu.Lock()
	t, ok := toggles[name]
	regMu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknown, name)
	}
	if on {
		switch {
		case ttl == 0:
			ttl = DefaultTTL
		case ttl < 0 || ttl > MaxTTL:
			return fmt.Errorf("ttl %s is out of range (0, %s]", ttl, MaxTTL)
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.timer != nil {
		// Stop 返回 false 时旧的回调可能已经在等这把锁：它醒来后看到 until 变了，什么都不做。
		t.timer.Stop()
		t.timer = nil
	}
	t.on.Store(on)
	t.until = time.Time{}
	if !on {
		return nil
	}
	until := time.Now().Add(ttl)
	t.until = until
	t.timer = time.AfterFunc(ttl, func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		if t.until.Equal(until) {
			t.on.Store(false)
			t.until = time.Time{}
			t.timer = nil
		}
	})
	return nil
}

// View 是一个开关给人看的样子。
type View struct {
	Name string `json:"name"`
	On   bool   `json:"on"`
	// Until 是自动关的时刻；关着或者是从文件打开的时候没有。
	Until time.Time `json:"until,omitzero"`
}

// Snapshot 返回全部开关现在的样子，按名字排。
func Snapshot() []View {
	regMu.Lock()
	defer regMu.Unlock()
	views := make([]View, 0, len(toggles))
	for _, t := range toggles {
		t.mu.Lock()
		views = append(views, View{Name: t.name, On: t.on.Load(), Until: t.until})
		t.mu.Unlock()
	}
	slices.SortFunc(views, func(a, b View) int { return strings.Compare(a.Name, b.Name) })
	return views
}

// Register 把查看和修改开关的接口挂到 mux 上，见包注释。mux 必须是服务内部的那个端口。
func Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /fault", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, Snapshot())
	})
	mux.HandleFunc("PUT /fault/{name}", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		var on bool
		switch q.Get("mode") {
		case "on":
			on = true
		case "off":
		default:
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "mode must be on or off"})
			return
		}
		var ttl time.Duration
		if s := q.Get("ttl"); s != "" {
			d, err := time.ParseDuration(s)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ttl: " + err.Error()})
				return
			}
			ttl = d
		}
		err := Set(r.PathValue("name"), on, ttl)
		switch {
		case errors.Is(err, ErrUnknown):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		case err != nil:
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		default:
			writeJSON(w, http.StatusOK, Snapshot())
		}
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// 写响应失败说明客户端已断开，没有可做的补救。
	_ = json.MarshalWrite(w, v)
}
