// Package runlog 把每次运行的全过程落盘，并读回来（列出、取文件、回放）。设计见 3d 的 3.7。
//
// 一次运行一个目录：
//
//	<运行号>/meta.json      开始时写：触发方式、问题原文、模型、上限、版本
//	<运行号>/events.jsonl   只追加，一行一个事件，写完一行就落盘
//	<运行号>/raw/<步骤>.json 工具调用的原始结果（pprof 的剖析是 <步骤>.pb.gz）
//	<运行号>/report.json、report.md
//	<运行号>/summary.json   结束时写；没有它说明这次运行没有正常结束
//
// 所有路径都经 os.Root 打开：运行号和文件名来自 HTTP 请求，Root 保证它们出不了这个目录。
package runlog

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
)

const (
	metaFile    = "meta.json"
	eventsFile  = "events.jsonl"
	summaryFile = "summary.json"
	rawDir      = "raw"
	idTimeFmt   = "20060102T150405Z"
)

// OutcomeIncomplete 是没有 summary.json 的运行在列表里显示的结束原因（进程中途死了）。
const OutcomeIncomplete = "incomplete"

// Meta 是一次运行开始时就知道的事。
type Meta struct {
	ID       string         `json:"id"`
	Trigger  string         `json:"trigger"`
	Question string         `json:"question"`
	Alert    jsontext.Value `json:"alert,omitzero"`
	// Provider 是模型的后端（anthropic、deepseek 等）。加这个字段之前的记录里没有它。
	Provider  string    `json:"provider,omitzero"`
	Model     string    `json:"model"`
	Effort    string    `json:"effort"`
	Limits    any       `json:"limits"`
	Version   string    `json:"version"`
	Prompt    string    `json:"prompt_sha256"`
	StartedAt time.Time `json:"started_at"`
}

// Summary 是一次运行结束时的统计。
type Summary struct {
	ID         string         `json:"id"`
	Trigger    string         `json:"trigger"`
	Outcome    string         `json:"outcome"`
	Unverified bool           `json:"unverified,omitzero"`
	LimitHit   string         `json:"limit_hit,omitzero"`
	Turns      int            `json:"turns"`
	ToolCalls  map[string]int `json:"tool_calls"`
	Usage      llm.Usage      `json:"usage"`
	CostUSD    float64        `json:"cost_usd"`
	StartedAt  time.Time      `json:"started_at"`
	DurationMS int64          `json:"duration_ms"`
	Error      string         `json:"error,omitzero"`
}

// Event 是 events.jsonl 的一行。
type Event struct {
	Seq  int            `json:"seq"`
	Time time.Time      `json:"time"`
	Kind string         `json:"kind"`
	Data jsontext.Value `json:"data"`
}

// NewID 返回运行号：UTC 时间加 4 位随机字符。按字典序排就是按时间排。
func NewID(now time.Time) string {
	return now.UTC().Format(idTimeFmt) + "-" + strings.ToLower(rand.Text()[:4])
}

// Store 是放运行记录的目录。
type Store struct {
	root   *os.Root
	logger *slog.Logger
}

// Open 打开（没有就建）放运行记录的目录。
func Open(dir string, logger *slog.Logger) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create runs dir: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open runs dir: %w", err)
	}
	return &Store{root: root, logger: logger}, nil
}

// Dir 返回目录的路径。
func (s *Store) Dir() string { return s.root.Name() }

// Close 关闭目录。
func (s *Store) Close() error { return s.root.Close() }

// Create 建一次运行的目录并写下 meta.json。
func (s *Store) Create(meta Meta) (*Run, error) {
	if err := s.root.MkdirAll(meta.ID+"/"+rawDir, 0o750); err != nil {
		return nil, fmt.Errorf("create run dir: %w", err)
	}
	root, err := s.root.OpenRoot(meta.ID)
	if err != nil {
		return nil, fmt.Errorf("open run dir: %w", err)
	}
	run := &Run{id: meta.ID, root: root, logger: s.logger.With("run", meta.ID)}
	if err := run.writeJSON(metaFile, meta); err != nil {
		_ = root.Close()
		return nil, err
	}
	run.events, err = root.OpenFile(eventsFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("open events: %w", err)
	}
	return run, nil
}

// List 返回全部运行，新的在前。没有正常结束的运行只有 meta 里的信息，Outcome 是 OutcomeIncomplete。
func (s *Store) List() ([]Summary, error) {
	entries, err := fs.ReadDir(s.root.FS(), ".")
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	out := make([]Summary, 0, len(entries))
	for _, e := range slices.Backward(entries) {
		if !e.IsDir() {
			continue
		}
		var sum Summary
		if b, err := s.root.ReadFile(e.Name() + "/" + summaryFile); err == nil && json.Unmarshal(b, &sum) == nil {
			out = append(out, sum)
			continue
		}
		var meta Meta
		b, err := s.root.ReadFile(e.Name() + "/" + metaFile)
		if err != nil || json.Unmarshal(b, &meta) != nil {
			continue // 不是运行目录，或者刚建到一半
		}
		out = append(out, Summary{ID: meta.ID, Trigger: meta.Trigger, Outcome: OutcomeIncomplete, StartedAt: meta.StartedAt})
	}
	return out, nil
}

// ReadFile 读一次运行里的一个文件。name 可以带子目录（raw/s1.json）。
func (s *Store) ReadFile(id, name string) ([]byte, error) {
	b, err := s.root.ReadFile(id + "/" + name)
	if err != nil {
		return nil, fmt.Errorf("read %s/%s: %w", id, name, err)
	}
	return b, nil
}

// Events 读回一次运行的全部事件。最后一行不完整（进程死在写的中途）时丢掉那一行。
func (s *Store) Events(id string) ([]Event, error) {
	b, err := s.ReadFile(id, eventsFile)
	if err != nil {
		return nil, err
	}
	var events []Event
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(nil, 16<<20)
	for sc.Scan() {
		var ev Event
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			break
		}
		events = append(events, ev)
	}
	if err := sc.Err(); err != nil {
		return events, fmt.Errorf("scan events of %s: %w", id, err)
	}
	return events, nil
}

// Prune 删掉开始时刻早于 now-maxAge 的运行；之后总量还超过 maxBytes 就从最老的删起。返回删掉的个数。
func (s *Store) Prune(now time.Time, maxAge time.Duration, maxBytes int64) (int, error) {
	entries, err := fs.ReadDir(s.root.FS(), ".")
	if err != nil {
		return 0, fmt.Errorf("list runs: %w", err)
	}
	type runDir struct {
		name string
		size int64
	}
	var dirs []runDir
	var total int64
	removed := 0
	for _, e := range entries { // ReadDir 按名字排，也就是按时间从老到新
		if !e.IsDir() {
			continue
		}
		stamp, _, _ := strings.Cut(e.Name(), "-")
		started, err := time.Parse(idTimeFmt, stamp)
		if err != nil {
			continue // 不是我们建的目录，不碰
		}
		if now.Sub(started) > maxAge {
			if err := s.root.RemoveAll(e.Name()); err != nil {
				return removed, fmt.Errorf("remove %s: %w", e.Name(), err)
			}
			removed++
			continue
		}
		size := dirSize(s.root.FS(), e.Name())
		dirs = append(dirs, runDir{e.Name(), size})
		total += size
	}
	for _, d := range dirs {
		if total <= maxBytes {
			break
		}
		if err := s.root.RemoveAll(d.name); err != nil {
			return removed, fmt.Errorf("remove %s: %w", d.name, err)
		}
		total -= d.size
		removed++
	}
	return removed, nil
}

func dirSize(fsys fs.FS, dir string) int64 {
	var size int64
	_ = fs.WalkDir(fsys, dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil //nolint:nilerr // 量大小时读不了的文件按 0 算
		}
		if info, err := d.Info(); err == nil {
			size += info.Size()
		}
		return nil
	})
	return size
}

// Run 是一次正在进行的运行的记录。方法可以并发调用（工具是并发执行的）。
//
// 写记录失败只打日志、不让诊断失败：盘满的时候，一份没有留痕的报告仍然比没有报告好。
type Run struct {
	id     string
	root   *os.Root
	logger *slog.Logger

	mu     sync.Mutex
	events *os.File
	seq    int
}

// ID 返回运行号。
func (r *Run) ID() string { return r.id }

// Event 追加一个事件。data 是任何能编码成 JSON 的值。
func (r *Run) Event(kind string, data any) {
	payload, err := json.Marshal(data)
	if err != nil {
		r.logger.Error("encode run event failed", "kind", kind, "error", err)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.events == nil {
		return
	}
	r.seq++
	line, err := json.Marshal(Event{Seq: r.seq, Time: time.Now().UTC(), Kind: kind, Data: payload})
	if err != nil {
		r.logger.Error("encode run event failed", "kind", kind, "error", err)
		return
	}
	// 一次 Write 写一整行：O_APPEND 下不会和别的写交错，进程死了最多丢最后半行。
	if _, err := r.events.Write(append(line, '\n')); err != nil {
		r.logger.Error("write run event failed", "kind", kind, "error", err)
	}
}

// Raw 保存一步工具调用的原始结果。file 是 raw/ 下的文件名（s1.json、s3.pb.gz）。
func (r *Run) Raw(file string, raw []byte) {
	if len(raw) == 0 {
		return
	}
	if err := r.root.WriteFile(rawDir+"/"+file, raw, 0o640); err != nil {
		r.logger.Error("write raw tool result failed", "file", file, "error", err)
	}
}

// WriteFile 在运行目录里写一个文件（报告）。
func (r *Run) WriteFile(name string, data []byte) {
	if err := r.root.WriteFile(name, data, 0o640); err != nil {
		r.logger.Error("write run file failed", "file", name, "error", err)
	}
}

// Finish 写下 summary.json 并关闭。之后的 Event 调用被忽略。
func (r *Run) Finish(sum Summary) {
	if err := r.writeJSON(summaryFile, sum); err != nil {
		r.logger.Error("write run summary failed", "error", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.events != nil {
		if err := errors.Join(r.events.Close(), r.root.Close()); err != nil {
			r.logger.Error("close run failed", "error", err)
		}
		r.events = nil
	}
}

func (r *Run) writeJSON(name string, v any) error {
	b, err := json.Marshal(v, jsontext.WithIndent("  "))
	if err != nil {
		return fmt.Errorf("encode %s: %w", name, err)
	}
	if err := r.root.WriteFile(name, append(b, '\n'), 0o640); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	return nil
}
