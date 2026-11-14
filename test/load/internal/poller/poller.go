// Package poller 实现压测的 poll 子命令：只发 GET /api/v1/orders/{id}，对最近受理的订单做稳定速率的查询，
// 用来叠加在下单压力上，看查询压力对落库吞吐的影响。
//
// 订单来自 run 写的账本（JSON Lines）：poller 边读边追（tail），只保留最近受理的 Recent 个，
// 所以可以和一个正在运行的 load run 同时跑。
package poller

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/palemoky/oppenheimer/test/load/internal/ledger"
	"github.com/palemoky/oppenheimer/test/load/internal/stats"
)

const (
	pathOrders   = "/api/v1/orders"
	headerUserID = "X-User-Id"
	maxBody      = 1 << 20
)

// Config 是一次查询压力的参数。
type Config struct {
	BaseURL string
	// OrdersFrom 是订单来源：run 写的账本（每行一个 JSON），或每行 "order_id user_id" 的文本。文件可以还不存在。
	OrdersFrom string
	// Concurrency 个 worker，每个查询完等 Interval 再查下一个，所以目标速率约为 Concurrency/Interval。
	Concurrency    int
	Interval       time.Duration
	Duration       time.Duration
	RequestTimeout time.Duration
	// Recent 是保留的最近受理订单数，查询从中随机选。
	Recent int
	// Refresh 是重新读订单来源文件的间隔。
	Refresh time.Duration
	Seed    uint64

	Logger     *slog.Logger
	HTTPClient *http.Client
}

// Validate 检查参数。
func (c *Config) Validate() error {
	var errs []error
	u, err := url.Parse(c.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, fmt.Errorf("invalid gateway address %q: want http(s)://host:port", c.BaseURL))
	}
	if c.OrdersFrom == "" {
		errs = append(errs, errors.New("orders-from is required"))
	}
	if c.Concurrency < 1 {
		errs = append(errs, errors.New("concurrency must be >= 1"))
	}
	if c.Interval < 0 {
		errs = append(errs, errors.New("interval must not be negative"))
	}
	if c.Duration <= 0 || c.RequestTimeout <= 0 || c.Refresh <= 0 {
		errs = append(errs, errors.New("duration, request timeout and refresh must be > 0"))
	}
	if c.Recent < 1 {
		errs = append(errs, errors.New("recent must be >= 1"))
	}
	return errors.Join(errs...)
}

// Latency 是延迟分位数，单位毫秒。
type Latency struct {
	P50Ms float64 `json:"p50_ms"`
	P90Ms float64 `json:"p90_ms"`
	P99Ms float64 `json:"p99_ms"`
	MaxMs float64 `json:"max_ms"`
}

// Report 是一次查询压力的汇总。
type Report struct {
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	Requests       int64   `json:"requests"`
	PerSecond      float64 `json:"per_second"`
	// Statuses 是响应状态码的分布；没有拿到响应的记在 error 下。
	Statuses map[string]int64 `json:"statuses"`
	Latency  Latency          `json:"latency"`
	// OrdersSeen 是从来源读到的订单总数；Starved 是 worker 因为没有订单可查而等待的次数。
	OrdersSeen int64 `json:"orders_seen"`
	Starved    int64 `json:"starved"`
}

// WriteText 把报告以人读的格式写出。
func (r *Report) WriteText(w io.Writer) error {
	_, err := fmt.Fprintf(w, "poll  %d requests in %.1fs  %.2f/s  p50 %.2fms  p90 %.2fms  p99 %.2fms  max %.2fms\n"+
		"      orders seen %d, waits for orders %d, statuses %v\n",
		r.Requests, r.ElapsedSeconds, r.PerSecond, r.Latency.P50Ms, r.Latency.P90Ms, r.Latency.P99Ms, r.Latency.MaxMs,
		r.OrdersSeen, r.Starved, r.Statuses)
	if err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

type target struct {
	orderID string
	userID  string
}

// source 边追文件边保留最近的 N 个订单。
type source struct {
	path string
	max  int

	off  int64
	rest []byte // 上次读到的不完整的最后一行

	mu   sync.RWMutex
	ring []target
	head int // ring 满之后下一个被覆盖的位置
	seen atomic.Int64
}

func (s *source) add(t target) {
	s.seen.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ring) < s.max {
		s.ring = append(s.ring, t)
		return
	}
	s.ring[s.head] = t
	s.head = (s.head + 1) % s.max
}

func (s *source) pick(rng *rand.Rand) (target, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.ring) == 0 {
		return target{}, false
	}
	return s.ring[rng.IntN(len(s.ring))], true
}

// refresh 读文件里新追加的完整行。文件还不存在不算错误。
func (s *source) refresh() error {
	f, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open orders source: %w", err)
	}
	defer f.Close()
	if _, err := f.Seek(s.off, io.SeekStart); err != nil {
		return fmt.Errorf("seek orders source: %w", err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("read orders source: %w", err)
	}
	s.off += int64(len(data))
	data = append(s.rest, data...)
	// 只处理到最后一个换行；后面不完整的部分留到下次。
	cut := bytes.LastIndexByte(data, '\n') + 1
	s.rest = bytes.Clone(data[cut:])
	sc := bufio.NewScanner(bytes.NewReader(data[:cut]))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		if t, ok := parseLine(bytes.TrimSpace(sc.Bytes())); ok {
			s.add(t)
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan orders source: %w", err)
	}
	return nil
}

// parseLine 认两种行：账本里提交成功的一行，或 "order_id user_id"。其他行忽略。
func parseLine(line []byte) (target, bool) {
	if len(line) == 0 {
		return target{}, false
	}
	if line[0] == '{' {
		var e ledger.Entry
		if json.Unmarshal(line, &e) != nil {
			return target{}, false
		}
		if e.Op != ledger.OpSubmit || e.Outcome != ledger.OutcomeOK || e.OrderID == "" {
			return target{}, false
		}
		return target{orderID: e.OrderID, userID: strconv.FormatInt(e.UserID, 10)}, true
	}
	fields := strings.Fields(string(line))
	if len(fields) != 2 {
		return target{}, false
	}
	return target{orderID: fields[0], userID: fields[1]}, true
}

// Run 运行到 Duration 或 ctx 被取消，返回汇总。
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.HTTPClient == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns, t.MaxIdleConnsPerHost = cfg.Concurrency*2, cfg.Concurrency*2
		cfg.HTTPClient = &http.Client{Transport: t}
	}
	base := strings.TrimRight(cfg.BaseURL, "/")
	src := &source{path: cfg.OrdersFrom, max: cfg.Recent}
	if err := src.refresh(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()
	var (
		wg       sync.WaitGroup
		lat      stats.Recorder
		mu       sync.Mutex
		statuses = map[string]int64{}
		requests atomic.Int64
		starved  atomic.Int64
	)
	wg.Go(func() {
		t := time.NewTicker(cfg.Refresh)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := src.refresh(); err != nil {
					cfg.Logger.WarnContext(ctx, "refresh orders source failed", "error", err)
				}
			}
		}
	})
	start := time.Now()
	seed := cfg.Seed
	if seed == 0 {
		seed = rand.Uint64() //nolint:gosec // 压测用的伪随机数，不涉及安全
	}
	for w := range cfg.Concurrency {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(seed, uint64(w))) //nolint:gosec // 压测用的伪随机数；w 非负
			for ctx.Err() == nil {
				t, ok := src.pick(rng)
				if !ok {
					starved.Add(1)
					sleep(ctx, 50*time.Millisecond)
					continue
				}
				status, d := get(ctx, cfg.HTTPClient, base, t, cfg.RequestTimeout)
				if ctx.Err() != nil {
					return // 被中断或到时的那个请求不算
				}
				requests.Add(1)
				lat.Add(d)
				mu.Lock()
				statuses[status]++
				mu.Unlock()
				sleep(ctx, cfg.Interval)
			}
		})
	}
	wg.Wait()

	elapsed := time.Since(start)
	s := lat.Summary()
	rep := &Report{
		ElapsedSeconds: math.Round(elapsed.Seconds()*1000) / 1000,
		Requests:       requests.Load(),
		Statuses:       statuses,
		Latency:        Latency{P50Ms: ms(s.P50), P90Ms: ms(s.P90), P99Ms: ms(s.P99), MaxMs: ms(s.Max)},
		OrdersSeen:     src.seen.Load(),
		Starved:        starved.Load(),
	}
	if elapsed > 0 {
		rep.PerSecond = math.Round(float64(rep.Requests)/elapsed.Seconds()*100) / 100
	}
	return rep, nil
}

func ms(d time.Duration) float64 { return math.Round(float64(d)/float64(time.Microsecond)) / 1000 }

// get 查询一个订单，返回状态码（字符串）或 "error"，以及耗时。
func get(ctx context.Context, c *http.Client, base string, t target, timeout time.Duration) (string, time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+pathOrders+"/"+t.orderID, nil)
	if err != nil {
		return "error", time.Since(start)
	}
	req.Header.Set(headerUserID, t.userID)
	resp, err := c.Do(req)
	if err != nil {
		return "error", time.Since(start)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, maxBody)); err != nil {
		return "error", time.Since(start)
	}
	return strconv.Itoa(resp.StatusCode), time.Since(start)
}

func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
