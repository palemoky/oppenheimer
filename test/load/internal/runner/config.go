// Package runner 实现压测的 run 子命令：一组虚拟用户循环“提交订单 → 轮询到落库 → 按比例支付、
// 取消或放着不管”，把每个请求写进客户端账本，结束时汇总指标。
//
// 压测期间服务可能被杀掉重启。连接被拒绝、超时、5xx 都只记账，不让工具退出；
// 同一个订单的提交重试复用同一个幂等键，服务端最多产生一个订单。
package runner

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"time"
)

// 压测模式。
const (
	// ModeClosed 是闭环：每个虚拟用户提交、轮询到终态、再提交下一单。系统变慢时请求也跟着变少。
	ModeClosed = "closed"
	// ModeOpen 是开环：按固定的到达速率发请求，不管上一个有没有返回。见 open.go。
	ModeOpen = "open"
)

// Config 是一次压测的参数。
type Config struct {
	// Mode 是 ModeClosed 或 ModeOpen，空表示闭环。
	Mode string
	// Rate、MaxInFlight、Drain 只用于开环：每秒发出的下单请求数、在途请求上限、
	// 发送停止后收尾（轮询受理订单的最终状态）的最长时间。
	Rate        float64
	MaxInFlight int
	Drain       time.Duration

	// BaseURL 是网关地址，例如 http://127.0.0.1:8080。
	BaseURL string
	// Concurrency 是虚拟用户数。
	Concurrency int
	// Duration 和 Orders 二选一：按时长或按订单总数结束。
	// 到时（或订单数发完）后不再开始新的订单，已经在进行的订单走完流程（受轮询超时约束）。
	Duration time.Duration
	Orders   int64
	// SKUs 是参与压测的商品，每个订单随机选一个。
	SKUs []int64
	// Quantity 是每单的数量。
	Quantity int32
	// PayRatio、CancelRatio 是落库成功（pending_payment）的订单里支付、取消的比例，
	// 其余的放着不管，等超时关单。
	PayRatio    float64
	CancelRatio float64
	// PollInterval 是两次查询订单的间隔，PollTimeout 是每一轮轮询（等落库、等支付结果）的上限。
	PollInterval time.Duration
	PollTimeout  time.Duration
	// RequestTimeout 是单个 HTTP 请求的超时。
	RequestTimeout time.Duration
	// SubmitAttempts 是一个订单流程里提交请求的总次数上限（含首次和查询到 404 后的重新提交）。
	SubmitAttempts int
	// RetryBackoff 是两次提交重试之间的等待。
	RetryBackoff time.Duration
	// RejectBackoff 是提交被业务拒绝（库存不足、超出限购）之后、开始下一个订单之前的等待。
	// 0 表示不等：卖完之后每个虚拟用户会以网络往返的速度连续提交。
	RejectBackoff time.Duration
	// Users 是 user_id 的取值范围 [1, Users]，用来分散限购。
	Users int64
	// Seed 是随机种子，为 0 时随机生成。相同的种子产生相同的用户、商品和动作序列
	// （各虚拟用户内部有序，虚拟用户之间的交错仍取决于调度）。
	Seed uint64
	// RunID 是幂等键的前缀，区分不同次的压测。为空时按启动时间生成。
	RunID string
	// ProgressInterval 是打印进度日志的间隔，0 表示不打印。
	ProgressInterval time.Duration
	// FlushInterval 是账本刷盘的间隔，默认 1 秒。
	FlushInterval time.Duration

	// Logger 为 nil 时用 slog.Default()。
	Logger *slog.Logger
	// HTTPClient 为 nil 时按并发数新建一个。
	HTTPClient *http.Client
}

// maxRunIDLen 保证幂等键（RunID-vu-seq）不超过网关允许的 64 个字符。
const maxRunIDLen = 24

// Validate 检查参数。
func (c *Config) Validate() error {
	u, err := url.Parse(c.BaseURL)
	if err != nil {
		return fmt.Errorf("invalid gateway address %q: %w", c.BaseURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" || u.Host == "" {
		return fmt.Errorf("invalid gateway address %q: want http(s)://host:port", c.BaseURL)
	}
	var errs []error
	if c.Concurrency < 1 {
		errs = append(errs, errors.New("concurrency must be >= 1"))
	}
	if (c.Duration > 0) == (c.Orders > 0) {
		errs = append(errs, errors.New("exactly one of duration and orders must be set"))
	}
	if c.Duration < 0 || c.Orders < 0 {
		errs = append(errs, errors.New("duration and orders must not be negative"))
	}
	if len(c.SKUs) == 0 {
		errs = append(errs, errors.New("at least one sku is required"))
	}
	for _, s := range c.SKUs {
		if s <= 0 {
			errs = append(errs, fmt.Errorf("invalid sku id %d", s))
		}
	}
	if c.Quantity < 1 {
		errs = append(errs, errors.New("quantity must be >= 1"))
	}
	if c.PayRatio < 0 || c.CancelRatio < 0 || c.PayRatio+c.CancelRatio > 1 {
		errs = append(errs, errors.New("pay and cancel ratios must be >= 0 and sum to <= 1"))
	}
	if c.PollInterval <= 0 || c.PollTimeout <= 0 || c.RequestTimeout <= 0 {
		errs = append(errs, errors.New("poll interval, poll timeout and request timeout must be > 0"))
	}
	if c.SubmitAttempts < 1 {
		errs = append(errs, errors.New("submit attempts must be >= 1"))
	}
	if c.RejectBackoff < 0 {
		errs = append(errs, errors.New("reject backoff must not be negative"))
	}
	if c.RetryBackoff < 0 {
		errs = append(errs, errors.New("retry backoff must not be negative"))
	}
	if c.Users < 1 {
		errs = append(errs, errors.New("users must be >= 1"))
	}
	if len(c.RunID) > maxRunIDLen {
		errs = append(errs, fmt.Errorf("run id longer than %d characters", maxRunIDLen))
	}
	switch c.Mode {
	case "", ModeClosed:
	case ModeOpen:
		if !(c.Rate > 0) || math.IsInf(c.Rate, 0) {
			errs = append(errs, errors.New("open mode: rate must be > 0"))
		}
		if c.Duration <= 0 || c.Orders > 0 {
			errs = append(errs, errors.New("open mode: set duration (and not orders)"))
		}
		if c.MaxInFlight < 1 {
			errs = append(errs, errors.New("open mode: max in flight must be >= 1"))
		}
		if c.Drain <= 0 {
			errs = append(errs, errors.New("open mode: drain must be > 0"))
		}
	default:
		errs = append(errs, fmt.Errorf("mode must be %s or %s", ModeClosed, ModeOpen))
	}
	return errors.Join(errs...)
}
