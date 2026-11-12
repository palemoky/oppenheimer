// Package ledger 定义压测工具的客户端账本：每个请求一行 JSON（JSON Lines），
// 记下客户端发了什么、看到了什么。run 子命令写它，verify 子命令读它并和服务端的数据对账。
//
// 账本记录的是“客户端相信的事实”，不是服务端的真相：
//   - Outcome 为 unknown 的请求（超时、连接断开、被中断）服务端可能已经执行，也可能没有。
//   - Outcome 为 http_error 且状态码是 5xx 的请求同样不能断定服务端没有执行
//     （例如网关调下游超时返回 504，下游可能已经落库）。
//
// 同一个订单流程（一次提交及其后续的轮询、支付、取消）的所有行带同一个 IdempotencyKey，
// 提交失败、拿不到订单号时也能靠它把这些行串起来。
package ledger

import (
	"bufio"
	"bytes"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"sync"
	"time"
)

// Op 是请求的种类。
type Op string

// 账本里出现的操作。
const (
	// OpSubmit 是一次提交订单的请求。重试时每次尝试各占一行，Attempt 从 1 开始递增。
	OpSubmit Op = "submit"
	// OpPollFinal 是一轮轮询的结果，只记最后看到的状态，见 Stage。
	OpPollFinal Op = "poll_final"
	// OpPay 是一次发起支付的请求。
	OpPay Op = "pay"
	// OpCancel 是一次取消订单的请求。
	OpCancel Op = "cancel"
)

// Stage 区分两轮轮询，只用于 OpPollFinal。
type Stage string

// 轮询的阶段。
const (
	// StageLanding 是提交之后等订单离开 creating 的轮询。
	StageLanding Stage = "landing"
	// StageSettle 是支付之后等订单进入终态的轮询。
	StageSettle Stage = "settle"
)

// Outcome 是客户端对一个请求结果的分类。
type Outcome string

// 结果分类。
const (
	// OutcomeOK：请求得到了预期的成功响应。submit 为 201/202；pay 为 202；cancel 为 200；
	// poll_final 为等到了目标状态（landing：离开 creating；settle：进入终态）。
	OutcomeOK Outcome = "ok"
	// OutcomeRejected：业务拒绝，属于正常结果。submit 为 409 库存不足或超出限购；
	// pay、cancel 为 409（订单状态不允许）。ErrorCode 是具体原因码。
	OutcomeRejected Outcome = "rejected"
	// OutcomeHTTPError：其他非预期的 HTTP 状态码。5xx 不代表服务端没有执行。
	OutcomeHTTPError Outcome = "http_error"
	// OutcomeUnknown：没有拿到完整的响应（超时、连接被拒绝或断开、进程被中断），结果未知。
	OutcomeUnknown Outcome = "unknown"
	// OutcomeTimeout：poll_final 在轮询超时内没有等到目标状态，OrderStatus 是最后看到的状态。
	OutcomeTimeout Outcome = "timeout"
	// OutcomeNotFound：poll_final 查询订单一直返回 404，重新提交的次数也用完了。
	OutcomeNotFound Outcome = "not_found"
)

// Entry 是账本的一行。64 位 ID 和网关接口一样编码成十进制字符串。
type Entry struct {
	// Time 是请求结束（拿到响应或放弃）的时间。
	Time time.Time `json:"ts"`
	Op   Op        `json:"op"`
	// Stage 只在 Op 为 poll_final 时出现。
	Stage Stage `json:"stage,omitempty"`
	// OrderID 是订单号；提交没有成功时为空。
	OrderID  string `json:"order_id"`
	UserID   int64  `json:"user_id,string"`
	SKUID    int64  `json:"sku_id,string"`
	Quantity int32  `json:"quantity"`
	// IdempotencyKey 是这个订单流程提交时用的幂等键，同一流程的每一行都带它。
	IdempotencyKey string `json:"idempotency_key"`
	// Attempt 是提交的第几次尝试（从 1 开始），只用于 submit。
	Attempt int `json:"attempt,omitzero"`
	// HTTPStatus 是响应状态码；没有拿到响应时为 0。poll_final 为最后一次查询的状态码。
	HTTPStatus int `json:"http_status"`
	// OrderStatus 是客户端到这一行为止最后看到的订单状态（来自响应体）；从没看到过时为空。
	OrderStatus string  `json:"order_status"`
	Outcome     Outcome `json:"outcome"`
	// ErrorCode 是错误响应里的 error.code。
	ErrorCode string `json:"error_code,omitempty"`
	// Error 是传输层错误的说明（超时、连接被拒绝等）。
	Error string `json:"error,omitempty"`
	// LatencyMicros 是耗时（微秒）。submit、pay、cancel 为这一次请求的耗时；
	// poll_final 为从这个订单第一次提交到看到最终状态的耗时。
	LatencyMicros int64 `json:"latency_us"`
}

// Writer 把账本行追加写入文件，可以被多个 goroutine 同时调用。
type Writer struct {
	mu     sync.Mutex
	bw     *bufio.Writer
	file   *os.File // 为 nil 时 Close 不做 Sync 和 Close
	err    error    // 第一次写入错误，之后的写入直接返回它
	closed bool
}

// Create 创建（截断）账本文件。
func Create(path string) (*Writer, error) {
	f, err := os.Create(path) //nolint:gosec // 路径来自命令行参数，由操作者指定
	if err != nil {
		return nil, fmt.Errorf("create ledger %s: %w", path, err)
	}
	return &Writer{bw: bufio.NewWriterSize(f, 1<<16), file: f}, nil
}

// NewWriter 把账本写到任意 io.Writer，Close 只刷缓冲。
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, 1<<16)}
}

// Write 追加一行。写入出错后后续的写入都返回同一个错误。
func (w *Writer) Write(e Entry) error {
	e.Time = e.Time.UTC()
	line, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("encode ledger entry: %w", err)
	}
	line = append(line, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("ledger: write after close")
	}
	if w.err != nil {
		return w.err
	}
	if _, err := w.bw.Write(line); err != nil {
		w.err = fmt.Errorf("write ledger: %w", err)
		return w.err
	}
	return nil
}

// Flush 把缓冲里的内容写到底层文件（不 fsync）。
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	return w.flushLocked()
}

func (w *Writer) flushLocked() error {
	if w.err != nil {
		return w.err
	}
	if err := w.bw.Flush(); err != nil {
		w.err = fmt.Errorf("flush ledger: %w", err)
		return w.err
	}
	return nil
}

// Close 刷缓冲、fsync 并关闭文件。可以重复调用。
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	err := w.flushLocked()
	if w.file != nil {
		if serr := w.file.Sync(); serr != nil && err == nil {
			err = fmt.Errorf("sync ledger: %w", serr)
		}
		if cerr := w.file.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close ledger: %w", cerr)
		}
	}
	return err
}

// maxLine 是单行的上限，远大于实际的行长，只用来防止读到损坏的文件时无限增长。
const maxLine = 1 << 20

// Read 逐行解码账本。空行跳过。某一行解码失败时产出带行号的错误，调用方可以选择继续或停止
// （进程被强杀时最后一行可能不完整）。
func Read(r io.Reader) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64<<10), maxLine)
		lineNo := 0
		for sc.Scan() {
			lineNo++
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 {
				continue
			}
			var e Entry
			if err := json.Unmarshal(line, &e); err != nil {
				if !yield(Entry{}, fmt.Errorf("ledger line %d: %w", lineNo, err)) {
					return
				}
				continue
			}
			if !yield(e, nil) {
				return
			}
		}
		if err := sc.Err(); err != nil {
			yield(Entry{}, fmt.Errorf("read ledger: %w", err))
		}
	}
}

// ReadFile 读出整个账本文件，遇到第一个错误就返回。
func ReadFile(path string) ([]Entry, error) {
	f, err := os.Open(path) //nolint:gosec // 路径来自命令行参数，由操作者指定
	if err != nil {
		return nil, fmt.Errorf("open ledger %s: %w", path, err)
	}
	defer f.Close()
	var out []Entry
	for e, err := range Read(f) {
		if err != nil {
			return out, err
		}
		out = append(out, e)
	}
	return out, nil
}
