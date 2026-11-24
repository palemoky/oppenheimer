// Package loop 是诊断 Agent 的主循环：模型和工具之间来回，直到交出报告或者撞到上限。设计见 3d 的 3.2。
package loop

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/report"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

// Outcome 是一次运行的结束原因。它是指标的标签，取值只有这几个。
type Outcome string

// 结束原因。
const (
	// OutcomeReported 正常交了报告。
	OutcomeReported Outcome = "reported"
	// OutcomeReportedTruncated 撞到上限之后，用已有的证据交了报告。
	OutcomeReportedTruncated Outcome = "reported_truncated"
	// OutcomeNoReport 没有交出报告（催过、或者收尾的机会用完了）。
	OutcomeNoReport Outcome = "no_report"
	// OutcomeRefused 模型拒绝了请求（安全分类器）。
	OutcomeRefused Outcome = "refused"
	// OutcomeAPIError 调模型失败（SDK 重试之后仍然失败）。
	OutcomeAPIError Outcome = "api_error"
	// OutcomeCanceled 运行被取消（进程在停）。
	OutcomeCanceled Outcome = "canceled"
)

// 撞到的上限，写进记录和报告。
const (
	LimitTurns     = "turns"
	LimitCost      = "cost"
	LimitDuration  = "duration"
	LimitMaxTokens = "max_tokens"
)

// 工具调用的结果，指标的标签。
const (
	CallOK      = "ok"
	CallError   = "error"
	CallTimeout = "timeout"
)

// wrapUpTurns 是撞到上限之后还给模型几轮：一轮交报告，一轮改被打回的报告。
const wrapUpTurns = 2

// maxRejections 是报告最多被打回几次；再不合格就按原样收下并标成未核实。
const maxRejections = 1

// Limits 是一次运行的上限。
//
// 轮数、费用、时长是“软”的：撞到之后不再查，进入收尾。所以一次运行最坏的总时长约是
// MaxDuration 加一次工具的时限再加 wrapUpTurns 次模型调用的时限，最坏的费用是 MaxCostUSD 加收尾那几轮的。
type Limits struct {
	MaxTurns     int
	MaxCostUSD   float64
	MaxDuration  time.Duration
	ModelTimeout time.Duration
	ToolTimeout  time.Duration
}

// Record 返回写进运行记录的形式。时长按秒写：json/v2 对 time.Duration 没有默认的表示。
func (l Limits) Record() map[string]any {
	return map[string]any{
		"max_turns":             l.MaxTurns,
		"max_cost_usd":          l.MaxCostUSD,
		"max_duration_seconds":  l.MaxDuration.Seconds(),
		"model_timeout_seconds": l.ModelTimeout.Seconds(),
		"tool_timeout_seconds":  l.ToolTimeout.Seconds(),
	}
}

// DefaultLimits 是设计第 2 节第 7 项定的值。
func DefaultLimits() Limits {
	return Limits{
		MaxTurns:     25,
		MaxCostUSD:   2,
		MaxDuration:  10 * time.Minute,
		ModelTimeout: 3 * time.Minute,
		ToolTimeout:  15 * time.Second,
	}
}

// Recorder 接收一次运行的每一步，*runlog.Run 实现它。方法要能并发调用。
type Recorder interface {
	Event(kind string, data any)
	Raw(step string, raw []byte)
}

// Runner 跑诊断。建好之后只读，可以并发跑多次运行。
type Runner struct {
	LLM    llm.Client
	Tools  *tool.Registry
	System string
	Limits Limits
	// Price 是配置的模型的单价；回退到别的模型而它不在价格表里时用这个价。
	Price llm.Price
	// Now 为 nil 时用 time.Now。
	Now func() time.Time
}

// Result 是一次运行的结果。
type Result struct {
	Outcome Outcome
	// Report 在 Outcome 为 reported、reported_truncated 时不为 nil。
	Report *report.Report
	// Unverified 表示报告没有通过溯源的校验，是按原样收下的。
	Unverified bool
	// LimitHit 是撞到的上限，没有撞到时为空。
	LimitHit string
	Turns    int
	Usage    llm.Usage
	CostUSD  float64
	Duration time.Duration
	// Model 是最后一轮实际作答的模型。
	Model string
	// Steps 是全部工具调用，按步骤号。
	Steps map[string]report.Step
	// Calls 按“工具名 → 结果 → 次数”统计。
	Calls map[string]map[string]int
	// Err 在 Outcome 为 api_error、canceled 时有值。
	Err error
}

// 写进运行记录的事件。
type (
	evModelRequest struct {
		Turn    int              `json:"turn"`
		Text    string           `json:"text,omitzero"`
		Results []llm.ToolResult `json:"results,omitzero"`
	}
	evModelResponse struct {
		Turn            int            `json:"turn"`
		MessageID       string         `json:"message_id"`
		Model           string         `json:"model"`
		FellBack        bool           `json:"fell_back,omitzero"`
		Stop            llm.StopReason `json:"stop_reason"`
		RefusalCategory string         `json:"refusal_category,omitzero"`
		Thinking        []string       `json:"thinking,omitzero"`
		Text            string         `json:"text,omitzero"`
		Calls           []llm.ToolCall `json:"calls,omitzero"`
		Usage           llm.Usage      `json:"usage"`
		CostUSD         float64        `json:"cost_usd"`
		DurationMS      int64          `json:"duration_ms"`
	}
	evToolCall struct {
		Step       string         `json:"step"`
		Turn       int            `json:"turn"`
		Tool       string         `json:"tool"`
		Input      jsontext.Value `json:"input"`
		Result     string         `json:"result"`
		RawBytes   int            `json:"raw_bytes"`
		Text       string         `json:"text"`
		DurationMS int64          `json:"duration_ms"`
	}
	evLimitHit struct {
		Limit string `json:"limit"`
		Turn  int    `json:"turn"`
	}
	evReportRejected struct {
		Turn   int            `json:"turn"`
		Reason string         `json:"reason"`
		Input  jsontext.Value `json:"input"`
	}
	evEnd struct {
		Outcome  Outcome `json:"outcome"`
		LimitHit string  `json:"limit_hit,omitzero"`
		Error    string  `json:"error,omitzero"`
	}
)

// run 是一次运行的可变状态，只在跑循环的那个 goroutine 里读写（工具的 goroutine 只写各自的槽位）。
type run struct {
	*Runner
	rec   Recorder
	now   func() time.Time
	start time.Time
	res   Result

	wrapUp      bool
	wrapUpSends int
	nudged      bool
	rejections  int
	stepSeq     int
}

// Run 对 question 跑一次诊断。它总是返回一个 Result，不返回错误：失败也是一种要记录的结果。
// ctx 取消时以 OutcomeCanceled 结束。
func (r *Runner) Run(ctx context.Context, question string, rec Recorder) Result {
	now := r.Now
	if now == nil {
		now = time.Now
	}
	st := &run{Runner: r, rec: rec, now: now, start: now()}
	st.res.Steps = make(map[string]report.Step)
	st.res.Calls = make(map[string]map[string]int)

	defs := append(r.Tools.Defs(), llm.ToolDef{
		Name:        report.ToolName,
		Description: report.Description,
		Schema:      report.Schema(),
		Strict:      true,
	})
	conv := r.LLM.NewConversation(r.System, defs)

	st.loop(ctx, conv, question)

	st.res.Duration = now().Sub(st.start)
	end := evEnd{Outcome: st.res.Outcome, LimitHit: st.res.LimitHit}
	if st.res.Err != nil {
		end.Error = st.res.Err.Error()
	}
	rec.Event("end", end)
	return st.res
}

func (st *run) loop(ctx context.Context, conv llm.Conversation, question string) {
	turn := llm.Turn{Text: question}
	for {
		// 上限在发下一轮之前检查。撞到了不直接断掉：把说明附在这一轮的末尾，让模型用已有的证据交报告。
		if !st.wrapUp {
			if limit := st.limitHit(); limit != "" {
				st.enterWrapUp(limit)
				turn.Text = joinText(turn.Text, wrapUpNotice(limit))
			}
		}
		if st.wrapUp {
			if st.wrapUpSends >= wrapUpTurns {
				st.res.Outcome = OutcomeNoReport
				return
			}
			st.wrapUpSends++
		}

		reply, err := st.send(ctx, conv, turn)
		if err != nil {
			st.res.Err = err
			st.res.Outcome = OutcomeAPIError
			if ctx.Err() != nil {
				st.res.Outcome = OutcomeCanceled
			}
			return
		}

		if reply.Stop == llm.StopRefusal {
			st.res.Outcome = OutcomeRefused
			return
		}

		if len(reply.Calls) > 0 {
			// 回复里只要有工具调用，下一轮就必须给每一个都回结果，不管结束原因是什么。
			truncated := reply.Stop == llm.StopMaxTokens
			results, done := st.handleCalls(ctx, reply.Calls, truncated)
			if done {
				return
			}
			turn = llm.Turn{Results: results}
			if truncated && !st.wrapUp {
				st.enterWrapUp(LimitMaxTokens)
				turn.Text = wrapUpNotice(LimitMaxTokens)
			}
			continue
		}

		switch {
		case reply.Stop == llm.StopMaxTokens && !st.wrapUp:
			st.enterWrapUp(LimitMaxTokens)
			turn = llm.Turn{Text: wrapUpNotice(LimitMaxTokens)}
		case st.wrapUp:
			// 收尾时没有调工具：下一轮开头的检查会在机会用完时结束。
			turn = llm.Turn{Text: nudgeText}
		case st.nudged:
			st.res.Outcome = OutcomeNoReport
			return
		default:
			// 模型说完了却没有交报告：催一次。
			st.nudged = true
			turn = llm.Turn{Text: nudgeText}
		}
	}
}

// limitHit 返回撞到的上限，没有撞到返回空。
func (st *run) limitHit() string {
	switch {
	case st.res.Turns >= st.Limits.MaxTurns:
		return LimitTurns
	case st.res.CostUSD >= st.Limits.MaxCostUSD:
		return LimitCost
	case st.now().Sub(st.start) >= st.Limits.MaxDuration:
		return LimitDuration
	}
	return ""
}

func (st *run) enterWrapUp(limit string) {
	st.wrapUp = true
	st.res.LimitHit = limit
	st.rec.Event("limit_hit", evLimitHit{Limit: limit, Turn: st.res.Turns})
}

// send 发一轮并记账。
func (st *run) send(ctx context.Context, conv llm.Conversation, turn llm.Turn) (*llm.Reply, error) {
	n := st.res.Turns + 1
	st.rec.Event("model_request", evModelRequest{Turn: n, Text: turn.Text, Results: turn.Results})

	callCtx, cancel := context.WithTimeout(ctx, st.Limits.ModelTimeout)
	defer cancel()
	began := st.now()
	reply, err := conv.Send(callCtx, turn)
	if err != nil {
		return nil, fmt.Errorf("turn %d: %w", n, err)
	}

	cost := llm.CostOf(reply.Model, st.Price, reply.Usage)
	st.res.Turns = n
	st.res.Usage.Add(reply.Usage)
	st.res.CostUSD += cost
	st.res.Model = reply.Model
	st.rec.Event("model_response", evModelResponse{
		Turn: n, MessageID: reply.MessageID, Model: reply.Model, FellBack: reply.FellBack,
		Stop: reply.Stop, RefusalCategory: reply.RefusalCategory,
		Thinking: reply.Thinking, Text: reply.Text, Calls: reply.Calls,
		Usage: reply.Usage, CostUSD: cost, DurationMS: st.now().Sub(began).Milliseconds(),
	})
	return reply, nil
}

// handleCalls 处理一轮里的全部工具调用，返回要回给模型的结果（和 calls 一一对应、顺序相同）。
// done 为真表示收下了报告，运行结束。truncated 表示这一轮被 max_tokens 截断，参数可能不完整，一个都不执行。
func (st *run) handleCalls(ctx context.Context, calls []llm.ToolCall, truncated bool) (results []llm.ToolResult, done bool) {
	results = make([]llm.ToolResult, len(calls))
	type pending struct {
		idx  int
		step string
		t    tool.Tool
	}
	var todo []pending
	submit := -1

	for i, c := range calls {
		results[i].CallID = c.ID
		switch {
		case truncated:
			results[i].IsError = true
			results[i].Content = "Your previous response was cut off by the output limit, so this call was not executed."
		case c.Name == report.ToolName:
			if submit >= 0 {
				results[i].IsError = true
				results[i].Content = "Only one submit_report call per turn is accepted; this one was ignored."
				continue
			}
			submit = i
		case st.wrapUp:
			results[i].IsError = true
			results[i].Content = "Limit reached: no further queries are executed. Call submit_report now with the evidence you already have."
		default:
			t, ok := st.Tools.Get(c.Name)
			if !ok {
				results[i].IsError = true
				results[i].Content = fmt.Sprintf("Unknown tool %q.", c.Name)
				continue
			}
			// 步骤号按调用在回复里的顺序编，先编号再并发执行：号和执行的快慢无关。
			st.stepSeq++
			todo = append(todo, pending{idx: i, step: fmt.Sprintf("s%d", st.stepSeq), t: t})
		}
	}

	// 并发执行。每个 goroutine 只写自己的槽位，等全部结束后再汇总到 st.res。
	steps := make([]report.Step, len(todo))
	outcomes := make([]string, len(todo))
	var wg sync.WaitGroup
	for j, p := range todo {
		wg.Go(func() {
			call := calls[p.idx]
			steps[j], outcomes[j] = st.execute(ctx, p.step, p.t, call)
		})
	}
	wg.Wait()
	for j, p := range todo {
		step := steps[j]
		st.res.Steps[step.ID] = step
		st.count(step.Tool, outcomes[j])
		results[p.idx].IsError = step.Failed
		// 步骤号写在结果的开头，模型在报告里引用的就是它。
		results[p.idx].Content = "[" + step.ID + "]\n" + step.Text
	}

	if submit < 0 {
		return results, false
	}
	// 报告在这一轮的查询都做完之后再校验：它可以引用同一轮里的步骤。
	rep, err := report.Parse(calls[submit].Input)
	if err == nil {
		err = rep.Validate(st.res.Steps)
	}
	if err == nil {
		st.accept(rep, false)
		return nil, true
	}
	st.rec.Event("report_rejected", evReportRejected{Turn: st.res.Turns, Reason: err.Error(), Input: calls[submit].Input})
	if rep != nil && (st.rejections >= maxRejections || st.wrapUp && st.wrapUpSends >= wrapUpTurns) {
		// 已经打回过了还不合格，或者收尾的机会用完了：按原样收下，标成未核实，好过把它丢掉。
		// 解析不了的（rep 为 nil）没有东西可收。
		st.accept(rep, true)
		return nil, true
	}
	st.rejections++
	results[submit].IsError = true
	results[submit].Content = err.Error() + ". Fix the report and call submit_report again."
	return results, false
}

func (st *run) accept(rep *report.Report, unverified bool) {
	st.res.Report = rep
	st.res.Unverified = unverified
	st.res.Outcome = OutcomeReported
	if st.wrapUp {
		st.res.Outcome = OutcomeReportedTruncated
	}
}

func (st *run) count(name, outcome string) {
	if st.res.Calls[name] == nil {
		st.res.Calls[name] = make(map[string]int)
	}
	st.res.Calls[name][outcome]++
}

// execute 执行一次工具调用。在自己的 goroutine 里跑，不碰 st 的可变状态。
func (st *run) execute(ctx context.Context, stepID string, t tool.Tool, call llm.ToolCall) (step report.Step, outcome string) {
	callCtx, cancel := context.WithTimeout(ctx, st.Limits.ToolTimeout)
	defer cancel()
	began := st.now()

	res, err := safeCall(callCtx, t, call.Input)
	step = report.Step{ID: stepID, Tool: call.Name, Input: call.Input}
	switch {
	case err == nil:
		outcome = CallOK
		step.Text = tool.Cap(res.Text)
	case errors.Is(err, context.DeadlineExceeded):
		outcome = CallTimeout
		step.Failed = true
		step.Text = fmt.Sprintf("Timed out after %s. Narrow the time range or make the query cheaper.", st.Limits.ToolTimeout)
	default:
		outcome = CallError
		step.Failed = true
		step.Text = tool.Cap("Error: " + err.Error())
	}

	st.rec.Raw(stepID, res.Raw)
	st.rec.Event("tool_call", evToolCall{
		Step: stepID, Turn: st.res.Turns, Tool: call.Name, Input: call.Input,
		Result: outcome, RawBytes: len(res.Raw), Text: step.Text,
		DurationMS: st.now().Sub(began).Milliseconds(),
	})
	return step, outcome
}

// safeCall 把工具里的 panic 变成错误。工具跑在另开的 goroutine 里，那里的 panic 外层接不住，
// 不接的话一个工具的 bug 会让整个进程退出。
func safeCall(ctx context.Context, t tool.Tool, input jsontext.Value) (res tool.Result, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("tool crashed: %v", p)
		}
	}()
	return t.Call(ctx, input)
}

const nudgeText = "You ended your turn without submitting a report. Call submit_report now. " +
	"If the root cause is still unknown, say so and report what you did establish."

func wrapUpNotice(limit string) string {
	var why string
	switch limit {
	case LimitTurns:
		why = "the turn limit for this run"
	case LimitCost:
		why = "the cost limit for this run"
	case LimitDuration:
		why = "the time limit for this run"
	case LimitMaxTokens:
		why = "the output limit of a single response"
	}
	return "You have reached " + why + ". No further queries will be executed. " +
		"Call submit_report now using only the evidence you already have; " +
		"if it is not enough to determine the root cause, say so and list what should be checked next."
}

func joinText(a, b string) string {
	return strings.TrimSpace(a + "\n\n" + b)
}
