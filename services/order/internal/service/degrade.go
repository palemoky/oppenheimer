package service

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/palemoky/oppenheimer/pkg/breaker"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/errx"
)

// 本文件是 order 的降级（设计文档 1d 第 6 节）：闸门的熔断器、落库健康、人工开关，
// 以及它们对外的错误。服务之间的熔断在 Envoy 边车上，这里只做边车看不到的依赖。

// 降级开关的名字。
const (
	// SwitchAdmission 关掉之后拒绝新的下单，其余接口照常。用于维护窗口和演练。
	SwitchAdmission = "admission"
	// SwitchCache 关掉之后订单查询不经过缓存，每次都读库。由 NewOrderCache 登记。
	SwitchCache = "cache"
)

// 降级时对外的错误（errx.Degraded：FAILED_PRECONDITION 加原因码，网关映射成 503；
// 不用 UNAVAILABLE 的理由见 errx.Degraded）。都是“什么都没有发生，稍后用同一个幂等键重试”。
var (
	errAdmissionOff = errx.Degraded(errx.ReasonFeatureDisabled,
		"ordering is temporarily switched off, retry later")
	errGateUnavailable = errx.Degraded(errx.ReasonStockGateUnavailable,
		"stock gate unavailable, retry with the same idempotency key")
	errLandingUnavailable = errx.Degraded(errx.ReasonOrderLandingUnavailable,
		"orders cannot be processed right now, retry with the same idempotency key")
	errHoldPending = errx.Degraded(errx.ReasonStockGateUnavailable,
		"stock gate unavailable, the order may still be queued; retry later")
)

// depOutcome 把一次对依赖的调用的结果归类，喂给熔断器。ctx 是调用方的 ctx，不是为这一跳另设时限的子 ctx：
// 调用方自己取消或者它的截止时间先到了，说明不了依赖的状态（Ignored）；
// 调用方还在等、而这一跳超时或出错，才是依赖的失败。和 contextError 判断取消的依据相同。
func depOutcome(ctx context.Context, err error) breaker.Outcome {
	switch {
	case err == nil:
		return breaker.Success
	case ctx.Err() != nil:
		return breaker.Ignored
	default:
		return breaker.Failure
	}
}

// isBreakerOpen 判断 err 是不是“熔断器没有放行”，也就是依赖根本没有被访问。
func isBreakerOpen(err error) bool { return errors.Is(err, breaker.ErrOpen) }

// newBreaker 建一个状态变化时记日志的熔断器。
func (s *Service) newBreaker(name string, failures int, cooldown time.Duration) *breaker.Breaker {
	return breaker.New(breaker.Config{
		Name: name, Failures: failures, Cooldown: cooldown, Now: s.clock,
		OnChange: s.breakerChanged,
	})
}

// gateBreakerName 是闸门熔断器的名字，breakerChanged 靠它认出闸门的恢复。
const gateBreakerName = "stock-gate"

// breakerChanged 是各熔断器状态变化时的回调：记日志，并记下闸门恢复的时刻。
func (s *Service) breakerChanged(name string, from, to breaker.State) {
	switch to {
	case breaker.Open:
		s.logger.Warn("dependency breaker opened, degrading", "breaker", name, "from", from.String())
	case breaker.Closed:
		s.logger.Info("dependency breaker closed, back to normal", "breaker", name, "from", from.String())
		if name == gateBreakerName {
			s.gateRecoveredAt.Store(s.clock().UnixNano())
		}
	case breaker.HalfOpen:
		s.logger.Info("dependency breaker half-open, probing", "breaker", name)
	}
}

// gateRecoveredWithin 报告闸门的熔断器是否在最近 d 之内从打开恢复过来。
//
// 闸门不可用期间有些事情没做成，恢复之后的一小段时间里要按“状态还不可信”来处理：
//   - 回收任务：排队的订单在故障期间一直没法被认领，它们的“等待时长”不能算数（见 Reaper.RunOnce）。
//   - 读己之写的标记：故障期间的写入没打上标记，恢复后的一段时间里展示读不能信从库。
func (s *Service) gateRecoveredWithin(d time.Duration) bool {
	at := s.gateRecoveredAt.Load()
	return at != 0 && s.clock().Sub(time.Unix(0, at)) < d
}

// ---------- 落库健康 ----------

// landingHealth 是一个订单库的落库健康：一个熔断器，加上“已经发出去、还不知道结果的探测”。
type landingHealth struct {
	b *breaker.Breaker

	mu sync.Mutex
	// pending 是半开时放进去的那个订单的 done，pendingAt 是它的消息发出去的时刻。见 admitLanding。
	pending   func(breaker.Outcome)
	pendingAt time.Time
}

// admitLanding 在受理之前检查这个用户的订单现在落不落得了库（设计文档 1d 的 6.5）。
//
// 受理这一侧只碰 Redis 和 Kafka，库存服务或者订单库坏了它什么都不知道，照常返回 202；
// 这些订单随后在落库时失败，或者排队超过宽限期被回收。用户拿到 202 之后订单消失或必然失败，
// 比一开始就被拒绝糟糕。所以每个订单库一个熔断器，由落库消费者的处理结果喂（reportLanding），
// 受理时先问它：打开时直接拒绝。
//
// 放行时返回 settle，受理结束时调用，published 表示落库的消息是否已经发出去。
//
// 这是每个实例自己的判断：没有分到分区的实例看不到落库的结果，它的熔断器不会打开，
// 那时兜底的仍然是闸门里的背压（所有实例共用）。
func (s *Service) admitLanding(userID int64) (settle func(published bool), err error) {
	if s.store == nil || len(s.landing) == 0 {
		return func(bool) {}, nil
	}
	router := s.store.Router()
	h := s.landing[router.DB(router.ForUser(userID))]
	h.resolveStaleProbe(s.clock(), s.landingCooldown)
	done, probe, err := h.b.AllowProbe()
	if err != nil {
		return nil, errLandingUnavailable
	}
	if !probe {
		// 关闭状态：结果由落库那一侧报告，这里没有要说的。
		return func(bool) { done(breaker.Ignored) }, nil
	}
	// 半开状态，这个请求是探测。走到这里说明落库的消费者手里已经没有在重试的消息了
	// （有的话它们的失败会不断把冷却重新算起，进不了半开），只能放一个真实的订单进去试。
	return func(published bool) {
		if !published {
			// 没受理成（库存不足、闸门拒绝……），什么也说明不了：把探测的名额让给下一个请求。
			done(breaker.Ignored)
			return
		}
		// 消息发出去了，落库的结果要过一会儿才知道：记下来，由 resolveStaleProbe 收尾。
		h.mu.Lock()
		h.pending, h.pendingAt = done, s.clock()
		h.mu.Unlock()
	}, nil
}

// resolveStaleProbe 处理一个发出去超过 cooldown 还没有结论的探测：当它成功。
//
// 探测的订单落库失败会由 reportLanding 报上来、把熔断器重新打开（那之后这里的 done 已经过期，不起作用）；
// 落库成功也会报上来、直接关闭。两种都没有，通常是落库的结果没有回到这个实例：消息所在的分区
// 在别的实例上。一个冷却时间内没有失败报上来，就当它成功了。不这样的话探测永远没有结果，
// 熔断器会一直停在半开，每个冷却时间只放一个订单。
//
// 在下一次受理时顺带检查，不用定时器：没有新的下单请求时，熔断器停在半开也没有影响。
func (h *landingHealth) resolveStaleProbe(now time.Time, cooldown time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pending != nil && now.Sub(h.pendingAt) >= cooldown {
		h.pending(breaker.Success)
		h.pending = nil
	}
}

// reportLanding 把一次落库的结果告诉订单所在库的熔断器。
//
// ok 为 true 只在这一次处理真的把依赖走通了的时候报告（库存服务回答了，订单库写进去了）。
// “占用已经不在了，丢弃消息”“订单早就是终态”这类什么依赖都没碰的结束不报告：
// 否则库存服务坏着的时候，一条被 sweeper 置为失败的消息重试结束，会把熔断器错误地关上。
func (s *Service) reportLanding(orderID int64, ok bool) {
	if db := s.store.DBOfOrder(orderID); db < len(s.landing) {
		s.landing[db].b.Report(ok)
	}
}

// ---------- 状态展示 ----------

// admissionStatus 是下单受理当前的降级状态，给 GET /degrade 用。
func (s *Service) admissionStatus() degrade.Status {
	var (
		parts    []string
		degraded bool
	)
	add := func(name string, b *breaker.Breaker) {
		st := b.Stats()
		if st.State != breaker.Closed {
			degraded = true
		}
		parts = append(parts, fmt.Sprintf("%s=%s(opens=%d,rejected=%d)", name, st.State, st.Opens, st.Rejected))
	}
	add(gateBreakerName, s.gateBreaker)
	for i, h := range s.landing {
		add("landing_db_"+strconv.Itoa(i), h.b)
	}
	return degrade.Status{Degraded: degraded, Detail: strings.Join(parts, " ")}
}
