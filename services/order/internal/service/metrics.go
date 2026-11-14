package service

import (
	"context"
	"errors"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	"github.com/palemoky/oppenheimer/pkg/bulkhead"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// 本文件是 order 的业务指标（设计文档 1d 的 8.8）。gRPC、Kafka、MySQL、Redis、outbox、熔断器、开关、缓存的
// 基础指标由公共库自动产生，这里只记业务上的结局。
//
// 标签只用有限的枚举（结局、状态名、读的来源和模式、名额的名字）；订单号、用户号放在 span 上（otelx.SetOrder）。
// 标签组合是有限的，每种组合的 metric.WithAttributes 预先建好，热路径上记一次不分配。
//
// 指标只在已有的分支上记，不改变任何判断、调用顺序和返回值。

var meter = otelx.Meter("github.com/palemoky/oppenheimer/services/order/internal/service")

var (
	admissionsCounter  = otelx.Counter(meter, "order.admissions", "Order submissions by outcome")
	persistsCounter    = otelx.Counter(meter, "order.persists", "Order submitted messages handled, by outcome")
	persistDelayHist   = otelx.Histogram(meter, "order.persist.delay", "Time from admission to the order landing in the database")
	transitionsCounter = otelx.Counter(meter, "order.transitions", "Order status transitions completed by this process")
	reclaimedCounter   = otelx.Counter(meter, "order.sweeper.reclaimed", "Orders and holds reclaimed by background tasks, by kind")
	readsCounter       = otelx.Counter(meter, "order.reads", "Display reads by the source of the result and read mode")
)

const (
	bulkheadInFlightDesc = "In-flight display reads holding a slot of the read limit"
	bulkheadRejectedDesc = "Display reads rejected by the read limit (cumulative)"
)

// optsByValue 为一个标签的每个取值预先建好 metric.WithAttributes。
func optsByValue(key string, values ...string) map[string]metric.MeasurementOption {
	m := make(map[string]metric.MeasurementOption, len(values))
	for _, v := range values {
		m[v] = metric.WithAttributes(attribute.String(key, v))
	}
	return m
}

// ---------- 受理 ----------

// 受理的结局（order.admissions 的 outcome）。能和原因码对上的用原因码的小写。
const (
	admissionAdmitted = "admitted"
	// admissionDuplicate：幂等键重复，返回已有订单的当前状态。
	admissionDuplicate = "duplicate"
	// admissionDuplicateAbandoned：幂等键还在，订单和占用都已被回收，让客户端重试（原因码是 STOCK_GATE_UNAVAILABLE）。
	admissionDuplicateAbandoned = "duplicate_abandoned"
	admissionInvalidArgument    = "invalid_argument"
	admissionFeatureDisabled    = "feature_disabled"
	admissionLandingUnavailable = "order_landing_unavailable"
	admissionGateUnavailable    = "stock_gate_unavailable"
	admissionInventoryUnavail   = "inventory_unavailable"
	admissionKeyReused          = "idempotency_key_reused"
	admissionSKUNotFound        = "sku_not_found"
	admissionInsufficientStock  = "insufficient_stock"
	admissionPurchaseLimit      = "purchase_limit_exceeded"
	admissionBacklogged         = "backlogged"
	admissionQueueUnavailable   = "order_queue_unavailable"
	admissionArchiveUnavailable = "archive_unavailable"
	admissionReadBusy           = "read_busy"
	admissionCanceled           = "canceled"
	admissionDeadlineExceeded   = "deadline_exceeded"
	admissionUnavailable        = "unavailable"
	admissionInternal           = "internal"
)

var admissionOpts = optsByValue("outcome",
	admissionAdmitted, admissionDuplicate, admissionDuplicateAbandoned, admissionInvalidArgument,
	admissionFeatureDisabled, admissionLandingUnavailable, admissionGateUnavailable, admissionInventoryUnavail,
	admissionKeyReused, admissionSKUNotFound, admissionInsufficientStock, admissionPurchaseLimit,
	admissionBacklogged, admissionQueueUnavailable, admissionArchiveUnavailable, admissionReadBusy,
	admissionCanceled, admissionDeadlineExceeded, admissionUnavailable, admissionInternal,
)

// admissionByReason 把受理路径上返回的原因码映射到结局。
var admissionByReason = map[string]string{
	errx.ReasonInvalidArgument:         admissionInvalidArgument,
	errx.ReasonFeatureDisabled:         admissionFeatureDisabled,
	errx.ReasonOrderLandingUnavailable: admissionLandingUnavailable,
	errx.ReasonStockGateUnavailable:    admissionGateUnavailable,
	errx.ReasonInventoryUnavailable:    admissionInventoryUnavail,
	errx.ReasonIdempotencyKeyReused:    admissionKeyReused,
	errx.ReasonSKUNotFound:             admissionSKUNotFound,
	errx.ReasonInsufficientStock:       admissionInsufficientStock,
	errx.ReasonPurchaseLimit:           admissionPurchaseLimit,
	errx.ReasonBacklogged:              admissionBacklogged,
	errx.ReasonOrderQueueUnavailable:   admissionQueueUnavailable,
	errx.ReasonArchiveUnavailable:      admissionArchiveUnavailable,
	ReasonReadBusy:                     admissionReadBusy,
}

// admissionOutcomeOf 按受理返回的错误归类：先看原因码，没有原因码的按 gRPC 状态码（调用方取消、超时、
// 分片暂时不可用、依赖超时），其余都是内部错误。
func admissionOutcomeOf(err error) string {
	if o, ok := admissionByReason[errx.Reason(err)]; ok {
		return o
	}
	switch status.Code(err) {
	case codes.Canceled:
		return admissionCanceled
	case codes.DeadlineExceeded:
		return admissionDeadlineExceeded
	case codes.Unavailable:
		return admissionUnavailable
	default:
		return admissionInternal
	}
}

// admissionDone 记一次受理的结局。
func admissionDone(ctx context.Context, outcome string) {
	// 表里没有的结局（以后加了取值却忘了登记）不能让受理 panic：nil 的选项传给 Add 会空指针。
	opt, ok := admissionOpts[outcome]
	if !ok {
		opt = admissionOpts[admissionInternal]
	}
	admissionsCounter.Add(ctx, 1, opt)
}

// admissionFailed 按 err 记一次没有受理的结局，把原因码记在 span 上，原样返回 err。
func admissionFailed(ctx context.Context, err error) error {
	return admissionFailedAs(ctx, admissionOutcomeOf(err), err)
}

// admissionFailedAs 同 admissionFailed，结局由调用方指定。
func admissionFailedAs(ctx context.Context, outcome string, err error) error {
	admissionDone(ctx, outcome)
	if r := errx.Reason(err); r != "" {
		otelx.SetReason(ctx, r)
	}
	return err
}

// ---------- 落库 ----------

// 落库（处理一条 order.submit 消息）的结局（order.persists 的 outcome）。只记处理结束（返回 nil）的那些：
// 返回错误的处理会被重试或进死信，次数由 kafkax 的消费指标记。
const (
	// persistPendingPayment：本次预占成功，订单进入待支付。
	persistPendingPayment = "pending_payment"
	// persistReserveRejected：本次预占被库存拒绝，订单置为失败。
	persistReserveRejected = "reserve_rejected"
	// persistExpired：落库时支付时限已过，不预占直接置为失败。
	persistExpired = "expired"
	// persistTooOld：订单号老到可能已经归档，消息被丢弃。
	persistTooOld = "too_old"
	// persistHoldMissing：占用已不存在（已被回收或已走完），消息被丢弃。
	persistHoldMissing = "hold_missing"
	// persistReplayDropped：幂等键在 Redis 过期后的重放，消息被丢弃。
	persistReplayDropped = "replay_dropped"
	// persistAlreadyProcessed：订单已经不在 CREATING（重复投递），本次什么都没做。
	persistAlreadyProcessed = "already_processed"
)

var persistOpts = optsByValue("outcome",
	persistPendingPayment, persistReserveRejected, persistExpired, persistTooOld,
	persistHoldMissing, persistReplayDropped, persistAlreadyProcessed,
)

// persistDone 记一次落库的结局。
func persistDone(ctx context.Context, outcome string) {
	if opt, ok := persistOpts[outcome]; ok {
		persistsCounter.Add(ctx, 1, opt)
	}
}

// persistLanded 记一次落库的结局，以及从受理（消息里的受理时间，和判断是否过期用的是同一份）到现在的耗时。
func (s *Service) persistLanded(ctx context.Context, outcome string, acceptedAt time.Time) {
	opt, ok := persistOpts[outcome]
	if !ok {
		return
	}
	persistsCounter.Add(ctx, 1, opt)
	persistDelayHist.Record(ctx, s.clock().Sub(acceptedAt).Seconds(), opt)
}

// ---------- 状态迁移 ----------

// 状态机里由 order 完成的迁移（order.transitions 的 from、to）。
var (
	transCreatingToPending  = transitionOpt("creating", "pending_payment")
	transCreatingToFailed   = transitionOpt("creating", "failed")
	transPendingToPaid      = transitionOpt("pending_payment", "paid")
	transPendingToCancelled = transitionOpt("pending_payment", "cancelled")
	transPendingToClosed    = transitionOpt("pending_payment", "closed")
)

func transitionOpt(from, to string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("from", from), attribute.String("to", to))
}

// transitionDone 记一次由本次调用完成的迁移（条件更新影响了 1 行、事务已提交）。
func transitionDone(ctx context.Context, opt metric.MeasurementOption) {
	transitionsCounter.Add(ctx, 1, opt)
}

// transitionOfEvent 返回 transition 写的事件对应的迁移。transition 只用于取消、关单、置为失败这三种。
func transitionOfEvent(ev *eventv1.OrderEvent) (metric.MeasurementOption, bool) {
	switch ev.GetBody().(type) {
	case *eventv1.OrderEvent_Failed:
		return transCreatingToFailed, true
	case *eventv1.OrderEvent_Cancelled:
		return transPendingToCancelled, true
	case *eventv1.OrderEvent_Closed:
		return transPendingToClosed, true
	default:
		return nil, false
	}
}

// ---------- 回收 ----------

// 后台任务回收的种类（order.sweeper.reclaimed 的 kind）。
var (
	reclaimCloseExpired  = metric.WithAttributes(attribute.String("kind", "close_expired"))
	reclaimCreateTimeout = metric.WithAttributes(attribute.String("kind", "create_timeout"))
	reclaimOrphanHold    = metric.WithAttributes(attribute.String("kind", "orphan_hold"))
)

func reclaimed(ctx context.Context, kind metric.MeasurementOption) {
	reclaimedCounter.Add(ctx, 1, kind)
}

// ---------- 读 ----------

// readSource 是展示读的结果最终从哪里来（order.reads 的 source）。
type readSource int

const (
	readFromCache readSource = iota
	// readFromHold：还没落库的订单，从闸门里的占用合成（CREATING）。
	readFromHold
	readFromReplica
	readFromPrimary
	readFromCold
	readSourceCount
)

var readSourceNames = [readSourceCount]string{"cache", "hold", "replica", "primary", "cold"}

// readOpts[source][mode][partial]。mode 是 ReadMode 的取值（primary、window、gtid）。
var readOpts = func() (o [readSourceCount][ReadGTID + 1][2]metric.MeasurementOption) {
	for src, name := range readSourceNames {
		for mode := ReadPrimary; mode <= ReadGTID; mode++ {
			for partial := range 2 {
				o[src][mode][partial] = metric.WithAttributes(
					attribute.String("source", name),
					attribute.String("mode", mode.String()),
					attribute.Bool("partial", partial == 1),
				)
			}
		}
	}
	return o
}()

// readDone 记一次展示读的结果来源。partial 只在用户列表因为冷库不可用而只返回了一部分时为真。
func (s *Service) readDone(ctx context.Context, src readSource, partial bool) {
	mode := s.readMode
	if mode < ReadPrimary || mode > ReadGTID {
		return
	}
	p := 0
	if partial {
		p = 1
	}
	readsCounter.Add(ctx, 1, readOpts[src][mode][p])
}

// ---------- 舱壁 ----------

// RegisterLimitMetrics 登记展示读名额（每个订单库一个，冷库一个）的在用数和累计拒绝数，
// 标签 name 是 read_db_<库号> 或 cold。没有设上限的名额不登记。返回的 unregister 在服务停止时调用。
func (s *Service) RegisterLimitMetrics() (unregister func(), err error) {
	var regs []func()
	unregister = func() {
		for _, u := range regs {
			u()
		}
	}
	add := func(name string, b *bulkhead.Bulkhead) error {
		if b == nil {
			return nil
		}
		attr := attribute.String("name", name)
		u, err := otelx.ObserveGauge(meter, "order.bulkhead.in_flight", bulkheadInFlightDesc, b.InFlight, attr)
		if err != nil {
			return err
		}
		regs = append(regs, u)
		u, err = observeCounter(meter, "order.bulkhead.rejected", bulkheadRejectedDesc, b.Rejected, attr)
		if err != nil {
			return err
		}
		regs = append(regs, u)
		return nil
	}
	var errs []error
	for i, b := range s.limits.dbs {
		errs = append(errs, add("read_db_"+strconv.Itoa(i), b))
	}
	errs = append(errs, add("cold", s.limits.cold))
	errs = append(errs, add("read_gate", s.limits.gate))
	if err := errors.Join(errs...); err != nil {
		unregister()
		return nil, err
	}
	return unregister, nil
}

// observeCounter 登记一个在每次导出时现读的累计值（只增）。和 otelx.ObserveGauge 一样，read 要快、不能阻塞。
// otelx 没有这个工具，先放在这里。
func observeCounter(m metric.Meter, name, desc string, read func() int64, attrs ...attribute.KeyValue) (unregister func(), err error) {
	c, err := m.Int64ObservableCounter(name, metric.WithDescription(desc))
	if err != nil {
		return nil, err
	}
	opt := metric.WithAttributes(attrs...)
	reg, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(c, read(), opt)
		return nil
	}, c)
	if err != nil {
		return nil, err
	}
	return func() { _ = reg.Unregister() }, nil
}

// ---------- 闸门的队列 ----------

// RegisterQueueMetrics 登记闸门里的占用个数：order.gate.unclaimed_holds 是已受理、还没被落库认领的
// （受理端的背压拿它和 ADMIT_MAX_QUEUE 比），order.gate.claimed_holds 是已认领、订单还没结束的。没有标签。
//
// 值由回收任务每轮顺带读一次（Reaper.refreshQueued），这里只读它存下的结果。回收任务只在 leader 上跑，
// 别的实例上没有人更新这个数，所以没读到过或者已经不新鲜时不上报（而不是报 0）：后端看到的只有 leader 的值。
// 返回的 unregister 在服务停止时调用。
func (r *Reaper) RegisterQueueMetrics() (unregister func(), err error) {
	unclaimed, err := meter.Int64ObservableGauge("order.gate.unclaimed_holds",
		metric.WithDescription("Holds admitted but not yet claimed by order landing (the queue the admission backlog limit counts)"))
	if err != nil {
		return nil, err
	}
	claimed, err := meter.Int64ObservableGauge("order.gate.claimed_holds",
		metric.WithDescription("Holds claimed by order landing whose orders have not ended yet"))
	if err != nil {
		return nil, err
	}
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		q, ok := r.queuedNow()
		if !ok {
			return nil
		}
		o.ObserveInt64(unclaimed, q.unclaimed)
		o.ObserveInt64(claimed, q.claimed)
		return nil
	}, unclaimed, claimed)
	if err != nil {
		return nil, err
	}
	return func() { _ = reg.Unregister() }, nil
}
