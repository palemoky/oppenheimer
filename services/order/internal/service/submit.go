package service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/palemoky/oppenheimer/api/eventx"
	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/services/order/internal/archive"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// releaseTimeout 是受理失败后查询订单当前状态的时限（publishFailed）。这一步在请求的 ctx 可能已经到期之后执行，
// 所以用独立的 ctx；它是尽力而为的。归还占用（abandon）同样用独立的 ctx，时限是闸门调用统一的 gateTimeout。
const releaseTimeout = time.Second

// errRetry 是“这一次没有受理，原因是暂时的”对外的错误。按降级错误返回（errx.Degraded），不用 UNAVAILABLE：
// 这些都是 order 的依赖出了问题，不是 order 自己不行了，不应该让调用方的边车把 order 摘掉。
func errRetry(reason, msg string) error {
	return errx.Degraded(reason, "%s, retry with the same idempotency key", msg)
}

// ---------- 受理 ----------

// SubmitOrder 实现 OrderServiceServer：在库存闸门里扣减并记下占用，把下单请求写进 Kafka 后立即返回。
//
// 这条路径只访问 Redis 和 Kafka，不访问 MySQL（削峰的目的）。唯一的例外是幂等键重复时
// 要返回已有订单的当前状态，那时才读一次订单库。
//
// 两边的顺序：先 Hold（Redis 变小）再发消息。反过来的话，消息先到、占用还没写，
// 消费者 Claim 不到就会把一个合法的订单丢掉。
func (s *Service) SubmitOrder(ctx context.Context, req *orderv1.SubmitOrderRequest) (*orderv1.SubmitOrderResponse, error) {
	items, err := validateSubmit(req)
	if err != nil {
		return nil, admissionFailed(ctx, err)
	}
	ctx, cancel := context.WithTimeout(ctx, s.submitTimeout)
	defer cancel()

	userID := req.GetUserId()
	// 不受理落不了库的订单（设计文档 1d 的 6.2、6.5）。三项检查都在 Hold 之前：被拒绝时什么都没有发生。
	if s.swAdmission.Off() {
		return nil, admissionFailed(ctx, errAdmissionOff)
	}
	if s.gateMemoryFull() {
		return nil, admissionFailedAs(ctx, admissionGateMemoryFull, errGateFull)
	}
	settle, err := s.admitLanding(userID)
	if err != nil {
		return nil, admissionFailed(ctx, err)
	}
	published := false
	defer func() { settle(published) }()

	orderID, err := s.ids.Next(userID)
	if errors.Is(err, idgen.ErrLeaseExpired) {
		// 节点号的租约续不上（租约所在的订单库不可用，ADR-0028）：什么都还没发生，按“落不了库”降级，
		// 不是内部错误。续租成功后自动恢复。
		s.logger.WarnContext(ctx, "order id lease expired, rejecting admission", "user_id", userID)
		return nil, admissionFailed(ctx, errLandingUnavailable)
	}
	if err != nil {
		return nil, admissionFailed(ctx, s.internal(ctx, "generate order id", err, "user_id", userID))
	}
	otelx.SetOrder(ctx, orderID, userID)
	// 受理时间截断到毫秒：占用里的 ts 是毫秒精度，GetOrder 从占用合成的 create_time、expire_time
	// 才能和这里返回的、写进消息的值完全一样。
	accepted := s.clock().UTC().Truncate(time.Millisecond)
	hreq := stockgate.HoldRequest{
		OrderID:        orderID,
		UserID:         userID,
		IdempotencyKey: req.GetIdempotencyKey(),
		RequestHash:    requestHash(items),
		Items:          gateItems(items),
		Now:            accepted,
		IdempotencyTTL: s.idemTTL,
		Backlog:        s.backlog,
	}
	res, err := s.hold(ctx, hreq)
	if err != nil {
		return nil, admissionFailed(ctx, err)
	}
	// 闸门说“这个幂等键已经受理过”，而受理的正是这次调用刚生成的订单号：占用是这次调用自己写的。
	// Redis 客户端在读超时之后会自动重试命令；Hold 的脚本第一次已经在 Redis 里执行成功、只是回复没收到时，
	// 重试的那一次看到的就是自己刚写下的幂等键。订单号由发号器保证每次调用都不同，所以订单号相同
	// 只可能是这种情况。按受理成功继续往下走（发消息）。当成“以前受理过”的话不会发消息，
	// 用户拿到 202，占用却等不到落库，宽限期一过被回收，订单凭空消失。
	if res.Outcome == stockgate.Duplicate && res.OrderID == orderID {
		res.Outcome = stockgate.Held
	}
	switch res.Outcome {
	case stockgate.Held:
	case stockgate.Duplicate:
		// 已经受理过：返回那个订单的当前状态，不再发消息（第一次受理时已经发过）。
		otelx.SetOrder(ctx, res.OrderID, userID)
		o, err := s.lookup(ctx, res.OrderID, userID)
		if errx.Reason(err) == errx.ReasonOrderNotFound {
			// 幂等键还在，订单和占用都没了：刚好被回收（回收会同时删掉幂等键），重试就会重新受理。
			return nil, admissionFailedAs(ctx, admissionDuplicateAbandoned,
				errRetry(errx.ReasonStockGateUnavailable, "previous attempt was abandoned"))
		}
		if err != nil {
			return nil, admissionFailed(ctx, err)
		}
		admissionDone(ctx, admissionDuplicate)
		return &orderv1.SubmitOrderResponse{Order: o}, nil
	case stockgate.KeyReused:
		return nil, admissionFailed(ctx, errx.New(codes.AlreadyExists, errx.ReasonIdempotencyKeyReused,
			"idempotency key was used for a different request"))
	case stockgate.NotLoaded:
		return nil, admissionFailed(ctx, errx.New(codes.NotFound, errx.ReasonSKUNotFound, "sku %d not found", res.SKUID))
	case stockgate.Insufficient:
		return nil, admissionFailed(ctx, errx.New(codes.FailedPrecondition, errx.ReasonInsufficientStock, "insufficient stock for sku %d", res.SKUID))
	case stockgate.LimitExceeded:
		return nil, admissionFailed(ctx, errx.New(codes.FailedPrecondition, errx.ReasonPurchaseLimit, "purchase limit exceeded for sku %d", res.SKUID))
	case stockgate.Backlogged:
		// 闸门里什么都没改，幂等键也没写：稍后用同一个幂等键重试是安全的。
		// 用 RESOURCE_EXHAUSTED 而不是 UNAVAILABLE：这是有意的削峰，不是故障。grpcx 把 UNAVAILABLE
		// 记成 ERROR，积压时每秒上百条，会淹没真正的下游故障。网关把它映射为 429。
		// 也不逐条记日志，积压本身由落库延迟和消费积压的监控发现（1d、阶段 3）。
		return nil, admissionFailed(ctx, errx.New(codes.ResourceExhausted, errx.ReasonBacklogged,
			"too many orders are waiting to be processed, retry later"))
	default:
		return nil, admissionFailed(ctx, s.internal(ctx, "hold", fmt.Errorf("unexpected outcome %s", res.Outcome), "order_id", orderID))
	}

	expire := accepted.Add(s.paymentTimeout)
	if err := s.publish(ctx, hreq, items, expire); err != nil {
		return s.publishFailed(ctx, orderID, userID, items, accepted, err)
	}
	published = true
	admissionDone(ctx, admissionAdmitted)
	return &orderv1.SubmitOrderResponse{Order: s.creatingProto(orderID, userID, items, accepted)}, nil
}

// hold 执行 Hold；遇到未装载的 SKU 时请库存服务预热，再试一次。返回的错误已是 gRPC 错误。
func (s *Service) hold(ctx context.Context, req stockgate.HoldRequest) (stockgate.HoldResult, error) {
	res, err := s.gateHold(ctx, req)
	if err != nil {
		return res, s.holdUnknown(ctx, req.OrderID, err)
	}
	if res.Outcome != stockgate.NotLoaded {
		return res, nil
	}

	// 一次把这个请求的全部 SKU 都预热：Hold 只报告第一个未装载的，逐个预热要来回好几趟。
	// 未装载时 Hold 在任何写入之前就返回了，重试用同一个订单号是安全的。
	skus := make([]int64, 0, len(req.Items))
	for _, it := range req.Items {
		skus = append(skus, it.SKUID)
	}
	warm, err := s.warmUp(ctx, skus)
	if err != nil {
		s.logger.WarnContext(ctx, "warm up stock gate failed", "sku_ids", skus, "error", err)
		return res, errRetry(errx.ReasonInventoryUnavailable, "inventory unavailable")
	}
	if missing := warm.GetMissing(); len(missing) > 0 {
		return stockgate.HoldResult{Outcome: stockgate.NotLoaded, SKUID: slices.Min(missing)}, nil
	}
	res, err = s.gateHold(ctx, req)
	if err != nil {
		return res, s.holdUnknown(ctx, req.OrderID, err)
	}
	return res, nil
}

// warmUpTimeout 是一次合并后的预热调用的时限。到 inventory 的连接上 WarmUp 另有 1 秒的按跳时限
// （Envoy 边车的路由超时 1 秒，进程里的 inventoryHopTimeout 比它长一截做兜底；Reserve 的是 3 秒，见 main.go），实际生效的是其中最短的那个。
const warmUpTimeout = 3 * time.Second

// warmUp 请库存服务把这些 SKU 装进闸门，同一组 SKU 的并发调用合并成一次。
//
// 为什么要合并：一个新 SKU 开卖的第一瞬间，几百个并发下单同时发现“未装载”。不合并的话
// 每个请求各调一次 WarmUp，库存服务那边同时有几百个查库加写 Redis，连接池一下子要新建
// 几百个连接，个别调用超时，用户拿到 503（端到端测试的偶发失败就是这样来的）。
// 而装载本来就只需要做一次：Load 只在 SKU 不存在时写入，其余的调用都是白做。
//
// 合并后的调用用独立的时限，不用发起者的 ctx：发起者被取消时，搭便车的请求不应该跟着失败。
func (s *Service) warmUp(ctx context.Context, skus []int64) (*inventoryv1.WarmUpResponse, error) {
	sorted := slices.Sorted(slices.Values(skus))
	key := fmt.Sprint(sorted)
	ch := s.warmUps.DoChan(key, func() (any, error) {
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), warmUpTimeout)
		defer cancel()
		return s.inventory.WarmUp(callCtx, &inventoryv1.WarmUpRequest{SkuIds: sorted})
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		// 共享的响应只读：调用方只读 Missing。
		return r.Val.(*inventoryv1.WarmUpResponse), nil
	}
}

// holdUnknown 处理 Hold 返回错误的情况：结果未知，可能扣了也可能没扣。
//
// 向调用方返回 UNAVAILABLE，让它用同一个幂等键重试；在此之前尽力撤销这次可能已经生效的占用。
// 撤销是安全的：这个订单号还没发出任何消息，不可能有消费者认领它，所以它若存在必定是孤儿；
// 撤销只对未认领的占用生效、只删除指向这个订单号的幂等键，占用不存在时什么都不做。
// 撤销是值得的：不撤的话，重试会命中幂等键得到这个永远不会落库的订单（CREATING），
// 等回收任务在宽限期后把它还掉，用户看到的是订单凭空消失。
// 撤销失败（Redis 仍不可用）也无妨，占用留给回收任务。
//
// Hold 的时限是 gateTimeout（300 毫秒），“脚本其实执行成功了、只是回复超时”比原来常见。这条路径对它仍然正确：
// 撤销在 Hold 之后发出，同一个 Redis 上它排在 Hold 后面执行，撤销掉的正是那次已经生效的占用。
// 例外是 Hold 的请求在网络上比撤销晚到 Redis（Hold 的包丢了、重传时撤销已经走另一条连接先到）：撤销看到
// “占用不存在”，随后 Hold 才执行，留下一个孤儿占用，由回收任务在宽限期后归还。这个窗口在加时限之前就有，
// 时限变短只是让它更容易出现。
//
// 闸门的熔断器没有放行时（isBreakerOpen）不属于“结果未知”：Hold 根本没有发给 Redis，没有东西要撤销，
// 直接拒绝，也不逐条记日志（Redis 坏着的时候每个下单请求都会走到这里）。
func (s *Service) holdUnknown(ctx context.Context, orderID int64, holdErr error) error {
	if isBreakerOpen(holdErr) {
		return errGateUnavailable
	}
	s.logger.WarnContext(ctx, "hold outcome unknown", "order_id", orderID, "error", holdErr)
	if _, err := s.abandon(ctx, orderID); err != nil {
		s.logger.WarnContext(ctx, "abandon hold failed, left for reaper", "order_id", orderID, "error", err)
	}
	return errGateUnavailable
}

// abandon 撤销一个受理失败的订单的占用：只对未认领的占用生效，并删除指向它的幂等键，
// 让用户可以用同一个幂等键重新下单。
//
// 用 ReturnIfUnclaimed 而不是 Abort：发消息失败不代表消息没写进去，消费者可能已经 Claim 了它
// 并在落库。Abort 不看认领状态，会把一个即将在 MySQL 里预占成功的订单的占用还掉（Redis 偏大），
// 还删掉幂等键，用户重试就会再下一单。ReturnIfUnclaimed 与 Claim 互斥，两者只有一方得手。
//
// 不跟着请求的 ctx 取消（请求可能正是因为超时才走到这里），时限是闸门调用统一的 gateTimeout。
func (s *Service) abandon(ctx context.Context, orderID int64) (r stockgate.ReapResult, err error) {
	err = s.gateCall(context.WithoutCancel(ctx), func(ctx context.Context) error {
		var err error
		r, err = s.gate.ReturnIfUnclaimed(ctx, orderID)
		return err
	})
	return r, err
}

// publish 在一次 Send 里发出 order.submit 和 order.close.delay。
// 只有 order.submit 失败才算失败：延迟关单的消息丢了由 sweeper 兜底（ADR-0009）。
func (s *Service) publish(ctx context.Context, req stockgate.HoldRequest, items []store.Item, expire time.Time) error {
	submitted := &eventv1.OrderSubmitted{
		OrderId:        req.OrderID,
		UserId:         req.UserID,
		IdempotencyKey: req.IdempotencyKey,
		RequestHash:    req.RequestHash,
		AcceptTime:     timestamppb.New(req.Now),
		ExpireTime:     timestamppb.New(expire),
	}
	for _, it := range items {
		submitted.Items = append(submitted.Items, &eventv1.SubmittedItem{SkuId: it.SKUID, Quantity: it.Quantity})
	}
	key, value, err := eventx.MarshalOrderSubmitted(submitted)
	if err != nil {
		return err
	}
	_, dueValue, err := eventx.MarshalOrderCloseDue(&eventv1.OrderCloseDue{
		OrderId: req.OrderID, UserId: req.UserID, ExpireTime: timestamppb.New(expire),
	})
	if err != nil {
		return err
	}
	errs, err := s.sender.Send(ctx,
		kafkax.Record{Topic: s.submitTopic, Key: key, Value: value},
		kafkax.NewDelayRecord(s.closeTopic, key, dueValue, expire),
	)
	if err == nil {
		return nil
	}
	if len(errs) == 2 && errs[0] == nil {
		s.logger.WarnContext(ctx, "close delay message not sent, sweeper will close the order",
			"order_id", req.OrderID, "error", errs[1])
		return nil
	}
	return err
}

// publishFailed 处理 order.submit 发送失败：撤销占用后返回 UNAVAILABLE。
// 发送失败不代表消息没写进去，所以撤销的结果决定答复：
//   - 撤销成功：消费者之后 Claim 不到，会丢弃那条可能写进去了的消息。本次没有受理。
//   - 已被认领：消息其实到了，消费者已经在落库。按受理成功作答，否则用户重试会命中幂等键，
//     结果一样，却多了一次无谓的失败。
//   - 占用已不存在：消息到了且订单已经走完（或被回收）。查一次当前状态作答。
//   - 撤销失败：占用和幂等键都还在，留给回收任务。用户重试会命中幂等键，宽限期内看到 CREATING。
func (s *Service) publishFailed(ctx context.Context, orderID, userID int64, items []store.Item, accepted time.Time, sendErr error) (*orderv1.SubmitOrderResponse, error) {
	s.logger.WarnContext(ctx, "publish order submission failed", "order_id", orderID, "error", sendErr)
	r, err := s.abandon(ctx, orderID)
	switch {
	case err != nil:
		s.logger.ErrorContext(ctx, "abandon hold failed, left for reaper", "order_id", orderID, "error", err)
	case r == stockgate.ReapClaimed:
		s.logger.InfoContext(ctx, "submission reached consumer despite send error", "order_id", orderID)
		admissionDone(ctx, admissionAdmitted)
		return &orderv1.SubmitOrderResponse{Order: s.creatingProto(orderID, userID, items, accepted)}, nil
	case r == stockgate.ReapMissing:
		lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer cancel()
		if o, err := s.lookup(lctx, orderID, userID); err == nil {
			admissionDone(ctx, admissionAdmitted)
			return &orderv1.SubmitOrderResponse{Order: o}, nil
		}
	}
	return nil, admissionFailed(ctx, errRetry(errx.ReasonOrderQueueUnavailable, "order queue unavailable"))
}

func gateItems(items []store.Item) []stockgate.Item {
	out := make([]stockgate.Item, 0, len(items))
	for _, it := range items {
		out = append(out, stockgate.Item{SKUID: it.SKUID, Quantity: it.Quantity})
	}
	return out
}

// ---------- 落库 ----------

// HandleOrderSubmitted 消费 order.submit 的一条消息：认领占用、写订单行、Reserve。
//
// 按至少一次投递设计，同一条消息处理多少次（包括并发处理）都只有一个订单、一份预占：
// Claim 可重复；订单号是主键，重复插入读出已有的行；Reserve 以订单号幂等；状态更新带前置条件。
//
// 不访问 Redis 与 MySQL 的事务交叉：Claim 在写订单之前、在事务之外完成。
// 返回普通错误表示稍后重试，ErrInvalidEvent、ErrUnrecoverable 表示重试也没用。
func (s *Service) HandleOrderSubmitted(ctx context.Context, m *eventv1.OrderSubmitted) error {
	o, err := orderFromSubmitted(m)
	if err != nil {
		return err
	}
	otelx.SetOrder(ctx, o.OrderID, o.UserID)
	acceptedAt := o.CreatedAt

	// 订单号老到可能已经归档的，不再落库（设计文档 1c 第 6.6 节，不变量“数据只从热到冷”）。
	// 这只会是一条被重放的陈年消息。放它过去的话，热库里会重新插入一个已经归档的订单号：
	// 新行的版本号从 0 开始、状态是创建失败，而读路径先查热库，会用它盖住冷库里真正的订单。
	// 在认领之前丢弃；它的占用早已被回收任务处理。
	if archive.MaybeCold(o.OrderID, s.now()) {
		s.logger.ErrorContext(ctx, "order submission dropped, order id is older than the minimum hot age",
			"order_id", o.OrderID, "accepted_at", idgen.Time(o.OrderID).Format(time.RFC3339), "min_hot_age", archive.MinHotAge.String())
		persistDone(ctx, persistTooOld)
		return nil
	}

	// 1. 认领。占用不存在说明它已被回收（受理方发消息失败后撤销，或回收任务），
	// 或者订单早已走到终态、占用已被归还（重复投递）。两种情况都应当丢弃这条消息。
	var (
		hold stockgate.HoldInfo
		ok   bool
	)
	err = s.gateCall(ctx, func(ctx context.Context) error {
		var err error
		hold, ok, err = s.gate.Claim(ctx, o.OrderID, s.now())
		return err
	})
	if err != nil {
		// 闸门不可用：返回错误，消费者停在这条消息上重试，直到 Redis 恢复（不进死信，见 cmd/order 里消费者的配置）。
		// 不报告给落库健康：闸门有自己的熔断器，受理那一侧会因为它而拒绝。
		return fmt.Errorf("claim hold of order %d: %w", o.OrderID, err)
	}
	if !ok {
		s.logger.InfoContext(ctx, "order submission dropped, hold no longer exists", "order_id", o.OrderID)
		persistDone(ctx, persistHoldMissing)
		return nil
	}
	if hold.UserID != o.UserID {
		return fmt.Errorf("%w: order %d hold belongs to user %d, message says %d",
			ErrInvalidEvent, o.OrderID, hold.UserID, o.UserID)
	}

	// 2. 写订单行（CREATING）。订单号已存在是重复投递，读出来继续推进。
	o.UpdatedAt = s.now()
	s.pin(ctx, o.OrderID)
	err = s.store.InsertOrder(ctx, o)
	if err == nil {
		s.stamp(ctx, o.OrderID)
	}
	switch {
	case errors.Is(err, store.ErrDuplicate):
		existing, gerr := s.store.GetOrder(ctx, o.OrderID)
		switch {
		case gerr == nil:
			o = existing
		case errors.Is(gerr, store.ErrNotFound):
			// 3. 订单号不存在而唯一键冲突：幂等键在 Redis 里过期后的重放。
			return s.dropReplay(ctx, o)
		default:
			s.landingFailed(ctx, o.OrderID)
			return fmt.Errorf("load order %d: %w", o.OrderID, gerr)
		}
	case errors.Is(err, store.ErrWrongShard):
		// orderFromSubmitted 已经按基因挡过，走到这里说明两边的判断不一致。重试也一样，进死信。
		return fmt.Errorf("%w: insert order %d: %w", ErrInvalidEvent, o.OrderID, err)
	case err != nil:
		s.landingFailed(ctx, o.OrderID)
		return fmt.Errorf("insert order %d: %w", o.OrderID, err)
	}

	// 落库时支付时限已经过了（消费积压超过了支付时限）：不再预占，直接置为失败。
	// 继续推进的话订单会进入 PENDING_PAYMENT 然后立刻被关闭，用户没有任何时间支付，
	// 库存却白白被占用一轮、再释放一轮。failed 事件让库存服务留下“已释放”标记并归还闸门里的占用。
	if o.Status == orderv1.OrderStatus_ORDER_STATUS_CREATING {
		if now := s.now(); !o.ExpireAt.After(now) {
			ref := store.OrderRef{OrderID: o.OrderID, UserID: o.UserID}
			if _, err := s.failCreating(ctx, ref, ReasonCreateTimeout, now); err != nil {
				return fmt.Errorf("fail expired order %d: %w", o.OrderID, err)
			}
			s.logger.WarnContext(ctx, "order landed after its payment deadline, failed without reserving",
				"order_id", o.OrderID, "expire_at", o.ExpireAt.Format(time.RFC3339Nano))
			otelx.SetReason(ctx, ReasonCreateTimeout)
			s.persistLanded(ctx, persistExpired, acceptedAt)
			return nil
		}
	}

	// 4. 推进：同步 Reserve。被拒绝时 advance 已把订单置为 FAILED 并写了 failed 事件，
	// 库存服务据此 Release 并归还占用，这里不用再做什么。
	// 落库健康（设计文档 1d 的 6.5）只在这一次真的调用了库存服务时报告：订单已经不是 CREATING 的话
	// advance 什么都不做，说明不了依赖的状态。
	exercised := o.Status == orderv1.OrderStatus_ORDER_STATUS_CREATING
	orderID := o.OrderID
	o, err = s.advance(ctx, o)
	if err != nil {
		// ErrUnrecoverable 是库存服务回答了、但回答的内容有问题，不是它不可用。
		if exercised && !errors.Is(err, ErrUnrecoverable) {
			s.landingFailed(ctx, orderID)
		}
		return err
	}
	if exercised {
		// 库存服务回答了（预占成功或者业务上拒绝），订单库也写进去了。
		s.reportLanding(orderID, true)
	}
	if o.Status == orderv1.OrderStatus_ORDER_STATUS_FAILED {
		s.logger.InfoContext(ctx, "order failed", "order_id", o.OrderID, "reason", o.FailReason)
	}
	switch {
	case !exercised:
		persistDone(ctx, persistAlreadyProcessed)
	case o.Status == orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT:
		s.persistLanded(ctx, persistPendingPayment, acceptedAt)
	case o.Status == orderv1.OrderStatus_ORDER_STATUS_FAILED:
		otelx.SetReason(ctx, o.FailReason)
		s.persistLanded(ctx, persistReserveRejected, acceptedAt)
	default:
		persistDone(ctx, persistAlreadyProcessed)
	}
	return nil
}

// landingFailed 把一次因为依赖不可用而失败的落库告诉落库健康。
// 消费者被强制停止（ctx 被取消）时的失败不算：那是进程在退出，不是依赖坏了。
func (s *Service) landingFailed(ctx context.Context, orderID int64) {
	if errors.Is(ctx.Err(), context.Canceled) {
		return
	}
	s.reportLanding(orderID, false)
}

// dropReplay 处理幂等键在 Redis 过期后的重放：订单库里同一个 (user_id, 幂等键) 早已有订单 A，
// 这次受理的订单 B 不成立。
//
// 先把幂等键补回指向 A，再撤销 B 的占用（B 从未到过库存服务，没有需要释放的预占）。
// 顺序保证每一步失败后重试都能接着做：补回之后 Abort 不会删掉指向 A 的幂等键；
// Abort 之前占用还在，重试能再次 Claim 走到这里；Abort 之后重试 Claim 不到，直接丢弃。
// 这里用 Abort 而不是 ReturnIfUnclaimed：占用刚被本消费者认领，B 不会再落库，认领状态不能挡住归还。
func (s *Service) dropReplay(ctx context.Context, b *store.Order) error {
	a, err := s.store.GetOrderByIdempotencyKey(ctx, b.UserID, b.IdempotencyKey)
	if err != nil {
		// 唯一键冲突却两个键都查不到：冲突的那一行刚好被删了，不应发生。重试一次看看。
		return fmt.Errorf("load order by idempotency key for replayed order %d: %w", b.OrderID, err)
	}
	if err := s.gateCall(ctx, func(ctx context.Context) error {
		return s.gate.PointIdempotency(ctx, a.UserID, a.IdempotencyKey, a.OrderID, a.RequestHash, s.idemTTL)
	}); err != nil {
		return fmt.Errorf("point idempotency key to order %d: %w", a.OrderID, err)
	}
	if err := s.gateCall(ctx, func(ctx context.Context) error {
		_, err := s.gate.Abort(ctx, b.OrderID)
		return err
	}); err != nil {
		return fmt.Errorf("abort hold of replayed order %d: %w", b.OrderID, err)
	}
	s.logger.WarnContext(ctx, "replayed submission dropped, idempotency key already has an order",
		"order_id", b.OrderID, "existing_order_id", a.OrderID, "user_id", b.UserID)
	persistDone(ctx, persistReplayDropped)
	return nil
}

// orderFromSubmitted 校验消息并构造要写入的订单行。
func orderFromSubmitted(m *eventv1.OrderSubmitted) (*store.Order, error) {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: order submitted %d: %s", ErrInvalidEvent, m.GetOrderId(), fmt.Sprintf(format, args...))
	}
	switch {
	case m.GetOrderId() <= 0 || m.GetUserId() <= 0:
		return nil, invalid("order_id and user_id must be positive")
	case idgen.Gene(m.GetOrderId()) != idgen.GeneOf(m.GetUserId()):
		// 订单号由受理方以 user_id 为分片键生成，基因必然一致。不一致说明上游有缺陷，
		// 写进去的话按用户和按订单号会找到两个不同的分片。在认领之前拒绝：占用留给回收任务。
		return nil, invalid("order id gene %d does not match user %d", idgen.Gene(m.GetOrderId()), m.GetUserId())
	case m.GetIdempotencyKey() == "" || len(m.GetIdempotencyKey()) > maxIdempotencyKeyLen:
		return nil, invalid("bad idempotency key")
	case len(m.GetRequestHash()) != 32:
		return nil, invalid("bad request hash")
	case len(m.GetItems()) == 0:
		return nil, invalid("no items")
	case m.GetAcceptTime() == nil || m.GetExpireTime() == nil:
		return nil, invalid("missing accept or expire time")
	}
	items := make([]store.Item, 0, len(m.GetItems()))
	for _, it := range m.GetItems() {
		if it.GetSkuId() <= 0 || it.GetQuantity() <= 0 {
			return nil, invalid("bad item %v", it)
		}
		items = append(items, store.Item{SKUID: it.GetSkuId(), Quantity: it.GetQuantity()})
	}
	sortItems(items)
	return &store.Order{
		OrderID:        m.GetOrderId(),
		UserID:         m.GetUserId(),
		Status:         orderv1.OrderStatus_ORDER_STATUS_CREATING,
		Currency:       placeholderCurrency,
		IdempotencyKey: m.GetIdempotencyKey(),
		RequestHash:    m.GetRequestHash(),
		// 创建时间是受理时间，支付截止时间是受理时定下的那个（用户在受理时就已经看到了它）。
		CreatedAt: m.GetAcceptTime().AsTime().UTC().Truncate(time.Microsecond),
		ExpireAt:  m.GetExpireTime().AsTime().UTC().Truncate(time.Microsecond),
		Items:     items,
	}, nil
}

// ---------- 延迟关单 ----------

// errNotYetExpired 表示到期提醒到了，但按本机时钟订单还没过期（两台机器的时钟有偏差），稍后重试。
var errNotYetExpired = errors.New("order not yet expired by local clock")

// HandleOrderCloseDue 消费 order.close.delay 的一条到期提醒（ADR-0009）。
//
// 提醒只说明“到点了”，订单该不该关由它当时的状态决定：
//   - 订单不存在或还在 CREATING：什么都不做。创建超时由 sweeper 处理；落库时已过支付时限的订单
//     由 HandleOrderSubmitted 直接置为 FAILED，不预占库存。落库时还没到期、但提醒先于它进入
//     PENDING_PAYMENT 到达的订单，由 sweeper 的超时关单兜底。
//   - 已不是 PENDING_PAYMENT（已支付、已取消、已关闭、已失败）：什么都不做。
//   - PENDING_PAYMENT 但按本机时钟还没过期：返回错误，让消费者稍后重试。不能当作不需要关而跳过，
//     否则只能等 sweeper。
//   - 已过期：与 sweeper 相同的条件更新，成功则同事务写 closed 事件。与支付、取消并发时只有一方得手。
func (s *Service) HandleOrderCloseDue(ctx context.Context, m *eventv1.OrderCloseDue) error {
	if m.GetOrderId() <= 0 {
		return fmt.Errorf("%w: order close due without order_id", ErrInvalidEvent)
	}
	otelx.SetOrder(ctx, m.GetOrderId(), m.GetUserId())
	o, err := s.store.GetOrder(ctx, m.GetOrderId())
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load order %d: %w", m.GetOrderId(), err)
	}
	if o.Status != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT {
		return nil
	}
	now := s.now()
	// 以订单行上的 expire_at 为准（与 CloseExpired 的条件一致），不用消息里的副本。
	if !o.ExpireAt.Before(now) {
		return fmt.Errorf("order %d expires at %s, now %s: %w",
			o.OrderID, o.ExpireAt.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), errNotYetExpired)
	}
	ok, err := s.closeExpired(ctx, store.OrderRef{OrderID: o.OrderID, UserID: o.UserID}, now)
	if err != nil {
		return fmt.Errorf("close order %d: %w", o.OrderID, err)
	}
	if ok {
		s.logger.InfoContext(ctx, "order closed for payment timeout", "order_id", o.OrderID, "by", "delay")
	}
	return nil
}

// ---------- Kafka 适配 ----------

// SubmitHandler 把 HandleOrderSubmitted 接到 Kafka 消费者上。timeout <= 0 时取 DefaultEventTimeout。
func (s *Service) SubmitHandler(timeout time.Duration) kafkax.Handler {
	return adapt(s, "order submitted", timeout, eventx.UnmarshalOrderSubmitted, s.HandleOrderSubmitted)
}

// CloseDueHandler 把 HandleOrderCloseDue 接到延迟消费者上。timeout <= 0 时取 DefaultEventTimeout。
func (s *Service) CloseDueHandler(timeout time.Duration) kafkax.Handler {
	return adapt(s, "order close due", timeout, eventx.UnmarshalOrderCloseDue, s.HandleOrderCloseDue)
}

// adapt 构造一个 Kafka handler：解码消息、加上时限，把“重试也没用”的错误标记为 Permanent 送进死信。
func adapt[T any](s *Service, kind string, timeout time.Duration,
	decode func([]byte) (T, error), handle func(context.Context, T) error,
) kafkax.Handler {
	if timeout <= 0 {
		timeout = DefaultEventTimeout
	}
	return func(ctx context.Context, msg kafkax.Message) error {
		m, err := decode(msg.Value)
		if err != nil {
			return kafkax.Permanent(err)
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		err = handle(ctx, m)
		if errors.Is(err, ErrInvalidEvent) || errors.Is(err, ErrUnrecoverable) {
			s.logger.ErrorContext(ctx, "message cannot be processed, sent to dead letter", "kind", kind,
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
			return kafkax.Permanent(err)
		}
		return err
	}
}
