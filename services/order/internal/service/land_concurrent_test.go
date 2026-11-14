package service

import (
	"errors"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// 这一组测试确认落库消费者在分区内并发（设计文档 1d 的 2.2、2.3）之后落库路径仍然正确。
//
// 和 land_test.go 一样直接调用 handler，不经过真的 Kafka；用 deliverConcurrently 模拟 kafkax 并发消费时
// 给 handler 的保证，而且只给这些保证，不多给：
//   - 同一个订单号（消息 key）的消息不会被同时处理；
//   - 同一条消息可以被投递多次（提交的是连续完成的前缀，重启、再平衡后已处理的消息会重来）；
//   - 不同订单号之间任意并发、任意先后。
// kafkax 实际上还保证同一个 key 按位点顺序处理，这里的重复投递都是同一条消息的副本，顺序无所谓，
// 所以用“每个 key 一把锁”代替“每个 key 固定一条 lane”：后者只是前者的一种特例，前者能排出更多的交错。

// newRand 返回一个带种子的随机源，并把种子写进测试日志，失败时可以用它复现同样的投递顺序。
func newRand(t *testing.T) *rand.Rand {
	t.Helper()
	seed := uint64(time.Now().UnixNano())
	t.Logf("random seed %d", seed)
	return rand.New(rand.NewPCG(seed, seed))
}

// deliverConcurrently 用 workers 个 goroutine 按 msgs 的顺序取消息交给落库 handler，见文件头的说明。
//
// handler 返回错误时测试失败，不像 kafkax 那样原地重试：这里的依赖都是本地的、不注入故障，
// 并发本身不应当产生任何错误（死锁重试之类的在 store 内部已经处理）。允许重试会把真实的并发缺陷
// （例如偶发的唯一键冲突走错分支）藏在重试后面。
func (e *env) deliverConcurrently(t *testing.T, msgs []kafkax.Message, workers int) {
	t.Helper()
	handler := e.svc.SubmitHandler(0)
	keyLocks := map[string]*sync.Mutex{}
	for _, m := range msgs {
		if keyLocks[string(m.Key)] == nil {
			keyLocks[string(m.Key)] = &sync.Mutex{}
		}
	}
	queue := make(chan kafkax.Message)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for m := range queue {
				mu := keyLocks[string(m.Key)] // 只读，建好之后不再改
				mu.Lock()
				err := handler(t.Context(), m)
				mu.Unlock()
				if err != nil {
					t.Errorf("land order %s: %v", m.Key, err)
				}
			}
		})
	}
	for _, m := range msgs {
		queue <- m
	}
	close(queue)
	wg.Wait()
}

// testPoolSize 是这组测试里每个订单库的连接池上限。
//
// 测试库的连接池默认不设上限，16 路并发、几个测试同时跑、每个测试两个库，连接数会冲破本机 MySQL 的
// max_connections（151，还要和正在运行的服务共用），落库报 Too many connections。生产上每个库的池也是有限的
// （MYSQL_MAX_OPEN_CONNS），并发度大于池子时落库在池上排队，所以这里故意取得比并发度小，顺带覆盖“在池上排队”。
const testPoolSize = 4

// newConcurrentEnv 是 newEnv 加上连接池上限，见 testPoolSize。
func newConcurrentEnv(t *testing.T) *env {
	t.Helper()
	e := newEnv(t)
	for _, db := range e.dbs {
		db.SetMaxOpenConns(testPoolSize)
	}
	return e
}

// submitted 是测试受理的一个订单和下单时的明细（按 sku 升序，与订单行里的顺序一致）。
type submitted struct {
	orderID, userID int64
	key             string
	items           []*orderv1.SubmitOrderItem
}

// total 是按测试商品的单价算出的订单总额。
func (s submitted) total(t *testing.T) int64 {
	t.Helper()
	var sum int64
	for _, it := range s.items {
		switch it.GetSkuId() {
		case skuA:
			sum += priceA * int64(it.GetQuantity())
		case skuB:
			sum += priceB * int64(it.GetQuantity())
		default:
			t.Fatalf("no price for sku %d", it.GetSkuId())
		}
	}
	return sum
}

// qty 是订单里 sku 的件数。
func (s submitted) qty(sku int64) int64 {
	var n int64
	for _, it := range s.items {
		if it.GetSkuId() == sku {
			n += int64(it.GetQuantity())
		}
	}
	return n
}

// submitMany 受理 n 个订单：用户在 users 个之间轮转（覆盖多个 slot、两个库），每单都有 skuA，
// 隔一单再加 skuB，件数 1 到 3 轮转，让所有订单同时抢 skuA。
func (e *env) submitMany(t *testing.T, n, users int) []submitted {
	t.Helper()
	out := make([]submitted, 0, n)
	for i := range n {
		userID := int64(i%users) + 1
		key := "k" + strconv.Itoa(i)
		it := items(skuA, int64(i%3+1))
		if i%2 == 0 {
			it = items(skuA, int64(i%3+1), skuB, int64(i%2+1))
		}
		o := e.submit(t, userID, key, it)
		out = append(out, submitted{orderID: o.GetOrderId(), userID: userID, key: key, items: it})
	}
	return out
}

// messages 返回 orders 的 order.submit 消息，每个订单一条。
func (e *env) messages(t *testing.T, orders []submitted) []kafkax.Message {
	t.Helper()
	msgs := make([]kafkax.Message, 0, len(orders))
	for _, o := range orders {
		msgs = append(msgs, e.submitMessage(t, o.orderID))
	}
	return msgs
}

// redeliveries 把每条消息复制 2 到 3 份并整体打乱，模拟并发消费下重启、再平衡带来的重复投递。
func redeliveries(r *rand.Rand, msgs []kafkax.Message) []kafkax.Message {
	var out []kafkax.Message
	for _, m := range msgs {
		for range 2 + r.IntN(2) {
			out = append(out, m)
		}
	}
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// wantAllPending 断言 orders 全部落到待支付、金额和明细正确、各只有一行，库存和闸门的账与它们一致：
// 只有它们在预占 skuA、skuB（测试开始时两边各 1000 件）。落库成功不写事件，outbox 必须是空的。
func (e *env) wantAllPending(t *testing.T, orders []submitted) {
	t.Helper()
	var wantA, wantB int64
	itemRows := 0
	slots := map[int]bool{}
	dbs := map[int]bool{}
	for _, o := range orders {
		stored := e.load(t, o.orderID)
		wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
		if stored.UserID != o.userID || stored.IdempotencyKey != o.key {
			t.Errorf("order %d: user %d key %q, want %d %q", o.orderID, stored.UserID, stored.IdempotencyKey, o.userID, o.key)
		}
		if want := o.total(t); stored.TotalAmount != want || stored.Currency != "CNY" {
			t.Errorf("order %d: total %d %s, want %d CNY", o.orderID, stored.TotalAmount, stored.Currency, want)
		}
		if len(stored.Items) != len(o.items) {
			t.Fatalf("order %d: %d items, want %d", o.orderID, len(stored.Items), len(o.items))
		}
		for i, it := range stored.Items {
			want := o.items[i]
			price := int64(priceA)
			if want.GetSkuId() == skuB {
				price = priceB
			}
			if it.SKUID != want.GetSkuId() || it.Quantity != want.GetQuantity() || it.UnitPrice != price {
				t.Errorf("order %d item %d = %+v, want sku %d qty %d price %d",
					o.orderID, i, it, want.GetSkuId(), want.GetQuantity(), price)
			}
		}
		if h, ok := e.hold(t, o.orderID); !ok || !h.Claimed {
			t.Errorf("order %d: hold = %+v, %v; want claimed", o.orderID, h, ok)
		}
		wantA += o.qty(skuA)
		wantB += o.qty(skuB)
		itemRows += len(o.items)
		slot := e.router.ForOrder(o.orderID)
		slots[slot] = true
		dbs[e.router.DB(slot)] = true
	}
	t.Logf("%d orders over %d slots in %d dbs", len(orders), len(slots), len(dbs))
	if n := e.orderRows(t); n != len(orders) {
		t.Errorf("orders = %d, want %d", n, len(orders))
	}
	if n := e.count(t, "order_items", ""); n != itemRows {
		t.Errorf("order_items = %d, want %d", n, itemRows)
	}
	if n := e.inv.appliedCount("Reserve"); n != len(orders) {
		t.Errorf("reserve applied %d times, want %d", n, len(orders))
	}
	if n := e.outboxRows(t); n != 0 {
		t.Errorf("outbox = %d rows, want 0 (landing writes no event)", n)
	}
	wantStock(t, e.inv, skuA, 1000-wantA, wantA, 0)
	wantStock(t, e.inv, skuB, 1000-wantB, wantB, 0)
	e.wantGate(t, skuA, 1000-wantA, wantA)
	e.wantGate(t, skuB, 1000-wantB, wantB)
	e.wantBalanced(t, skuA)
	e.wantBalanced(t, skuB)
}

// cancelAllAndSettle 取消 orders，确认每个订单恰好一个 cancelled 事件，库存服务消费之后两边都回到 1000 件。
func (e *env) cancelAllAndSettle(t *testing.T, orders []submitted) {
	t.Helper()
	for _, o := range orders {
		if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.orderID, o.userID)); err != nil {
			t.Fatalf("cancel order %d: %v", o.orderID, err)
		}
	}
	e.wantEventCounts(t, orders, "cancelled")
	e.consumeEvents(t)
	for _, sku := range []int64{skuA, skuB} {
		wantStock(t, e.inv, sku, 1000, 0, 0)
		e.wantGate(t, sku, 1000, 0)
	}
}

// eventsByOrder 按订单返回 outbox 里的事件种类（种类名同 eventKinds），每个订单内按写入顺序。
// 一次读完整个 outbox：逐个订单调用 eventKinds 会把 outbox 读几百遍。
func (e *env) eventsByOrder(t *testing.T) map[int64][]string {
	t.Helper()
	out := map[int64][]string{}
	for _, ev := range e.allEvents(t) {
		kind := "unknown"
		switch ev.GetBody().(type) {
		case *eventv1.OrderEvent_Paid:
			kind = "paid"
		case *eventv1.OrderEvent_Cancelled:
			kind = "cancelled"
		case *eventv1.OrderEvent_Closed:
			kind = "closed"
		case *eventv1.OrderEvent_Failed:
			kind = "failed"
		case *eventv1.OrderEvent_PaymentResultHandled:
			kind = "handled"
		}
		out[ev.GetOrderId()] = append(out[ev.GetOrderId()], kind)
	}
	return out
}

// wantEventCounts 断言 outbox 里 orders 的每个订单恰好一个 kind 事件，并且没有别的订单的事件。
func (e *env) wantEventCounts(t *testing.T, orders []submitted, kind string) {
	t.Helper()
	got := e.eventsByOrder(t)
	for _, o := range orders {
		if evs := got[o.orderID]; !slices.Equal(evs, []string{kind}) {
			t.Errorf("order %d events = %v, want [%s]", o.orderID, evs, kind)
		}
		delete(got, o.orderID)
	}
	for id, evs := range got {
		t.Errorf("unexpected events %v for order %d", evs, id)
	}
}

// 200 个订单、16 路并发落库：40 个用户分布在多个 slot 和两个库上，所有订单同时抢 skuA。
// 结果与串行落库相同：全部待支付、金额明细正确、两边的账平，每个订单之后恰好能产生一个事件。
func TestLandConcurrentDistinctOrders(t *testing.T) {
	t.Parallel()
	e := newConcurrentEnv(t)
	r := newRand(t)
	orders := e.submitMany(t, 200, 40)
	msgs := e.messages(t, orders)
	r.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })

	e.deliverConcurrently(t, msgs, 16)
	e.wantAllPending(t, orders)
	// 落库成功不写事件；取消之后每个订单恰好一个 cancelled 事件，消费完两边回到初始值。
	e.cancelAllAndSettle(t, orders)
}

// 并发落库时一部分订单的预占被 inventory 以“库存不足”拒绝：被拒绝的恰好走创建失败的路径，
// 成功和失败的订单数加起来不多不少，失败订单的占用在消费 failed 事件后归还。
//
// 这是 order 一侧的防御用例，不代表系统现在的行为。构造方式是让假的 inventory 在可售数不够时拒绝
// （闸门放进来 100 单，假 inventory 只有 30 件）。真实的 inventory 自 ADR-0018 起不再在预占时检查
// 可售数：闸门有货而 MySQL 没货时预占照常成功，由后台任务事后发现。所以真实系统里这 100 单都会成功，
// “MySQL 兜底闸门、不超卖”不是这个测试能证明的性质。order 仍然要正确处理这个错误码，
// 因为它是契约的一部分，inventory 以后的版本（例如降级到数据库扣减时）可能再次返回它。
func TestLandConcurrentScarceStock(t *testing.T) {
	t.Parallel()
	const (
		sku      = 10
		accepted = 100
		stock    = 30
	)
	e := newConcurrentEnv(t)
	r := newRand(t)
	e.inv.addSKU(sku, 7, accepted)
	var orders []submitted
	for i := range accepted {
		userID := int64(i%25) + 1
		key := "s" + strconv.Itoa(i)
		o := e.submit(t, userID, key, items(sku, 1))
		orders = append(orders, submitted{orderID: o.GetOrderId(), userID: userID, key: key})
	}
	e.wantGate(t, sku, 0, accepted)
	e.inv.setAvailable(sku, stock)

	msgs := e.messages(t, orders)
	r.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
	e.deliverConcurrently(t, msgs, 16)

	var pending, failed []submitted
	for _, o := range orders {
		stored := e.load(t, o.orderID)
		switch stored.Status {
		case orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT:
			if stored.TotalAmount != 7 {
				t.Errorf("order %d total = %d, want 7", o.orderID, stored.TotalAmount)
			}
			pending = append(pending, o)
		case orderv1.OrderStatus_ORDER_STATUS_FAILED:
			if stored.FailReason != errx.ReasonInsufficientStock {
				t.Errorf("order %d fail_reason = %q", o.orderID, stored.FailReason)
			}
			failed = append(failed, o)
		default:
			t.Errorf("order %d status = %s, want pending payment or failed", o.orderID, stored.Status)
		}
	}
	if len(pending) != stock || len(failed) != accepted-stock {
		t.Fatalf("pending %d, failed %d; want %d, %d", len(pending), len(failed), stock, accepted-stock)
	}
	wantStock(t, e.inv, sku, 0, stock, 0)
	e.wantEventCounts(t, failed, "failed")

	e.consumeEvents(t)
	for _, o := range failed {
		if _, ok := e.hold(t, o.orderID); ok {
			t.Errorf("failed order %d still has a hold", o.orderID)
		}
	}
	for _, o := range pending {
		if h, ok := e.hold(t, o.orderID); !ok || !h.Claimed {
			t.Errorf("pending order %d hold = %+v, %v; want claimed", o.orderID, h, ok)
		}
	}
	// 闸门里失败的 70 件回到可放行，成功的 30 件仍是占用；库存那边 30 件预占、0 件可售（没有超卖）。
	e.wantGate(t, sku, accepted-stock, stock)
	wantStock(t, e.inv, sku, 0, stock, 0)
}

// 每条消息投递 2 到 3 次、整体打乱后并发处理（同一订单的多次投递之间串行）：
// 结果与每条只投递一次完全相同。之后取消全部订单、库存服务归还占用，再把所有消息重放一遍
// （并发消费下重启后的常态）：迟到的消息全部被丢弃，什么都不变。
func TestLandConcurrentRedeliveredShuffled(t *testing.T) {
	t.Parallel()
	e := newConcurrentEnv(t)
	r := newRand(t)
	orders := e.submitMany(t, 120, 30)
	msgs := e.messages(t, orders)

	e.deliverConcurrently(t, redeliveries(r, msgs), 16)
	e.wantAllPending(t, orders)
	if n := e.inv.callCount("Reserve"); n != len(orders) {
		// 重复投递读到已是待支付的订单行就不再调 Reserve。
		t.Errorf("reserve called %d times, want %d", n, len(orders))
	}

	e.cancelAllAndSettle(t, orders)
	e.deliverConcurrently(t, redeliveries(r, msgs), 16)
	for _, o := range orders {
		wantStatus(t, e.load(t, o.orderID), orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	}
	e.wantEventCounts(t, orders, "cancelled")
	if n := e.orderRows(t); n != len(orders) {
		t.Errorf("orders = %d, want %d", n, len(orders))
	}
	if n := e.inv.callCount("Reserve"); n != len(orders) {
		t.Errorf("reserve called %d times after replay, want %d", n, len(orders))
	}
	for _, sku := range []int64{skuA, skuB} {
		wantStock(t, e.inv, sku, 1000, 0, 0)
		e.wantGate(t, sku, 1000, 0)
	}
}

// 同一个用户的多个订单（同一个 slot、同一张表）并发落库，互不影响。
func TestLandConcurrentSameUser(t *testing.T) {
	t.Parallel()
	e := newConcurrentEnv(t)
	r := newRand(t)
	orders := e.submitMany(t, 40, 1)
	msgs := e.messages(t, orders)
	r.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
	e.deliverConcurrently(t, msgs, 16)
	e.wantAllPending(t, orders)
}

// 同一个用户、同一个幂等键的两个订单并发落库：幂等键在 Redis 里过期后用户重放了请求（TestLandReplayAfterIdempotencyExpiry
// 的构造），只是这次两个订单都还没落库，两条消息被并发处理。订单号不同，kafkax 允许它们并发。
//
// 订单库的唯一键 (user_id, idempotency_key) 只让一个插入成功，另一个走 dropReplay：丢弃、归还占用、
// 把幂等键指向赢的那个。谁赢不确定，所以按“恰好一个”断言。20 对一起落库，加上重复投递。
func TestLandConcurrentSameIdempotencyKey(t *testing.T) {
	t.Parallel()
	const pairs = 20
	e := newConcurrentEnv(t)
	r := newRand(t)
	first := make([]submitted, pairs)
	for i := range pairs {
		userID := int64(i%5) + 1
		key := "dup" + strconv.Itoa(i)
		// 带上 skuB：wantAllPending 要核对两个 SKU 的闸门，没下过单的 SKU 没有装载进闸门。
		it := items(skuA, 2, skuB, 1)
		o := e.submit(t, userID, key, it)
		first[i] = submitted{orderID: o.GetOrderId(), userID: userID, key: key, items: it}
	}
	e.expireIdempotencyKeys(t)
	second := make([]submitted, pairs)
	for i, a := range first {
		o := e.submit(t, a.userID, a.key, a.items)
		if o.GetOrderId() == a.orderID {
			t.Fatal("replay after expiry should be accepted as a new order by the gate")
		}
		second[i] = submitted{orderID: o.GetOrderId(), userID: a.userID, key: a.key, items: a.items}
	}
	e.wantGate(t, skuA, 1000-4*pairs, 4*pairs)

	// 第一轮：每一对的两条消息由两个 goroutine 在同一时刻放行，尽量让两个插入真的重叠
	// （后到的插入在先到的那个未提交的唯一键上等锁，先到的提交后它才拿到唯一键冲突）。
	// 打乱的投递里一对消息多半一前一后处理，走的是串行时就有的重放路径，测不到这种重叠。
	handler := e.svc.SubmitHandler(0)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range pairs {
		for _, m := range []kafkax.Message{e.submitMessage(t, first[i].orderID), e.submitMessage(t, second[i].orderID)} {
			wg.Go(func() {
				<-start
				if err := handler(t.Context(), m); err != nil {
					t.Errorf("land order %s: %v", m.Key, err)
				}
			})
		}
	}
	close(start)
	wg.Wait()
	// 第二轮：全部消息重复投递、打乱，结果不变。
	msgs := append(e.messages(t, first), e.messages(t, second)...)
	e.deliverConcurrently(t, redeliveries(r, msgs), 16)

	winners := make([]submitted, 0, pairs)
	for i := range pairs {
		var won, lost []submitted
		for _, o := range []submitted{first[i], second[i]} {
			_, err := e.store.GetOrder(t.Context(), o.orderID)
			switch {
			case err == nil:
				won = append(won, o)
			case errors.Is(err, store.ErrNotFound):
				lost = append(lost, o)
			default:
				t.Fatalf("load order %d: %v", o.orderID, err)
			}
		}
		if len(won) != 1 || len(lost) != 1 {
			t.Fatalf("pair %d (orders %d, %d): %d landed, want exactly 1", i, first[i].orderID, second[i].orderID, len(won))
		}
		winners = append(winners, won[0])
		if _, ok := e.hold(t, lost[0].orderID); ok {
			t.Errorf("dropped order %d still has a hold", lost[0].orderID)
		}
		_, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: lost[0].orderID, UserId: lost[0].userID})
		wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
		if n := e.count(t, "orders", "user_id = ? AND idempotency_key = ?", first[i].userID, first[i].key); n != 1 {
			t.Errorf("pair %d: %d rows for (user, key), want 1", i, n)
		}
	}
	// 赢的订单和普通的并发落库结果一样；输的订单从未到过库存服务。
	e.wantAllPending(t, winners)
	// 幂等键已指向赢的那个：再重放直接得到它，不再扣减。
	for _, w := range winners {
		again := e.submit(t, w.userID, w.key, w.items)
		if again.GetOrderId() != w.orderID || again.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT {
			t.Errorf("replay of (%d, %q) = order %d %s, want %d", w.userID, w.key, again.GetOrderId(), again.GetStatus(), w.orderID)
		}
	}
	e.wantGate(t, skuA, 1000-2*pairs, 2*pairs)
}

// 落库的同时取消一半订单：每个订单最后要么待支付（取消请求都赶在落库之前，答“不可取消”），要么已取消
// （某次取消请求成功），没有中间状态；取消成功的恰好一个 cancelled 事件，库存服务消费之后两边的账平。
func TestLandConcurrentWithCancel(t *testing.T) {
	t.Parallel()
	e := newConcurrentEnv(t)
	r := newRand(t)
	orders := e.submitMany(t, 100, 20)
	msgs := e.messages(t, orders)
	r.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })

	// 每个被取消的订单发 3 次取消请求，各自随机等 0 到 3 毫秒，让一部分落在落库之前、一部分在之后。
	// 随机数在主 goroutine 里取好：rand.Rand 不是并发安全的。
	cancelled := make([]bool, len(orders))
	delays := make([][3]time.Duration, len(orders))
	for i := range orders {
		for j := range delays[i] {
			delays[i][j] = time.Duration(r.IntN(3000)) * time.Microsecond
		}
	}
	var wg sync.WaitGroup
	for i, o := range orders {
		if i%2 != 0 {
			continue
		}
		wg.Go(func() {
			for _, d := range delays[i] {
				time.Sleep(d)
				_, err := e.svc.CancelOrder(t.Context(), cancelReq(o.orderID, o.userID))
				if err == nil {
					cancelled[i] = true
					return
				}
				if errx.Code(err) != codes.FailedPrecondition || errx.Reason(err) != errx.ReasonOrderNotCancellable {
					t.Errorf("cancel order %d: %v, want nil or not cancellable", o.orderID, err)
					return
				}
			}
		})
	}
	wg.Go(func() { e.deliverConcurrently(t, msgs, 16) })
	wg.Wait()

	var nCancelled, nRacedPending int
	var wantA, wantB int64
	events := e.eventsByOrder(t)
	for i, o := range orders {
		stored := e.load(t, o.orderID)
		switch {
		case cancelled[i]:
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
			if evs := events[o.orderID]; !slices.Equal(evs, []string{"cancelled"}) {
				t.Errorf("cancelled order %d events = %v, want [cancelled]", o.orderID, evs)
			}
			nCancelled++
		default:
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
			if evs := events[o.orderID]; len(evs) != 0 {
				t.Errorf("pending order %d events = %v, want none", o.orderID, evs)
			}
			wantA += o.qty(skuA)
			wantB += o.qty(skuB)
			if i%2 == 0 {
				nRacedPending++
			}
		}
	}
	// 两种结果各有多少取决于调度，只记下来：反复跑时两边都应当出现，否则这个测试没有测到竞争。
	t.Logf("cancel won %d, cancel lost to landing %d", nCancelled, nRacedPending)

	e.consumeEvents(t)
	wantStock(t, e.inv, skuA, 1000-wantA, wantA, 0)
	wantStock(t, e.inv, skuB, 1000-wantB, wantB, 0)
	e.wantGate(t, skuA, 1000-wantA, wantA)
	e.wantGate(t, skuB, 1000-wantB, wantB)
	e.wantBalanced(t, skuA)
	e.wantBalanced(t, skuB)
}
