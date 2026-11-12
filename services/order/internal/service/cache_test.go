package service

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"

	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/idgen"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

// cachedService 返回一个与 e.svc 共用依赖、但 GetOrder 走查询缓存的 Service。
// st 为空时用 e.store；传入一个读不了的 store 可以证明某次查询没有读库。
func (e *env) cachedService(t *testing.T, st *store.Store) *Service {
	t.Helper()
	c, err := NewOrderCache(e.rdb, e.prefix, e.svc.logger)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	if st == nil {
		st = e.store
	}
	return New(Deps{
		Store: st, Gate: e.gate, Sender: e.sender, Inventory: e.inv, IDs: ids,
		Logger: e.svc.logger, OrderCache: c,
	}, Config{
		Clock:            e.clock.Now,
		OrderEventsTopic: testEventsTopic,
		SubmitTopic:      testSubmitTopic,
		CloseDelayTopic:  testCloseTopic,
		Relay:            e.relay,
	})
}

// brokenStore 返回一个任何查询都会失败的 store：连到一个不监听的端口。
func brokenStore(t *testing.T) *store.Store {
	t.Helper()
	db, err := sql.Open("mysql", "root:x@tcp(127.0.0.1:1)/none?timeout=200ms")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return store.New(db)
}

func getOrder(t *testing.T, svc *Service, orderID, userID int64) (*orderv1.Order, error) {
	t.Helper()
	resp, err := svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: orderID, UserId: userID})
	return resp.GetOrder(), err
}

func mustGetStatus(t *testing.T, svc *Service, orderID, userID int64, want orderv1.OrderStatus) *orderv1.Order {
	t.Helper()
	o, err := getOrder(t, svc, orderID, userID)
	if err != nil {
		t.Fatalf("GetOrder(%d): %v", orderID, err)
	}
	if o.GetStatus() != want {
		t.Fatalf("GetOrder(%d) status = %s, want %s", orderID, o.GetStatus(), want)
	}
	return o
}

// cacheEntry 读订单在缓存里的原始值，没有时 ok 为 false。
func (e *env) cacheEntry(t *testing.T, orderID int64) (data []byte, ok bool) {
	t.Helper()
	data, err := e.rdb.Get(t.Context(), e.prefix+orderCacheKey(orderID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return data, true
}

// 每个状态都必须明确归入“缓存”或“不缓存”。枚举新增状态时这里失败，逼着决定它能不能缓存。
func TestCacheableCoversEveryStatus(t *testing.T) {
	t.Parallel()
	want := map[orderv1.OrderStatus]bool{
		orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED:     false,
		orderv1.OrderStatus_ORDER_STATUS_CREATING:        false,
		orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT: false,
		orderv1.OrderStatus_ORDER_STATUS_PAID:            true,
		orderv1.OrderStatus_ORDER_STATUS_CANCELLED:       true,
		orderv1.OrderStatus_ORDER_STATUS_CLOSED:          true,
		orderv1.OrderStatus_ORDER_STATUS_FAILED:          true,
	}
	for v, name := range orderv1.OrderStatus_name {
		st := orderv1.OrderStatus(v)
		w, ok := want[st]
		if !ok {
			t.Errorf("status %s is not classified: decide whether it is cacheable (terminal and never changes)", name)
			continue
		}
		if got := cacheable(st); got != w {
			t.Errorf("cacheable(%s) = %v, want %v", name, got, w)
		}
	}
	for st := range want {
		if _, ok := orderv1.OrderStatus_name[int32(st)]; !ok {
			t.Errorf("test lists %d which is not in the enum", st)
		}
	}
}

// 非终态的订单不进缓存，状态转换之后立即查询看到新状态；终态才写入缓存。
func TestGetOrderCacheSkipsNonTerminal(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := e.cachedService(t, nil)
	o := e.mustPending(t, 1, "k")

	mustGetStatus(t, svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	if _, ok := e.cacheEntry(t, o.GetOrderId()); ok {
		t.Fatal("pending order was cached")
	}

	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	mustGetStatus(t, svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	if _, ok := e.cacheEntry(t, o.GetOrderId()); !ok {
		t.Fatal("cancelled order was not cached")
	}
}

// 终态订单第二次查询不读库：改掉库里的行之后读到的仍是缓存的值；读不了库的实例也能读到。
func TestGetOrderCacheServesTerminalFromCache(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := e.cachedService(t, nil)
	o := e.mustPending(t, 1, "k")
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	first := mustGetStatus(t, svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)

	if _, err := e.db.ExecContext(t.Context(), "UPDATE orders SET total_amount = 1 WHERE order_id = ?", o.GetOrderId()); err != nil {
		t.Fatal(err)
	}
	second := mustGetStatus(t, svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	if second.GetTotal().GetAmount() != first.GetTotal().GetAmount() {
		t.Errorf("second query total = %d, want cached %d", second.GetTotal().GetAmount(), first.GetTotal().GetAmount())
	}
	// 对照：不走缓存的实例读到的是改过的行，说明上面确实没有读库。
	direct := mustGetStatus(t, e.svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	if direct.GetTotal().GetAmount() != 1 {
		t.Fatalf("direct query total = %d, want 1", direct.GetTotal().GetAmount())
	}

	mustGetStatus(t, e.cachedService(t, brokenStore(t)), o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
}

// 已受理未落库的订单不会被写成空值：先查一次（CREATING），再落库，再查能看到订单。
func TestGetOrderCacheDoesNotNullAcceptedOrder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := e.cachedService(t, nil)
	sub := e.submit(t, 1, "k", items(skuA, 1))

	mustGetStatus(t, svc, sub.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_CREATING)
	if _, ok := e.cacheEntry(t, sub.GetOrderId()); ok {
		t.Fatal("accepted but not landed order left a cache entry")
	}
	// 别的用户查不到，也不会因此写下空值。
	_, err := getOrder(t, svc, sub.GetOrderId(), 2)
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	if _, ok := e.cacheEntry(t, sub.GetOrderId()); ok {
		t.Fatal("query by another user left a cache entry")
	}

	e.mustLand(t, sub.GetOrderId())
	mustGetStatus(t, svc, sub.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
}

// 缓存的内容与请求方无关：无论谁先查，别人的订单都是 NOT_FOUND，本人都能查到。
func TestGetOrderCacheChecksOwner(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := e.cachedService(t, nil)
	terminal := func(key string) int64 {
		o := e.mustPending(t, 1, key)
		if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
			t.Fatal(err)
		}
		return o.GetOrderId()
	}

	// 本人先查（写入缓存），别人再查命中缓存。
	a := terminal("a")
	mustGetStatus(t, svc, a, 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	_, err := getOrder(t, svc, a, 2)
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)

	// 别人先查（未命中，回源后写入的是真实订单而不是空值），本人再查能看到。
	b := terminal("b")
	_, err = getOrder(t, svc, b, 2)
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	if data, ok := e.cacheEntry(t, b); !ok || len(data) <= 1 {
		t.Fatalf("cache entry after another user's query = %v, %v; want the order", data, ok)
	}
	mustGetStatus(t, svc, b, 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
}

// 不存在的订单号写空值，第二次查询命中空值、不读库。
func TestGetOrderCacheNull(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := e.cachedService(t, nil)
	const missing = 12345

	_, err := getOrder(t, svc, missing, 1)
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	if data, ok := e.cacheEntry(t, missing); !ok || len(data) != 1 {
		t.Fatalf("cache entry = %v, %v; want a null marker", data, ok)
	}
	ttl, err := e.rdb.PTTL(t.Context(), e.prefix+orderCacheKey(missing)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > DefaultOrderCacheNullTTL*2 {
		t.Errorf("null entry ttl = %v, want about %v", ttl, DefaultOrderCacheNullTTL)
	}

	// 读不了库的实例仍然答 NOT_FOUND（而不是 Internal），说明命中了空值。
	_, err = getOrder(t, e.cachedService(t, brokenStore(t)), missing, 1)
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
}

// 读库失败不能被缓存成“不存在”。
func TestGetOrderCacheDoesNotCacheErrors(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	_, err := getOrder(t, e.cachedService(t, brokenStore(t)), 777, 1)
	if status := codes.Internal; errx.Code(err) != status {
		t.Fatalf("GetOrder with broken store = %v, want %s", err, status)
	}
	if _, ok := e.cacheEntry(t, 777); ok {
		t.Fatal("database error was cached")
	}
}

// denyGuard 总是不放行，并记下被问了几次。
type denyGuard struct{ calls atomic.Int64 }

func (g *denyGuard) Allow(context.Context) (bool, error) {
	g.calls.Add(1)
	return false, nil
}

// waitCalls 等到 Guard 被问过至少 n 次。
func (g *denyGuard) waitCalls(t *testing.T, n int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for g.calls.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("guard called %d times, want at least %d", g.calls.Load(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type component interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// runUntilGuarded 启动 c，等 Guard 拒绝了几轮之后停下。
func runUntilGuarded(t *testing.T, c component, g *denyGuard) {
	t.Helper()
	errc := make(chan error, 1)
	go func() { errc <- c.Start(t.Context()) }()
	g.waitCalls(t, 3)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := c.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestSweeperGuardDenies(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	e.clock.Advance(DefaultPaymentTimeout + time.Second)
	g := &denyGuard{}
	sw, err := NewSweeper(e.svc, SweeperConfig{Interval: 5 * time.Millisecond, Guard: g})
	if err != nil {
		t.Fatal(err)
	}
	runUntilGuarded(t, sw, g)
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
}

func TestReaperGuardDenies(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.submit(t, 1, "k", items(skuA, 1))
	e.clock.Advance(DefaultReapGrace + time.Second)
	g := &denyGuard{}
	r, err := NewReaper(e.svc, ReaperConfig{Interval: 5 * time.Millisecond, Guard: g})
	if err != nil {
		t.Fatal(err)
	}
	runUntilGuarded(t, r, g)
	if _, ok := e.hold(t, o.GetOrderId()); !ok {
		t.Fatal("orphan hold was reaped although the guard denied")
	}
}
