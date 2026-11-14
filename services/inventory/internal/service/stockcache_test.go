package service

import (
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/oppenheimer/pkg/cache"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/pkg/testredis"
	"github.com/palemoky/oppenheimer/services/inventory/internal/store"
)

// cacheEnv 在 env 之外多一个带缓存的 Service。env.svc 仍不缓存，供 seed 的恒等式检查读库。
type cacheEnv struct {
	*env
	cached *Service
}

func newCacheEnv(t *testing.T, ttl time.Duration) *cacheEnv {
	t.Helper()
	e := newEnv(t)
	rdb, prefix := testredis.New(t)
	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	c, err := cache.New(cache.Config{Client: rdb, KeyPrefix: prefix, Logger: logger},
		cache.ProtoCodec[*inventoryv1.GetStockResponse]{})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	return &cacheEnv{
		env:    e,
		cached: New(store.New(e.db), e.gate, logger, WithStockCache(c, ttl)),
	}
}

func (e *cacheEnv) get(t *testing.T, skuID int64) (*inventoryv1.GetStockResponse, error) {
	t.Helper()
	return e.cached.GetStock(t.Context(), &inventoryv1.GetStockRequest{SkuId: skuID})
}

func (e *cacheEnv) setAvailable(t *testing.T, skuID, n int64) {
	t.Helper()
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET available = ? WHERE sku_id = ?`, n, skuID)
}

// insert 不经 seed：这些测试直接改库里的数量，会破坏 seed 的恒等式检查。
func (e *cacheEnv) insert(t *testing.T, skuID, available int64) {
	t.Helper()
	testdb.MustExec(t, e.db,
		`INSERT INTO sku_stock (sku_id, name, unit_price, available) VALUES (?, ?, ?, ?)`,
		skuID, "sku", 100, available)
}

func TestGetStockCacheHit(t *testing.T) {
	const ttl = 300 * time.Millisecond
	e := newCacheEnv(t, ttl)
	e.insert(t, 1, 10)

	first, err := e.get(t, 1)
	if err != nil || first.GetAvailable() != 10 {
		t.Fatalf("first = %v, %v; want available 10", first, err)
	}
	e.setAvailable(t, 1, 99)
	second, err := e.get(t, 1)
	if err != nil || second.GetAvailable() != 10 {
		t.Fatalf("second = %v, %v; want stale available 10", second, err)
	}
	// 抖动上限是 TTL×1.2，等 1.5 倍一定过期。
	time.Sleep(ttl * 3 / 2)
	third, err := e.get(t, 1)
	if err != nil || third.GetAvailable() != 99 {
		t.Fatalf("third = %v, %v; want fresh available 99", third, err)
	}
}

func TestGetStockCacheNotFound(t *testing.T) {
	const ttl = 300 * time.Millisecond
	e := newCacheEnv(t, ttl)

	_, err := e.get(t, 7)
	assertErr(t, err, codes.NotFound, errx.ReasonSKUNotFound)
	e.insert(t, 7, 5)
	_, err = e.get(t, 7)
	assertErr(t, err, codes.NotFound, errx.ReasonSKUNotFound)
	time.Sleep(ttl * 3 / 2)
	got, err := e.get(t, 7)
	if err != nil || got.GetAvailable() != 5 {
		t.Fatalf("after ttl = %v, %v; want available 5", got, err)
	}
}

func TestGetStockCacheDisabled(t *testing.T) {
	e := newCacheEnv(t, 0)
	e.insert(t, 1, 10)
	if got, err := e.get(t, 1); err != nil || got.GetAvailable() != 10 {
		t.Fatalf("first = %v, %v", got, err)
	}
	e.setAvailable(t, 1, 99)
	if got, err := e.get(t, 1); err != nil || got.GetAvailable() != 99 {
		t.Fatalf("second = %v, %v; want 99 (no cache)", got, err)
	}
}

// TestReserveDoesNotInvalidateOrReadCache：Reserve 不失效 GetStock 的缓存，自己也不读它。
//
// 以前“不读缓存”是这样验证的：缓存里还是 10，库里只剩 7，再要 8 必须 INSUFFICIENT_STOCK。
// ADR-0018 之后 Reserve 不再检查可售数，那个断言不再成立。Reserve 仍然从库里读、并且必须读对的
// 是单价和商家（价格快照），所以改用单价验证：库里改了价，缓存里还是旧价，Reserve 返回的是库里的新价。
func TestReserveDoesNotInvalidateOrReadCache(t *testing.T) {
	e := newCacheEnv(t, time.Minute)
	e.seed(t, 1, 100, 10)

	if got, err := e.get(t, 1); err != nil || got.GetAvailable() != 10 {
		t.Fatalf("warm = %v, %v", got, err)
	}
	if _, err := e.cached.Reserve(t.Context(), &inventoryv1.ReserveRequest{
		OrderId: 1, Items: []*inventoryv1.ReserveItem{{SkuId: 1, Quantity: 3}},
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 写时不失效：缓存仍是旧值。
	if got, err := e.get(t, 1); err != nil || got.GetAvailable() != 10 || got.GetReserved() != 0 {
		t.Fatalf("after reserve = %v, %v; want stale (10, 0)", got, err)
	}
	// Reserve 自己读库：改价之后的预占拿到新价，不会被缓存里的旧价骗过。
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET unit_price = 150 WHERE sku_id = 1`)
	resp, err := e.cached.Reserve(t.Context(), &inventoryv1.ReserveRequest{
		OrderId: 2, Items: []*inventoryv1.ReserveItem{{SkuId: 1, Quantity: 2}},
	})
	mustOK(t, err)
	if got := resp.GetItems()[0].GetUnitPrice().GetAmount(); got != 150 {
		t.Errorf("reserve after price change: unit price %d, want 150 from mysql", got)
	}
	if got, err := e.get(t, 1); err != nil || got.GetUnitPrice().GetAmount() != 100 || got.GetAvailable() != 10 {
		t.Fatalf("cache after second reserve = %v, %v; want stale (price 100, available 10)", got, err)
	}
	e.assertStock(t, 1, 5, 5, 0)
}

// TestGetStockCacheSwitchOff：降级开关 cache 关掉之后 GetStock 不读也不写缓存，每次都读库；
// 回到 auto 之后恢复经过缓存。用 NewStockCache 构造，和 main 里一样。
func TestGetStockCacheSwitchOff(t *testing.T) {
	e := newEnv(t)
	rdb, prefix := testredis.New(t)
	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	set := degrade.New("inventory", logger)
	c, err := NewStockCache(rdb, prefix, logger, set)
	if err != nil {
		t.Fatalf("new stock cache: %v", err)
	}
	svc := New(store.New(e.db), e.gate, logger, WithStockCache(c, time.Minute))
	get := func() int64 {
		t.Helper()
		resp, err := svc.GetStock(t.Context(), &inventoryv1.GetStockRequest{SkuId: 1})
		mustOK(t, err)
		return resp.GetAvailable()
	}
	cached := func() bool {
		t.Helper()
		n, err := rdb.Exists(t.Context(), prefix+StockCacheKey(1)).Result()
		mustOK(t, err)
		return n == 1
	}
	// 不经 seed：下面直接改库里的数量，会破坏 seed 的恒等式检查。
	testdb.MustExec(t, e.db, `INSERT INTO sku_stock (sku_id, name, unit_price, available) VALUES (1, 'sku', 100, 10)`)

	// 关着：读库，不写缓存。
	if err := set.Apply(SwitchCache + "=" + degrade.ModeOff); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := get(); got != 10 {
		t.Fatalf("available = %d, want 10", got)
	}
	if cached() {
		t.Fatal("cache written while switched off")
	}

	// 开着：经过缓存，缓存里是 10。
	mustOK(t, set.SetMode(SwitchCache, degrade.ModeAuto))
	if got := get(); got != 10 || !cached() {
		t.Fatalf("available = %d, cached = %v; want 10 and cached", got, cached())
	}

	// 改库之后关掉：不读缓存（拿到库里的 11，不是缓存里的 10），也不覆盖缓存。
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET available = 11 WHERE sku_id = 1`)
	mustOK(t, set.SetMode(SwitchCache, degrade.ModeOff))
	if got := get(); got != 11 {
		t.Fatalf("available = %d, want 11 from mysql while switched off", got)
	}

	// 回到 auto：又经过缓存，读到的是关掉之前缓存的 10，说明关着的那次没有写缓存。
	mustOK(t, set.SetMode(SwitchCache, degrade.ModeAuto))
	if got := get(); got != 10 {
		t.Fatalf("available = %d, want stale 10 from cache after switching back", got)
	}

	// GET /degrade 能看到这个开关；熔断器是关闭的，没有自动降级。
	views := set.Snapshot()
	if len(views) != 1 || views[0].Name != SwitchCache || views[0].Degraded {
		t.Fatalf("snapshot = %+v, want the cache switch, not degraded", views)
	}
}
