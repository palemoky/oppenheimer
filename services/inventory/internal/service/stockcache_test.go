package service

import (
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/Oppenheimer/pkg/cache"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
	"github.com/palemoky/Oppenheimer/pkg/testredis"
	"github.com/palemoky/Oppenheimer/services/inventory/internal/store"
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
	// Reserve 自己读库：库存只剩 7，再要 8 必须失败，不会被缓存里的 10 骗过。
	_, err := e.cached.Reserve(t.Context(), &inventoryv1.ReserveRequest{
		OrderId: 2, Items: []*inventoryv1.ReserveItem{{SkuId: 1, Quantity: 8}},
	})
	assertErr(t, err, codes.FailedPrecondition, errx.ReasonInsufficientStock)
	e.assertStock(t, 1, 7, 3, 0)
}
