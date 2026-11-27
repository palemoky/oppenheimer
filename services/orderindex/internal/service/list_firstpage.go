//go:build faults

package service

import (
	"context"
	"sync"
	"time"

	"github.com/palemoky/oppenheimer/pkg/fault"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
)

var firstPageReuse = fault.Switch("f05")

// 商家订单列表首页的短缓存。
//
// 商家后台的订单页每隔几秒自动刷新一次，同一个商家常常开着好几个页签，同一秒里同一页被查很多次，
// 而且几乎都是不带游标的第一页。索引本来就比订单库落后几百毫秒到几秒（设计文档 1c 4.10），
// 第一页在一秒之内复用，商家看不出区别，索引库上的范围扫描少掉一大半。
// 翻页（带 before_order_id）的请求分散，复用不上，照旧直接查库。
const (
	// firstPageTTL 是一页结果复用的时长，也是清理过期页的间隔：过期的页最多多留一个 TTL。
	firstPageTTL = time.Second
	// firstPageMaxRows 是所有商家缓存的行数合计的上限。一行几百字节，两万行是几兆。
	firstPageMaxRows = 20_000
)

var firstPages = newPageCache(firstPageTTL, firstPageMaxRows, time.Now)

// listRows 从索引表读一页。
func (s *Service) listRows(ctx context.Context, q store.ListQuery) ([]store.Row, error) {
	if !firstPageReuse.On() || q.BeforeOrderID != 0 {
		return s.store.List(ctx, q)
	}
	return firstPages.get(ctx, q, s.store.List)
}

// pageCache 按商家缓存第一页。mu 保护商家表和行数合计；每个商家自己的页由它的 mu 保护，
// 这样不同商家的查库互不等待。
type pageCache struct {
	ttl     time.Duration
	maxRows int
	now     func() time.Time

	mu        sync.Mutex
	merchants map[int64]*merchantPages
	rows      int // 所有商家缓存的行数合计
	sweptAt   time.Time
}

// merchantPages 是一个商家缓存的页，按筛选条件和页大小区分。
type merchantPages struct {
	id    int64
	mu    sync.Mutex
	pages map[pageKey]cachedPage
}

type pageKey struct {
	status int32
	limit  int
}

type cachedPage struct {
	rows []store.Row
	at   time.Time
}

func newPageCache(ttl time.Duration, maxRows int, now func() time.Time) *pageCache {
	return &pageCache{ttl: ttl, maxRows: maxRows, now: now, merchants: map[int64]*merchantPages{}}
}

// get 返回 q 那一页：缓存里有没过期的就直接用，否则用 load 查一次再记下来。
// 返回的切片是共享的，调用方只读。
func (c *pageCache) get(ctx context.Context, q store.ListQuery,
	load func(context.Context, store.ListQuery) ([]store.Row, error),
) ([]store.Row, error) {
	// 同一个商家同一时刻只让一个请求去查库，同时到的请求等它查完直接用结果，不一起打到索引库上。
	m := c.lockMerchant(q.MerchantID)
	defer m.mu.Unlock()
	key := pageKey{status: q.Status, limit: q.Limit}
	now := c.now()
	if p, ok := m.pages[key]; ok && now.Sub(p.at) < c.ttl {
		return p.rows, nil
	}
	rows, err := load(ctx, q)
	if err != nil {
		return nil, err
	}
	c.keep(m, key, cachedPage{rows: rows, at: now})
	return rows, nil
}

// keep 在行数合计的上限之内把 p 记进 m，替换同一个 key 的旧页；超过上限就不记，旧页也丢掉。调用方持有 m.mu。
func (c *pageCache) keep(m *merchantPages, key pageKey, p cachedPage) {
	old := len(m.pages[key].rows)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows -= old
	delete(m.pages, key)
	if c.rows+len(p.rows) > c.maxRows {
		return
	}
	c.rows += len(p.rows)
	m.pages[key] = p
}

// lockMerchant 返回商家 id 的缓存，没有就建一个，返回时已经拿着它的锁，调用方负责解开。
// 每隔一个 TTL 顺带清掉过期的页和空了的商家，不需要单独的清理 goroutine。
//
// 商家的锁在 c.mu 里面拿：出了 c.mu 再拿的话，刚查到的 m 可能在拿到它的锁之前就被清理从表里去掉了，
// 往一个不在表里的 m 上记页，没人用得上，行数合计还会算错。
func (c *pageCache) lockMerchant(id int64) *merchantPages {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now := c.now(); now.Sub(c.sweptAt) >= c.ttl {
		c.sweep(now)
		c.sweptAt = now
	}
	m, ok := c.merchants[id]
	if !ok {
		m = &merchantPages{id: id, pages: map[pageKey]cachedPage{}}
		c.merchants[id] = m
	}
	m.mu.Lock()
	return m
}

// sweep 清掉过期的页，商家的页全过期了就把商家也去掉。调用方持有 c.mu。
func (c *pageCache) sweep(now time.Time) {
	for id, m := range c.merchants {
		m.mu.Lock()
		for k, p := range m.pages {
			if now.Sub(p.at) >= c.ttl {
				c.rows -= len(p.rows)
				delete(m.pages, k)
			}
		}
		empty := len(m.pages) == 0
		m.mu.Unlock()
		if empty {
			delete(c.merchants, id)
		}
	}
}
