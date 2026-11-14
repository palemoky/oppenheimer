package ratelimit

import (
	"context"
	"hash/maphash"
	"slices"
	"sync"
	"time"
)

// 默认值。
const (
	// DefaultMaxKeys 是 Local 最多记多少个桶。
	DefaultMaxKeys = 100_000
	localShards    = 64
	// evictScan 是一个分片满了之后最多看多少个桶来找可以丢的。
	evictScan = 32
)

// LocalConfig 是 Local 的配置，零值取默认。
type LocalConfig struct {
	// MaxKeys 是最多记多少个桶，默认 DefaultMaxKeys。键是调用方给的（比如客户端地址），数量没有上限，
	// 不设上限的话内存会被撑满。超过时丢掉一些桶；被丢掉的桶下次出现时是满的，等于放宽了一次。
	MaxKeys int
	// Now 是时钟，默认 time.Now。
	Now func() time.Time
}

// Local 是进程内的令牌桶。并发安全。
type Local struct {
	now    func() time.Time
	seed   maphash.Seed
	shards [localShards]localShard
	// perShard 是每个分片的容量。
	perShard int
}

// localShard 是一部分键的桶。按键的哈希分片、每片一把锁，而不是整个表一把锁：
// 所有请求都要过限流，一把大锁会让它们在这里排成一队。
type localShard struct {
	mu      sync.Mutex
	buckets map[string]*localBucket
}

type localBucket struct {
	tokens float64
	last   time.Time
	// fullAt 是这个桶补满的时刻。过了这个时刻它和一个不存在的桶没有区别，可以直接丢掉。
	fullAt time.Time
}

// NewLocal 返回 Local。
func NewLocal(cfg LocalConfig) *Local {
	l := &Local{now: cfg.Now, seed: maphash.MakeSeed()}
	if l.now == nil {
		l.now = time.Now
	}
	maxKeys := cfg.MaxKeys
	if maxKeys <= 0 {
		maxKeys = DefaultMaxKeys
	}
	l.perShard = max(maxKeys/localShards, 1)
	for i := range l.shards {
		l.shards[i].buckets = make(map[string]*localBucket)
	}
	return l
}

// Allow 实现 Limiter。不访问网络，不会返回错误以外的失败；ctx 没有用到。
func (l *Local) Allow(_ context.Context, reqs ...Request) (Decision, error) {
	reqs, index, err := active(reqs)
	if err != nil {
		return Decision{}, err
	}
	if len(reqs) == 0 {
		return allowed, nil
	}

	// 一次调用里的几个桶可能在不同的分片上。按分片号升序加锁：两个调用以不同的顺序列出同样的桶时，
	// 加锁的顺序仍然相同，不会互相死锁。
	shardOf := make([]int, len(reqs))
	var lockOrder []int
	for i, r := range reqs {
		shardOf[i] = int(maphash.String(l.seed, r.Key) % localShards)
		if !slices.Contains(lockOrder, shardOf[i]) {
			lockOrder = append(lockOrder, shardOf[i])
		}
	}
	slices.Sort(lockOrder)
	for _, s := range lockOrder {
		l.shards[s].mu.Lock()
	}
	defer func() {
		for _, s := range lockOrder {
			l.shards[s].mu.Unlock()
		}
	}()

	// 先都看一遍：算出每个桶现在的令牌数，不改任何东西。
	now := l.now()
	tokens := make([]float64, len(reqs))
	d := allowed
	for i, r := range reqs {
		tokens[i] = float64(r.Limit.Burst) // 没见过的桶是满的
		if b := l.shards[shardOf[i]].buckets[r.Key]; b != nil {
			tokens[i] = refill(b.tokens, now.Sub(b.last), r.Limit)
		}
		if tokens[i] < 1 {
			if d.Allowed {
				d.Allowed, d.Denied = false, index[i]
			}
			d.RetryAfter = max(d.RetryAfter, wait(tokens[i], r.Limit))
		}
	}
	if !d.Allowed {
		// 被拒绝时不写回：桶里记的（令牌数、时刻）仍然正确，下次照样从它算起。
		return d, nil
	}

	// 都够，再都扣。
	for i, r := range reqs {
		sh := &l.shards[shardOf[i]]
		b := sh.buckets[r.Key]
		if b == nil {
			if len(sh.buckets) >= l.perShard {
				sh.evict(now)
			}
			b = &localBucket{}
			sh.buckets[r.Key] = b
		}
		b.tokens = tokens[i] - 1
		b.last = now
		b.fullAt = now.Add(time.Duration((float64(r.Limit.Burst) - b.tokens) / r.Limit.Rate * float64(time.Second)))
	}
	return allowed, nil
}

// evict 在分片满了的时候腾出至少一个位置。调用方持有分片的锁。
//
// 先丢已经补满的桶：它们和不存在的桶没有区别，丢掉不改变任何行为。看了 evictScan 个都没有补满的，
// 就丢掉看到的第一个：那个键的限制被放宽一次。不维护“最近使用”的顺序：map 的遍历顺序是随机的，
// 随机丢一个对“表被撑满时别让内存继续涨”这个目的足够了，而每个请求都去挪动一个链表不值得。
func (s *localShard) evict(now time.Time) {
	var (
		victim  string
		scanned int
		removed bool
	)
	for k, b := range s.buckets {
		if scanned == 0 {
			victim = k
		}
		if !b.fullAt.After(now) {
			delete(s.buckets, k) // 遍历时删除当前元素是安全的
			removed = true
		}
		if scanned++; scanned >= evictScan {
			break
		}
	}
	if !removed {
		delete(s.buckets, victim)
	}
}

// Len 返回当前记着的桶数，给测试和监控用。
func (l *Local) Len() int {
	n := 0
	for i := range l.shards {
		l.shards[i].mu.Lock()
		n += len(l.shards[i].buckets)
		l.shards[i].mu.Unlock()
	}
	return n
}
