// Package readmark 在 Redis 里记录订单最近被写过的痕迹，供展示读决定读主库还是从库（读己之写）。
//
// 写的一方留两种标记：
//   - pin：写之前打，带一个短 TTL，表示“这个订单正在写或刚写过，从库可能还没有”。到期自己消失。
//   - gtid：写入提交之后记下的主库位置，读的一方可以让从库等到这个位置再读。
//
// 标记放在闸门所在的 Redis 实例上（noeviction）：被淘汰的标记等于提前到期，读会落到旧的从库上。
// 标记丢了只影响新鲜度，不影响正确性：读从库的结果从来不作为任何写入的前提。
package readmark

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// Mark 是一个订单当前的标记。
type Mark struct {
	// Pinned 表示 pin 还没到期。
	Pinned bool
	// GTID 是记下的主库位置，空表示没有。
	GTID string
}

// Marks 读写订单的标记。并发安全。
type Marks struct {
	rdb    redis.UniversalClient
	prefix string
}

// New 返回使用 rdb 的 Marks。prefix 是所有 key 的前缀（配置 REDIS_PREFIX）。
func New(rdb redis.UniversalClient, prefix string) *Marks {
	return &Marks{rdb: rdb, prefix: prefix}
}

func (m *Marks) pinKey(orderID int64) string {
	return m.prefix + "order:rw:pin:" + strconv.FormatInt(orderID, 10)
}

func (m *Marks) gtidKey(orderID int64) string {
	return m.prefix + "order:rw:gtid:" + strconv.FormatInt(orderID, 10)
}

// Pin 给订单打上 pin，ttl 后自动消失。重复打只是续期。
func (m *Marks) Pin(ctx context.Context, orderID int64, ttl time.Duration) error {
	if err := m.rdb.Set(ctx, m.pinKey(orderID), "1", ttl).Err(); err != nil {
		return fmt.Errorf("readmark: pin order %d: %w", orderID, err)
	}
	return nil
}

// stampScript 只往前更新位置。值的格式是 "<At>:<GTID>"（GTID 自己也含冒号，按第一个冒号切）。
//
// 同一订单的两次写可能交错：先提交的那次读到的位置如果后写进 Redis，会用旧集合盖掉新集合，
// 读的一方就可能在一个还没复制到第二次写入的从库上读。较晚读到的集合一定包含较早的，
// 所以按读出位置时的主库时间 At 比较，已有的不比新的旧就保留（只续期）。
// At 是 Unix 微秒，约 1.8e15，在 Lua 的双精度数里精确。
var stampScript = redis.NewScript(`
local k, at, gtid, ttl = KEYS[1], tonumber(ARGV[1]), ARGV[2], ARGV[3]
local cur = redis.call('GET', k)
if cur then
  local sep = string.find(cur, ':', 1, true)
  local curAt = sep and tonumber(string.sub(cur, 1, sep - 1))
  if curAt and curAt >= at then
    redis.call('PEXPIRE', k, ttl)
    return 0
  end
end
redis.call('SET', k, ARGV[1] .. ':' .. gtid, 'PX', ttl)
return 1
`)

// Stamp 记下订单写入提交后的主库位置，ttl 后自动消失。已有的位置不比 pos 旧时只续期、不覆盖。
func (m *Marks) Stamp(ctx context.Context, orderID int64, pos store.Position, ttl time.Duration) error {
	if pos.GTID == "" {
		return fmt.Errorf("readmark: stamp order %d: empty gtid", orderID)
	}
	err := stampScript.Run(ctx, m.rdb, []string{m.gtidKey(orderID)},
		strconv.FormatInt(pos.At, 10), pos.GTID, ttl.Milliseconds()).Err()
	if err != nil {
		return fmt.Errorf("readmark: stamp order %d: %w", orderID, err)
	}
	return nil
}

// Get 一次往返读出订单的两种标记。
func (m *Marks) Get(ctx context.Context, orderID int64) (Mark, error) {
	vs, err := m.rdb.MGet(ctx, m.pinKey(orderID), m.gtidKey(orderID)).Result()
	if err != nil {
		return Mark{}, fmt.Errorf("readmark: get order %d: %w", orderID, err)
	}
	var mk Mark
	mk.Pinned = vs[0] != nil
	if vs[1] != nil {
		s, ok := vs[1].(string)
		if !ok {
			return Mark{}, fmt.Errorf("readmark: get order %d: unexpected gtid value %T", orderID, vs[1])
		}
		_, gtid, found := strings.Cut(s, ":")
		if !found || gtid == "" {
			return Mark{}, errors.New("readmark: malformed gtid mark of order " + strconv.FormatInt(orderID, 10))
		}
		mk.GTID = gtid
	}
	return mk, nil
}
