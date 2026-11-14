// Package stockgate 是 Redis 里的库存闸门：决定一个下单请求能不能进入系统。
//
// MySQL 仍是库存的唯一事实来源。闸门里的数量只用来挡住注定买不到的请求，
// 允许比 MySQL 小（少放进来），不允许比 MySQL 大（放进来的订单会在预占时失败）。
//
// 闸门被两个服务使用：订单服务在受理时扣减（Hold），库存服务负责装载（Load）、
// 订单结束后的归还与结清（Return、Settle）和对账（Adjust）。key 的约定和脚本放在这一个包里，
// 两边就不会各写各的而对不上。
//
// 核心概念是“占用”（hold）：以订单号为主键的一条记录，表示闸门为这个订单扣过哪些数量。
// 归还时以它是否存在为准并删除它，所以同一个订单归还多少次，只有第一次生效。
//
// 每个操作是一段 Lua 脚本。Redis 单线程执行脚本，脚本内的“检查再修改”不会被别的命令插入，
// 这就是它能代替锁的原因。
//
// 限制：脚本用前缀在内部拼出 key 名，没有全部通过 KEYS 声明，只能用于单实例 Redis。
// Redis Cluster 要求一段脚本访问的 key 都在同一个槽里，见设计文档 6.2。
//
// 设计见 docs/design/1b-high-concurrency.md 第 6 节。
package stockgate

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Item 是一个 SKU 和数量。
type Item struct {
	SKUID    int64
	Quantity int32
}

// Gate 访问库存闸门。并发安全。
type Gate struct {
	rdb    redis.Scripter
	client redis.Cmdable
	prefix string
}

// New 返回使用 rdb 的闸门。prefix 是所有 key 的前缀（配置 REDIS_PREFIX），
// 用来让不同环境共用一个 Redis 而互不干扰。
func New(rdb *redis.Client, prefix string) *Gate {
	return &Gate{rdb: rdb, client: rdb, prefix: prefix}
}

// ---------- Hold ----------

// Outcome 是 Hold 的结果。
type Outcome int

// Hold 的各种结果。除 Held 以外，闸门里什么都没有改变。
const (
	// Held：已扣减并记下占用。
	Held Outcome = iota + 1
	// Duplicate：这个幂等键已经受理过，内容相同。HoldResult.OrderID 是当时的订单号。
	Duplicate
	// KeyReused：这个幂等键已经受理过，但请求内容不同。
	KeyReused
	// NotLoaded：有 SKU 还没装载进闸门。HoldResult.SKUID 是其中一个。
	NotLoaded
	// Insufficient：有 SKU 数量不够。
	Insufficient
	// LimitExceeded：超出该 SKU 的每人限购数量。
	LimitExceeded
	// Backlogged：等待落库的订单积压过多，或落库长时间没有动静，暂不受理，见 BacklogLimit。
	// HoldResult.Queued 是触发拒绝的那个计数。
	Backlogged
)

func (o Outcome) String() string {
	switch o {
	case Held:
		return "HELD"
	case Duplicate:
		return "DUPLICATE"
	case KeyReused:
		return "KEY_REUSED"
	case NotLoaded:
		return "NOT_LOADED"
	case Insufficient:
		return "INSUFFICIENT"
	case LimitExceeded:
		return "LIMIT_EXCEEDED"
	case Backlogged:
		return "BACKLOGGED"
	default:
		return "UNKNOWN(" + strconv.Itoa(int(o)) + ")"
	}
}

// HoldRequest 是一次受理请求。
type HoldRequest struct {
	OrderID        int64
	UserID         int64
	IdempotencyKey string
	// RequestHash 标识请求内容，用来识别幂等键被复用到不同的请求上。
	RequestHash []byte
	// Items 不能为空，同一个 SKU 不能出现两次（调用方保证）。
	Items []Item
	// Now 是受理时间，记在占用上，回收任务据此判断占用存在了多久。
	Now time.Time
	// IdempotencyTTL 是幂等键的保留时长，必须为正。
	IdempotencyTTL time.Duration
	// Backlog 可选，零值表示不限制。
	Backlog BacklogLimit
}

// BacklogLimit 是受理端的背压。
//
// 受理之后、被认领之前，占用记在 gate:holds 里，按受理时间排序，它就是等待落库的队列。
//
// 为什么需要它：回收任务把超过宽限期仍未认领的占用当成孤儿还掉（设计文档 6.4）。
// 受理比落库快得多时，积压一旦超过宽限期，排在后面的订单还没轮到落库就被回收，
// 用户先拿到 202，之后查询变成 404。背压要让每个被受理的订单都能在宽限期内落库。
//
// 三个条件，任何一个成立就拒绝：
//
//   - 队列长度达到 MaxQueue。一个新订单要等多久，取决于排在它前面的有多少、落库有多快
//     （利特尔定律：等待时间 = 队列长度 / 处理速率），所以限制长度才能限制新订单的等待。
//     MaxQueue 应取“落库吞吐 × 允许的等待”，落库吞吐变了（例如加了分区）要跟着调。
//   - 排队超过 MaxWait 的占用多于 MaxStale 个。这一条管的是落库变慢或停住（消费者崩溃、
//     依赖方故障）：长度没到上限，但队列不动了，这时继续受理只是在制造会被回收的订单。
//     只看“最老的那一个”不够：它衡量的是过去的等待，受理远快于落库时，最老的订单
//     刚到 MaxWait，后面已经排了几分钟的量（压测里实际发生过）。按个数而不是看最老的一个，
//     是为了容忍真正的孤儿（受理方在发消息前崩溃，要留到宽限期才被回收）：孤儿是零星的。
//   - 落库已经至少 MaxWait 没有动静（gate:claim_at 不存在或早于 Now - MaxWait），
//     并且队列长度超过 MaxStale。第二条单独不够：落库停住时它在约 MaxWait 后拒绝，
//     但回收任务随后把排队超时的占用还掉，计数降回阈值以下，受理重新打开，新订单再排到超时、
//     再被回收，消费者停多久就循环多久，用户一直拿到 202 然后订单消失。回收抹掉了队列里的证据，
//     所以要一个回收抹不掉的信号：落库最后一次认领的时间（心跳，见 Claim）。
//     没有动静时只放进少量“探测”订单：消费者活着就会认领它们、刷新心跳，受理随即恢复；
//     消费者死了，每个宽限期最多损失 MaxStale+1 个订单，而不是受理速率 × MaxWait。
//     冷启动或长时间空闲之后心跳很旧，但队列是空的，第一批订单照常受理，它们本身就是探测。
//
// 第二、三条都只在 MaxWait > 0 时检查。心跳由落库一方的时钟写、受理一方的时钟读，
// 两边的时钟偏差要远小于 MaxWait。
type BacklogLimit struct {
	// MaxQueue 是等待落库的占用数上限，0 表示不限制。
	MaxQueue int64
	// MaxWait 是允许的排队时长，0 表示不检查。
	MaxWait time.Duration
	// MaxStale 有两个用途：允许排队超过 MaxWait 的占用个数（容忍零星的孤儿），
	// 以及落库没有动静时队列里最多容忍的占用个数（探测订单的数量，可以多放进一个）。
	// 取 0 时落库没有动静就只放进一个探测订单，不会因为没有心跳而永远关闭。
	MaxStale int64
}

// HoldResult 是 Hold 的结果。
type HoldResult struct {
	Outcome Outcome
	// OrderID：Held 时是本次的订单号；Duplicate、KeyReused 时是第一次受理的订单号。
	OrderID int64
	// SKUID：NotLoaded、Insufficient、LimitExceeded 时是不满足条件的 SKU。
	SKUID int64
	// Queued：Backlogged 时是触发拒绝的计数（队列长度，或排队超时的占用数）。
	Queued int64
}

// holdScript 先做完所有检查，再做所有修改：任何一个 SKU 不满足条件都在写入之前返回，
// 所以一个订单的多个 SKU 要么全部扣到，要么都不扣，不需要回滚。
//
// 积压检查（三条，见 BacklogLimit）放在库存和限购的检查之后：卖完了的请求应当得到“卖完”，
// 而不是“稍后重试”，否则用户会对一个注定买不到的商品反复重试。它们也只读不写，仍在所有修改之前。
// 心跳只由 Claim 写，Hold 只读它：受理成功不能算作落库活着。
//
// ARGV: 前缀, 订单号, 用户, 幂等键, 请求哈希, 幂等键保留毫秒数, 受理时间毫秒, items 编码,
// 队列长度上限（0 表示不限制）, 排队截止时间毫秒（0 表示不检查第二、三条）, MaxStale, 然后是成对的 sku, 数量。
// 第三条拿心跳和排队截止时间比：心跳早于它，说明落库至少 MaxWait 没有认领过任何消息。
var holdScript = redis.NewScript(`
local p, order, user, idem, hash = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local idemKey = p .. 'gate:idem:' .. user .. ':' .. idem

local prev = redis.call('GET', idemKey)
if prev then
  local sep = string.find(prev, ':', 1, true)
  local prevOrder = string.sub(prev, 1, sep - 1)
  if string.sub(prev, sep + 1) ~= hash then
    return {'REUSED', prevOrder}
  end
  return {'DUP', prevOrder}
end

local n = (#ARGV - 11) / 2
for i = 0, n - 1 do
  local sku, qty = ARGV[12 + 2 * i], tonumber(ARGV[13 + 2 * i])
  local v = redis.call('HMGET', p .. 'gate:sku:' .. sku, 'avail', 'limit')
  if not v[1] then
    return {'NOT_LOADED', sku}
  end
  if tonumber(v[1]) < qty then
    return {'INSUFFICIENT', sku}
  end
  local limit = tonumber(v[2] or '0')
  if limit > 0 then
    local bought = tonumber(redis.call('HGET', p .. 'gate:bought:' .. sku, user) or '0')
    if bought + qty > limit then
      return {'LIMIT', sku}
    end
  end
end

if ARGV[9] ~= '0' then
  local queued = redis.call('ZCARD', p .. 'gate:holds')
  if queued >= tonumber(ARGV[9]) then
    return {'BACKLOG', tostring(queued)}
  end
end
if ARGV[10] ~= '0' then
  local stale = redis.call('ZCOUNT', p .. 'gate:holds', '-inf', '(' .. ARGV[10])
  if stale > tonumber(ARGV[11]) then
    return {'BACKLOG', tostring(stale)}
  end
  local beat = redis.call('GET', p .. 'gate:claim_at')
  if not beat or tonumber(beat) < tonumber(ARGV[10]) then
    local queued = redis.call('ZCARD', p .. 'gate:holds')
    if queued > tonumber(ARGV[11]) then
      return {'BACKLOG', tostring(queued)}
    end
  end
end

for i = 0, n - 1 do
  local sku, qty = ARGV[12 + 2 * i], tonumber(ARGV[13 + 2 * i])
  redis.call('HINCRBY', p .. 'gate:sku:' .. sku, 'avail', -qty)
  redis.call('HINCRBY', p .. 'gate:sku:' .. sku, 'held', qty)
  redis.call('HINCRBY', p .. 'gate:bought:' .. sku, user, qty)
end
redis.call('HSET', p .. 'gate:hold:' .. order,
  'user', user, 'items', ARGV[8], 'claimed', '0', 'ts', ARGV[7], 'idem', idem)
redis.call('ZADD', p .. 'gate:holds', ARGV[7], order)
redis.call('SET', idemKey, order .. ':' .. hash, 'PX', ARGV[6])
return {'OK', order}
`)

// Hold 尝试为一个订单扣减并记下占用。
//
// 返回 error 只表示 Redis 调用本身失败（这时结果未知：可能扣了也可能没扣）。
// 调用方应当用同一个幂等键重试，重试会得到 Duplicate 或重新扣减，不会扣两次。
func (g *Gate) Hold(ctx context.Context, req HoldRequest) (HoldResult, error) {
	switch {
	case req.OrderID <= 0 || req.UserID <= 0:
		return HoldResult{}, errors.New("stockgate: order id and user id must be positive")
	case req.IdempotencyKey == "":
		return HoldResult{}, errors.New("stockgate: idempotency key is required")
	case len(req.Items) == 0:
		return HoldResult{}, errors.New("stockgate: items must not be empty")
	case req.IdempotencyTTL < time.Millisecond:
		return HoldResult{}, errors.New("stockgate: idempotency ttl must be positive")
	case req.Backlog.MaxQueue < 0 || req.Backlog.MaxWait < 0 || req.Backlog.MaxStale < 0:
		return HoldResult{}, errors.New("stockgate: backlog limit must not be negative")
	}
	var cutoff int64 // 0 表示不检查积压
	if req.Backlog.MaxWait > 0 {
		cutoff = max(req.Now.Add(-req.Backlog.MaxWait).UnixMilli(), 1)
	}
	encoded, err := encodeItems(req.Items)
	if err != nil {
		return HoldResult{}, err
	}

	args := make([]any, 0, 11+2*len(req.Items))
	args = append(args, g.prefix, req.OrderID, req.UserID, req.IdempotencyKey, hex.EncodeToString(req.RequestHash),
		req.IdempotencyTTL.Milliseconds(), req.Now.UnixMilli(), encoded, req.Backlog.MaxQueue, cutoff, req.Backlog.MaxStale)
	for _, it := range req.Items {
		args = append(args, it.SKUID, it.Quantity)
	}
	res, err := holdScript.Run(ctx, g.rdb, nil, args...).StringSlice()
	if err != nil {
		return HoldResult{}, fmt.Errorf("stockgate: hold: %w", err)
	}
	if len(res) != 2 {
		return HoldResult{}, fmt.Errorf("stockgate: hold: unexpected reply %v", res)
	}
	id, err := strconv.ParseInt(res[1], 10, 64)
	if err != nil {
		return HoldResult{}, fmt.Errorf("stockgate: hold: unexpected reply %v", res)
	}
	switch res[0] {
	case "OK":
		return HoldResult{Outcome: Held, OrderID: id}, nil
	case "DUP":
		return HoldResult{Outcome: Duplicate, OrderID: id}, nil
	case "REUSED":
		return HoldResult{Outcome: KeyReused, OrderID: id}, nil
	case "NOT_LOADED":
		return HoldResult{Outcome: NotLoaded, SKUID: id}, nil
	case "INSUFFICIENT":
		return HoldResult{Outcome: Insufficient, SKUID: id}, nil
	case "LIMIT":
		return HoldResult{Outcome: LimitExceeded, SKUID: id}, nil
	case "BACKLOG":
		return HoldResult{Outcome: Backlogged, Queued: id}, nil
	default:
		return HoldResult{}, fmt.Errorf("stockgate: hold: unexpected reply %v", res)
	}
}

// ---------- 占用的读取与认领 ----------

// HoldInfo 是一笔占用的内容。
type HoldInfo struct {
	OrderID int64
	UserID  int64
	Items   []Item
	// Claimed 表示订单服务的落库流程已经认领了它，回收任务不会再动它。
	Claimed bool
	// At 是受理时间。
	At time.Time
}

// claimScript 把占用标记为已认领并返回它的内容；mode 为 'get' 时只读不改。
//
// 认领时把订单号从 gate:holds 移到 gate:claimed。回收任务只扫 gate:holds：
// 已认领的占用要活到订单结束（待支付的订单最长是整个支付时限），如果留在同一个列表里，
// 它们会一直排在最前面，数量超过回收任务的一批之后，排在后面的孤儿就永远轮不到。
//
// claim 模式还把 gate:claim_at（落库的心跳，Hold 据此判断落库是否还活着）推进到 ARGV[4]，
// 占用不存在或已认领过也推进：这同样说明消费者活着、在处理消息，而消费者恢复后最先消费到的
// 往往正是已被回收的订单的消息，不记的话受理要等到它消费到一个还活着的占用才恢复。
// 只往前推：多个消费者实例的时钟和调用顺序不一致，回退会让一个刚恢复的落库看起来又停了。
//
// ARGV: 前缀, 订单号, mode, 当前时间毫秒（get 模式不用）。
var claimScript = redis.NewScript(`
local hk = ARGV[1] .. 'gate:hold:' .. ARGV[2]
local h = redis.call('HMGET', hk, 'user', 'items', 'claimed', 'ts')
if ARGV[3] == 'claim' then
  local bk = ARGV[1] .. 'gate:claim_at'
  local beat = redis.call('GET', bk)
  if not beat or tonumber(beat) < tonumber(ARGV[4]) then
    redis.call('SET', bk, ARGV[4])
  end
end
if not h[1] then
  return {}
end
if ARGV[3] == 'claim' and h[3] ~= '1' then
  redis.call('HSET', hk, 'claimed', '1')
  redis.call('ZREM', ARGV[1] .. 'gate:holds', ARGV[2])
  redis.call('ZADD', ARGV[1] .. 'gate:claimed', h[4], ARGV[2])
  h[3] = '1'
end
return h
`)

// Claim 认领一笔占用：订单服务在把订单写进数据库之前调用。
//
// 认领和回收（ReturnIfUnclaimed）在 Redis 里互斥：同一笔占用，要么被认领后由订单的终态归还，
// 要么被回收任务归还，不会两样都发生。占用不存在时返回 false，调用方应当放弃这笔订单。
// 重复认领是安全的。
//
// 每次调用（包括占用不存在、重复认领）都把落库的心跳推进到 now，Hold 的第三条背压靠它
// 区分“落库停住了”和“落库活着”，见 BacklogLimit。now 应当是调用方的当前时间。
func (g *Gate) Claim(ctx context.Context, orderID int64, now time.Time) (HoldInfo, bool, error) {
	return g.readHold(ctx, orderID, "claim", now.UnixMilli())
}

// Get 读取一笔占用，不改变它。
func (g *Gate) Get(ctx context.Context, orderID int64) (HoldInfo, bool, error) {
	return g.readHold(ctx, orderID, "get", 0)
}

func (g *Gate) readHold(ctx context.Context, orderID int64, mode string, nowMilli int64) (HoldInfo, bool, error) {
	res, err := claimScript.Run(ctx, g.rdb, nil, g.prefix, orderID, mode, nowMilli).StringSlice()
	if err != nil {
		return HoldInfo{}, false, fmt.Errorf("stockgate: %s hold %d: %w", mode, orderID, err)
	}
	if len(res) == 0 {
		return HoldInfo{}, false, nil
	}
	if len(res) != 4 {
		return HoldInfo{}, false, fmt.Errorf("stockgate: %s hold %d: unexpected reply %v", mode, orderID, res)
	}
	info := HoldInfo{OrderID: orderID, Claimed: res[2] == "1"}
	if info.UserID, err = strconv.ParseInt(res[0], 10, 64); err != nil {
		return HoldInfo{}, false, fmt.Errorf("stockgate: hold %d: bad user %q", orderID, res[0])
	}
	if info.Items, err = decodeItems(res[1]); err != nil {
		return HoldInfo{}, false, fmt.Errorf("stockgate: hold %d: %w", orderID, err)
	}
	ms, err := strconv.ParseInt(res[3], 10, 64)
	if err != nil {
		return HoldInfo{}, false, fmt.Errorf("stockgate: hold %d: bad ts %q", orderID, res[3])
	}
	info.At = time.UnixMilli(ms).UTC()
	return info, true, nil
}

// ---------- 归还与结清 ----------

// releaseScript 结束一笔占用。占用不存在返回 0，这是归还幂等的来源。
//
// ARGV: 前缀, 订单号, mode。
//   - return    加回数量，保留幂等键（订单已经落库，重放应当得到同一个订单）。
//   - abort     加回数量，并删除指向这个订单的幂等键（订单没有落库，用户重试应当能重新下单）。
//   - unclaimed 同 abort，但只处理未认领的占用；已认领返回 2。
//   - settle    不加回数量（已经卖掉），只把占用从 held 里减掉。用户的已购数量也不动。
//
// SKU 的 key 不存在（Redis 丢过数据后还没重新装载）时不创建它：
// 凭空造出一个只有部分字段的 hash 会让它看起来像已装载。这部分偏差由对账修正。
var releaseScript = redis.NewScript(`
local p, order, mode = ARGV[1], ARGV[2], ARGV[3]
local hk = p .. 'gate:hold:' .. order
local h = redis.call('HMGET', hk, 'user', 'items', 'claimed', 'idem')
if not h[1] then
  return 0
end
if mode == 'unclaimed' and h[3] == '1' then
  return 2
end
for sku, q in string.gmatch(h[2], '(%d+):(%d+)') do
  local qty = tonumber(q)
  local sk = p .. 'gate:sku:' .. sku
  if redis.call('EXISTS', sk) == 1 then
    redis.call('HINCRBY', sk, 'held', -qty)
    if mode ~= 'settle' then
      redis.call('HINCRBY', sk, 'avail', qty)
    end
  end
  if mode ~= 'settle' then
    local bk = p .. 'gate:bought:' .. sku
    if redis.call('HINCRBY', bk, h[1], -qty) <= 0 then
      redis.call('HDEL', bk, h[1])
    end
  end
end
redis.call('DEL', hk)
redis.call('ZREM', p .. 'gate:holds', order)
redis.call('ZREM', p .. 'gate:claimed', order)
if (mode == 'abort' or mode == 'unclaimed') and h[4] then
  local ik = p .. 'gate:idem:' .. h[1] .. ':' .. h[4]
  local cur = redis.call('GET', ik)
  if cur and string.sub(cur, 1, #order + 1) == order .. ':' then
    redis.call('DEL', ik)
  end
end
return 1
`)

func (g *Gate) release(ctx context.Context, orderID int64, mode string) (int64, error) {
	n, err := releaseScript.Run(ctx, g.rdb, nil, g.prefix, orderID, mode).Int64()
	if err != nil {
		return 0, fmt.Errorf("stockgate: %s hold %d: %w", mode, orderID, err)
	}
	return n, nil
}

// Return 归还一个已经落库的订单的占用：订单被取消、关闭或创建失败，并且 MySQL 里的预占已经释放之后调用。
// 返回这次调用是否真的归还了（占用已不存在时为 false）。
//
// 必须在 MySQL 释放之后调用。先还 Redis 的话，闸门会放进来一个 MySQL 还没有库存给它的订单。
func (g *Gate) Return(ctx context.Context, orderID int64) (bool, error) {
	n, err := g.release(ctx, orderID, "return")
	return n == 1, err
}

// Abort 归还一个不会落库的订单的占用，并删除它的幂等键，让用户可以用同一个幂等键重新下单。
//
// 它**不看认领状态**，只能由已经认领了这笔占用、并且确定不会把订单写进数据库的一方调用
// （订单服务落库时发现幂等键早已有订单）。受理方发消息失败后不能用它：发送失败不代表消息没到，
// 消费者可能已经认领并在落库，这时归还会让闸门比 MySQL 多出一份。受理方要用 ReturnIfUnclaimed。
func (g *Gate) Abort(ctx context.Context, orderID int64) (bool, error) {
	n, err := g.release(ctx, orderID, "abort")
	return n == 1, err
}

// ReapResult 是 ReturnIfUnclaimed 的结果。
type ReapResult int

// ReturnIfUnclaimed 的三种结果。
const (
	// ReapMissing：占用已经不存在。
	ReapMissing ReapResult = iota
	// ReapReturned：占用未被认领，已归还。
	ReapReturned
	// ReapClaimed：占用已被认领，没有动它。
	ReapClaimed
)

// ReturnIfUnclaimed 归还一笔没有被认领的占用，并删除它的幂等键。与 Claim 互斥，见 Claim 的说明。
//
// 两个调用方：回收任务处理“受理了但消息没发出去”的孤儿占用；受理方在发消息失败后撤销这次受理。
func (g *Gate) ReturnIfUnclaimed(ctx context.Context, orderID int64) (ReapResult, error) {
	n, err := g.release(ctx, orderID, "unclaimed")
	if err != nil {
		return ReapMissing, err
	}
	return ReapResult(n), nil
}

// Settle 结清一个已支付订单的占用：数量已经卖掉，不加回，只删除占用。
// 在 MySQL 里的预占确认之后调用。返回这次调用是否真的结清了。
func (g *Gate) Settle(ctx context.Context, orderID int64) (bool, error) {
	n, err := g.release(ctx, orderID, "settle")
	return n == 1, err
}

// PointIdempotency 让幂等键指向一个已有的订单。
//
// 用在幂等键在 Redis 里过期之后的重放上：订单库的唯一键发现这个幂等键早就有订单了，
// 把映射补回去，之后的重放在闸门里就能直接得到那个订单，不必再走一遍受理和落库。
func (g *Gate) PointIdempotency(ctx context.Context, userID int64, key string, orderID int64, requestHash []byte, ttl time.Duration) error {
	k := g.prefix + "gate:idem:" + strconv.FormatInt(userID, 10) + ":" + key
	v := strconv.FormatInt(orderID, 10) + ":" + hex.EncodeToString(requestHash)
	if err := g.client.Set(ctx, k, v, ttl).Err(); err != nil {
		return fmt.Errorf("stockgate: point idempotency key: %w", err)
	}
	return nil
}

// HoldsBefore 返回受理时间早于 before、**尚未被认领**的占用的订单号，最多 limit 个，最早的在前。
// 回收任务用它找孤儿占用。
func (g *Gate) HoldsBefore(ctx context.Context, before time.Time, limit int) ([]int64, error) {
	return g.listBefore(ctx, "gate:holds", before, limit)
}

// ClaimedBefore 返回受理时间早于 before、已被认领而仍未结束的占用的订单号，最多 limit 个，最早的在前。
// 正常情况下已认领的占用在订单进入终态时结束；长期不结束说明认领之后流程卡住了，需要人看。
func (g *Gate) ClaimedBefore(ctx context.Context, before time.Time, limit int) ([]int64, error) {
	return g.listBefore(ctx, "gate:claimed", before, limit)
}

// Queued 返回现有占用的个数：unclaimed 是已受理、还没被落库认领的（gate:holds），
// claimed 是已认领、订单还没结束的（gate:claimed）。只读，给监控用。
//
// unclaimed 就是受理端背压比较的那个量：holdScript 的第一条拿 ZCARD gate:holds 和 MaxQueue 比，
// 第三条也是拿它和 MaxStale 比，没有再加减别的。所以 MaxQueue - unclaimed 就是离背压拒绝还有多远。
// （第二条比的是其中受理时间早于 Now - MaxWait 的个数，那个要按时间算，这里不读。）
//
// 两个 ZCARD 都是 O(1)，放在一个 MULTI/EXEC 里一次往返，两个数取自同一时刻：
// 一笔占用被认领时从一个集合移到另一个，分两次读可能把它数两次或一次都没数到。
func (g *Gate) Queued(ctx context.Context) (unclaimed, claimed int64, err error) {
	var u, c *redis.IntCmd
	if _, err := g.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		u = p.ZCard(ctx, g.prefix+"gate:holds")
		c = p.ZCard(ctx, g.prefix+"gate:claimed")
		return nil
	}); err != nil {
		return 0, 0, fmt.Errorf("stockgate: count holds: %w", err)
	}
	return u.Val(), c.Val(), nil
}

func (g *Gate) listBefore(ctx context.Context, key string, before time.Time, limit int) ([]int64, error) {
	members, err := g.client.ZRangeByScore(ctx, g.prefix+key, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   "(" + strconv.FormatInt(before.UnixMilli(), 10),
		Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("stockgate: list %s: %w", key, err)
	}
	ids := make([]int64, 0, len(members))
	for _, m := range members {
		id, err := strconv.ParseInt(m, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("stockgate: list %s: bad member %q", key, m)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ---------- 装载与对账 ----------

// loadScript 只在 SKU 未装载时写入数量。已装载时数量由 Hold、Return 维护，
// 再用 MySQL 的值覆盖会抹掉还没落到 MySQL 的占用；限购数量是配置，可以随时更新。
var loadScript = redis.NewScript(`
local sk = ARGV[1] .. 'gate:sku:' .. ARGV[2]
if redis.call('EXISTS', sk) == 1 then
  redis.call('HSET', sk, 'limit', ARGV[4])
  return 0
end
redis.call('HSET', sk, 'avail', ARGV[3], 'held', '0', 'limit', ARGV[4])
redis.call('SADD', ARGV[1] .. 'gate:skus', ARGV[2])
return 1
`)

// Load 把一个 SKU 装载进闸门。avail 取 MySQL 的可售数量，limit 是每人限购数量（0 表示不限）。
// 返回这次调用是否写入了数量（已装载时为 false）。重复调用、并发调用都是安全的。
func (g *Gate) Load(ctx context.Context, skuID, avail, limit int64) (bool, error) {
	if avail < 0 || limit < 0 {
		return false, errors.New("stockgate: avail and limit must not be negative")
	}
	n, err := loadScript.Run(ctx, g.rdb, nil, g.prefix, skuID, avail, limit).Int64()
	if err != nil {
		return false, fmt.Errorf("stockgate: load sku %d: %w", skuID, err)
	}
	return n == 1, nil
}

// Stock 是闸门里一个 SKU 的数量。
type Stock struct {
	// Avail 是还能放行的数量。
	Avail int64
	// Held 是现有占用的数量之和。
	Held int64
	// Limit 是每人限购数量，0 表示不限。
	Limit int64
}

// Stock 读取一个 SKU 在闸门里的数量。未装载时返回 false。
func (g *Gate) Stock(ctx context.Context, skuID int64) (Stock, bool, error) {
	v, err := g.client.HMGet(ctx, g.prefix+"gate:sku:"+strconv.FormatInt(skuID, 10), "avail", "held", "limit").Result()
	if err != nil {
		return Stock{}, false, fmt.Errorf("stockgate: read sku %d: %w", skuID, err)
	}
	if v[0] == nil {
		return Stock{}, false, nil
	}
	var st Stock
	for i, dst := range []*int64{&st.Avail, &st.Held, &st.Limit} {
		s, _ := v[i].(string)
		if s == "" {
			continue
		}
		if *dst, err = strconv.ParseInt(s, 10, 64); err != nil {
			return Stock{}, false, fmt.Errorf("stockgate: read sku %d: bad value %q", skuID, s)
		}
	}
	return st, true, nil
}

// SKUs 返回已装载的全部 SKU。
func (g *Gate) SKUs(ctx context.Context) ([]int64, error) {
	members, err := g.client.SMembers(ctx, g.prefix+"gate:skus").Result()
	if err != nil {
		return nil, fmt.Errorf("stockgate: list skus: %w", err)
	}
	ids := make([]int64, 0, len(members))
	for _, m := range members {
		id, err := strconv.ParseInt(m, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("stockgate: list skus: bad member %q", m)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// adjustScript 给 avail 加上一个增量，结果限制在 [0, cap]。
var adjustScript = redis.NewScript(`
local sk = ARGV[1] .. 'gate:sku:' .. ARGV[2]
local cur = redis.call('HGET', sk, 'avail')
if not cur then
  return -1
end
local v = tonumber(cur) + tonumber(ARGV[3])
local cap = tonumber(ARGV[4])
if v > cap then v = cap end
if v < 0 then v = 0 end
redis.call('HSET', sk, 'avail', v)
return v
`)

// Adjust 给一个 SKU 的可放行数量加上 delta（可为负），对账任务用它修正偏差。返回修正后的数量。
//
// 用增量而不是直接设成某个值：对账时读 MySQL 和读 Redis 不是同一时刻，
// 直接覆盖会抹掉期间发生的扣减，增量则和并发的扣减互不影响。
//
// 结果不会超过 maxAvail（传 MySQL 的可售数量）：闸门比 MySQL 的可售还大一定是错的。
// SKU 未装载时返回 false。
func (g *Gate) Adjust(ctx context.Context, skuID, delta, maxAvail int64) (int64, bool, error) {
	if maxAvail < 0 {
		return 0, false, errors.New("stockgate: max avail must not be negative")
	}
	n, err := adjustScript.Run(ctx, g.rdb, nil, g.prefix, skuID, delta, maxAvail).Int64()
	if err != nil {
		return 0, false, fmt.Errorf("stockgate: adjust sku %d: %w", skuID, err)
	}
	if n < 0 {
		return 0, false, nil
	}
	return n, true, nil
}

// ---------- 编码 ----------

// 占用里的商品编码成 "sku:数量,sku:数量"，Lua 里用 string.gmatch 解析。
func encodeItems(items []Item) (string, error) {
	var b strings.Builder
	for i, it := range items {
		if it.SKUID <= 0 || it.Quantity <= 0 {
			return "", fmt.Errorf("stockgate: invalid item %+v", it)
		}
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatInt(it.SKUID, 10))
		b.WriteByte(':')
		b.WriteString(strconv.FormatInt(int64(it.Quantity), 10))
	}
	return b.String(), nil
}

func decodeItems(s string) ([]Item, error) {
	var items []Item
	for part := range strings.SplitSeq(s, ",") {
		sku, qty, ok := strings.Cut(part, ":")
		if !ok {
			return nil, fmt.Errorf("bad items %q", s)
		}
		id, err := strconv.ParseInt(sku, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad items %q", s)
		}
		q, err := strconv.ParseInt(qty, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("bad items %q", s)
		}
		items = append(items, Item{SKUID: id, Quantity: int32(q)})
	}
	return items, nil
}
