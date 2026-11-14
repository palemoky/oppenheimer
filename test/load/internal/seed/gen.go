// Package seed 直接向订单主库灌历史订单（设计文档 1c 的 5.3）。
//
// 生成是确定性的：同一组参数（Params）对同一个“slot 加一天”的块总是生成完全相同的订单，
// 包括订单号。所以中断后重跑只需要跳过记过进度的块，不会产生重复数据。
//
// 用户的抽取以“一天”为单位、所有 slot 共用一个随机源（种子由全局种子和日期算出）：先按 Zipf
// 抽出当天全部订单的下单用户，再按 user_id 分到各个 slot。这样每个 slot 分到多少订单由用户的分布
// 决定，量出来的分片均匀度才有意义。如果每个 slot 各抽各的、各自分到总数的 1/8，均匀度就是造出来的。
// 用户之外的一切（时间、商家、商品、状态）由（全局种子、slot、日期）算出的随机源生成。
package seed

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// generatorVersion 是生成算法的版本，写进参数指纹。改了生成逻辑（哪怕参数没变）就要加一：
// 否则续跑时前半段按旧算法、后半段按新算法，混在一起而且查不出来。
const generatorVersion = 1

const (
	// UserBase 是造数用户号的起点（设计 5.3）。对账工具据此把造出来的订单和真实流量分开。
	// 第 k 重的用户（Zipf 的第 k 名）的用户号见 UserID。
	UserBase int64 = 1 << 40
	// MerchantBase 是造数商家号的起点，与真实商家（inventory 里的小号）不重叠。见 MerchantID。
	MerchantBase int64 = 1 << 40
	// SKUBase 是造数 SKU 的起点。第 m 名商家的 SKU 是 SKUBase + m*SKUsPerMerchant + j。
	SKUBase int64 = 1 << 40
	// SKUsPerMerchant 是每个商家的 SKU 数。
	SKUsPerMerchant = 8

	// OrderWorkerID 是生成订单号用的 idgen 节点号，服务不会用到（设计 5.3）。
	OrderWorkerID = 63
	// 支付单号的节点号按日期奇偶交替用 62 和 61，见 assignPaymentIDs。
	paymentWorkerEven = 62
	paymentWorkerOdd  = 61

	// PaymentTimeout 与 order 服务的默认支付时限（service.DefaultPaymentTimeout）一致。
	PaymentTimeout = 15 * time.Minute
	// createTimeout 与 order 服务的默认创建超时（service.DefaultCreateTimeout）一致。
	createTimeout = time.Minute

	currency = "CNY"
	day      = 24 * time.Hour
)

// 订单状态的数值，与 proto 一致。
const (
	statusPaid      = int8(orderv1.OrderStatus_ORDER_STATUS_PAID)
	statusCancelled = int8(orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	statusClosed    = int8(orderv1.OrderStatus_ORDER_STATUS_CLOSED)
	statusFailed    = int8(orderv1.OrderStatus_ORDER_STATUS_FAILED)
)

// Params 是一次造数的全部参数。它们一起决定生成的每一行，写进 seed_progress 的指纹里。
type Params struct {
	// Total 是订单总数。
	Total int64
	// Seed 是全局随机种子。
	Seed uint64
	// From、To 是时间范围 [From, To)，都是 UTC 的零点。
	From, To time.Time
	// Users、Merchants 是用户数和商家数，按 Zipf 抽取。
	Users, Merchants uint64
	// UserS、UserV、MerchantS、MerchantV 是 Zipf 的参数：P(k) 正比于 (v + k)^(-s)，要求 s > 1、v >= 1。
	UserS, UserV         float64
	MerchantS, MerchantV float64
	// DBs 是订单库个数。slot 数固定是 sharding.OrderSlots，库数不影响生成的数据，
	// 但影响数据写到哪个库、进度记在哪个库，所以也算进指纹。
	DBs int
}

// Validate 检查参数。
func (p *Params) Validate() error {
	switch {
	case p.Total <= 0:
		return errors.New("total must be positive")
	case p.Total > 1<<40:
		return errors.New("total too large")
	case !p.From.Equal(p.From.Truncate(day)) || !p.To.Equal(p.To.Truncate(day)):
		return errors.New("from and to must be UTC midnights")
	case p.From.Before(idgen.Epoch):
		return fmt.Errorf("from %s is before idgen.Epoch %s", p.From.Format(time.DateOnly), idgen.Epoch.Format(time.DateOnly))
	case !p.To.After(p.From):
		return errors.New("to must be after from")
	case p.Users < 1 || p.Users > 1<<39:
		return errors.New("users must be in [1, 2^39]")
	case p.Merchants < 1 || p.Merchants > 1<<30:
		return errors.New("merchants must be in [1, 2^30]")
	case p.UserS <= 1 || p.MerchantS <= 1:
		return errors.New("zipf s must be > 1")
	case p.UserV < 1 || p.MerchantV < 1:
		return errors.New("zipf v must be >= 1")
	case p.DBs < 1:
		return errors.New("dbs must be positive")
	}
	if _, err := sharding.New(sharding.OrderLayout(p.DBs)); err != nil {
		return err
	}
	return nil
}

// Fingerprint 返回参数的规范文本，写进 seed_progress 的每一行。两次运行指纹不同就不能续跑。
func (p *Params) Fingerprint() string {
	return fmt.Sprintf("gen=%d total=%d seed=%d from=%s to=%s users=%d user_zipf=%g/%g merchants=%d merchant_zipf=%g/%g slots=%d dbs=%d",
		generatorVersion, p.Total, p.Seed, p.From.Format(time.DateOnly), p.To.Format(time.DateOnly),
		p.Users, p.UserS, p.UserV, p.Merchants, p.MerchantS, p.MerchantV, sharding.OrderSlots, p.DBs)
}

// Days 返回时间范围内的天数。
func (p *Params) Days() int { return int(p.To.Sub(p.From) / day) }

// DayStart 返回第 d 天（从 0 数）的零点。
func (p *Params) DayStart(d int) time.Time { return p.From.Add(time.Duration(d) * day) }

// DayTotal 返回第 d 天全部 slot 合计的订单数：总数按天均分，余数摊在各天上，各天之和恰好等于 Total。
func (p *Params) DayTotal(d int) int64 {
	n := int64(p.Days())
	return p.Total*int64(d+1)/n - p.Total*int64(d)/n
}

// Order 是要写入的一行订单及其明细。MerchantID、PaymentID 为 0，FailReason 为空，PaidAt 为零值时写 NULL。
type Order struct {
	OrderID, UserID, MerchantID int64
	Version                     int64
	Status                      int8
	TotalAmount                 int64
	Currency                    string
	IdempotencyKey              string
	RequestHash                 []byte
	PaymentID                   int64
	FailReason                  string
	ExpireAt, PaidAt            time.Time
	CreatedAt, UpdatedAt        time.Time
	Items                       []Item
}

// Item 是 order_items 的一行，created_at 等于订单的 created_at（与 store.InsertOrder 一致）。
type Item struct {
	SKUID     int64
	Quantity  int32
	UnitPrice int64
}

// splitmix64 把种子和若干个标识混成一个 64 位数，用作随机源的种子。
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func mix(vals ...uint64) uint64 {
	h := uint64(0x6f70_7065_6e68_6569) // "openhei"
	for _, v := range vals {
		h = splitmix64(h ^ v)
	}
	return h
}

// 不同用途的随机源用不同的标签区分，避免两个随机源恰好同种子。
const (
	tagDayUsers = 0x75736572 // "user"
	tagChunk    = 0x6368756e // "chun"
)

func newRand(vals ...uint64) *rand.Rand {
	return rand.New(rand.NewPCG(mix(vals...), mix(append(vals, 1)...))) //nolint:gosec // 造数要确定性的伪随机，不涉及安全
}

// dayUsers 返回第 d 天全部订单的下单用户（按抽取顺序），所有 slot 共用。
func (p *Params) dayUsers(d int) []int64 {
	n := p.DayTotal(d)
	r := newRand(p.Seed, tagDayUsers, uint64(d)) //nolint:gosec // d 非负
	z := rand.NewZipf(r, p.UserS, p.UserV, p.Users-1)
	out := make([]int64, n)
	for i := range out {
		out[i] = UserID(int64(z.Uint64())) //nolint:gosec // 不超过 Users-1 < 2^39
	}
	return out
}

// ChunkUsers 返回第 d 天落在 slot 上的订单的下单用户，按抽取顺序。
func (p *Params) ChunkUsers(slot, d int) []int64 {
	var out []int64
	for _, u := range p.dayUsers(d) {
		if SlotOf(u) == slot {
			out = append(out, u)
		}
	}
	return out
}

// SlotOf 返回用户所在的 slot，与 sharding.Router.ForUser 相同（slot 数与库数无关）。
func SlotOf(userID int64) int { return int(idgen.GeneOf(userID)) & (sharding.OrderSlots - 1) }

// UnitPrice 返回 SKU 的单价（分）：由 SKU 号确定，在 [100, 100000) 之间。
func UnitPrice(sku int64) int64 {
	return 100 + int64(splitmix64(uint64(sku))%99_900) //nolint:gosec // sku 为正，结果小于 10^5
}

// MerchantOfSKU 返回 SKU 所属的商家。
func MerchantOfSKU(sku int64) int64 { return MerchantID((sku - SKUBase) / SKUsPerMerchant) }

// UserID 返回 Zipf 第 rank 名（从 0 数，0 最重）的用户号。
//
// 不能直接用 UserBase + rank：分片看的是低位，那样第 0、8、16 名……全落在 slot 0，第 7、15……全落在 slot 7，
// slot 0 永远最重，量出来的不均匀是编号方式造成的。所以低 3 位取 rank 的哈希，高位是 rank 本身：
// 不同的 rank 得到不同的号，重度用户落在哪个 slot 是随机的。
func UserID(rank int64) int64 { return UserBase + scatterRank(rank) }

// MerchantID 返回 Zipf 第 rank 名商家的商家号，编号方式同 UserID（商家索引按低位分片）。
func MerchantID(rank int64) int64 { return MerchantBase + scatterRank(rank) }

func scatterRank(rank int64) int64 {
	return rank<<3 | int64(splitmix64(uint64(rank))&7) //nolint:gosec // rank 非负
}

// requestHash 与 order 服务的 requestHash 相同：对排好序的 (sku_id, quantity) 做 SHA-256。
func requestHash(items []Item) []byte {
	h := sha256.New()
	var buf [12]byte
	for _, it := range items {
		binary.BigEndian.PutUint64(buf[:8], uint64(it.SKUID))    //nolint:gosec // 正数
		binary.BigEndian.PutUint32(buf[8:], uint32(it.Quantity)) //nolint:gosec // 正数
		h.Write(buf[:])
	}
	return h.Sum(nil)
}

// fakeClock 是给 idgen 的假时钟：sleep 把时间往前拨，不真的睡。
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time        { return c.t }
func (c *fakeClock) sleep(d time.Duration) { c.t = c.t.Add(d) }

// advanceTo 把时钟拨到 t；t 早于当前时间时不动（时钟只往前走）。
func (c *fakeClock) advanceTo(t time.Time) {
	if t.After(c.t) {
		c.t = t
	}
}

// uniform 返回 [lo, hi) 里均匀分布的时长。
func uniform(r *rand.Rand, lo, hi time.Duration) time.Duration {
	return lo + time.Duration(r.Int64N(int64(hi-lo)))
}

// GenerateChunk 生成 slot 在第 d 天的全部订单，按订单号升序。同样的参数总是生成同样的结果。
//
// 每一行的各列按真实流程走完之后的样子来填（services/order/internal/store）：
//
//   - InsertOrder 写入 CREATING：version 0，total_amount 0，币种 CNY，明细单价 0，
//     created_at 是受理时间（订单号里的时间），expire_at 是受理时间加支付时限，updated_at 是落库时间。
//   - MarkReserved（CREATING -> PENDING_PAYMENT）：version 1，写 merchant_id、total_amount、明细单价。
//   - MarkPaid / Cancel / CloseExpired（PENDING_PAYMENT -> 终态）：version 2。
//     只有已支付写 payment_id 和 paid_at（paid_at 等于 updated_at）；关单发生在 expire_at 之后。
//   - MarkCreateFailed（CREATING -> FAILED）：version 1，只写 fail_reason，没有商家、金额，明细单价仍是 0。
func (p *Params) GenerateChunk(slot, d int) ([]Order, error) {
	users := p.ChunkUsers(slot, d)
	if len(users) == 0 {
		return nil, nil
	}
	start := p.DayStart(d)
	r := newRand(p.Seed, tagChunk, uint64(slot), uint64(d)) //nolint:gosec // 非负

	// 当天的受理时间，毫秒精度，升序。订单号按这个顺序生成，主键顺序插入。
	offsets := make([]int64, len(users))
	for i := range offsets {
		offsets[i] = r.Int64N(int64(day / time.Millisecond))
	}
	slices.Sort(offsets)

	clock := &fakeClock{t: start}
	gen, err := idgen.New(OrderWorkerID, idgen.WithClock(clock.now, clock.sleep))
	if err != nil {
		return nil, err
	}
	mz := rand.NewZipf(r, p.MerchantS, p.MerchantV, p.Merchants-1)
	end := start.Add(day)

	orders := make([]Order, len(users))
	for i, user := range users {
		clock.advanceTo(start.Add(time.Duration(offsets[i]) * time.Millisecond))
		id, err := gen.Next(user)
		if err != nil {
			return nil, fmt.Errorf("generate order id: %w", err)
		}
		created := idgen.Time(id).UTC()
		if !created.Before(end) {
			return nil, fmt.Errorf("slot %d day %s: order id time %s spilled into the next day", slot, start.Format(time.DateOnly), created)
		}

		// 一个订单的商品都属于同一个商家（库存服务拒绝跨商家的订单）。
		mRank := int64(mz.Uint64()) //nolint:gosec // 小于 Merchants
		nItems := 1 + r.IntN(3)
		picks := r.Perm(SKUsPerMerchant)[:nItems]
		items := make([]Item, nItems)
		for j, k := range picks {
			items[j] = Item{SKUID: SKUBase + mRank*SKUsPerMerchant + int64(k), Quantity: int32(1 + r.IntN(3))} //nolint:gosec // 1..3
		}
		slices.SortFunc(items, func(a, b Item) int { return cmp.Compare(a.SKUID, b.SKUID) })

		o := Order{
			OrderID:        id,
			UserID:         user,
			Currency:       currency,
			IdempotencyKey: "seed-" + strconv.FormatInt(id, 10),
			RequestHash:    requestHash(items),
			CreatedAt:      created,
			ExpireAt:       created.Add(PaymentTimeout),
			Items:          items,
		}
		// 落库在受理之后几毫秒到几十毫秒（经过 Kafka）。
		landed := created.Add(uniform(r, 2*time.Millisecond, 50*time.Millisecond))

		u := r.Float64()
		if u >= 0.98 { // 创建失败，约 2%
			o.Status, o.Version = statusFailed, 1
			switch v := r.Float64(); {
			case v < 0.85:
				o.FailReason = errx.ReasonInsufficientStock
				o.UpdatedAt = landed.Add(uniform(r, 5*time.Millisecond, 200*time.Millisecond))
			case v < 0.95:
				o.FailReason = errx.ReasonCreateTimeout
				// 后台任务把卡在 CREATING 超过创建超时的订单置为失败。
				o.UpdatedAt = landed.Add(createTimeout + uniform(r, time.Millisecond, 10*time.Second))
			default:
				o.FailReason = errx.ReasonSKUNotFound
				o.UpdatedAt = landed.Add(uniform(r, 5*time.Millisecond, 200*time.Millisecond))
			}
			orders[i] = o
			continue
		}

		// 预占成功：写商家、金额、单价。
		reserved := landed.Add(uniform(r, 5*time.Millisecond, 200*time.Millisecond))
		o.MerchantID = MerchantID(mRank)
		o.Version = 2
		for j := range o.Items {
			it := &o.Items[j]
			it.UnitPrice = UnitPrice(it.SKUID)
			o.TotalAmount += it.UnitPrice * int64(it.Quantity)
		}
		switch {
		case u < 0.70: // 已支付
			o.Status = statusPaid
			o.PaidAt = reserved.Add(uniform(r, 5*time.Second, 10*time.Minute))
			o.UpdatedAt = o.PaidAt
		case u < 0.90: // 超时关闭：截止时间之后由延迟消息或后台任务关闭
			o.Status = statusClosed
			o.UpdatedAt = o.ExpireAt.Add(uniform(r, 10*time.Millisecond, 3*time.Second))
		default: // 已取消，约 8%
			o.Status = statusCancelled
			o.UpdatedAt = reserved.Add(uniform(r, 2*time.Second, 14*time.Minute))
		}
		orders[i] = o
	}
	if err := assignPaymentIDs(orders, d); err != nil {
		return nil, err
	}
	return orders, nil
}

// assignPaymentIDs 给已支付的订单生成支付单号。
//
// 真实的支付单号由 payment 服务用 idgen 以 user_id 为分片键生成。这里同样用 idgen，按支付时间顺序发号，
// 节点号避开服务用的（payment 是 3）和订单用的 63。支付时间可能越过当天零点进入第二天，与第二天的块
// 重叠；两个块各自从头发号，同一毫秒可能发出相同的号。所以节点号按日期奇偶交替（62、61）：
// 第 d 天的支付时间不会晚于第 d+1 天结束，碰不到同奇偶的第 d+2 天。
func assignPaymentIDs(orders []Order, d int) error {
	var paid []*Order
	for i := range orders {
		if orders[i].Status == statusPaid {
			paid = append(paid, &orders[i])
		}
	}
	slices.SortStableFunc(paid, func(a, b *Order) int { return a.PaidAt.Compare(b.PaidAt) })
	worker := int64(paymentWorkerEven)
	if d%2 == 1 {
		worker = paymentWorkerOdd
	}
	clock := &fakeClock{}
	gen, err := idgen.New(worker, idgen.WithClock(clock.now, clock.sleep))
	if err != nil {
		return err
	}
	for _, o := range paid {
		clock.advanceTo(o.PaidAt)
		if o.PaymentID, err = gen.Next(o.UserID); err != nil {
			return fmt.Errorf("generate payment id: %w", err)
		}
	}
	return nil
}
