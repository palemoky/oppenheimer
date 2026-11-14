// Package service 实现 OrderService 的业务逻辑、事件消费与后台任务。
//
// 一致性的基本做法：订单的每个状态转换和描述它的 OrderEvent 写在同一个本地事务里（outbox），
// 由 outbox 中继发到 Kafka，库存和支付服务消费事件跟上订单状态。
//
// 下单分两段（设计文档 6.3）：受理（SubmitOrder）只碰 Redis 里的库存闸门和 Kafka，立即返回；
// 落库（HandleOrderSubmitted）消费 order.submit，写订单行并同步 Reserve。Reserve 以 order_id 幂等。
package service

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/palemoky/oppenheimer/api/eventx"
	commonv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/common/v1"
	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/breaker"
	"github.com/palemoky/oppenheimer/pkg/cache"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/outbox"
	"github.com/palemoky/oppenheimer/pkg/retry"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/services/order/internal/archive"
	"github.com/palemoky/oppenheimer/services/order/internal/readmark"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// ReasonCreateTimeout 是订单因迟迟没能完成预占而被后台任务置为失败时的 fail_reason。
const ReasonCreateTimeout = errx.ReasonCreateTimeout

// DefaultPaymentTimeout 是下单后等待支付的默认时限。
const DefaultPaymentTimeout = 15 * time.Minute

// DefaultIdempotencyTTL 是幂等键在 Redis 里的默认保留时长。
const DefaultIdempotencyTTL = 24 * time.Hour

// DefaultSubmitTimeout 是受理一个下单请求的时限（闸门、按需预热、发消息合计）。
// 调用方给的 deadline 更短时以调用方为准。
const DefaultSubmitTimeout = 3 * time.Second

// 预占成功前写入的币种占位，与表的默认值一致，预占成功后改为库存服务给出的币种。
const placeholderCurrency = "CNY"

const maxIdempotencyKeyLen = 64

// IDGenerator 生成订单号。*idgen.Generator 满足此接口。
type IDGenerator interface {
	Next(shardKey int64) (int64, error)
}

// Kicker 在事务提交后被调用，让 outbox 中继立即发送。*outbox.Relay 满足此接口。
type Kicker interface {
	Kick()
}

// Config 是 Service 的可选配置，零值字段使用默认值。
type Config struct {
	// PaymentTimeout 是下单后等待支付的时限，默认 DefaultPaymentTimeout。
	PaymentTimeout time.Duration
	// IdempotencyTTL 是幂等键在闸门里的保留时长，默认 DefaultIdempotencyTTL。
	IdempotencyTTL time.Duration
	// SubmitTimeout 是受理一个请求的时限，默认 DefaultSubmitTimeout。
	SubmitTimeout time.Duration
	// Clock 返回当前时间，默认 time.Now。测试里注入以控制超时。
	Clock func() time.Time
	// OrderEventsTopic 是 OrderEvent 写入 outbox 时的 topic（已带环境前缀），默认 eventx.OrderEvents。
	OrderEventsTopic string
	// SubmitTopic、CloseDelayTopic 是受理时发出的两条消息的 topic（已带环境前缀），
	// 默认 eventx.OrderSubmit、eventx.OrderCloseDelay。
	SubmitTopic     string
	CloseDelayTopic string
	// Relays 是各库的 outbox 中继，下标是库号（与 store 的库号一致）。可选，也可以只给一部分或含 nil：
	// 缺的库事件只靠中继的兜底轮询发出，正确性不受影响，只是延迟变大。
	// 事务提交后只踢事务所在库的中继：别的库的 outbox 里没有这次写入的行。
	Relays []Kicker
	// OrderCacheTTL 是终态订单在查询缓存里的保留时长，默认 DefaultOrderCacheTTL。
	// 只在 Deps.OrderCache 非空时有意义。
	OrderCacheTTL time.Duration
	// OrderCacheNullTTL 是“订单不存在”在查询缓存里的保留时长，默认 DefaultOrderCacheNullTTL。
	OrderCacheNullTTL time.Duration
	// BreakerFailures、BreakerCooldown 是进程里各熔断器（闸门、落库健康）的参数，
	// 默认 breaker.DefaultFailures、breaker.DefaultCooldown。测试里调小。
	BreakerFailures int
	BreakerCooldown time.Duration
	// Backlog 是受理端的背压（stockgate.BacklogLimit），零值表示不限制。
	// MaxWait 必须小于回收任务的宽限期，由调用方保证（见 cmd/order 的配置检查）。
	Backlog stockgate.BacklogLimit
	// ReadMode 决定 GetOrder 能否读从库，见 ReadMode 的说明。零值 ReadPrimary。
	// 不是 ReadPrimary 时 store 必须配置了从库、Deps.Marks 不能为空，否则 New 返回错误。
	ReadMode ReadMode
	// ReadPinTTL 是写之前打的 pin 的保留时长，默认 DefaultReadPinTTL。
	// 它要盖住“写入耗时 + 从库的正常延迟”，pin 到期之后的读（ReadWindow 下）就交给从库了。
	ReadPinTTL time.Duration
	// ReadGTIDTTL 是写入后记下的主库位置的保留时长，默认 DefaultReadGTIDTTL。
	ReadGTIDTTL time.Duration
	// ReadGTIDWait 是 ReadGTID 下展示读等从库追上的最长时间，默认 DefaultReadGTIDWait。等不到就读主库。
	ReadGTIDWait time.Duration
	// ReadMaxConcurrency 是每个订单库上展示读（GetOrder 读订单行、ListUserOrders）的并发上限，
	// 见 DefaultReadMaxConcurrency。零值表示不限制（测试用；cmd/order 总会给一个值）。
	ReadMaxConcurrency int
	// ArchiveMaxConcurrency 是读冷库的并发上限，见 DefaultArchiveMaxConcurrency。零值表示不限制。
	ArchiveMaxConcurrency int
}

// ReadMode 决定对外的展示读（GetOrder）读主库还是从库。
//
// 原则：用来做决定的读一律走主库，只用来展示的读才可能走从库，走从库的读的结果不能成为任何写的前提。
// 所以无论哪种模式，写路径、状态机、CancelOrder 之后的复查、受理时的幂等查询都读主库。
type ReadMode int

const (
	// ReadPrimary 全部读主库，不读写标记。
	ReadPrimary ReadMode = iota
	// ReadWindow 写之前打 pin，pin 还在时读主库，否则读从库。用一个固定的时间窗口假设从库的延迟，
	// 延迟超过窗口时会读到旧数据。
	ReadWindow
	// ReadGTID 在 ReadWindow 的基础上，写入提交后记下主库的 GTID 位置；pin 过期后的读先让从库等到这个位置，
	// 等到了读从库，等不到读主库。只要位置标记还在，就读不到早于它的数据。
	ReadGTID
)

// String 返回配置里使用的名字。
func (m ReadMode) String() string {
	switch m {
	case ReadPrimary:
		return "primary"
	case ReadWindow:
		return "window"
	case ReadGTID:
		return "gtid"
	default:
		return "ReadMode(" + strconv.Itoa(int(m)) + ")"
	}
}

// 读己之写的默认配置。
const (
	DefaultReadPinTTL   = 3 * time.Second
	DefaultReadGTIDTTL  = time.Minute
	DefaultReadGTIDWait = 100 * time.Millisecond
)

// markTimeout 是读写标记、读主库位置各自的时限。这些都是附带的步骤，Redis 或主库抖一下不应拖慢落库和查询。
const markTimeout = 500 * time.Millisecond

// Deps 是 Service 依赖的外部组件。
type Deps struct {
	Store     *store.Store
	Gate      *stockgate.Gate
	Sender    kafkax.Sender
	Inventory inventoryv1.InventoryServiceClient
	IDs       IDGenerator
	// Logger 为空时用 slog.Default()。
	Logger *slog.Logger
	// OrderCache 可选，GetOrder 的查询缓存（设计文档 7.2.3），用 NewOrderCache 创建。
	// 为空时 GetOrder 每次都读库。
	OrderCache *cache.Cache[*orderv1.Order]
	// Marks 是读己之写的标记（闸门所在的 Redis 实例）。Config.ReadMode 为 ReadPrimary 时可以为空。
	Marks *readmark.Marks
	// Degrade 可选，本服务的降级开关（设计文档 1d 的 6.9）。Service 把自己的开关登记到上面。
	// 为空时用一个私有的 Set：开关都在默认值，代码路径相同。
	Degrade *degrade.Set
	// Cold 可选，冷库（已归档订单，设计文档 1c 第 6 节）。为空时没有冷库路径：
	// 热库里没有的订单就是不存在。有数据归档之后必须配置，否则已归档的订单查不到，
	// 针对它们的支付结果会被当成“订单不存在”而要求退款。
	Cold *store.Cold
	// ColdMigrate 可选，给冷库建表。非空时冷库在它第一次成功之前不可用（展示读立刻返回 ARCHIVE_UNAVAILABLE），
	// 由 ArchiveMigrator 在后台重试到成功（设计文档 1d 的 6.8）；为空表示冷库的表已经建好。
	ColdMigrate func(ctx context.Context) error
}

// Service 实现 orderv1.OrderServiceServer。
type Service struct {
	orderv1.UnimplementedOrderServiceServer

	store     *store.Store
	gate      *stockgate.Gate
	sender    kafkax.Sender
	inventory inventoryv1.InventoryServiceClient
	// warmUps 合并对同一组 SKU 的并发预热，见 warmUp。
	warmUps        singleflight.Group
	ids            IDGenerator
	logger         *slog.Logger
	paymentTimeout time.Duration
	idemTTL        time.Duration
	submitTimeout  time.Duration
	clock          func() time.Time
	eventsTopic    string
	submitTopic    string
	closeTopic     string
	relays         []Kicker
	orderCache     *cache.Cache[*orderv1.Order]
	cacheTTL       time.Duration
	cacheNullTTL   time.Duration
	backlog        stockgate.BacklogLimit
	readMode       ReadMode
	marks          *readmark.Marks
	pinTTL         time.Duration
	gtidTTL        time.Duration
	gtidWait       time.Duration
	cold           *store.Cold
	// limits 是展示读和冷库读的并发上限，见 protect.go。
	limits readLimits

	// 以下是降级（设计文档 1d 第 6 节），见 degrade.go。
	degrade *degrade.Set
	// swAdmission 关掉时拒绝新的下单。
	swAdmission *degrade.Switch
	// gateBreaker 保护闸门所在的 Redis，所有闸门调用都经过它（gateCall）。
	gateBreaker *breaker.Breaker
	// gateRecoveredAt 是 gateBreaker 最近一次回到关闭的时刻（UnixNano），0 表示没有打开过。
	gateRecoveredAt atomic.Int64
	// landing 是每个订单库的落库健康（下标是库号），见 admitLanding。
	landing         []*landingHealth
	landingCooldown time.Duration
	// 读路径的降级，见 readdegrade.go。
	// replicas 是每个订单库的从库的健康标记（下标是库号），没有配置从库时为空。
	replicas  []*replicaHealth
	swReplica *degrade.Switch
	// coldBreaker 保护冷库；coldReady 表示冷库的表已经建好（ArchiveMigrator），coldMigrate 是建表的函数。
	coldBreaker *breaker.Breaker
	coldReady   atomic.Bool
	coldMigrate func(ctx context.Context) error
	swArchive   *degrade.Switch
}

// New 返回 Service。读模式与依赖不匹配（要读从库却没有从库或标记）时返回错误：
// 这种配置下要么读不到从库，要么读从库而没有任何新鲜度保护，都不应该悄悄运行。
func New(d Deps, cfg Config) (*Service, error) {
	switch cfg.ReadMode {
	case ReadPrimary:
	case ReadWindow, ReadGTID:
		if d.Store == nil || !d.Store.HasReplicas() {
			return nil, fmt.Errorf("service: read mode %s needs replicas in the store", cfg.ReadMode)
		}
		if d.Marks == nil {
			return nil, fmt.Errorf("service: read mode %s needs read marks", cfg.ReadMode)
		}
	default:
		return nil, fmt.Errorf("service: unknown read mode %d", int(cfg.ReadMode))
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	logger := d.Logger
	if logger == nil {
		logger = slog.Default()
	}
	var dbs int
	if d.Store != nil {
		dbs = d.Store.Router().DBs()
	}
	limits, err := newReadLimits(dbs, cfg.ReadMaxConcurrency, cfg.ArchiveMaxConcurrency)
	if err != nil {
		return nil, err
	}
	set := d.Degrade
	if set == nil {
		set = degrade.New("order", logger)
	}
	s := &Service{
		store:          d.Store,
		gate:           d.Gate,
		sender:         d.Sender,
		inventory:      d.Inventory,
		ids:            d.IDs,
		logger:         logger,
		paymentTimeout: cmp.Or(cfg.PaymentTimeout, DefaultPaymentTimeout),
		idemTTL:        cmp.Or(cfg.IdempotencyTTL, DefaultIdempotencyTTL),
		submitTimeout:  cmp.Or(cfg.SubmitTimeout, DefaultSubmitTimeout),
		clock:          clock,
		eventsTopic:    cmp.Or(cfg.OrderEventsTopic, eventx.OrderEvents),
		submitTopic:    cmp.Or(cfg.SubmitTopic, eventx.OrderSubmit),
		closeTopic:     cmp.Or(cfg.CloseDelayTopic, eventx.OrderCloseDelay),
		relays:         cfg.Relays,
		orderCache:     d.OrderCache,
		cacheTTL:       cmp.Or(cfg.OrderCacheTTL, DefaultOrderCacheTTL),
		cacheNullTTL:   cmp.Or(cfg.OrderCacheNullTTL, DefaultOrderCacheNullTTL),
		backlog:        cfg.Backlog,
		readMode:       cfg.ReadMode,
		marks:          d.Marks,
		pinTTL:         cmp.Or(cfg.ReadPinTTL, DefaultReadPinTTL),
		gtidTTL:        cmp.Or(cfg.ReadGTIDTTL, DefaultReadGTIDTTL),
		gtidWait:       cmp.Or(cfg.ReadGTIDWait, DefaultReadGTIDWait),
		cold:           d.Cold,
		limits:         limits,
		degrade:        set,
	}
	cooldown := cmp.Or(cfg.BreakerCooldown, breaker.DefaultCooldown)
	s.gateBreaker = s.newBreaker(gateBreakerName, cfg.BreakerFailures, cooldown)
	s.landingCooldown = cooldown
	s.landing = make([]*landingHealth, dbs)
	for i := range s.landing {
		s.landing[i] = &landingHealth{b: s.newBreaker("landing-db-"+strconv.Itoa(i), cfg.BreakerFailures, cooldown)}
	}
	s.swAdmission = set.Switch(SwitchAdmission, "reject new orders (503); queries, cancellation and payment keep working")
	s.swAdmission.Watch(s.admissionStatus)

	// 从库：初始都不健康，探测（ReplicaProber）连续通过之后才读（设计文档 1d 的 6.7）。
	if d.Store != nil && d.Store.HasReplicas() {
		s.replicas = make([]*replicaHealth, dbs)
		for i := range s.replicas {
			s.replicas[i] = newReplicaHealth()
		}
	}
	s.swReplica = set.Switch(SwitchReplicaRead, "display reads use the primary only")
	s.swReplica.Watch(s.replicaStatus)
	// 冷库：有建表函数时等它成功（ArchiveMigrator），没有时认为表已经建好。
	s.coldBreaker = s.newBreaker("archive", cfg.BreakerFailures, cooldown)
	s.coldMigrate = d.ColdMigrate
	s.coldReady.Store(d.Cold != nil && d.ColdMigrate == nil)
	s.swArchive = set.Switch(SwitchArchiveRead,
		"archived orders are not read: their queries return 503, user lists stop at the hot/cold boundary")
	s.swArchive.Watch(s.archiveStatus)
	return s, nil
}

// now 截断到微秒：列是 DATETIME(6)，MySQL 写入时会四舍五入多余的精度，
// 不截断的话内存里的时间和库里的会差一点，时限比较可能差一个边界。
func (s *Service) now() time.Time {
	return s.clock().UTC().Truncate(time.Microsecond)
}

var errInternal = status.Error(codes.Internal, "internal error")

// errShardUnavailable 是订单所在的分片暂时不可读写时返回给调用方的错误，见 internal。
var errShardUnavailable = status.Error(codes.Unavailable, "order shard is temporarily unavailable, retry later")

func invalidArgument(format string, args ...any) error {
	return errx.New(codes.InvalidArgument, errx.ReasonInvalidArgument, format, args...)
}

func orderNotFound(orderID int64) error {
	return errx.New(codes.NotFound, errx.ReasonOrderNotFound, "order %d not found", orderID)
}

// internal 记录真实原因后返回不带细节的 Internal。
// op 说明是哪一步失败，作为日志字段而不是消息，保持消息为常量便于检索。
//
// ctx 必须是请求自己的 ctx（不是为某一跳另设时限的子 ctx）：超时和取消按它来区分是调用方的还是这一跳的，
// 见 contextError。
func (s *Service) internal(ctx context.Context, op string, err error, attrs ...any) error {
	// 分片表不存在：扩容切换期间这个 slot 的表在旧库上被改名作围栏（ADR-0015），几秒后路由切到新库。
	// 这是“暂时不可用、稍后重试”，不是内部错误；告诉调用方可以重试，日志也不按 ERROR 记。
	// 排在超时和取消之前：这个错误是 MySQL 明确答复的，不会是超时或取消造成的。
	if mysqlx.IsNoSuchTable(err) {
		s.logger.WarnContext(ctx, "shard unavailable", append(attrs, "op", op, "error", err)...)
		return errShardUnavailable
	}
	// 调用方取消、调用方超时、这一跳超时：都不是本服务的内部错误（设计文档 1d 的 5.9）。
	if cerr := s.contextError(ctx, op, err, attrs...); cerr != nil {
		return cerr
	}
	s.logger.ErrorContext(ctx, "internal error", append(attrs, "op", op, "error", err)...)
	return errInternal
}

// ---------- 校验与请求哈希 ----------

// validateSubmit 校验请求，返回按 sku_id 排序的明细。
func validateSubmit(req *orderv1.SubmitOrderRequest) ([]store.Item, error) {
	if req.GetUserId() <= 0 {
		return nil, invalidArgument("user_id must be positive")
	}
	if n := len(req.GetIdempotencyKey()); n < 1 || n > maxIdempotencyKeyLen {
		return nil, invalidArgument("idempotency_key must be 1 to %d characters", maxIdempotencyKeyLen)
	}
	if len(req.GetItems()) == 0 {
		return nil, invalidArgument("items must not be empty")
	}
	items := make([]store.Item, 0, len(req.GetItems()))
	for _, it := range req.GetItems() {
		if it.GetSkuId() <= 0 {
			return nil, invalidArgument("sku_id must be positive")
		}
		if it.GetQuantity() <= 0 {
			return nil, invalidArgument("quantity of sku %d must be positive", it.GetSkuId())
		}
		items = append(items, store.Item{SKUID: it.GetSkuId(), Quantity: it.GetQuantity()})
	}
	sortItems(items)
	for i := 1; i < len(items); i++ {
		if items[i].SKUID == items[i-1].SKUID {
			return nil, invalidArgument("duplicate sku_id %d", items[i].SKUID)
		}
	}
	return items, nil
}

func sortItems(items []store.Item) {
	slices.SortFunc(items, func(a, b store.Item) int { return cmp.Compare(a.SKUID, b.SKUID) })
}

// requestHash 对排好序的 (sku_id, quantity) 序列做 SHA-256。
// 定长大端编码，不同序列不会拼出相同的字节串。
func requestHash(sorted []store.Item) []byte {
	h := sha256.New()
	var buf [12]byte
	for _, it := range sorted {
		binary.BigEndian.PutUint64(buf[:8], uint64(it.SKUID))    //nolint:gosec // 已校验为正数
		binary.BigEndian.PutUint32(buf[8:], uint32(it.Quantity)) //nolint:gosec // 已校验为正数
		h.Write(buf[:])
	}
	return h.Sum(nil)
}

// ---------- 推进：Reserve ----------

// ErrUnrecoverable 表示订单卡在了重试也解决不了的状态（预占结果不可用、预占明细不一致），
// 需要人工介入。订单保持 CREATING，由 sweeper 按创建超时置为失败并释放库存。
// Kafka 适配层把它转成 kafkax.Permanent。
var ErrUnrecoverable = errors.New("unrecoverable")

// errReserveUnknown 表示 Reserve 的结果未知（超时、不可达），预占可能成功也可能没有。
// 订单保持 CREATING，原地重试即可：Reserve 以 order_id 幂等。
var errReserveUnknown = errors.New("reserve outcome unknown")

// advance 把一个已落库的订单往前推进一步，返回推进后的订单。对任意状态的订单都可以安全地重复调用：
// Reserve 以 order_id 幂等，状态更新都带前置条件。
//
// 被库存拒绝不是错误：订单置为 FAILED（同事务写 failed 事件）后正常返回。
// 返回错误时订单保持 CREATING：errReserveUnknown 和数据库错误可以重试，ErrUnrecoverable 不行。
func (s *Service) advance(ctx context.Context, o *store.Order) (*store.Order, error) {
	if o.Status != orderv1.OrderStatus_ORDER_STATUS_CREATING {
		return o, nil
	}

	reserveItems := make([]*inventoryv1.ReserveItem, 0, len(o.Items))
	for _, it := range o.Items {
		reserveItems = append(reserveItems, &inventoryv1.ReserveItem{SkuId: it.SKUID, Quantity: it.Quantity})
	}
	resp, err := s.inventory.Reserve(ctx, &inventoryv1.ReserveRequest{OrderId: o.OrderID, Items: reserveItems})
	if err != nil {
		return s.handleReserveError(ctx, o, err)
	}

	priced, total, currency, err := priceItems(o.Items, resp.GetItems())
	if err != nil {
		// 预占已成功但结果不可用，且重试也会得到同样的结果。保持 CREATING，
		// 由后台任务按创建超时置为失败并释放库存。
		return nil, fmt.Errorf("%w: invalid reserve response for order %d: %w", ErrUnrecoverable, o.OrderID, err)
	}
	merchantID := resp.GetMerchantId()
	if merchantID <= 0 {
		// 同上：预占已成功，但没有商家的订单进不了商家维度的索引，不能让它走到待支付。
		return nil, fmt.Errorf("%w: reserve response for order %d has no merchant", ErrUnrecoverable, o.OrderID)
	}
	now := s.now()
	s.pin(ctx, o.OrderID)
	ok, err := s.store.MarkReserved(ctx, o.OrderID, merchantID, total, currency, priced, now)
	if err != nil {
		return nil, fmt.Errorf("mark order %d reserved: %w", o.OrderID, err)
	}
	if ok {
		s.stamp(ctx, o.OrderID)
		o.Status = orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT
		o.MerchantID = merchantID
		o.TotalAmount = total
		o.Currency = currency
		o.Items = priced
		o.UpdatedAt = now
		return o, nil
	}

	// 走到这里说明订单在 Reserve 期间被别人改了状态。若它已是 FAILED，本次成功的预占
	// 不需要在这里处理：把订单置为 FAILED 的一方在同一个事务里写了 failed 事件，
	// 库存服务消费它时会 Release 掉这份预占（或留下“已释放”标记挡住还在路上的 Reserve）。
	return s.reload(ctx, o.OrderID)
}

func (s *Service) reload(ctx context.Context, orderID int64) (*store.Order, error) {
	cur, err := s.store.GetOrder(ctx, orderID)
	if err != nil {
		return nil, fmt.Errorf("reload order %d: %w", orderID, err)
	}
	return cur, nil
}

func (s *Service) handleReserveError(ctx context.Context, o *store.Order, reserveErr error) (*store.Order, error) {
	switch reason := errx.Reason(reserveErr); reason {
	case errx.ReasonSKUNotFound, errx.ReasonInsufficientStock, errx.ReasonReservationReleased, errx.ReasonMixedMerchants:
		// 拒绝只说明“这一次”Reserve 没有占到库存，不能推出“这个订单没有预占”：
		// 同一订单可能有另一次推进在并发进行（同一条消息被重复投递），库存恰好在两次之间被释放出来，
		// 那一次的 Reserve 就会成功。所以不区分原因，一律置为 FAILED 并发 failed 事件，
		// 由库存服务 Release：它释放可能存在的预占，没有预占时留下标记挡住之后的 Reserve，
		// 之后再归还闸门里的占用。Release 幂等，多一次无害；这条路径上没有对库存的同步写调用。
		now := s.now()
		ok, err := s.failCreating(ctx, store.OrderRef{OrderID: o.OrderID, UserID: o.UserID}, reason, now)
		if err != nil {
			return nil, fmt.Errorf("mark order %d failed: %w", o.OrderID, err)
		}
		if !ok {
			return s.reload(ctx, o.OrderID)
		}
		o.Status = orderv1.OrderStatus_ORDER_STATUS_FAILED
		o.FailReason = reason
		o.UpdatedAt = now
		return o, nil
	case errx.ReasonReservationMismatch:
		// 每次推进发出的明细都来自同一份落库数据，不可能不一致。不自作主张修复。
		return nil, fmt.Errorf("%w: reservation mismatch for order %d: %w", ErrUnrecoverable, o.OrderID, reserveErr)
	default:
		// 超时、不可达等：预占可能成功也可能没有。保持 CREATING，由调用方重试。
		return nil, fmt.Errorf("order %d: %w: %w", o.OrderID, errReserveUnknown, reserveErr)
	}
}

// priceItems 按 Reserve 返回的单价计算各商品单价和总金额，检查明细一致、币种一致和溢出。
func priceItems(items []store.Item, reserved []*inventoryv1.ReservedItem) ([]store.Item, int64, string, error) {
	if len(reserved) != len(items) {
		return nil, 0, "", fmt.Errorf("reserve returned %d items, want %d", len(reserved), len(items))
	}
	bySKU := make(map[int64]*inventoryv1.ReservedItem, len(reserved))
	for _, r := range reserved {
		bySKU[r.GetSkuId()] = r
	}
	var (
		total    int64
		currency string
	)
	priced := make([]store.Item, 0, len(items))
	for _, it := range items {
		r, ok := bySKU[it.SKUID]
		if !ok || r.GetQuantity() != it.Quantity {
			return nil, 0, "", fmt.Errorf("reserve result does not match sku %d", it.SKUID)
		}
		price := r.GetUnitPrice()
		if price == nil || price.GetAmount() < 0 || len(price.GetCurrency()) != 3 {
			return nil, 0, "", fmt.Errorf("invalid unit price for sku %d", it.SKUID)
		}
		if currency == "" {
			currency = price.GetCurrency()
		} else if price.GetCurrency() != currency {
			return nil, 0, "", fmt.Errorf("mixed currencies %s and %s", currency, price.GetCurrency())
		}
		line, ok := mulAmount(price.GetAmount(), int64(it.Quantity))
		if !ok {
			return nil, 0, "", fmt.Errorf("amount overflow for sku %d", it.SKUID)
		}
		if total, ok = addAmount(total, line); !ok {
			return nil, 0, "", errors.New("total amount overflow")
		}
		priced = append(priced, store.Item{SKUID: it.SKUID, Quantity: it.Quantity, UnitPrice: price.GetAmount()})
	}
	return priced, total, currency, nil
}

// mulAmount 计算非负金额与正数量的积，溢出时返回 false。
func mulAmount(amount, quantity int64) (int64, bool) {
	if amount < 0 || quantity <= 0 {
		return 0, false
	}
	if amount > math.MaxInt64/quantity {
		return 0, false
	}
	return amount * quantity, true
}

// addAmount 计算两个非负金额的和，溢出时返回 false。
func addAmount(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}

func toProto(o *store.Order) *orderv1.Order {
	items := make([]*orderv1.OrderItem, 0, len(o.Items))
	for _, it := range o.Items {
		items = append(items, &orderv1.OrderItem{
			SkuId:     it.SKUID,
			Quantity:  it.Quantity,
			UnitPrice: &commonv1.Money{Amount: it.UnitPrice, Currency: o.Currency},
		})
	}
	p := &orderv1.Order{
		OrderId:    o.OrderID,
		UserId:     o.UserID,
		Status:     o.Status,
		Total:      &commonv1.Money{Amount: o.TotalAmount, Currency: o.Currency},
		Items:      items,
		CreateTime: timestamppb.New(o.CreatedAt),
		ExpireTime: timestamppb.New(o.ExpireAt),
	}
	if !o.PaidAt.IsZero() {
		p.PayTime = timestamppb.New(o.PaidAt)
	}
	return p
}

// cacheable 判断某个状态的订单能否放进查询缓存（设计文档 7.2.3）：只有终态可以。
// 终态不会再变，缓存永远不会和数据库不一致，所以不需要任何失效逻辑。
//
// 以后如果终态之后还能变（发货、退款），这里必须跟着改，否则缓存会返回旧状态。
// 测试要求枚举里每个状态都在这里明确归类，新增状态时测试失败，逼着做决定。
func cacheable(st orderv1.OrderStatus) bool {
	switch st {
	case orderv1.OrderStatus_ORDER_STATUS_PAID,
		orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
		orderv1.OrderStatus_ORDER_STATUS_CLOSED,
		orderv1.OrderStatus_ORDER_STATUS_FAILED:
		return true
	case orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED,
		orderv1.OrderStatus_ORDER_STATUS_CREATING,
		orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT:
		return false
	default:
		return false
	}
}

// ---------- GetOrder ----------

// 查询缓存的默认保留时长。
const (
	DefaultOrderCacheTTL     = 10 * time.Minute
	DefaultOrderCacheNullTTL = 2 * time.Second
)

// NewOrderCache 创建 GetOrder 用的查询缓存。client 应当是允许淘汰的缓存实例，prefix 遵守 redisx 的前缀约定。
//
// 不做进程间的重建互斥：按订单号的 key 没有热点，而非终态订单的回源结果又不写缓存，
// 互斥多出的两次 Redis 往返换不来东西（设计文档 7.2.5）。
//
// set 不为 nil 时把缓存的开关（SwitchCache）登记到它上面：关掉之后查询不经过缓存。
// 缓存的 Redis 出错时由 cache 包里的熔断器自动跳过（设计文档 1d 的 6.6），GET /degrade 上能看到它的状态。
func NewOrderCache(client redis.Cmdable, prefix string, logger *slog.Logger, set *degrade.Set) (*cache.Cache[*orderv1.Order], error) {
	var sw *degrade.Switch
	if set != nil {
		sw = set.Switch(SwitchCache, "order queries skip the cache and read the database")
	}
	c, err := cache.New(cache.Config{
		Client:      client,
		KeyPrefix:   prefix,
		DisableLock: true,
		Logger:      logger,
		Switch:      sw,
		Breaker: breaker.New(breaker.Config{Name: "order-cache", OnChange: func(name string, from, to breaker.State) {
			logger.Warn("cache redis breaker changed", "breaker", name, "from", from.String(), "to", to.String())
		}}),
	}, cache.ProtoCodec[*orderv1.Order]{})
	if err != nil {
		return nil, fmt.Errorf("new order cache: %w", err)
	}
	if sw != nil {
		sw.Watch(func() degrade.Status {
			st := c.Breaker().Stats()
			return degrade.Status{
				Degraded: st.State != breaker.Closed,
				Detail:   fmt.Sprintf("redis=%s(opens=%d,rejected=%d)", st.State, st.Opens, st.Rejected),
			}
		})
	}
	return c, nil
}

func orderCacheKey(orderID int64) string {
	return "cache:order:" + strconv.FormatInt(orderID, 10)
}

// GetOrder 实现 OrderServiceServer。
//
// 只有这条读路径走缓存。写路径、状态机和内部决策（例如 CancelOrder 之后的复查）都直接读库：
// 缓存里只有终态订单，读到它们本身不会出错，但没有理由让决策依赖一个可选组件。
func (s *Service) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	if req.GetOrderId() <= 0 || req.GetUserId() <= 0 {
		return nil, invalidArgument("order_id and user_id must be positive")
	}
	var (
		o   *orderv1.Order
		ok  bool
		err error
	)
	if s.orderCache == nil {
		o, ok, err = s.fetch(ctx, req.GetOrderId(), true)
	} else {
		o, ok, err = s.fetchCached(ctx, req.GetOrderId())
	}
	if err != nil {
		return nil, err
	}
	// 缓存的内容与请求方无关，归属在拿到结果之后核对（命中和未命中两条路径一样）。
	if !ok || o.GetUserId() != req.GetUserId() {
		return nil, orderNotFound(req.GetOrderId())
	}
	// o 可能是缓存或 singleflight 里与其他调用共享的对象，这里只读不改。
	return &orderv1.GetOrderResponse{Order: o}, nil
}

// fetchCached 是带查询缓存的 fetch。回源结果按内容决定是否缓存：
//   - 终态订单：缓存 cacheTTL。
//   - 非终态订单（包括从占用合成的 CREATING）：不缓存。
//   - 不存在（行和占用都没有，且复查仍没有）：缓存空值 cacheNullTTL。
//
// 已受理未落库的订单在 fetch 里从占用合成，Found 为 true，所以不会被写成空值。
func (s *Service) fetchCached(ctx context.Context, orderID int64) (*orderv1.Order, bool, error) {
	o, ok, err := s.orderCache.Get(ctx, orderCacheKey(orderID), func(ctx context.Context) (cache.Loaded[*orderv1.Order], error) {
		o, ok, err := s.fetch(ctx, orderID, true)
		if err != nil {
			return cache.Loaded[*orderv1.Order]{}, err
		}
		if !ok {
			return cache.Loaded[*orderv1.Order]{TTL: s.cacheNullTTL}, nil
		}
		l := cache.Loaded[*orderv1.Order]{Value: o, Found: true}
		if cacheable(o.GetStatus()) {
			l.TTL = s.cacheTTL
		}
		return l, nil
	})
	if err != nil {
		// fetch 的错误已经是对外的 gRPC 状态；缓存层在调用方 ctx 结束时返回裸的 context 错误，
		// 转成对应的状态码（Canceled、DeadlineExceeded），否则 gRPC 会把它报成 Unknown。
		if _, isStatus := status.FromError(err); !isStatus {
			return nil, false, status.FromContextError(err).Err()
		}
		return nil, false, err
	}
	return o, ok, nil
}

// lookup 返回属于 userID 的订单的当前状态。不属于该用户时与不存在一样返回 NOT_FOUND，不泄露订单是否存在。
// 它不读缓存、只读主库，给需要据此作答或做决定的路径（CancelOrder、受理时幂等键重复）用。
func (s *Service) lookup(ctx context.Context, orderID, userID int64) (*orderv1.Order, error) {
	o, ok, err := s.fetch(ctx, orderID, false)
	if err != nil {
		return nil, err
	}
	if !ok || o.GetUserId() != userID {
		return nil, orderNotFound(orderID)
	}
	return o, nil
}

// fetch 按 order_id 返回订单的当前状态，不核对归属。ok 为 false 表示订单不存在。
// 返回的错误已经是对外的 gRPC 状态。
//
// 先读闸门里的占用，再决定要不要读订单库（设计文档 1d 的 2.4）。占用从受理一直活到订单进入终态，
// 它的状态说明了订单行可能在哪里：
//   - 有占用、未认领：落库还没开始。落库流程先认领再插入订单行，所以主库上不可能有这一行。
//     直接从占用合成一个 CREATING 的订单（只有商品和数量），不读订单库。排队等落库的订单被轮询时
//     只打 Redis，不和落库抢订单库的连接。
//   - 有占用、已认领：订单行可能已经在主库上（也可能是“认领了、行还没插入”的那一小段）。
//     读订单行，没有就从占用合成。
//   - 没有占用：订单从未受理过、已被回收，或者已经走到终态、占用已归还。读订单行。
//
// 为什么没有占用时读一次行就够了：订单走到终态是先提交订单行、发事件，库存服务消费事件之后才归还占用。
// 所以“占用已经没有了”的那一刻，终态的订单行早已提交，随后这次读（主库）一定看得到；读不到就是不存在。
// 原来的顺序是先读行、再读占用、再读一次行，第三步就是为了排除“两次读之间订单走完了终态”，
// 先读占用之后这个窗口不存在了。
//
// 未认领时直接答 CREATING 可能比真实状态旧一瞬间（读完占用的下一刻落库开始了）。占用本来就不带状态，
// 原来的顺序在同样的时刻也会答 CREATING，下一次查询就会更新。
//
// display 为 true 表示结果只用来展示（GetOrder），读行可以按读模式走从库（见 loadDisplayRow）。
// “订单行不存在”无论如何由主库确认：从库没有时再读一次主库。否则从库落后时，一个已经落库
// 而从库还没复制到的订单会被答成 CREATING（状态往回退）或者不存在（还可能把不存在写进缓存）。
//
// 读占用失败（Redis 不可用）时退回到只读订单行：已经落库的订单照常能查到，只有行不存在时才报不可用，
// 因为这时分不清它是在排队还是不存在。闸门的熔断器打开之后，读占用立刻返回、不再等满时限（设计文档 1d 的 6.4）。
//
// 配置了冷库时，主库确认没有之后再按 loadCold 的规则查冷库（已归档的订单）。
func (s *Service) fetch(ctx context.Context, orderID int64, display bool) (*orderv1.Order, bool, error) {
	hold, held, err := s.gateGet(ctx, orderID)
	if err != nil {
		if ctx.Err() != nil {
			// 调用方已经取消或超时：不必再去读订单行，按调用方的原因作答（不记 ERROR）。
			return nil, false, s.internal(ctx, "read hold", err, "order_id", orderID)
		}
		// 熔断器打开期间闸门根本没被访问，每个查询都记一条的话会刷屏；打开和恢复各有一条日志（breakerChanged）。
		if !isBreakerOpen(err) {
			s.logger.WarnContext(ctx, "read hold failed, reading the order row only", "order_id", orderID, "error", err)
		}
		if o, ok, err := s.loadConfirmed(ctx, orderID, display); err != nil || ok {
			return o, ok, err
		}
		return nil, false, errHoldPending
	}
	if held && !hold.Claimed {
		return s.creatingProto(orderID, hold.UserID, holdItems(hold.Items), hold.At), true, nil
	}
	if o, ok, err := s.loadConfirmed(ctx, orderID, display); err != nil || ok {
		return o, ok, err
	}
	if held {
		// 已认领、主库上还没有行：落库正在进行，按占用合成。
		return s.creatingProto(orderID, hold.UserID, holdItems(hold.Items), hold.At), true, nil
	}
	// 走到这里，占用没有，热库主库也确认没有这一行。这是 fetch 里唯一一个返回“不存在”的出口，
	// 冷库只在这里查一次。
	return s.loadCold(ctx, orderID)
}

// loadConfirmed 读已落库的订单行。ok 为 false 时“没有这一行”是主库确认过的：
// display 为 true 时先按读模式读（可能是从库），从库没有再读一次主库。
//
// display 也是展示读和写路径的分界（设计文档 1d 的 5.7）：display 为 true 的主库读要先占这个库的展示读名额
// （loadDisplayPrimary），名额满了答 RESOURCE_EXHAUSTED；为 false 的（lookup，给取消、受理时的幂等查询这些
// 要据此作答或做决定的路径用）不占名额，查询再多也挤不掉它们。
func (s *Service) loadConfirmed(ctx context.Context, orderID int64, display bool) (*orderv1.Order, bool, error) {
	if !display {
		return s.loadRow(ctx, orderID)
	}
	o, ok, fromReplica, err := s.loadDisplayRow(ctx, orderID)
	if err != nil || ok || !fromReplica {
		return o, ok, err
	}
	return s.loadDisplayPrimary(ctx, orderID)
}

// loadDisplayPrimary 是展示读对主库的一次读：先占订单所在库的展示读名额，读完立刻归还。
//
// 名额只包住读库这一段（包括在连接池上等连接），不包住读闸门、读缓存、读标记：限制的是“查询同时占用多少个
// 主库连接”，其余步骤不碰主库，算进去只会让名额在没有占用连接的时候也被占着。
// 从库读不占这个名额：从库有自己的连接池，保护的对象是主库上留给写路径的连接（见 loadDisplayRow）。
func (s *Service) loadDisplayPrimary(ctx context.Context, orderID int64) (*orderv1.Order, bool, error) {
	release, err := acquire(s.limits.db(s.store.DBOfOrder(orderID)))
	if err != nil {
		return nil, false, err
	}
	defer release()
	return s.loadRow(ctx, orderID)
}

// loadCold 在热库主库确认没有订单之后查冷库（设计文档 1c 第 6.6 节）。ok 为 false 表示订单不存在。
//
// 只能在热库主库确认没有之后调用，不能提前、不能和热库并行：数据只从热流向冷，而且归档任务写冷在删热之前。
// 先查热没查到，要么订单不存在，要么已经从热库删了，而删之前冷库里一定已经有了，所以接着查冷不会漏。
// 反过来先查冷再查热，会在“还没写冷”时查冷、“已经删热”后查热，两头落空，把一个存在的订单说成不存在。
// 也不能拿从库的“没有”当依据：从库没有的行主库上可能有，而一个留在热库里的订单（例如卡住的非终态旧订单）
// 永远不会进冷库，只问冷库就会把它答成不存在。“热库没有”和“不存在”一样只由主库说了算，
// 所以 fetch 里从库没查到时一定先回主库确认，再到这里。
//
// 订单号新于 now - MinHotAge 的一定不在冷库，直接答不存在，不碰冷库：热订单的读路径没有额外开销，
// 冷库不可用也不影响它们。冷库查询出错时返回错误而不是“不存在”，上层不会把它写进负缓存。
//
// 冷库读有并发上限和 coldTimeout（设计文档 1d 的 5.7），还有熔断器（设计文档 1d 的 6.8，见 coldRead）：
// 冷库还没迁移完、熔断器打开、开关关掉时立刻答 ARCHIVE_UNAVAILABLE（降级错误），不访问冷库；
// 冷库这一跳超时或出错（调用方还在等）时也答它，熔断器打开之前最多有几个查询各等 1 秒。
// 名额满答 RESOURCE_EXHAUSTED；调用方取消、超时按 contextError 映射。都不会把热订单的查询一起拖住。
// 展示读和 lookup 都经过这里，冷库的名额对两者一样：它保护的是冷库的连接池，而冷库上没有写路径要让。
func (s *Service) loadCold(ctx context.Context, orderID int64) (*orderv1.Order, bool, error) {
	if s.cold == nil || !archive.MaybeCold(orderID, s.now()) {
		return nil, false, nil
	}
	var o *store.Order
	err := s.coldRead(ctx, func(ctx context.Context) error {
		var err error
		o, err = s.cold.GetOrder(ctx, orderID)
		return err
	})
	switch {
	case errors.Is(err, store.ErrNotFound):
		return nil, false, nil
	case err == nil:
		return toProto(o), true, nil
	case s.archiveUnavailable(ctx, "load archived order", err, "order_id", orderID):
		return nil, false, errArchiveUnavailable
	case errors.Is(err, errReadBusy):
		return nil, false, err
	default:
		return nil, false, s.internal(ctx, "load archived order", err, "order_id", orderID)
	}
}

// loadDisplayRow 是展示读的 loadRow：按读模式和标记决定读主库还是从库（见 useReplica）。
// fromReplica 表示结果来自从库（包括“从库上没有这一行”），fetch 据此决定要不要再去主库确认。
// 从库出错（不是“没有这一行”）或超过 replicaTimeout 时改读主库，不让一个出问题的从库拖垮查询。
//
// 从库读不占展示读的名额（名额是给主库的，见 loadDisplayPrimary）。从库不单独设上限：它的连接池只有展示读
// 和 sweeper 的扫描在用，没有写路径需要让；从库卡住时第一个读到它的请求等满 replicaTimeout 就退回主库，
// 并把这个从库标记为不健康，之后的请求直接读主库；退回的读照常占主库的名额，所以从库出问题也不会让主库上
// 的查询超过上限。
func (s *Service) loadDisplayRow(ctx context.Context, orderID int64) (o *orderv1.Order, ok, fromReplica bool, err error) {
	if !s.useReplica(ctx, orderID) {
		o, ok, err = s.loadDisplayPrimary(ctx, orderID)
		return o, ok, false, err
	}
	db := s.store.DBOfOrder(orderID)
	var row *store.Order
	err = hop(ctx, replicaTimeout, func(ctx context.Context) error {
		var err error
		row, err = s.store.GetOrderFromReplica(ctx, orderID)
		return err
	})
	switch {
	case errors.Is(err, store.ErrNotFound):
		// 从库没有不能说明订单不存在，fetch 会接着查占用、必要时由主库确认。
		return nil, false, true, nil
	case err != nil && ctx.Err() != nil:
		// 调用方已经取消或超时，退回主库也来不及了。
		return nil, false, false, s.internal(ctx, "load order from replica", err, "order_id", orderID)
	case err != nil:
		// 包括这一跳的超时（errHopTimeout）：从库慢，主库还来得及。立刻把这个从库标记为不健康，
		// 之后的展示读和 sweeper 不再试它，直到探测连续通过（设计文档 1d 的 6.7）；只在状态变化时记日志。
		s.markReplicaUnhealthy(ctx, db, err)
		o, ok, err = s.loadDisplayPrimary(ctx, orderID)
		return o, ok, false, err
	}
	return toProto(row), true, true, nil
}

// useReplica 决定这一次展示读能否读从库。任何不确定（从库不健康、标记读不到、等不到从库）都退回主库：
// 主库多承担一次读只是慢一点，读到旧数据是错的。
//
// 先看不访问任何依赖就能回答的条件（开关、健康标记、闸门刚恢复），都通过了才读标记：
// 从库不健康时不必为读标记再碰一次 Redis，也不做 WaitReplica。
func (s *Service) useReplica(ctx context.Context, orderID int64) bool {
	if s.readMode == ReadPrimary || s.swReplica.Off() {
		return false
	}
	db := s.store.DBOfOrder(orderID)
	if !s.replicaHealthy(db) {
		return false
	}
	// 闸门的 Redis 刚恢复：故障期间的写入没有打上标记，“没有标记”不能当作“最近没写过”。
	if s.gateRecoveredWithin(s.marksDistrustWindow()) {
		return false
	}
	var m readmark.Mark
	err := s.markCall(ctx, func(ctx context.Context) error {
		var err error
		m, err = s.marks.Get(ctx, orderID)
		return err
	})
	if err != nil {
		// 熔断器打开期间 Redis 根本没被访问，逐条记会刷屏（打开和恢复各有一条日志）。
		if !isBreakerOpen(err) {
			s.logger.WarnContext(ctx, "read marks failed, reading primary", "order_id", orderID, "error", err)
		}
		return false
	}
	if m.Pinned {
		return false
	}
	if s.readMode == ReadGTID && m.GTID != "" {
		caught, err := s.store.WaitReplica(ctx, orderID, m.GTID, s.gtidWait)
		if err != nil {
			if ctx.Err() == nil && !isServerAnswer(err) {
				// 从库连不上或挂住了（不是 MySQL 回答的错误，也不是调用方取消）：和读从库出错一样立刻标记。
				s.markReplicaUnhealthy(ctx, db, err)
			} else {
				s.logger.WarnContext(ctx, "wait replica failed, reading primary", "order_id", orderID, "error", err)
			}
			return false
		}
		return caught
	}
	return true
}

// loadRow 读主库上已落库的订单，转成对外的形式。ok 为 false 表示没有这一行。
func (s *Service) loadRow(ctx context.Context, orderID int64) (*orderv1.Order, bool, error) {
	o, err := s.store.GetOrder(ctx, orderID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, s.internal(ctx, "load order", err, "order_id", orderID)
	}
	return toProto(o), true, nil
}

// creatingProto 构造一个还没落库的订单的对外形式：只有商品和数量，没有价格。
// expire_time 由受理时间加支付时限算出。受理时间截断到毫秒（与占用里记的精度一致），
// 所以从占用合成的值与受理时返回、写进消息的值相同。
func (s *Service) creatingProto(orderID, userID int64, items []store.Item, acceptedAt time.Time) *orderv1.Order {
	out := make([]*orderv1.OrderItem, 0, len(items))
	for _, it := range items {
		out = append(out, &orderv1.OrderItem{SkuId: it.SKUID, Quantity: it.Quantity})
	}
	return &orderv1.Order{
		OrderId:    orderID,
		UserId:     userID,
		Status:     orderv1.OrderStatus_ORDER_STATUS_CREATING,
		Items:      out,
		CreateTime: timestamppb.New(acceptedAt),
		ExpireTime: timestamppb.New(acceptedAt.Add(s.paymentTimeout)),
	}
}

func holdItems(items []stockgate.Item) []store.Item {
	out := make([]store.Item, 0, len(items))
	for _, it := range items {
		out = append(out, store.Item{SKUID: it.SKUID, Quantity: it.Quantity})
	}
	sortItems(out)
	return out
}

// ---------- CancelOrder ----------

// CancelOrder 实现 OrderServiceServer。
func (s *Service) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
	if req.GetOrderId() <= 0 || req.GetUserId() <= 0 {
		return nil, invalidArgument("order_id and user_id must be positive")
	}
	// 不先查后改：直接做带前置条件的更新，并发的取消、支付、关单里只有一个能完成转换。
	// 订单不属于该用户时更新不到任何行，下面按不存在作答。
	ref := store.OrderRef{OrderID: req.GetOrderId(), UserID: req.GetUserId()}
	now := s.now()
	ev := newEvent(ref, now)
	ev.Body = &eventv1.OrderEvent_Cancelled{Cancelled: &eventv1.OrderCancelled{}}
	if _, err := s.transition(ctx, ev, func(ctx context.Context, tx *store.Tx) (bool, error) {
		return tx.Cancel(ctx, ref.OrderID, ref.UserID, now)
	}); err != nil {
		return nil, s.internal(ctx, "cancel order", err, "order_id", req.GetOrderId())
	}
	// 与 GetOrder 用同一套查询：还没落库的订单在 GetOrder 里是 CREATING，
	// 这里也按 CREATING 答“不可取消”而不是“不存在”，两个接口对同一个订单的说法一致。
	o, err := s.lookup(ctx, req.GetOrderId(), req.GetUserId())
	if err != nil {
		return nil, err
	}
	if o.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CANCELLED {
		return nil, errx.New(codes.FailedPrecondition, errx.ReasonOrderNotCancellable,
			"order %d is %s", o.GetOrderId(), o.GetStatus())
	}
	return &orderv1.CancelOrderResponse{Order: o}, nil
}

// ---------- 状态转换与事件 ----------

// txRetry 是事务遇到死锁时的重试策略。InnoDB 检测到死锁会回滚其中一个事务，整个事务重做即可。
var txRetry = retry.Policy{
	MaxAttempts: 3,
	Initial:     10 * time.Millisecond,
	Max:         200 * time.Millisecond,
	Retryable:   mysqlx.IsDeadlock,
}

// inTx 在 orderID 所在分片上的事务里执行 fn，遇到死锁时整个重做。fn 可能执行多次，不能在外部变量里累积结果。
// 事务只能操作这一个订单所在 slot 的表（store.Tx 会拒绝别的 slot 的订单）。
//
// 开始事务之前先打 pin（见 pin）。提交之后的 stamp 由调用方在确实写了东西时调用 afterCommit：
// 只有调用方知道这次事务是否真的改了数据，没改的事务不值得再去主库读一次位置。
func (s *Service) inTx(ctx context.Context, p retry.Policy, orderID int64, fn func(tx *store.Tx) error) error {
	s.pin(ctx, orderID)
	return retry.Do(ctx, p, func(ctx context.Context, _ int) error {
		return s.store.InTx(ctx, orderID, fn)
	})
}

// afterCommit 在 orderID 所在库的一个写事务提交后调用：踢中继，并记下主库位置（stamp）。
func (s *Service) afterCommit(ctx context.Context, orderID int64) {
	s.kick(orderID)
	s.stamp(ctx, orderID)
}

// pin 在写订单之前打上 pin，让写入期间及之后一小段时间里的展示读走主库。
//
// 先打标记再写：如果提交之后才打，进程在“已提交、标记未写”时崩溃，会留下一个没有任何保护的新写入，
// 之后的读可能落到还没复制到它的从库上；先打的话最坏只是写失败、标记白打。
// 失败只记日志、不阻止写入：不能因为 Redis 抖动拒绝业务写入，代价是这一次可能读到旧数据。
func (s *Service) pin(ctx context.Context, orderID int64) {
	if s.readMode == ReadPrimary {
		return
	}
	err := s.markCall(ctx, func(ctx context.Context) error { return s.marks.Pin(ctx, orderID, s.pinTTL) })
	// 闸门的熔断器打开时跳过、不记日志：这段时间里没有标记的写入由 marksDistrustWindow 兜住。
	if err != nil && !isBreakerOpen(err) {
		s.logger.WarnContext(ctx, "pin order before write failed", "order_id", orderID, "error", err)
	}
}

// stamp 在写入提交之后读出主库位置记进标记（只在 ReadGTID 下）。失败的处理同 pin。
// 读位置和写标记共用一个时限：两步合起来才有用，任何一步拖住都不该让调用方继续等。
//
// 写标记经过闸门的熔断器（markCall），它的结果按这个共用时限的 ctx 归类：读位置用掉了时限、
// 写标记跟着超时的话算 Ignored，不把主库的慢记到 Redis 头上。Redis 挂住的情况由 pin 和读标记数出来。
func (s *Service) stamp(ctx context.Context, orderID int64) {
	if s.readMode != ReadGTID {
		return
	}
	sctx, cancel := context.WithTimeout(ctx, markTimeout)
	defer cancel()
	pos, err := s.store.PrimaryPosition(sctx, orderID)
	if err == nil {
		err = s.markCall(sctx, func(mctx context.Context) error { return s.marks.Stamp(mctx, orderID, pos, s.gtidTTL) })
	}
	if err != nil && !isBreakerOpen(err) {
		s.logger.WarnContext(ctx, "stamp order after write failed", "order_id", orderID, "error", err)
	}
}

// kick 在 orderID 所在库的事务提交后通知那个库的中继立即发送。
// 提交前调用没有意义：中继看不到未提交的行。
func (s *Service) kick(orderID int64) {
	db := s.store.DBOfOrder(orderID)
	if db < len(s.relays) && s.relays[db] != nil {
		s.relays[db].Kick()
	}
}

// newEvent 构造一条 OrderEvent。occur_time 用状态变更时写入库里的同一个时间。
func newEvent(ref store.OrderRef, now time.Time) *eventv1.OrderEvent {
	return &eventv1.OrderEvent{OrderId: ref.OrderID, UserId: ref.UserID, OccurTime: timestamppb.New(now)}
}

// addEvents 把事件序列化后写入本事务的 outbox。
func (s *Service) addEvents(ctx context.Context, tx *store.Tx, evs ...*eventv1.OrderEvent) error {
	msgs := make([]outbox.Message, 0, len(evs))
	for _, ev := range evs {
		key, value, err := eventx.MarshalOrderEvent(ev)
		if err != nil {
			return err
		}
		msgs = append(msgs, outbox.Message{Topic: s.eventsTopic, Key: key, Payload: value})
	}
	if err := tx.AddOutbox(ctx, msgs...); err != nil {
		return fmt.Errorf("add order events: %w", err)
	}
	return nil
}

// transition 在一个事务里执行一次带前置条件的状态更新，并且只在它真的完成了转换（影响 1 行）时
// 写入对应的事件。这样“事件存在”与“转换由本事务完成”严格等价：
// 并发的几方里只有赢的那一方发事件，输的一方什么都不写。
func (s *Service) transition(ctx context.Context, ev *eventv1.OrderEvent,
	update func(ctx context.Context, tx *store.Tx) (bool, error),
) (bool, error) {
	var done bool
	err := s.inTx(ctx, txRetry, ev.GetOrderId(), func(tx *store.Tx) error {
		done = false
		ok, err := update(ctx, tx)
		if err != nil || !ok {
			return err
		}
		if err := s.addEvents(ctx, tx, ev); err != nil {
			return err
		}
		done = true
		return nil
	})
	if err != nil {
		return false, err
	}
	if done {
		s.afterCommit(ctx, ev.GetOrderId())
	}
	return done, nil
}

// failCreating 把 CREATING 的订单置为 FAILED 并发 failed 事件。返回是否由本次完成了转换。
func (s *Service) failCreating(ctx context.Context, ref store.OrderRef, reason string, now time.Time) (bool, error) {
	ev := newEvent(ref, now)
	ev.Body = &eventv1.OrderEvent_Failed{Failed: &eventv1.OrderFailed{Reason: reason}}
	return s.transition(ctx, ev, func(ctx context.Context, tx *store.Tx) (bool, error) {
		return tx.MarkCreateFailed(ctx, ref.OrderID, reason, now)
	})
}

// closeExpired 关闭超时未支付的订单并发 closed 事件。返回是否由本次完成了转换。
func (s *Service) closeExpired(ctx context.Context, ref store.OrderRef, now time.Time) (bool, error) {
	ev := newEvent(ref, now)
	ev.Body = &eventv1.OrderEvent_Closed{Closed: &eventv1.OrderClosed{}}
	return s.transition(ctx, ev, func(ctx context.Context, tx *store.Tx) (bool, error) {
		return tx.CloseExpired(ctx, ref.OrderID, now)
	})
}
