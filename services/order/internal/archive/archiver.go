package archive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// ErrColdMismatch 表示写进冷库的内容读回来与热库不同（字符集、截断、列对应关系出错）。
// 这是缺陷，不是可以跳过的个别情况：继续下去会把冷库存不下原样的数据从热库删掉。
var ErrColdMismatch = errors.New("archive: cold copy differs from the hot rows")

// 配置的默认值。
const (
	DefaultBatchSize      = 500
	DefaultReplicaWait    = 30 * time.Second
	DefaultBatchTimeout   = 2 * time.Minute
	DefaultQueryTimeout   = time.Minute
	DefaultReclaimTimeout = 2 * time.Hour
)

// Config 是 Archiver 的配置。零值字段使用默认值，OlderThan 除外。
type Config struct {
	// OlderThan 是截止点：订单号里的受理时间早于 now - OlderThan 的终态订单会被归档。
	// 不能小于 MinArchiveAge。
	OlderThan time.Duration
	// BatchSize 是一批的订单数，默认 DefaultBatchSize，不超过 store.MaxArchiveBatch。
	BatchSize int
	// Rate 是每个库每秒最多处理（扫描到）的订单数，0 表示不限。
	Rate int
	// ReplicaWait 是每批删除后等从库追上主库的上限，默认 DefaultReplicaWait。没有从库时不等。
	ReplicaWait time.Duration
	// BatchTimeout 是一批（读热、写冷、读回、删热）的总时限，默认 DefaultBatchTimeout。
	// 一批开始之后就不随 ctx 取消，只受这个时限约束。
	BatchTimeout time.Duration
	// QueryTimeout 是列出和计数这类单条查询的时限，默认 DefaultQueryTimeout。
	QueryTimeout time.Duration
	// ReclaimTimeout 是空间回收时重建一张表的时限，默认 DefaultReclaimTimeout。
	ReclaimTimeout time.Duration
	// DryRun 只数不搬。
	DryRun bool
	// Now 是时钟，默认 time.Now。只用来算截止点；限速和用时用真实时间。
	Now func() time.Time
	// Logger 默认 slog.Default()。
	Logger *slog.Logger
}

// Archiver 把热库里足够旧的终态订单搬到冷库（设计文档 1c 第 6.3、6.4 节）。
//
// 一批订单的步骤：在主库上按主键范围列出终态订单 → 读出 bundle → upsert 进冷库 → 从冷库读回比对摘要
// → 在热库的订单行锁里重读、摘要相同才删。每一步都可以重做，任何一步之后中断都不会丢订单：
// 写冷在删热之前，删热只删锁里确认与冷库相同的内容。
type Archiver struct {
	st   *store.Store
	cold *store.Cold
	cfg  Config
	log  *slog.Logger

	// afterColdWrite 是测试用的钩子：一批写进冷库之后、读回比对之前调用，返回错误时这一批就此中止。
	afterColdWrite func(slot int, ids []int64) error
}

// New 返回 Archiver。cfg.OlderThan 小于 MinArchiveAge 时返回错误：再小就可能归档读路径认为
// “一定不在冷库”的订单（见 MinHotAge）。
func New(st *store.Store, cold *store.Cold, cfg Config) (*Archiver, error) {
	if st == nil || cold == nil {
		return nil, errors.New("archive: nil store")
	}
	if cfg.OlderThan < MinArchiveAge {
		return nil, fmt.Errorf("archive: older-than %s is below the minimum %s", cfg.OlderThan, MinArchiveAge)
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.BatchSize < 0 || cfg.BatchSize > store.MaxArchiveBatch {
		return nil, fmt.Errorf("archive: batch size %d, want 1..%d", cfg.BatchSize, store.MaxArchiveBatch)
	}
	if cfg.Rate < 0 {
		return nil, fmt.Errorf("archive: negative rate %d", cfg.Rate)
	}
	cfg.ReplicaWait = cmpOr(cfg.ReplicaWait, DefaultReplicaWait)
	cfg.BatchTimeout = cmpOr(cfg.BatchTimeout, DefaultBatchTimeout)
	cfg.QueryTimeout = cmpOr(cfg.QueryTimeout, DefaultQueryTimeout)
	cfg.ReclaimTimeout = cmpOr(cfg.ReclaimTimeout, DefaultReclaimTimeout)
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Archiver{st: st, cold: cold, cfg: cfg, log: log}, nil
}

// cmpOr 返回 d，d 不是正数时返回 def。
func cmpOr(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// Counts 是归档的计数。
type Counts struct {
	// Scanned 是列出的终态订单数。
	Scanned int64 `json:"scanned"`
	// Archived 是写进冷库并从热库删除的订单数。
	Archived int64 `json:"archived"`
	// Skipped 是写进了冷库、但删除时锁里的内容与冷库不同（或不是终态）而留在热库的订单数。
	// 本次运行不再重试，下次运行会重新处理；在那之前冷库里是它的一份旧拷贝，读路径先查热库，不受影响。
	Skipped int64 `json:"skipped"`
	// Gone 是列出之后、处理之前已经不在热库的订单数（例如同时有另一个归档任务）。
	Gone int64 `json:"gone"`
	// NonTerminal 是早于截止点却不是终态的订单数，它们留在热库。
	NonTerminal int64 `json:"non_terminal"`
	// Archivable 只在 DryRun 时有：早于截止点的终态订单数。
	Archivable int64 `json:"archivable"`
	// Batches 是处理的批数。
	Batches int64 `json:"batches"`
}

func (c *Counts) add(o Counts) {
	c.Scanned += o.Scanned
	c.Archived += o.Archived
	c.Skipped += o.Skipped
	c.Gone += o.Gone
	c.NonTerminal += o.NonTerminal
	c.Archivable += o.Archivable
	c.Batches += o.Batches
}

// SlotReport 是一个 slot 的结果。
type SlotReport struct {
	Slot      int    `json:"slot"`
	DB        int    `json:"db"`
	Counts    Counts `json:"counts"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

// Report 是一次运行的结果。出错或被取消时是已完成部分的结果。
type Report struct {
	DryRun bool `json:"dry_run"`
	// Cutoff 是截止时间，Below 是它对应的订单号上界：只处理 order_id < Below 的订单。
	Cutoff    time.Time    `json:"cutoff"`
	Below     int64        `json:"below"`
	Slots     []SlotReport `json:"slots"`
	Total     Counts       `json:"total"`
	ElapsedMS int64        `json:"elapsed_ms"`
}

// Run 归档所有 slot。同一个库上的 slot 串行，不同库并行。
//
// 不持久化进度：归档过的订单已经从热库删掉，重跑时同样的扫描条件只会看到剩下的。
// ctx 取消时每个库做完手上这一批再停，返回已完成部分的报告和 ctx 的错误。
// 任何一个库出错（包括 ErrColdMismatch）时其他库也做完当前批后停下，返回第一个错误。
func (a *Archiver) Run(ctx context.Context) (*Report, error) {
	start := time.Now()
	cutoff := a.cfg.Now().Add(-a.cfg.OlderThan)
	router := a.st.Router()
	rep := &Report{
		DryRun: a.cfg.DryRun,
		Cutoff: cutoff.UTC(),
		Below:  idgen.LowerBound(cutoff),
		Slots:  make([]SlotReport, router.Slots()),
	}
	a.log.InfoContext(ctx, "archive started",
		"cutoff", rep.Cutoff, "below", rep.Below, "dry_run", a.cfg.DryRun, "batch", a.cfg.BatchSize, "rate", a.cfg.Rate)

	g, gctx := errgroup.WithContext(ctx)
	for db := range router.DBs() {
		g.Go(func() error {
			p := newPacer(a.cfg.Rate)
			for _, slot := range router.SlotsOf(db) {
				// 每个 slot 的报告只由这一个 goroutine 写。
				sr := &rep.Slots[slot]
				sr.Slot, sr.DB = slot, db
				if err := a.runSlot(gctx, slot, rep.Below, p, sr); err != nil {
					return fmt.Errorf("slot %d: %w", slot, err)
				}
			}
			return nil
		})
	}
	err := g.Wait()

	for _, sr := range rep.Slots {
		rep.Total.add(sr.Counts)
	}
	rep.ElapsedMS = time.Since(start).Milliseconds()
	return rep, err
}

func (a *Archiver) runSlot(ctx context.Context, slot int, below int64, p *pacer, sr *SlotReport) error {
	start := time.Now()
	defer func() { sr.ElapsedMS = time.Since(start).Milliseconds() }()

	if a.cfg.DryRun {
		qctx, cancel := context.WithTimeout(ctx, a.cfg.QueryTimeout)
		defer cancel()
		terminal, nonTerminal, err := a.st.CountBelow(qctx, slot, below)
		if err != nil {
			return err
		}
		sr.Counts.Archivable, sr.Counts.NonTerminal = terminal, nonTerminal
		a.log.InfoContext(ctx, "slot counted", "slot", slot, "archivable", terminal, "non_terminal", nonTerminal)
		return nil
	}

	// 游标只在内存里：跳过的订单本次不再重试，也避免反复扫过删除后还没被 purge 的记录。
	var cursor int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ids, err := a.list(ctx, slot, cursor, below)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		cursor = ids[len(ids)-1]

		c, err := a.archiveBatch(ctx, slot, ids)
		sr.Counts.add(c)
		if err != nil {
			return err
		}
		if err := a.throttle(ctx, slot, ids[0], c, p); err != nil {
			return err
		}
	}

	// 早于截止点却不是终态的订单不归档，数出来报告给人看。归档之后数，数的是留下来的。
	qctx, cancel := context.WithTimeout(ctx, a.cfg.QueryTimeout)
	defer cancel()
	_, nonTerminal, err := a.st.CountBelow(qctx, slot, below)
	if err != nil {
		return err
	}
	sr.Counts.NonTerminal = nonTerminal
	a.log.InfoContext(ctx, "slot archived", "slot", slot,
		"scanned", sr.Counts.Scanned, "archived", sr.Counts.Archived, "skipped", sr.Counts.Skipped,
		"non_terminal", nonTerminal, "batches", sr.Counts.Batches)
	return nil
}

func (a *Archiver) list(ctx context.Context, slot int, after, below int64) ([]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, a.cfg.QueryTimeout)
	defer cancel()
	return a.st.ListArchivable(ctx, slot, after, below, a.cfg.BatchSize)
}

// archiveBatch 归档一批订单。返回的计数在出错时也是这一批已经确定的部分。
//
// 一旦开始就做完：用不随 ctx 取消的 context（加 BatchTimeout），免得停止信号把一批切在
// “写了冷、本可以删热却没删”的地方。切在那里也不是错误（重跑会再写一遍、再删），只是多余。
func (a *Archiver) archiveBatch(ctx context.Context, slot int, ids []int64) (Counts, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.BatchTimeout)
	defer cancel()
	c := Counts{Scanned: int64(len(ids)), Batches: 1}

	hot, err := a.st.ReadBundles(ctx, slot, ids)
	if err != nil {
		return c, err
	}
	c.Gone = int64(len(ids) - len(hot))
	// 列出时是终态，终态不会再变；这里再挡一次，冷库里只能有终态订单。
	hot = slices.DeleteFunc(hot, func(b store.Bundle) bool {
		if store.IsTerminal(b.Order.Status) {
			return false
		}
		a.log.ErrorContext(ctx, "listed order is not terminal", "order_id", b.Order.OrderID, "status", b.Order.Status.String())
		c.Skipped++
		return true
	})
	if len(hot) == 0 {
		return c, nil
	}

	want := make(map[int64]store.Digest, len(hot))
	hotIDs := make([]int64, len(hot))
	for i := range hot {
		hotIDs[i] = hot[i].Order.OrderID
		want[hotIDs[i]] = hot[i].Digest()
	}

	if err := a.cold.UpsertBundles(ctx, hot); err != nil {
		return c, fmt.Errorf("write cold: %w", err)
	}
	if a.afterColdWrite != nil {
		if err := a.afterColdWrite(slot, hotIDs); err != nil {
			return c, err
		}
	}

	back, err := a.cold.ReadBundles(ctx, hotIDs)
	if err != nil {
		return c, fmt.Errorf("read back cold: %w", err)
	}
	got := make(map[int64]store.Digest, len(back))
	for i := range back {
		got[back[i].Order.OrderID] = back[i].Digest()
	}
	for _, id := range hotIDs {
		if d, ok := got[id]; !ok || d != want[id] {
			return c, fmt.Errorf("%w: order %d in slot %d (hot %s, cold %s, cold found %t)",
				ErrColdMismatch, id, slot, want[id], d, ok)
		}
	}

	res, err := a.st.DeleteArchived(ctx, slot, want)
	if err != nil {
		return c, err
	}
	c.Archived = int64(len(res.Deleted))
	c.Skipped += int64(len(res.Mismatched))
	c.Gone += int64(len(res.Gone))
	if len(res.Mismatched) > 0 {
		// 写冷之后它的三张表在热库里变了（例如来了一笔取消之后的支付结果）。它留在热库，
		// 冷库里是一份旧拷贝；下次运行会重新写冷、再删。
		a.log.WarnContext(ctx, "orders changed after cold write, kept in hot", "slot", slot, "order_ids", res.Mismatched)
	}
	a.log.DebugContext(ctx, "batch archived", "slot", slot, "first", ids[0], "last", ids[len(ids)-1],
		"archived", c.Archived, "skipped", c.Skipped, "gone", c.Gone)
	return c, nil
}

// throttle 在一批之后限速：先等从库追上这批删除，再按 Rate 补足间隔。orderID 用来定位 slot 所在的库。
// 只有等待被 ctx 打断时返回错误；从库等超时不算错误，这一批已经为它等了 ReplicaWait。
func (a *Archiver) throttle(ctx context.Context, slot int, orderID int64, c Counts, p *pacer) error {
	if c.Archived > 0 && a.st.HasReplicas() {
		// 大批量删除最先压垮的是复制。拿复制进度做反馈：从库没执行完这批删除就不做下一批。
		pos, err := a.st.PrimaryPosition(ctx, orderID)
		if err != nil {
			return err
		}
		caught, err := a.st.WaitReplica(ctx, orderID, pos.GTID, a.cfg.ReplicaWait)
		if err != nil {
			return err
		}
		if !caught {
			a.log.WarnContext(ctx, "replica still behind after waiting", "slot", slot, "wait", a.cfg.ReplicaWait.String())
		}
	}
	return p.wait(ctx, c.Scanned)
}

// pacer 让一个库上的处理速度不超过 rate 个订单每秒：处理了 n 个之后，至少要过去 n/rate 秒。
// 按累计量算而不是每批固定睡眠，前面慢下来（例如等从库）的时间可以抵掉后面的间隔。
type pacer struct {
	rate  int
	start time.Time
	done  int64
}

func newPacer(rate int) *pacer { return &pacer{rate: rate, start: time.Now()} }

func (p *pacer) wait(ctx context.Context, n int64) error {
	if p.rate <= 0 {
		return nil
	}
	p.done += n
	due := p.start.Add(time.Duration(float64(p.done) / float64(p.rate) * float64(time.Second)))
	d := time.Until(due)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// TableReclaim 是一张表的空间回收结果。字节数是 InnoDB 的估计值。
type TableReclaim struct {
	Slot        int    `json:"slot"`
	DB          int    `json:"db"`
	Table       string `json:"table"`
	BeforeBytes int64  `json:"before_bytes"`
	AfterBytes  int64  `json:"after_bytes"`
	ElapsedMS   int64  `json:"elapsed_ms"`
}

// ReclaimReport 是空间回收的结果。
type ReclaimReport struct {
	Tables    []TableReclaim `json:"tables"`
	ElapsedMS int64          `json:"elapsed_ms"`
}

// Reclaim 逐张重建所有 slot 的热表，回收归档删除后留下的空间（设计文档 6.8）。全部串行：
// 每次重建都要额外的磁盘空间放新表，并在从库上重放一遍，同时做几张会叠加这两样开销。
// 不自动跟在 Run 后面，由人决定什么时候做。ctx 取消时在两张表之间停下。
func (a *Archiver) Reclaim(ctx context.Context) (*ReclaimReport, error) {
	start := time.Now()
	router := a.st.Router()
	rep := &ReclaimReport{}
	defer func() { rep.ElapsedMS = time.Since(start).Milliseconds() }()
	for slot := range router.Slots() {
		tables, err := a.st.HotTables(slot)
		if err != nil {
			return rep, err
		}
		for _, table := range tables {
			if err := ctx.Err(); err != nil {
				return rep, err
			}
			r, err := a.reclaimTable(ctx, slot, table)
			if err != nil {
				return rep, err
			}
			r.DB = router.DB(slot)
			rep.Tables = append(rep.Tables, r)
			a.log.InfoContext(ctx, "table rebuilt", "table", table, "slot", slot,
				"before_bytes", r.BeforeBytes, "after_bytes", r.AfterBytes, "elapsed_ms", r.ElapsedMS)
		}
	}
	return rep, nil
}

func (a *Archiver) reclaimTable(ctx context.Context, slot int, table string) (TableReclaim, error) {
	ctx, cancel := context.WithTimeout(ctx, a.cfg.ReclaimTimeout)
	defer cancel()
	r := TableReclaim{Slot: slot, Table: table}
	var err error
	if r.BeforeBytes, err = a.st.TableBytes(ctx, slot, table); err != nil {
		return r, err
	}
	start := time.Now()
	if err := a.st.OptimizeTable(ctx, slot, table); err != nil {
		return r, err
	}
	r.ElapsedMS = time.Since(start).Milliseconds()
	if r.AfterBytes, err = a.st.TableBytes(ctx, slot, table); err != nil {
		return r, err
	}
	return r, nil
}
