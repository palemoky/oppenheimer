package changes

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// 本文件是三个派生端：商家索引（index）、订单库的从库（replica）、冷库（archive）。
//
// 三者的做法相同：一批变更按库、按表分组，每张表一条按主键的 IN 查询；读出来的值和映像里的值都规范成
// 同一种形式再比（整数按数值、字符按内容、二进制按字节、时间按时刻、NULL 只等于 NULL）。
//
// 映像里的值是 CDC 的 cdcv1.Value（services/cdc/internal/capture/value.go）：DATETIME(6) 是 MySQL 的文本形式，
// 小数位数等于列定义的位数，小数是 0 也写出来，例如 "2026-06-01 12:00:00.000000"，按 UTC 解释。
// 读库时不依赖 DSN 的 parseTime：驱动给的是 time.Time 就取它的墙上时间按 UTC 解释，给的是文本就按上面的格式解析。

// 表名。分片表的物理表名由 sharding.Table 生成，拼进 SQL 的表名只有这几个来源。
const (
	indexTableBase   = "merchant_orders" // orderindex 的 store.TableBase
	archiveTable     = "orders_archive"
	indexVersionCol  = "src_version"
	datetimeLayout   = "2006-01-02 15:04:05" // 解析时允许秒后面带任意位数的小数，也允许没有
	datetimeDisplay  = "2006-01-02 15:04:05.000000"
	maxStatementKeys = 1000 // 一条 IN 语句最多带的主键个数；调用方一批最多 500 条，正常用不到切分
	maxShownRunes    = 24   // Detail 里一个值最多显示的字符数
	maxShownBytes    = 12   // Detail 里一个二进制值最多显示的字节数
)

// kind 是一列按什么规则比较。
type kind int

const (
	kindInt kind = iota + 1
	kindStr
	kindBytes
	kindTime
)

func (k kind) String() string {
	switch k {
	case kindInt:
		return "integer"
	case kindStr:
		return "string"
	case kindBytes:
		return "binary"
	case kindTime:
		return "datetime"
	default:
		return "unknown"
	}
}

// column 是一列。image 是映像里的列名，db 是派生端表里的列名，通常相同（商家索引的版本列不同）。
type column struct {
	image, db string
	kind      kind
}

func col(name string, k kind) column { return column{image: name, db: name, kind: k} }

// orderColumns 是订单表 orders_N 的全部列（services/order/migrations 的 00001 加 00002），冷库的 orders_archive
// 与它一一对应（另有 archived_at，不比）。版本列单独读，不在这里。
var orderColumns = []column{
	col(ColOrderID, kindInt),
	col("user_id", kindInt),
	col(ColMerchantID, kindInt),
	col("status", kindInt),
	col("total_amount", kindInt),
	col("currency", kindStr),
	col("idempotency_key", kindStr),
	col("request_hash", kindBytes),
	col("payment_id", kindInt),
	col("fail_reason", kindStr),
	col("expire_at", kindTime),
	col("paid_at", kindTime),
	col("created_at", kindTime),
	col("updated_at", kindTime),
}

// indexColumns 是商家索引一行里从映像来的列（services/orderindex/internal/indexer/decode.go）。
// merchant_id、order_id 是主键，查询条件已经保证相同；synced_at 是索引自己的，不比。
var indexColumns = []column{
	col("user_id", kindInt),
	col("status", kindInt),
	col("total_amount", kindInt),
	col("currency", kindStr),
	col("created_at", kindTime),
	col("paid_at", kindTime),
}

// table 描述一个派生端的表怎么读。
type table struct {
	// versionCol 是派生端表里记源行版本的列。
	versionCol string
	// cols 是要比较的列。
	cols []column
	// byMerchant 为 true 时主键是 (merchant_id, order_id)，否则是 order_id。
	byMerchant bool
}

// selectSQL 返回读 n 行的语句。第一列是 order_id，第二列是版本，之后是 cols。
func (t *table) selectSQL(name string, n int) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	b.WriteString(ColOrderID)
	b.WriteString(", ")
	b.WriteString(t.versionCol)
	for _, c := range t.cols {
		b.WriteString(", ")
		b.WriteString(c.db)
	}
	b.WriteString(" FROM ")
	b.WriteString(name)
	if t.byMerchant {
		// 行构造器的 IN 走主键的范围扫描（本机 MySQL 实测两万行的表 EXPLAIN 是 range / PRIMARY；
		// 只有几行的表优化器会直接扫一遍主键，那时也不贵）。
		b.WriteString(" WHERE (merchant_id, order_id) IN (")
		b.WriteString(strings.TrimSuffix(strings.Repeat("(?, ?), ", n), ", "))
	} else {
		b.WriteString(" WHERE order_id IN (")
		b.WriteString(strings.TrimSuffix(strings.Repeat("?, ", n), ", "))
	}
	b.WriteString(")")
	return b.String()
}

// row 是派生端读出来的一行，值已经规范过（见 dbValue）。
type row struct {
	version int64
	vals    []any // 与 table.cols 一一对应
}

// read 读一张表里 cs 对应的行，返回按订单号索引的行。读不到时返回错误。
func (t *table) read(ctx context.Context, db *sql.DB, name string, cs []Change) (map[int64]row, error) {
	out := make(map[int64]row, len(cs))
	for chunk := range slices.Chunk(cs, maxStatementKeys) {
		args := make([]any, 0, 2*len(chunk))
		for _, c := range chunk {
			if t.byMerchant {
				args = append(args, c.MerchantID)
			}
			args = append(args, c.OrderID)
		}
		if err := t.query(ctx, db, name, len(chunk), args, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (t *table) query(ctx context.Context, db *sql.DB, name string, n int, args []any, out map[int64]row) error {
	rows, err := db.QueryContext(ctx, t.selectSQL(name, n), args...)
	if err != nil {
		return fmt.Errorf("query %s: %w", name, err)
	}
	defer rows.Close()
	dest := make([]any, 2+len(t.cols))
	raw := make([]any, len(dest))
	for i := range dest {
		dest[i] = &raw[i]
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("scan %s: %w", name, err)
		}
		orderID, err := dbInt(raw[0])
		if err != nil {
			return fmt.Errorf("scan %s: column %s: %w", name, ColOrderID, err)
		}
		version, err := dbInt(raw[1])
		if err != nil {
			return fmt.Errorf("scan %s: order %d column %s: %w", name, orderID, t.versionCol, err)
		}
		r := row{version: version, vals: make([]any, len(t.cols))}
		for i, c := range t.cols {
			// 规范不了的值留着原样，比较时报成这一列不一致（数据本身的问题，不是读不到）。
			v, err := dbValue(c.kind, raw[2+i])
			if err != nil {
				v = badValue{err: err}
			}
			r.vals[i] = v
		}
		out[orderID] = r
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read %s: %w", name, err)
	}
	return nil
}

// compare 按 cols 的顺序比较映像和派生端的一行，返回第一个不同的列名和说明；都相同时返回空串。
// 映像里没有的列不比（表结构新增列时忽略不认识的列，反过来映像比派生端旧时也一样）。
func compare(cols []column, img map[string]*cdcv1.Value, r row) (diff, detail string) {
	for i, c := range cols {
		iv, exists := img[c.image]
		if !exists {
			continue
		}
		want, err := imageValue(c.kind, iv)
		if err != nil {
			return c.image, fmt.Sprintf("column %s: image value %s cannot be read as %s: %v", c.image, showImage(iv), c.kind, err)
		}
		got := r.vals[i]
		if bad, ok := got.(badValue); ok {
			return c.image, fmt.Sprintf("column %s: derived value cannot be read as %s: %v", c.image, c.kind, bad.err)
		}
		if !equal(want, got) {
			return c.image, fmt.Sprintf("column %s: derived %s, change %s", c.image, show(got), show(want))
		}
	}
	return "", ""
}

// badValue 是派生端一个规范不了的值。
type badValue struct{ err error }

// 规范后的值只有五种：nil（NULL）、int64、string、[]byte、time.Time（UTC，微秒精度）。

func equal(a, b any) bool {
	switch x := a.(type) {
	case nil:
		return b == nil
	case int64:
		y, ok := b.(int64)
		return ok && x == y
	case string:
		y, ok := b.(string)
		return ok && x == y
	case []byte:
		y, ok := b.([]byte)
		return ok && bytes.Equal(x, y)
	case time.Time:
		y, ok := b.(time.Time)
		return ok && x.Equal(y)
	default:
		return false
	}
}

// imageValue 把映像里的一个值按列的类型规范。kind 未设置是 NULL。
func imageValue(k kind, v *cdcv1.Value) (any, error) {
	if v.GetKind() == nil {
		return nil, nil
	}
	switch k {
	case kindInt:
		switch x := v.GetKind().(type) {
		case *cdcv1.Value_Int:
			return x.Int, nil
		case *cdcv1.Value_Uint:
			if x.Uint > math.MaxInt64 {
				return nil, fmt.Errorf("unsigned value %d overflows int64", x.Uint)
			}
			return int64(x.Uint), nil
		}
	case kindStr:
		if x, ok := v.GetKind().(*cdcv1.Value_Str); ok {
			return x.Str, nil
		}
	case kindBytes:
		if x, ok := v.GetKind().(*cdcv1.Value_Raw); ok {
			return x.Raw, nil
		}
	case kindTime:
		if x, ok := v.GetKind().(*cdcv1.Value_Str); ok {
			return parseDatetime(x.Str)
		}
	}
	return nil, fmt.Errorf("unexpected value kind %T", v.GetKind())
}

// dbValue 把驱动扫描出来的一个值按列的类型规范。驱动给的类型取决于协议（文本协议全是 []byte，
// 预处理语句的整数是 int64）和 parseTime（DATETIME 是 time.Time 还是 []byte），两种都接受。
func dbValue(k kind, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch k {
	case kindInt:
		return dbInt(v)
	case kindStr:
		switch x := v.(type) {
		case []byte:
			return string(x), nil
		case string:
			return x, nil
		}
	case kindBytes:
		switch x := v.(type) {
		case []byte:
			return x, nil
		case string:
			return []byte(x), nil
		}
	case kindTime:
		switch x := v.(type) {
		case time.Time:
			// parseTime 打开时驱动按 DSN 的 loc 解释库里的墙上时间。库里存的是 UTC 的墙上时间，
			// 不管 loc 是什么，取回墙上时间按 UTC 解释。
			return time.Date(x.Year(), x.Month(), x.Day(), x.Hour(), x.Minute(), x.Second(), x.Nanosecond(), time.UTC), nil
		case []byte:
			return parseDatetime(string(x))
		case string:
			return parseDatetime(x)
		}
	}
	return nil, fmt.Errorf("unexpected driver type %T", v)
}

// dbInt 规范一个 NOT NULL 的整数。
func dbInt(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case []byte:
		n, err := strconv.ParseInt(string(x), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse integer: %w", err)
		}
		return n, nil
	case nil:
		return 0, errors.New("unexpected NULL")
	default:
		return 0, fmt.Errorf("unexpected driver type %T for an integer", v)
	}
}

// parseDatetime 按 UTC 解析 MySQL DATETIME 的文本形式。
func parseDatetime(s string) (time.Time, error) {
	t, err := time.ParseInLocation(datetimeLayout, s, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse datetime %q: %w", truncate(s), err)
	}
	return t, nil
}

// show 把规范后的值写成给人看的样子，截短。
func show(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case int64:
		return strconv.FormatInt(x, 10)
	case string:
		return strconv.Quote(truncate(x))
	case []byte:
		if len(x) > maxShownBytes {
			return "0x" + hex.EncodeToString(x[:maxShownBytes]) + fmt.Sprintf("...(%d bytes)", len(x))
		}
		return "0x" + hex.EncodeToString(x)
	case time.Time:
		return x.Format(datetimeDisplay)
	default:
		return fmt.Sprintf("%T", v)
	}
}

// showImage 把映像里一个没法规范的值写成给人看的样子。
func showImage(v *cdcv1.Value) string {
	switch x := v.GetKind().(type) {
	case *cdcv1.Value_Int:
		return "int " + strconv.FormatInt(x.Int, 10)
	case *cdcv1.Value_Uint:
		return "uint " + strconv.FormatUint(x.Uint, 10)
	case *cdcv1.Value_Str:
		return "str " + strconv.Quote(truncate(x.Str))
	case *cdcv1.Value_Raw:
		return "raw " + show(x.Raw)
	case *cdcv1.Value_Float:
		return "float " + strconv.FormatFloat(x.Float, 'g', -1, 64)
	default:
		return "NULL"
	}
}

func truncate(s string) string {
	if utf8.RuneCountInString(s) <= maxShownRunes {
		return s
	}
	return string([]rune(s)[:maxShownRunes]) + "..."
}

// group 是一批里落在同一张表上的变更。
type group struct {
	db    *sql.DB
	table string
	idx   []int // 在 cs 里的下标
}

// readGroups 并发读各张表，返回 found[i]、rows[i] 对应 cs[i]。任何一张表读不到都返回错误。
func readGroups(ctx context.Context, t *table, cs []Change, groups []*group) ([]row, []bool, error) {
	rows := make([]row, len(cs))
	found := make([]bool, len(cs))
	errs := make([]error, len(groups))
	var wg sync.WaitGroup
	for gi, g := range groups {
		wg.Go(func() {
			sub := make([]Change, len(g.idx))
			for j, i := range g.idx {
				sub[j] = cs[i]
			}
			got, err := t.read(ctx, g.db, g.table, sub)
			if err != nil {
				errs[gi] = err
				return
			}
			// 各组的下标互不相交，写不同的元素不用加锁。
			for _, i := range g.idx {
				rows[i], found[i] = got[cs[i].OrderID]
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, nil, err
	}
	return rows, found, nil
}

// groupBy 按 locate 给出的库和表把 cs 分组，顺序按第一次出现的先后。
func groupBy(cs []Change, locate func(Change) (*sql.DB, string, error)) ([]*group, error) {
	var out []*group
	at := map[string]*group{}
	for i, c := range cs {
		db, name, err := locate(c)
		if err != nil {
			return nil, err
		}
		g := at[name]
		if g == nil {
			g = &group{db: db, table: name}
			at[name] = g
			out = append(out, g)
		}
		g.idx = append(g.idx, i)
	}
	return out, nil
}

// judged 按包注释里那张表给出判定和说明。found、r 是派生端的那一行。
func judged(cols []column, c Change, r row, found bool) Result {
	d := Derived{Found: found, Version: r.version}
	var detail string
	if found && r.version == c.Version {
		d.Diff, detail = compare(cols, c.Image, r)
	}
	switch v := Judge(c.Version, d); v {
	case Behind:
		if !found {
			return Result{Verdict: Behind, Detail: "derived row not found"}
		}
		return Result{Verdict: Behind, Detail: fmt.Sprintf("derived version %d < change version %d", r.version, c.Version)}
	case Mismatch:
		return Result{Verdict: Mismatch, Detail: detail}
	default:
		return Result{Verdict: v}
	}
}

func checkShards(dbs []*sql.DB, router *sharding.Router, what string) error {
	if router == nil {
		return fmt.Errorf("changes: %s: nil router", what)
	}
	if len(dbs) != router.DBs() {
		return fmt.Errorf("changes: %s: got %d databases, sharding layout has %d", what, len(dbs), router.DBs())
	}
	for i, db := range dbs {
		if db == nil {
			return fmt.Errorf("changes: %s: database %d is nil", what, i)
		}
	}
	return nil
}

// indexTarget 是商家索引：merchant_orders_<slot>，按 merchant_id 分片（sharding.MerchantIndexLayout）。
type indexTarget struct {
	dbs    []*sql.DB
	router *sharding.Router
	t      table
}

// NewIndexTarget 返回商家索引的目标。dbs 的个数和顺序与 router 的库一致。
//
// 没有商家的订单不进索引，不核对。DELETE（归档）也核对：索引保留归档订单的行，版本等于 before 映像的版本。
func NewIndexTarget(dbs []*sql.DB, router *sharding.Router) (Target, error) {
	if err := checkShards(dbs, router, "index target"); err != nil {
		return nil, err
	}
	return &indexTarget{dbs: dbs, router: router, t: table{versionCol: indexVersionCol, cols: indexColumns, byMerchant: true}}, nil
}

func (x *indexTarget) Name() string { return "index" }

func (x *indexTarget) Wants(c Change) bool { return c.MerchantID != 0 }

func (x *indexTarget) Check(ctx context.Context, cs []Change) ([]Result, error) {
	groups, err := groupBy(cs, func(c Change) (*sql.DB, string, error) {
		slot := x.router.ForMerchant(c.MerchantID)
		return x.dbs[x.router.DB(slot)], sharding.Table(indexTableBase, slot), nil
	})
	if err != nil {
		return nil, err
	}
	rows, found, err := readGroups(ctx, &x.t, cs, groups)
	if err != nil {
		return nil, fmt.Errorf("changes: index: %w", err)
	}
	out := make([]Result, len(cs))
	for i, c := range cs {
		out[i] = judged(x.t.cols, c, rows[i], found[i])
	}
	return out, nil
}

// replicaTarget 是订单库的从库：orders_<slot>，布局与主库相同（sharding.OrderLayout）。
type replicaTarget struct {
	dbs    []*sql.DB
	router *sharding.Router
	t      table
}

// NewReplicaTarget 返回订单库从库的目标。dbs 的个数和顺序与 router 的库一致。
//
// 非 DELETE 按包注释的规则判定，同版本时比 orders 的全部列。DELETE（归档）在从库上已经没有这一行是通过，
// 还在是没追上（从库还没应用这次删除）。
func NewReplicaTarget(dbs []*sql.DB, router *sharding.Router) (Target, error) {
	if err := checkShards(dbs, router, "replica target"); err != nil {
		return nil, err
	}
	return &replicaTarget{dbs: dbs, router: router, t: table{versionCol: ColVersion, cols: orderColumns}}, nil
}

func (x *replicaTarget) Name() string { return "replica" }

func (x *replicaTarget) Wants(Change) bool { return true }

func (x *replicaTarget) Check(ctx context.Context, cs []Change) ([]Result, error) {
	groups, err := groupBy(cs, func(c Change) (*sql.DB, string, error) {
		if c.Slot < 0 || c.Slot >= x.router.Slots() {
			// slot 来自消息。越界说明 CDC 和这里的分片布局对不上，是配置问题，不能当成数据不一致。
			return nil, "", fmt.Errorf("changes: replica: order %d: slot %d out of range [0, %d)", c.OrderID, c.Slot, x.router.Slots())
		}
		return x.dbs[x.router.DB(c.Slot)], sharding.Table(SourceTable, c.Slot), nil
	})
	if err != nil {
		return nil, err
	}
	rows, found, err := readGroups(ctx, &x.t, cs, groups)
	if err != nil {
		return nil, fmt.Errorf("changes: replica: %w", err)
	}
	out := make([]Result, len(cs))
	for i, c := range cs {
		switch {
		case !c.Deleted:
			out[i] = judged(x.t.cols, c, rows[i], found[i])
		case found[i]:
			out[i] = Result{Verdict: Behind, Detail: fmt.Sprintf("row deleted on the source is still on the replica (version %d)", rows[i].version)}
		default:
			out[i] = Result{Verdict: OK}
		}
	}
	return out, nil
}

// archiveTarget 是冷库：orders_archive，一个库一张表。
type archiveTarget struct {
	db *sql.DB
	t  table
}

// NewArchiveTarget 返回冷库的目标。
//
// 只核对 DELETE。归档程序先写冷库、核对过再删热库（services/order/internal/archive 的 archiveBatch），
// 所以看到 DELETE 时冷库里一定已经有这一行，而且和删掉的那个版本相同。这个目标不返回 Behind：
// 冷库里没有这一行、版本不同、有列不同都是不一致。
func NewArchiveTarget(cold *sql.DB) (Target, error) {
	if cold == nil {
		return nil, errors.New("changes: archive target: nil database")
	}
	return &archiveTarget{db: cold, t: table{versionCol: ColVersion, cols: orderColumns}}, nil
}

func (x *archiveTarget) Name() string { return "archive" }

func (x *archiveTarget) Wants(c Change) bool { return c.Deleted }

func (x *archiveTarget) Check(ctx context.Context, cs []Change) ([]Result, error) {
	got, err := x.t.read(ctx, x.db, archiveTable, cs)
	if err != nil {
		return nil, fmt.Errorf("changes: archive: %w", err)
	}
	out := make([]Result, len(cs))
	for i, c := range cs {
		r, found := got[c.OrderID]
		switch {
		case !found:
			out[i] = Result{Verdict: Mismatch, Detail: "archived row not found in the cold database"}
		case r.version != c.Version:
			out[i] = Result{Verdict: Mismatch, Detail: fmt.Sprintf("column %s: derived %d, change %d", ColVersion, r.version, c.Version)}
		default:
			if diff, detail := compare(x.t.cols, c.Image, r); diff != "" {
				out[i] = Result{Verdict: Mismatch, Detail: detail}
			} else {
				out[i] = Result{Verdict: OK}
			}
		}
	}
	return out, nil
}
