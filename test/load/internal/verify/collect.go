package verify

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/palemoky/Oppenheimer/api/eventx"
	"github.com/palemoky/Oppenheimer/pkg/sharding"
	"github.com/palemoky/Oppenheimer/pkg/stockgate"
)

// Sources 是对账要读的外部数据源。
type Sources struct {
	InventoryDB *sql.DB
	// OrderDBs 按库序号排列，第 i 个是 order_db_i。订单按 slot 分表，slot 在库间的分布见 pkg/sharding。
	OrderDBs  []*sql.DB
	PaymentDB *sql.DB

	// Redis 是闸门所在的实例，RedisPrefix 是服务的 REDIS_PREFIX。
	Redis       *redis.Client
	RedisPrefix string

	// Kafka 为 nil 时不读死信 topic。KafkaBrokers 是它的引导地址（读死信时另建一个客户端），
	// KafkaPrefix 是服务的 KAFKA_PREFIX。
	Kafka        *kgo.Client
	KafkaBrokers []string
	KafkaPrefix  string

	// InventoryLog 是库存服务的日志文件，为空时不检查。
	InventoryLog string
}

// orderShards 列出每个订单库上的每张分片表所在的库和 slot。对账工具允许全分片扫描。
//
// 布局取 sharding.OrderLayout，与 order 服务是同一份。传入的库数必须与布局一致：少给一个库时
// 若按传入的个数重算布局，slot 会被分到错误的库上，对账就悄悄漏读了分片。
func orderShards(src Sources) (map[int][]int, error) {
	if len(src.OrderDBs) != sharding.OrderDBs {
		return nil, fmt.Errorf("verify: got %d order dbs, layout has %d", len(src.OrderDBs), sharding.OrderDBs)
	}
	r, err := sharding.New(sharding.OrderLayout())
	if err != nil {
		return nil, fmt.Errorf("build order shard router: %w", err)
	}
	out := make(map[int][]int, len(src.OrderDBs))
	for db := range src.OrderDBs {
		out[db] = r.SlotsOf(db)
	}
	return out, nil
}

// ConsumedTopics 是被消费的 topic 的基础名，每个有一个死信 topic。
var ConsumedTopics = []string{eventx.OrderEvents, eventx.PaymentEvents, eventx.OrderSubmit, eventx.OrderCloseDelay}

// ConsumerGroups 是各消费者组（基础名）及其消费的 topic（基础名）。
var ConsumerGroups = map[string]string{
	eventx.GroupInventoryOrderEvents: eventx.OrderEvents,
	eventx.GroupPaymentOrderEvents:   eventx.OrderEvents,
	eventx.GroupOrderPaymentEvents:   eventx.PaymentEvents,
	eventx.GroupOrderSubmit:          eventx.OrderSubmit,
	eventx.GroupOrderCloseDelay:      eventx.OrderCloseDelay,
}

// 库存服务的闸门对账在日志里留下的消息，见 services/inventory/internal/reconcile。
const (
	logMsgCorrected = `"msg":"stock gate deviation corrected"`
	logMsgCapped    = `"msg":"stock gate deviation persists, correction capped"`
)

// Collect 读出对账需要的全部数据。
func Collect(ctx context.Context, src Sources, initial map[int64]int64) (*Snapshot, error) {
	s := &Snapshot{Initial: initial, OutboxPending: map[string]int64{}}
	steps := []struct {
		name string
		fn   func(context.Context, Sources, *Snapshot) error
	}{
		{"inventory_db", collectInventory},
		{"order_db", collectOrders},
		{"payment_db", collectPayments},
		{"stock gate", collectGate},
		{"dead letter topics", collectDLQ},
		{"inventory log", collectLog},
	}
	for _, st := range steps {
		if err := st.fn(ctx, src, s); err != nil {
			return nil, fmt.Errorf("collect %s: %w", st.name, err)
		}
	}
	return s, nil
}

func collectInventory(ctx context.Context, src Sources, s *Snapshot) error {
	db := src.InventoryDB
	s.Stock = map[int64]SKUStock{}
	if err := queryEach(ctx, db, "SELECT sku_id, available, reserved, sold FROM sku_stock", func(r *sql.Rows) error {
		var st SKUStock
		if err := r.Scan(&st.SKUID, &st.Available, &st.Reserved, &st.Sold); err != nil {
			return err
		}
		s.Stock[st.SKUID] = st
		return nil
	}); err != nil {
		return err
	}

	s.Reservations = map[int64]Reservation{}
	if err := queryEach(ctx, db, "SELECT order_id, status FROM reservation", func(r *sql.Rows) error {
		res := Reservation{Items: map[int64]int64{}}
		if err := r.Scan(&res.OrderID, &res.Status); err != nil {
			return err
		}
		s.Reservations[res.OrderID] = res
		return nil
	}); err != nil {
		return err
	}

	return queryEach(ctx, db, "SELECT order_id, sku_id, quantity FROM reservation_item", func(r *sql.Rows) error {
		var orderID, sku, qty int64
		if err := r.Scan(&orderID, &sku, &qty); err != nil {
			return err
		}
		res, ok := s.Reservations[orderID]
		if !ok {
			// 明细没有头记录：当作一条状态未知的预占，让第 6 条报出来。
			res = Reservation{OrderID: orderID, Items: map[int64]int64{}}
			s.Reservations[orderID] = res
		}
		res.Items[sku] += qty
		return nil
	})
}

func collectOrders(ctx context.Context, src Sources, s *Snapshot) error {
	shards, err := orderShards(src)
	if err != nil {
		return err
	}
	s.Orders = map[int64]Order{}
	s.OrdersByIdem = map[IdemRef]int64{}
	var pending int64
	for db, slots := range shards {
		conn := src.OrderDBs[db]
		for _, slot := range slots {
			orders, items := sharding.Table("orders", slot), sharding.Table("order_items", slot)
			if err := queryEach(ctx, conn, `SELECT order_id, user_id, status, total_amount, currency,
		idempotency_key, COALESCE(payment_id, 0), COALESCE(fail_reason, '') FROM `+orders, func(r *sql.Rows) error {
				o := Order{Items: map[int64]int64{}}
				if err := r.Scan(&o.OrderID, &o.UserID, &o.Status, &o.TotalAmount, &o.Currency,
					&o.IdempotencyKey, &o.PaymentID, &o.FailReason); err != nil {
					return err
				}
				s.Orders[o.OrderID] = o
				s.OrdersByIdem[IdemRef{UserID: o.UserID, Key: o.IdempotencyKey}] = o.OrderID
				return nil
			}); err != nil {
				return err
			}
			// 订单和明细同槽同库，明细只会落在订单所在的那张分片表。
			if err := queryEach(ctx, conn, "SELECT order_id, sku_id, quantity FROM "+items, func(r *sql.Rows) error {
				var orderID, sku, qty int64
				if err := r.Scan(&orderID, &sku, &qty); err != nil {
					return err
				}
				if o, ok := s.Orders[orderID]; ok {
					o.Items[sku] += qty
				}
				return nil
			}); err != nil {
				return err
			}
		}
		n, err := countPendingOutbox(ctx, conn)
		if err != nil {
			return fmt.Errorf("order_db_%d: %w", db, err)
		}
		pending += n
	}
	s.OutboxPending["order"] = pending
	return nil
}

func collectPayments(ctx context.Context, src Sources, s *Snapshot) error {
	if err := queryEach(ctx, src.PaymentDB,
		"SELECT payment_id, order_id, user_id, status, amount, currency, notified FROM payments", func(r *sql.Rows) error {
			var p Payment
			if err := r.Scan(&p.PaymentID, &p.OrderID, &p.UserID, &p.Status, &p.Amount, &p.Currency, &p.Notified); err != nil {
				return err
			}
			s.Payments = append(s.Payments, p)
			return nil
		}); err != nil {
		return err
	}
	n, err := countPendingOutbox(ctx, src.PaymentDB)
	if err != nil {
		return err
	}
	s.OutboxPending["payment"] = n
	return nil
}

// queryEach 执行查询并对每一行调用 scan。
func queryEach(ctx context.Context, db *sql.DB, query string, scan func(*sql.Rows) error) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query %q: %w", query, err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return fmt.Errorf("scan %q: %w", query, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %q: %w", query, err)
	}
	return nil
}

func countPendingOutbox(ctx context.Context, db *sql.DB) (int64, error) {
	var n int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL").Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending outbox: %w", err)
	}
	return n, nil
}

func collectGate(ctx context.Context, src Sources, s *Snapshot) error {
	gate := stockgate.New(src.Redis, src.RedisPrefix)
	skus, err := gate.SKUs(ctx)
	if err != nil {
		return fmt.Errorf("list gate skus: %w", err)
	}
	s.Gate = map[int64]GateStock{}
	s.Bought = map[int64]map[int64]int64{}
	for _, sku := range skus {
		st, ok, err := gate.Stock(ctx, sku)
		if err != nil {
			return fmt.Errorf("read gate sku %d: %w", sku, err)
		}
		if !ok {
			continue
		}
		s.Gate[sku] = GateStock{Avail: st.Avail, Held: st.Held, Limit: st.Limit}

		m, err := src.Redis.HGetAll(ctx, src.RedisPrefix+"gate:bought:"+strconv.FormatInt(sku, 10)).Result()
		if err != nil {
			return fmt.Errorf("read bought of sku %d: %w", sku, err)
		}
		bought := make(map[int64]int64, len(m))
		for u, q := range m {
			uid, err1 := strconv.ParseInt(u, 10, 64)
			qty, err2 := strconv.ParseInt(q, 10, 64)
			if err := errors.Join(err1, err2); err != nil {
				return fmt.Errorf("bad bought entry %q=%q of sku %d: %w", u, q, sku, err)
			}
			bought[uid] = qty
		}
		s.Bought[sku] = bought
	}

	s.Holds = map[int64]bool{}
	holdPrefix := src.RedisPrefix + "gate:hold:"
	it := src.Redis.Scan(ctx, 0, holdPrefix+"*", 1000).Iterator()
	for it.Next(ctx) {
		id, err := strconv.ParseInt(strings.TrimPrefix(it.Val(), holdPrefix), 10, 64)
		if err != nil {
			return fmt.Errorf("bad hold key %q: %w", it.Val(), err)
		}
		s.Holds[id] = true
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("scan hold keys: %w", err)
	}

	if s.Unclaimed, err = zsetMembers(ctx, src.Redis, src.RedisPrefix+"gate:holds"); err != nil {
		return err
	}
	if s.Claimed, err = zsetMembers(ctx, src.Redis, src.RedisPrefix+"gate:claimed"); err != nil {
		return err
	}
	return nil
}

func zsetMembers(ctx context.Context, rdb *redis.Client, key string) ([]int64, error) {
	ms, err := rdb.ZRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", key, err)
	}
	out := make([]int64, 0, len(ms))
	for _, m := range ms {
		id, err := strconv.ParseInt(m, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad member %q of %s: %w", m, key, err)
		}
		out = append(out, id)
	}
	return out, nil
}

// DLQTopics 返回带前缀的死信 topic 名。
func DLQTopics(prefix string) []string {
	out := make([]string, 0, len(ConsumedTopics))
	for _, t := range ConsumedTopics {
		out = append(out, eventx.Topic(prefix, t)+eventx.DLQSuffix)
	}
	return out
}

// dlqReadTimeout 是读完死信 topic 的时限。死信 topic 只有几条消息，读不完说明连接有问题。
const dlqReadTimeout = 30 * time.Second

func collectDLQ(ctx context.Context, src Sources, s *Snapshot) error {
	if src.Kafka == nil {
		return nil
	}
	topics := DLQTopics(src.KafkaPrefix)
	adm := kadm.NewClient(src.Kafka)
	starts, err := adm.ListStartOffsets(ctx, topics...)
	if err != nil {
		return fmt.Errorf("list start offsets: %w", err)
	}
	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return fmt.Errorf("list end offsets: %w", err)
	}
	want := map[string]map[int32]kgo.Offset{}
	remaining := map[string]map[int32]int64{} // 每个分区还要读到的 end offset
	total := 0
	var errs []error
	ends.Each(func(e kadm.ListedOffset) {
		if e.Err != nil {
			errs = append(errs, fmt.Errorf("end offset of %s/%d: %w", e.Topic, e.Partition, e.Err))
			return
		}
		st, ok := starts.Lookup(e.Topic, e.Partition)
		if !ok || st.Err != nil {
			errs = append(errs, fmt.Errorf("no start offset of %s/%d", e.Topic, e.Partition))
			return
		}
		if e.Offset <= st.Offset {
			return
		}
		if want[e.Topic] == nil {
			want[e.Topic] = map[int32]kgo.Offset{}
			remaining[e.Topic] = map[int32]int64{}
		}
		want[e.Topic][e.Partition] = kgo.NewOffset().At(st.Offset)
		remaining[e.Topic][e.Partition] = e.Offset
		total += int(e.Offset - st.Offset)
	})
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if total == 0 {
		return nil
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(src.KafkaBrokers...),
		kgo.ConsumePartitions(want))
	if err != nil {
		return fmt.Errorf("new dlq reader: %w", err)
	}
	defer cl.Close()

	rctx, cancel := context.WithTimeout(ctx, dlqReadTimeout)
	defer cancel()
	for len(remaining) > 0 {
		fs := cl.PollFetches(rctx)
		if err := rctx.Err(); err != nil {
			return fmt.Errorf("read dead letter topics (%d messages expected, got %d): %w", total, len(s.DLQ), err)
		}
		var ferr error
		fs.EachError(func(t string, p int32, err error) {
			ferr = errors.Join(ferr, fmt.Errorf("fetch %s/%d: %w", t, p, err))
		})
		if ferr != nil {
			return ferr
		}
		fs.EachRecord(func(r *kgo.Record) {
			end, ok := remaining[r.Topic][r.Partition]
			if !ok || r.Offset >= end {
				return
			}
			s.DLQ = append(s.DLQ, dlqMessage(r))
			if r.Offset+1 >= end {
				delete(remaining[r.Topic], r.Partition)
				if len(remaining[r.Topic]) == 0 {
					delete(remaining, r.Topic)
				}
			}
		})
	}
	return nil
}

func dlqMessage(r *kgo.Record) DLQMessage {
	m := DLQMessage{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: string(r.Key), Headers: map[string]string{}}
	for _, h := range r.Headers {
		m.Headers[h.Key] = string(h.Value)
	}
	m.Body = decodeBody(m.Headers["dlq-topic"], r.Value)
	return m
}

// decodeBody 按原 topic 把消息体解码成文本。
func decodeBody(topic string, value []byte) string {
	var (
		msg proto.Message
		err error
	)
	switch {
	case strings.HasSuffix(topic, eventx.OrderEvents):
		msg, err = eventx.UnmarshalOrderEvent(value)
	case strings.HasSuffix(topic, eventx.PaymentEvents):
		msg, err = eventx.UnmarshalPaymentEvent(value)
	case strings.HasSuffix(topic, eventx.OrderSubmit):
		msg, err = eventx.UnmarshalOrderSubmitted(value)
	case strings.HasSuffix(topic, eventx.OrderCloseDelay):
		msg, err = eventx.UnmarshalOrderCloseDue(value)
	default:
		return "hex:" + hex.EncodeToString(value)
	}
	if err != nil {
		return "hex:" + hex.EncodeToString(value)
	}
	b, err := prototext.MarshalOptions{}.Marshal(msg)
	if err != nil {
		return "hex:" + hex.EncodeToString(value)
	}
	return "{" + strings.TrimSpace(string(b)) + "}"
}

func collectLog(_ context.Context, src Sources, s *Snapshot) error {
	if src.InventoryLog == "" {
		return nil
	}
	f, err := os.Open(src.InventoryLog)
	if err != nil {
		return fmt.Errorf("open %s: %w", src.InventoryLog, err)
	}
	defer f.Close()
	s.Corrections, s.Capped, err = scanLog(f)
	return err
}

// scanLog 找出日志里闸门对账修正和被截住的行。
func scanLog(f io.Reader) (corrected, capped []string, err error) {
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.Contains(line, logMsgCorrected):
			corrected = append(corrected, line)
		case strings.Contains(line, logMsgCapped):
			capped = append(capped, line)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, nil, fmt.Errorf("read log: %w", err)
	}
	return corrected, capped, nil
}
