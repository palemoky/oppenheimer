package verify

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/palemoky/Oppenheimer/api/eventx"
	eventv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/event/v1"
	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/pkg/sharding"
	"github.com/palemoky/Oppenheimer/pkg/stockgate"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
	"github.com/palemoky/Oppenheimer/pkg/testkafka"
	"github.com/palemoky/Oppenheimer/pkg/testredis"
)

// newDB 用仓库里服务的迁移文件建库，不依赖服务模块。
func newDB(t *testing.T, service string) *sql.DB {
	t.Helper()
	return testdb.New(t, os.DirFS(filepath.Join("..", "..", "..", "..", "services", service)), "migrations")
}

// newOrderDBs 建两个订单库，按 pkg/sharding 的分布手工建分片表：库 0 有偶数 slot，库 1 有奇数 slot，
// 每个库还有一张 outbox。表结构与 services/order 的迁移一致。
func newOrderDBs(t *testing.T) []*sql.DB {
	t.Helper()
	r, err := sharding.New(sharding.Config{Slots: OrderSlots, DBs: 2})
	if err != nil {
		t.Fatal(err)
	}
	dbs := make([]*sql.DB, 2)
	for i := range dbs {
		var sb strings.Builder
		sb.WriteString("-- +goose Up\n")
		for _, slot := range r.SlotsOf(i) {
			fmt.Fprintf(&sb, `CREATE TABLE %s (
	order_id BIGINT NOT NULL, user_id BIGINT NOT NULL, status TINYINT NOT NULL,
	total_amount BIGINT NOT NULL DEFAULT 0, currency CHAR(3) NOT NULL DEFAULT 'CNY',
	idempotency_key VARCHAR(64) NOT NULL, request_hash BINARY(32) NOT NULL,
	payment_id BIGINT NULL, fail_reason VARCHAR(64) NULL, expire_at DATETIME(6) NOT NULL,
	paid_at DATETIME(6) NULL, created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
	PRIMARY KEY (order_id), UNIQUE KEY uk_user_idem (user_id, idempotency_key)
);
CREATE TABLE %s (
	order_id BIGINT NOT NULL, sku_id BIGINT NOT NULL, quantity INT NOT NULL, unit_price BIGINT NOT NULL DEFAULT 0,
	created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), PRIMARY KEY (order_id, sku_id)
);
`, sharding.Table("orders", slot), sharding.Table("order_items", slot))
		}
		sb.WriteString(`CREATE TABLE outbox (
	id BIGINT NOT NULL AUTO_INCREMENT, topic VARCHAR(128) NOT NULL, msg_key VARBINARY(128) NOT NULL,
	payload MEDIUMBLOB NOT NULL, created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	published_at DATETIME(6) NULL, PRIMARY KEY (id)
);
-- +goose Down
`)
		fsys := fstest.MapFS{"m/00001_init.sql": {Data: []byte(sb.String())}}
		dbs[i] = testdb.New(t, fsys, "m")
	}
	return dbs
}

func exec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

func TestCollectAndProbe(t *testing.T) {
	inv, pay := newDB(t, "inventory"), newDB(t, "payment")
	ords := newOrderDBs(t)
	rdb, prefix := testredis.New(t)
	brokers := testkafka.New(t)
	ctx := t.Context()

	kprefix := "test-verify-" + strings.ToLower(rand.Text()[:8]) + "."
	for _, base := range ConsumedTopics {
		testkafka.TopicNamed(t, brokers, eventx.Topic(kprefix, base), 1)
		testkafka.TopicNamed(t, brokers, eventx.Topic(kprefix, base)+eventx.DLQSuffix, 1)
	}
	kc, err := kafkax.NewClient(ctx, kafkax.Config{Brokers: brokers, ClientID: "verify-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kc.Close)

	// 库存：一个 SKU，订单 1 已确认 2 件，订单 2 预占中 1 件。
	exec(t, inv, "INSERT INTO sku_stock (sku_id, name, unit_price, available, reserved, sold) VALUES (1001, 'x', 100, 7, 1, 2)")
	exec(t, inv, "INSERT INTO reservation (order_id, status) VALUES (1, 2), (2, 1)")
	exec(t, inv, "INSERT INTO reservation_item (order_id, sku_id, quantity, unit_price, currency) VALUES (1, 1001, 2, 100, 'CNY'), (2, 1001, 1, 100, 'CNY')")
	// 订单：1 已付清，2 待支付。
	// 订单 1 在库 0 的 orders_2，订单 2 在库 1 的 orders_3：汇总必须跨库跨 slot 表。
	const insOrder = `INSERT INTO %s (order_id, user_id, status, total_amount, currency, idempotency_key, request_hash, payment_id, expire_at)
		VALUES (%s, NOW(6))`
	exec(t, ords[0], fmt.Sprintf(insOrder, "orders_2", "1, 11, 3, 200, 'CNY', 'k1', REPEAT('a', 32), 101"))
	exec(t, ords[1], fmt.Sprintf(insOrder, "orders_3", "2, 12, 2, 100, 'CNY', 'k2', REPEAT('b', 32), NULL"))
	exec(t, ords[0], "INSERT INTO order_items_2 (order_id, sku_id, quantity, unit_price) VALUES (1, 1001, 2, 100)")
	exec(t, ords[1], "INSERT INTO order_items_3 (order_id, sku_id, quantity, unit_price) VALUES (2, 1001, 1, 100)")
	// 两个库各有一条未发布的 outbox，汇总为 2。
	exec(t, ords[0], "INSERT INTO outbox (topic, msg_key, payload) VALUES ('t', 'k', 'v')")
	exec(t, ords[1], "INSERT INTO outbox (topic, msg_key, payload) VALUES ('t', 'k', 'v')")
	// 支付：101 成功已通知，102 处理中。
	exec(t, pay, `INSERT INTO payments (payment_id, order_id, user_id, status, amount, currency, outcome, notified)
		VALUES (101, 1, 11, 2, 200, 'CNY', 1, 1), (102, 2, 12, 1, 100, 'CNY', 1, 0)`)

	// 闸门：装载 10 件；订单 2 有一笔已认领的占用，订单 3 有一笔未认领的。
	gate := stockgate.New(rdb, prefix)
	if _, err := gate.Load(ctx, 1001, 10, 0); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, h := range []struct {
		order, user int64
		key         string
	}{{2, 12, "k2"}, {3, 13, "k3"}} {
		res, err := gate.Hold(ctx, stockgate.HoldRequest{
			OrderID: h.order, UserID: h.user, IdempotencyKey: h.key,
			RequestHash: make([]byte, 32), Items: []stockgate.Item{{SKUID: 1001, Quantity: 1}}, Now: now, IdempotencyTTL: time.Hour,
		})
		if err != nil || res.Outcome != stockgate.Held {
			t.Fatalf("hold %d: %v %v", h.order, res, err)
		}
	}
	if _, ok, err := gate.Claim(ctx, 2); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}

	// Kafka：order.events 写 3 条，inventory 的组提交到 1，payment 的组从未提交；死信里一条。
	evTopic := eventx.Topic(kprefix, eventx.OrderEvents)
	for i := range 3 {
		k, v, err := eventx.MarshalOrderEvent(&eventv1.OrderEvent{OrderId: int64(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
		if err := kc.ProduceSync(ctx, &kgo.Record{Topic: evTopic, Key: k, Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	adm := kadm.NewClient(kc)
	var offs kadm.Offsets
	offs.Add(kadm.Offset{Topic: evTopic, Partition: 0, At: 1, LeaderEpoch: -1})
	if _, err := adm.CommitOffsets(ctx, eventx.Group(kprefix, eventx.GroupInventoryOrderEvents), offs); err != nil {
		t.Fatal(err)
	}
	dlqValue, err := proto.Marshal(&eventv1.OrderSubmitted{OrderId: 42, UserId: 7})
	if err != nil {
		t.Fatal(err)
	}
	if err := kc.ProduceSync(ctx, &kgo.Record{
		Topic: eventx.Topic(kprefix, eventx.OrderSubmit) + eventx.DLQSuffix, Key: []byte("42"), Value: dlqValue,
		Headers: []kgo.RecordHeader{
			{Key: kafkax.HeaderDLQError, Value: []byte("boom")},
			{Key: kafkax.HeaderDLQTopic, Value: []byte(eventx.Topic(kprefix, eventx.OrderSubmit))},
		},
	}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(t.TempDir(), "inventory.log")
	if err := os.WriteFile(logPath, []byte(`{"msg":"stock gate deviation corrected","sku_id":1001}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	src := Sources{
		InventoryDB: inv, OrderDBs: ords, PaymentDB: pay,
		Redis: rdb, RedisPrefix: prefix,
		Kafka: kc, KafkaBrokers: brokers, KafkaPrefix: kprefix,
		InventoryLog: logPath,
	}

	snap, err := Collect(ctx, src, map[int64]int64{1001: 10})
	if err != nil {
		t.Fatal(err)
	}
	if st := snap.Stock[1001]; st != (SKUStock{SKUID: 1001, Available: 7, Reserved: 1, Sold: 2}) {
		t.Errorf("stock = %+v", st)
	}
	if r := snap.Reservations[1]; r.Status != ReservationConfirmed || r.Items[1001] != 2 {
		t.Errorf("reservation 1 = %+v", r)
	}
	if o := snap.Orders[1]; o.Status != OrderPaid || o.PaymentID != 101 || o.Items[1001] != 2 || o.IdempotencyKey != "k1" {
		t.Errorf("order 1 = %+v", o)
	}
	if snap.OrdersByIdem[IdemRef{UserID: 12, Key: "k2"}] != 2 {
		t.Errorf("orders by idem = %v", snap.OrdersByIdem)
	}
	if len(snap.Payments) != 2 || snap.OutboxPending["order"] != 2 || snap.OutboxPending["payment"] != 0 {
		t.Errorf("payments %v outbox %v", snap.Payments, snap.OutboxPending)
	}
	if g := snap.Gate[1001]; g.Avail != 8 || g.Held != 2 {
		t.Errorf("gate = %+v", g)
	}
	if snap.Bought[1001][12] != 1 || snap.Bought[1001][13] != 1 {
		t.Errorf("bought = %v", snap.Bought)
	}
	if !snap.Holds[2] || !snap.Holds[3] || !slices.Equal(snap.Unclaimed, []int64{3}) || !slices.Equal(snap.Claimed, []int64{2}) {
		t.Errorf("holds %v unclaimed %v claimed %v", snap.Holds, snap.Unclaimed, snap.Claimed)
	}
	if len(snap.DLQ) != 1 || snap.DLQ[0].Key != "42" || snap.DLQ[0].Headers["dlq-error"] != "boom" ||
		!strings.Contains(snap.DLQ[0].Body, "order_id:42") {
		t.Errorf("dlq = %+v", snap.DLQ)
	}
	if len(snap.Corrections) != 1 {
		t.Errorf("corrections = %v", snap.Corrections)
	}

	q, err := Probe(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if q.OutboxPending["order"] != 2 || q.PendingPaymentOrders != 1 || q.CreatingOrders != 0 || q.PendingPayments != 1 ||
		q.UnclaimedHolds != 1 || q.ClaimedHolds != 1 {
		t.Errorf("quiet state = %+v", q)
	}
	if len(q.LagErr) != 0 {
		t.Errorf("lag errors = %v", q.LagErr)
	}
	// inventory 的组提交到 1，积压 2；payment 的组从未提交，从头算 3；其余 topic 为空。
	if got := q.Lag[eventx.Group(kprefix, eventx.GroupInventoryOrderEvents)]; got != 2 {
		t.Errorf("inventory lag = %d", got)
	}
	if got := q.Lag[eventx.Group(kprefix, eventx.GroupPaymentOrderEvents)]; got != 3 {
		t.Errorf("payment lag = %d", got)
	}
	if got := q.Lag[eventx.Group(kprefix, eventx.GroupOrderSubmit)]; got != 0 {
		t.Errorf("submit lag = %d", got)
	}

	// 超时时返回错误和最后的状态。
	_, last, err := WaitQuiet(context.Background(), src, 300*time.Millisecond, 100*time.Millisecond, discardLogger())
	if err == nil || len(last.Problems()) == 0 {
		t.Fatalf("wait quiet err %v problems %v", err, last.Problems())
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }
