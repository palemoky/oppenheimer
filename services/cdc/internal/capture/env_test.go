package capture

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/pkg/testkafka"
)

// 集成测试的公共部分：每个测试一个临时库、两个临时 topic、自己的 server-id。

// serverIDs 给每条测试流水线分配不同的 server-id。同一个 server-id 的两个 dump 连接会互相踢掉，
// 所以并行的测试（以及同时跑的另一个 go test 进程）都不能重复。
var serverIDs atomic.Uint32

func init() { serverIDs.Store(100000 + rand.Uint32N(1<<30)) }

// ordersDDL 仿照订单表，另加几列覆盖其他类型。
const ordersDDL = `CREATE TABLE %s (
    order_id     BIGINT       NOT NULL,
    user_id      BIGINT       NOT NULL,
    merchant_id  BIGINT       NULL,
    status       TINYINT      NOT NULL,
    total_amount BIGINT       NOT NULL DEFAULT 0,
    currency     CHAR(3)      NOT NULL DEFAULT 'CNY',
    request_hash BINARY(32)   NOT NULL,
    fail_reason  VARCHAR(64)  NULL,
    expire_at    DATETIME(6)  NOT NULL,
    paid_at      DATETIME(6)  NULL,
    version      BIGINT       NOT NULL DEFAULT 0,
    qty          INT UNSIGNED NOT NULL DEFAULT 0,
    ratio        FLOAT        NULL,
    score        DOUBLE       NULL,
    price        DECIMAL(10,2) NULL,
    note         TEXT         NULL,
    tag          VARBINARY(16) NULL,
    payload      BLOB         NULL,
    PRIMARY KEY (order_id)
) ENGINE = InnoDB`

// requestHash 的末尾是 0x00：binlog 会截掉它们，用来检验补齐。
var requestHash = append([]byte("0123456789abcdef0123456789abcd"), 0, 0)

type env struct {
	t           *testing.T
	db          *sql.DB
	dsn         string
	brokers     []string
	topic       string
	store       *CheckpointStore
	checkpoints string
}

func newEnv(t *testing.T, tables ...string) *env {
	t.Helper()
	db, name := testdb.Empty(t)
	brokers := testkafka.New(t)
	cfg, err := driver.ParseDSN(os.Getenv(testdb.DSNEnv))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = name

	e := &env{
		t:           t,
		db:          db,
		dsn:         cfg.FormatDSN(),
		brokers:     brokers,
		topic:       testkafka.Topic(t, brokers, 4),
		checkpoints: testkafka.Topic(t, brokers, 1),
	}
	writer := e.producer()
	e.store = NewCheckpointStore(e.kafkaConfig(), e.checkpoints, writer)
	for _, table := range tables {
		testdb.MustExec(t, db, fmt.Sprintf(ordersDDL, table))
	}
	return e
}

func (e *env) kafkaConfig() kafkax.Config {
	return kafkax.Config{Brokers: e.brokers, ClientID: "cdc-test"}
}

func (e *env) producer() *kgo.Client {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := kafkax.NewClient(ctx, e.kafkaConfig(), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordDeliveryTimeout(time.Minute))
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(c.Close)
	return c
}

type pipelineOpt func(*Config)

func withBatch(n int) pipelineOpt { return func(c *Config) { c.SnapshotBatch = n } }

func (e *env) pipeline(serverID uint32, opts ...pipelineOpt) *Pipeline {
	e.t.Helper()
	cfg := Config{
		Name:     "order-test",
		DSN:      e.dsn,
		ServerID: serverID,
		Rules: []Rule{{
			Pattern:       regexp.MustCompile(`^orders_(\d+)$`),
			Table:         "orders",
			Topic:         e.topic,
			KeyColumn:     "order_id",
			VersionColumn: "version",
		}},
		Producer:           e.producer(),
		Checkpoints:        e.store,
		CheckpointInterval: 100 * time.Millisecond,
		Logger:             slog.New(slog.NewTextHandler(testWriter{e.t}, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}
	for _, o := range opts {
		o(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	return p
}

// testWriter 把日志写到 t.Log，测试失败时才显示。
type testWriter struct{ t *testing.T }

func (w testWriter) Write(b []byte) (int, error) {
	w.t.Log(string(b))
	return len(b), nil
}

// running 是一条在后台运行的流水线。
type running struct {
	p      *Pipeline
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func (e *env) start(p *Pipeline) *running {
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{p: p, cancel: cancel, done: make(chan struct{})}
	go func() {
		r.err = p.Start(ctx)
		close(r.done)
	}()
	e.t.Cleanup(func() {
		p.requestStop()
		cancel()
		<-r.done
	})
	return r
}

// wait 等 Start 返回，给出它的结果。
func (r *running) wait() error {
	<-r.done
	return r.err
}

// stop 优雅停止，返回 Start 的结果。
func (r *running) stop(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := r.p.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	return r.wait()
}

// crash 模拟进程被杀：取消 Start 的 ctx，不走优雅停止，最后一次位点写不进去。
func (r *running) crash() error {
	r.cancel()
	r.p.requestStop()
	return r.wait()
}

// waitStreaming 等到全量完成、开始读 binlog（位点里不再有全量游标）。
func (e *env) waitStreaming(r *running) {
	e.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-r.done:
			e.t.Fatalf("pipeline exited before streaming: %v", r.err)
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cp, err := e.store.Load(ctx, "order-test")
		cancel()
		if err != nil {
			e.t.Fatal(err)
		}
		if cp != nil && cp.GetSnapshot() == nil {
			// 位点写在 StartSyncGTID 之前；之后的写入都在 G 之后，一定会从 binlog 读到。
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.t.Fatal("pipeline did not reach streaming")
}

// collector 从头消费变更 topic，累积收到的消息。
type collector struct {
	t      *testing.T
	client *kgo.Client
	msgs   []*cdcv1.RowChange
	keys   []string
}

func (e *env) collector() *collector {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := kafkax.NewClient(ctx, e.kafkaConfig(),
		kgo.ConsumeTopics(e.topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(client.Close)
	return &collector{t: e.t, client: client}
}

// waitFor 一直消费，直到 done 对目前收到的全部消息返回真，返回这些消息。
func (c *collector) waitFor(desc string, done func([]*cdcv1.RowChange) bool) []*cdcv1.RowChange {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for !done(c.msgs) {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			c.t.Fatalf("timed out waiting for %s; got %d messages", desc, len(c.msgs))
		}
		fetches.EachError(func(topic string, p int32, err error) { c.t.Fatalf("fetch %s/%d: %v", topic, p, err) })
		fetches.EachRecord(func(r *kgo.Record) {
			m := &cdcv1.RowChange{}
			if err := proto.Unmarshal(r.Value, m); err != nil {
				c.t.Fatalf("unmarshal: %v", err)
			}
			c.msgs = append(c.msgs, m)
			c.keys = append(c.keys, string(r.Key))
		})
	}
	return c.msgs
}

// drain 再消费一小段时间，收下可能还在路上的消息（用来断言“没有多余的消息”）。
func (c *collector) drain(d time.Duration) []*cdcv1.RowChange {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	for ctx.Err() == nil {
		c.client.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			m := &cdcv1.RowChange{}
			if err := proto.Unmarshal(r.Value, m); err != nil {
				c.t.Fatalf("unmarshal: %v", err)
			}
			c.msgs = append(c.msgs, m)
			c.keys = append(c.keys, string(r.Key))
		})
	}
	return c.msgs
}

func find(ms []*cdcv1.RowChange, orderID int64, op cdcv1.Op) *cdcv1.RowChange {
	for _, m := range ms {
		if m.GetOp() == op && orderIDOf(m) == orderID {
			return m
		}
	}
	return nil
}

func orderIDOf(m *cdcv1.RowChange) int64 {
	img := m.GetAfter()
	if m.GetOp() == cdcv1.Op_OP_DELETE {
		img = m.GetBefore()
	}
	return img["order_id"].GetInt()
}

// insertOrder 插入一行，各类型的列都填上值。
func insertOrder(t *testing.T, db *sql.DB, table string, orderID int64) {
	t.Helper()
	testdb.MustExec(t, db, "INSERT INTO "+table+` (order_id, user_id, status, currency, request_hash, fail_reason,
		expire_at, version, qty, ratio, score, price, note, tag, payload)
		VALUES (?, 42, 2, 'USD', ?, NULL, '2026-06-01 12:00:00.123456', 0, 4000000000, 0.1, 0.1, 12.30, 'hello', ?, ?)`,
		orderID, requestHash, []byte{0, 1}, []byte{1, 2, 3})
}
