// Package outbox 实现本地消息表：把“要发布的事件”和业务变更写在同一个数据库事务里，
// 再由中继（Relay）把它们发到 Kafka。
//
// 它解决的问题是：服务既要改自己的库，又要通知别人，而这两件事不在一个事务里。
// 先提交再发消息，中间崩溃会丢消息；先发消息再提交，事务回滚就通知了一件没发生的事。
// 把消息本身作为一行数据和业务变更一起提交，“状态变了”和“消息一定会发出去”就由本地事务保证了。
//
// 投递语义是至少一次：发送成功但还没来得及标记就崩溃，重启后会再发一遍。消费者必须幂等。
//
// 设计与取舍见 docs/adr/0008-outbox-and-events.md 和 docs/design/1b-high-concurrency.md。
package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/pkg/worker"
)

// Schema 是 outbox 表的建表语句。各服务把它放进自己的迁移文件（表名固定为 outbox）。
//
// 索引 (published_at, id)：中继按 “published_at IS NULL ORDER BY id” 取待发送的行，
// 清理任务按 “published_at < ? ORDER BY id” 删旧行，两者都走这个索引。
const Schema = `CREATE TABLE outbox (
    id            BIGINT          NOT NULL AUTO_INCREMENT,
    topic         VARCHAR(128)    NOT NULL,
    msg_key       VARBINARY(128)  NOT NULL,
    payload       MEDIUMBLOB      NOT NULL,
    created_at    DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at  DATETIME(6)     NULL,
    PRIMARY KEY (id),
    KEY idx_outbox_pending (published_at, id)
) ENGINE = InnoDB`

// EventIDHeader 是消息里携带事件唯一标识的 header，值为 "<中继名>:<outbox 行 id>"。
// 同一个事件被重发时这个值不变，消费者可以用它去重或排查问题。
const EventIDHeader = "event-id"

// Message 是一条待发布的消息。
type Message struct {
	Topic string
	// Key 决定消息进哪个分区。同一个 Key 的消息保持顺序，一般用业务主键（如订单号）。
	Key []byte
	// Payload 是消息体，由调用方序列化。
	Payload []byte
}

// Add 在调用方的事务里写入消息。
//
// 参数类型是 *sql.Tx 而不是 *sql.DB：从类型上保证它只能在事务里调用。
// 在事务外写 outbox 就失去了和业务变更的原子性，这个包也就没有意义了。
func Add(ctx context.Context, tx *sql.Tx, msgs ...Message) error {
	for _, m := range msgs {
		if m.Topic == "" {
			return errors.New("outbox: empty topic")
		}
		// 驱动把 nil 的 []byte 写成 NULL，而这两列是 NOT NULL。没有 key（由客户端挑分区）
		// 和空消息体都是合法的消息，所以统一存成空串；fetch 再把空 key 还原成 nil。
		key, payload := m.Key, m.Payload
		if key == nil {
			key = []byte{}
		}
		if payload == nil {
			payload = []byte{}
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO outbox (topic, msg_key, payload) VALUES (?, ?, ?)",
			m.Topic, key, payload,
		); err != nil {
			return fmt.Errorf("outbox: insert: %w", err)
		}
	}
	return nil
}

// Pending 返回尚未发布的消息数，用于测试和监控（积压）。
func Pending(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL").Scan(&n); err != nil {
		return 0, fmt.Errorf("outbox: count pending: %w", err)
	}
	return n, nil
}

// RelayConfig 是中继的配置。零值字段取默认值。
type RelayConfig struct {
	// Name 是中继的名字，一般用服务名。它出现在组件名和 event-id 里，必填。
	Name string
	DB   *sql.DB
	// Sender 是消息的去向，生产环境是 *kafkax.Producer。
	Sender kafkax.Sender
	Logger *slog.Logger

	// Interval 是兜底的轮询间隔，默认 100 毫秒。
	// 正常情况下事务提交后会调用 Kick 立即触发，轮询只负责捡起“提交后、Kick 前进程崩溃”留下的消息。
	Interval time.Duration
	// BatchSize 是每次从表里取出并发送的最大行数，默认 200。
	BatchSize int
	// Retention 是已发布消息的保留时长，默认 1 小时。保留一段时间便于排查问题。
	Retention time.Duration
	// CleanupInterval 是清理已发布消息的间隔，默认 1 分钟。
	CleanupInterval time.Duration
	// Guard 可选。传入分布式锁可以让多个实例里只有一个在发送。
	// 这只是为了避免重复劳动：多个中继同时运行也是正确的，只会多发几次。
	Guard worker.Guard
	// Now 是可注入的时钟，默认 time.Now。
	Now func() time.Time
}

// Relay 把 outbox 表里未发布的消息发出去。它实现 app.Component。
type Relay struct {
	cfg  RelayConfig
	loop *worker.Loop

	// lastCleanup 只在循环的 goroutine 里读写（worker.Loop 保证同一时刻只有一轮在运行）。
	lastCleanup time.Time
}

// NewRelay 校验配置并创建中继。
func NewRelay(cfg RelayConfig) (*Relay, error) {
	switch {
	case cfg.Name == "":
		return nil, errors.New("outbox: relay name is required")
	case cfg.DB == nil:
		return nil, errors.New("outbox: relay db is required")
	case cfg.Sender == nil:
		return nil, errors.New("outbox: relay sender is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 100 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 200
	}
	if cfg.Retention <= 0 {
		cfg.Retention = time.Hour
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	r := &Relay{cfg: cfg}
	loop, err := worker.New(worker.Config{
		Name:     cfg.Name + "-outbox-relay",
		Interval: cfg.Interval,
		Logger:   cfg.Logger,
		Guard:    cfg.Guard,
		// 启动后立即跑一轮：上次停机时可能留有未发送的消息。
		RunOnStart: true,
	}, r.round)
	if err != nil {
		return nil, fmt.Errorf("outbox: %w", err)
	}
	r.loop = loop
	return r, nil
}

// Name 实现 app.Component。
func (r *Relay) Name() string { return r.loop.Name() }

// Start 实现 app.Component。
func (r *Relay) Start(ctx context.Context) error { return r.loop.Start(ctx) }

// Stop 实现 app.Component。它等正在进行的一轮发送结束。
func (r *Relay) Stop(ctx context.Context) error { return r.loop.Stop(ctx) }

// Kick 让中继立即开始一轮发送。业务代码在事务**提交之后**调用它。
//
// 提交之前调用没有意义：中继在另一个连接上，看不到未提交的行。
// 忘记调用也不会丢消息，只是要等到下一次轮询。
func (r *Relay) Kick() { r.loop.Kick() }

func (r *Relay) round(ctx context.Context) error {
	if _, err := r.Flush(ctx); err != nil {
		return err
	}
	now := r.cfg.Now()
	if now.Sub(r.lastCleanup) >= r.cfg.CleanupInterval {
		r.lastCleanup = now
		if err := r.cleanup(ctx, now.Add(-r.cfg.Retention)); err != nil {
			return err
		}
	}
	return nil
}

type row struct {
	id      int64
	topic   string
	key     []byte
	payload []byte
}

// Flush 把当前所有未发布的消息发送一遍，返回成功发布的条数。
// 有消息发送失败时返回错误，失败的消息留在表里等下一轮。
//
// 中继的每一轮就是调用它；导出是为了让测试和工具能同步地触发一次发送。
func (r *Relay) Flush(ctx context.Context) (published int, err error) {
	// afterID 只是**这一轮之内**的翻页位置，每一轮都从 0 开始。
	//
	// 不能把它当成跨轮的游标（“上次发到哪了”）保存下来。自增 id 是在 INSERT 时分配的，
	// 不是在提交时：事务 A 拿到 id 5，事务 B 拿到 id 6，B 先提交。这时只能看到 6，
	// 如果把游标推进到 6，等 A 提交之后 5 就永远不会被发出去。
	// 所以“哪些还没发”只能以每一行的 published_at 为准，每一轮重新扫描。
	//
	// 这一轮之内用 afterID 翻页，是为了让发送失败的行不会一直占着队首：
	// 如果每次都从头取一批，而队首恰好有一批发不出去的消息，后面的消息就永远轮不到。
	var afterID int64
	var sendErr error
	for {
		rows, err := r.fetch(ctx, afterID)
		if err != nil {
			return published, err
		}
		if len(rows) == 0 {
			break
		}
		afterID = rows[len(rows)-1].id

		recs := make([]kafkax.Record, len(rows))
		for i, rw := range rows {
			recs[i] = kafkax.Record{
				Topic:   rw.topic,
				Key:     rw.key,
				Value:   rw.payload,
				Headers: map[string]string{EventIDHeader: r.cfg.Name + ":" + strconv.FormatInt(rw.id, 10)},
			}
		}
		errs, err := r.cfg.Sender.Send(ctx, recs...)
		if err != nil {
			// 记下错误，但继续标记这一批里成功的，并处理后面的批次。
			sendErr = errors.Join(sendErr, err)
		}

		sent := make([]int64, 0, len(rows))
		failed := 0
		for i, rw := range rows {
			// errs 与 recs 一一对应；Sender 违反约定少返回了，就按失败处理，宁可重发。
			if i < len(errs) && errs[i] == nil {
				sent = append(sent, rw.id)
			} else {
				failed++
			}
		}
		if failed > 0 && err == nil {
			// Sender 只在 errs 里报了失败（或少返回了），汇总错误却是 nil。
			// 不能因此向调用方报告成功：有消息留在表里就必须让这一轮以错误结束。
			sendErr = errors.Join(sendErr, fmt.Errorf("%d of %d records not acknowledged", failed, len(rows)))
		}
		// 消息已经发出去了，标记不应该因为调用方的 ctx 取消而被放弃（那样只会平白多一次重发）。
		// 标记失败不影响正确性：下一轮会重发，消费者是幂等的。
		markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err = r.mark(markCtx, sent)
		cancel()
		if err != nil {
			return published, errors.Join(sendErr, err)
		}
		published += len(sent)

		if len(rows) < r.cfg.BatchSize {
			break
		}
	}
	if sendErr != nil {
		return published, fmt.Errorf("outbox: send: %w", sendErr)
	}
	return published, nil
}

func (r *Relay) fetch(ctx context.Context, afterID int64) ([]row, error) {
	// 按 id 升序：同一个 key 的消息，先写入的先发送。
	// 同一订单的两次状态变更被订单行的行锁串行，后一个事务在前一个提交之后才会插入 outbox，
	// 所以同一个 key 的消息 id 递增，并且前一条一定先于后一条可见。
	rs, err := r.cfg.DB.QueryContext(ctx,
		"SELECT id, topic, msg_key, payload FROM outbox WHERE published_at IS NULL AND id > ? ORDER BY id LIMIT ?",
		afterID, r.cfg.BatchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("outbox: fetch: %w", err)
	}
	defer rs.Close() //nolint:errcheck // 只读查询，关闭失败没有可做的事

	var rows []row
	for rs.Next() {
		var rw row
		if err := rs.Scan(&rw.id, &rw.topic, &rw.key, &rw.payload); err != nil {
			return nil, fmt.Errorf("outbox: scan: %w", err)
		}
		if len(rw.key) == 0 {
			// 空 key 表示“没有 key”（见 Add）。还原成 nil，让生产者自己挑分区，
			// 而不是把所有无 key 的消息按空串哈希到同一个分区。
			rw.key = nil
		}
		rows = append(rows, rw)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("outbox: fetch: %w", err)
	}
	return rows, nil
}

func (r *Relay) mark(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	args := make([]any, 0, len(ids)+1)
	args = append(args, r.cfg.Now().UTC())
	for _, id := range ids {
		args = append(args, id)
	}
	// published_at IS NULL：另一个中继实例可能已经标记过，不覆盖它的时间。
	// 拼进 SQL 的只有占位符，数量由 ids 的长度决定，值全部走参数。
	query := "UPDATE outbox SET published_at = ? WHERE published_at IS NULL AND id IN (?" + //nolint:gosec // 只拼接占位符
		strings.Repeat(",?", len(ids)-1) + ")"
	if _, err := r.cfg.DB.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("outbox: mark published: %w", err)
	}
	return nil
}

// cleanupBatch 是每次清理删除的最大行数。分批删是为了不长时间持有大量行锁。
const cleanupBatch = 1000

func (r *Relay) cleanup(ctx context.Context, before time.Time) error {
	for {
		res, err := r.cfg.DB.ExecContext(ctx,
			"DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < ? ORDER BY published_at, id LIMIT ?",
			before.UTC(), cleanupBatch,
		)
		if err != nil {
			return fmt.Errorf("outbox: cleanup: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("outbox: cleanup: %w", err)
		}
		if n < cleanupBatch {
			return nil
		}
	}
}
