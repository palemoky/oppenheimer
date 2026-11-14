package verify

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/palemoky/oppenheimer/api/eventx"
	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// QuietState 是 7.3.2 第 3 步的各项静止条件的当前值。全部为零才算静止。
type QuietState struct {
	// OutboxPending 是各服务 outbox 未发布的行数。
	OutboxPending map[string]int64
	// Lag 是各消费者组（带前缀的组名）在其 topic 上的积压之和；LagErr 是算不出积压的原因。
	Lag    map[string]int64
	LagErr map[string]string
	// CreatingOrders、PendingPaymentOrders、PendingPayments 是处于非终态的订单和支付单数。
	CreatingOrders       int64
	PendingPaymentOrders int64
	PendingPayments      int64
	// UnclaimedHolds、ClaimedHolds 是 gate:holds、gate:claimed 的成员数。
	UnclaimedHolds int64
	ClaimedHolds   int64
	// StockPending 是 inventory 的 stock_pending 里还没有应用到 sku_stock 的增量。
	StockPending PendingState
}

// PendingState 是 stock_pending 的当前值。
type PendingState struct {
	Rows int64
	// OldestAge 是最老一行写入到现在的时长，SKUs 是涉及的 SKU（至多 10 个，升序）。
	OldestAge time.Duration
	SKUs      []int64
}

func (p PendingState) String() string {
	more := ""
	if p.Rows > 0 && len(p.SKUs) == pendingProbeSKUs {
		more = ", ..."
	}
	return fmt.Sprintf("stock_pending has %d rows not applied to sku_stock: oldest written %s ago, skus %s%s",
		p.Rows, p.OldestAge.Round(time.Millisecond), strings.Trim(fmt.Sprint(p.SKUs), "[]"), more)
}

// Problems 列出没有归零的项，全部归零时返回空。
func (q QuietState) Problems() []string {
	var out []string
	for _, svc := range slices.Sorted(maps.Keys(q.OutboxPending)) {
		if n := q.OutboxPending[svc]; n != 0 {
			out = append(out, fmt.Sprintf("%s outbox has %d unpublished rows", svc, n))
		}
	}
	for _, g := range slices.Sorted(maps.Keys(q.LagErr)) {
		out = append(out, fmt.Sprintf("consumer group %s lag unknown: %s", g, q.LagErr[g]))
	}
	for _, g := range slices.Sorted(maps.Keys(q.Lag)) {
		if n := q.Lag[g]; n != 0 {
			out = append(out, fmt.Sprintf("consumer group %s lag %d", g, n))
		}
	}
	add := func(n int64, what string) {
		if n != 0 {
			out = append(out, fmt.Sprintf("%d %s", n, what))
		}
	}
	add(q.CreatingOrders, "orders in CREATING")
	add(q.PendingPaymentOrders, "orders in PENDING_PAYMENT")
	add(q.PendingPayments, "payments in PENDING")
	add(q.UnclaimedHolds, "holds in gate:holds")
	add(q.ClaimedHolds, "holds in gate:claimed")
	if q.StockPending.Rows != 0 {
		out = append(out, q.StockPending.String())
	}
	return out
}

// Probe 读一次各项静止条件。
func Probe(ctx context.Context, src Sources) (QuietState, error) {
	q := QuietState{OutboxPending: map[string]int64{}, Lag: map[string]int64{}, LagErr: map[string]string{}}
	var err error
	if q.OutboxPending["payment"], err = countPendingOutbox(ctx, src.PaymentDB); err != nil {
		return q, fmt.Errorf("payment: %w", err)
	}
	shards, err := orderShards(src)
	if err != nil {
		return q, err
	}
	for db, slots := range shards {
		conn := src.OrderDBs[db]
		n, err := countPendingOutbox(ctx, conn)
		if err != nil {
			return q, fmt.Errorf("order_db_%d: %w", db, err)
		}
		q.OutboxPending["order"] += n
		for _, slot := range slots {
			var creating, pendingPay int64
			if err := conn.QueryRowContext(ctx,
				"SELECT COALESCE(SUM(status = ?), 0), COALESCE(SUM(status = ?), 0) FROM "+sharding.Table("orders", slot)+" WHERE status IN (?, ?)",
				OrderCreating, OrderPendingPayment, OrderCreating, OrderPendingPayment,
			).Scan(&creating, &pendingPay); err != nil {
				return q, fmt.Errorf("count unfinished orders in order_db_%d slot %d: %w", db, slot, err)
			}
			q.CreatingOrders += creating
			q.PendingPaymentOrders += pendingPay
		}
	}
	if err := src.PaymentDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM payments WHERE status = ?", PaymentPending).
		Scan(&q.PendingPayments); err != nil {
		return q, fmt.Errorf("count pending payments: %w", err)
	}
	if q.UnclaimedHolds, err = src.Redis.ZCard(ctx, src.RedisPrefix+"gate:holds").Result(); err != nil {
		return q, fmt.Errorf("count gate:holds: %w", err)
	}
	if q.ClaimedHolds, err = src.Redis.ZCard(ctx, src.RedisPrefix+"gate:claimed").Result(); err != nil {
		return q, fmt.Errorf("count gate:claimed: %w", err)
	}
	pp, err := probePending(ctx, src.InventoryDB)
	if err != nil {
		return q, err
	}
	q.StockPending = PendingState(pp)
	if src.Kafka != nil {
		if err := probeLag(ctx, src, &q); err != nil {
			return q, err
		}
	}
	return q, nil
}

// probeLag 按“已知的组 → topic”算积压：组还没有提交过某个分区时，从该分区的起始位置算起。
// 不用 kadm 的 Lag：组不存在（消费者一次都没提交过）时它不知道这个组该消费哪些 topic，会算成零。
func probeLag(ctx context.Context, src Sources, q *QuietState) error {
	adm := kadm.NewClient(src.Kafka)
	topics := make([]string, 0, len(ConsumedTopics))
	for _, t := range ConsumedTopics {
		topics = append(topics, eventx.Topic(src.KafkaPrefix, t))
	}
	starts, err := adm.ListStartOffsets(ctx, topics...)
	if err != nil {
		return fmt.Errorf("list start offsets: %w", err)
	}
	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return fmt.Errorf("list end offsets: %w", err)
	}
	for base, topicBase := range ConsumerGroups {
		group := eventx.Group(src.KafkaPrefix, base)
		topic := eventx.Topic(src.KafkaPrefix, topicBase)
		commits, err := adm.FetchOffsets(ctx, group)
		if err != nil {
			q.LagErr[group] = err.Error()
			continue
		}
		parts, ok := ends[topic]
		if !ok {
			q.LagErr[group] = "topic " + topic + " not found"
			continue
		}
		var lag int64
		var errs []string
		for p, end := range parts {
			if end.Err != nil {
				errs = append(errs, fmt.Sprintf("end offset of partition %d: %v", p, end.Err))
				continue
			}
			from := int64(-1)
			if c, ok := commits.Lookup(topic, p); ok && c.Err == nil && c.At >= 0 {
				from = c.At
			}
			if from < 0 {
				st, ok := starts.Lookup(topic, p)
				if !ok || st.Err != nil {
					errs = append(errs, fmt.Sprintf("no start offset of partition %d", p))
					continue
				}
				from = st.Offset
			}
			lag += max(end.Offset-from, 0)
		}
		if len(errs) > 0 {
			q.LagErr[group] = strings.Join(errs, "; ")
			continue
		}
		q.Lag[group] = lag
	}
	return nil
}

// stuckPolls 是 stock_pending 的行数连续这么多次轮询都没有变化（而且其余条件都已归零）时判定为卡住。
// 变量而不是常量，测试里调小。
var stuckPolls = 10

// ErrStockPendingStuck 表示 stock_pending 不再减少：有 SKU 的增量合并进去会让某个数变负（超卖或账不平）。
var ErrStockPendingStuck = errors.New("stock_pending is stuck")

// WaitQuiet 每隔 interval 读一次静止条件，直到全部归零或超过 timeout。
// 返回等待的时长和最后一次读到的状态；超时返回错误，调用方用 last.Problems() 打印没有归零的项。
func WaitQuiet(ctx context.Context, src Sources, timeout, interval time.Duration, logger *slog.Logger) (time.Duration, QuietState, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last QuietState
	lastLog := time.Time{}
	var stuckRows int64 // 上一次轮询的行数
	same := 0           // 行数连续没有变化的次数
	for {
		pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
		q, err := Probe(pctx, src)
		pcancel()
		switch {
		case err == nil:
			last = q
			problems := q.Problems()
			if len(problems) == 0 {
				return time.Since(start), q, nil
			}
			// 只有其余条件都已归零、stock_pending 的行数却连续不变，才说明是它自己卡住了；
			// 其余条件还没归零时，行数不变可能只是消费者还没把增量送来。
			if q.StockPending.Rows > 0 && q.StockPending.Rows == stuckRows && len(problems) == 1 {
				same++
			} else {
				same = 0
			}
			stuckRows = q.StockPending.Rows
			if same >= stuckPolls {
				return time.Since(start), q, fmt.Errorf("%w: %s (unchanged for %d polls); applying them would make a number negative "+
					"(oversold) or the books do not balance, check 22 of verify shows the detail", ErrStockPendingStuck, q.StockPending, same)
			}
			if time.Since(lastLog) >= 10*time.Second {
				logger.InfoContext(ctx, "waiting for quiet", "elapsed", time.Since(start).Round(time.Second).String(), "pending", problems)
				lastLog = time.Now()
			}
		case errors.Is(err, errNoStockPending):
			return time.Since(start), last, err
		case ctx.Err() == nil:
			logger.WarnContext(ctx, "probe failed, retrying", "error", err)
		}
		t := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return time.Since(start), last, fmt.Errorf("system not quiet after %s: %w", timeout, ctx.Err())
		case <-t.C:
		}
	}
}
