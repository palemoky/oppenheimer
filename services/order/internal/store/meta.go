package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// ErrShardMetaMismatch 表示某个库里记录的分片身份与配置算出的不一致。
var ErrShardMetaMismatch = errors.New("store: shard_meta does not match the configured layout")

// ShardMeta 是一个库在分片布局里的身份，存在该库的 shard_meta 表里（只有一行）。
type ShardMeta struct {
	DBIndex   int
	DBCount   int
	SlotCount int
	Slots     []int
}

func metaFor(router *sharding.Router, db int) ShardMeta {
	return ShardMeta{DBIndex: db, DBCount: router.DBs(), SlotCount: router.Slots(), Slots: router.SlotsOf(db)}
}

func formatSlots(slots []int) string {
	parts := make([]string, len(slots))
	for i, s := range slots {
		parts[i] = strconv.Itoa(s)
	}
	return strings.Join(parts, ",")
}

// EnsureShardMeta 核对 dbs[i] 确实是 router 布局里的 i 号库，在服务开始读写之前调用。
//
// 库数来自部署配置（ADR-0015），配置和数据对不上时路由会把订单写到别的实例找不到的地方，
// 而且不报任何错。这里把这类错误变成启动失败：
//
//   - DSN 的顺序写反：库里记的序号与它在列表里的位置不同。
//   - 拿扩容前的配置连扩容后的库（或反过来）：库里记的总库数与 DSN 的个数不同。
//
// 表是空的说明这个库第一次被使用，按当前布局写入。分两遍做：先核对所有已有身份的库，全部通过后
// 才给空库写入，否则一份错误的配置会在被拦下之前先把空库标成错误的身份。写入用 INSERT IGNORE：
// 多个实例同时启动时只有一个写成功，其余的读到它写的那一行再核对，不会各写各的。
func EnsureShardMeta(ctx context.Context, dbs []*sql.DB, router *sharding.Router) error {
	if len(dbs) != router.DBs() {
		return fmt.Errorf("store: got %d dbs for a layout of %d", len(dbs), router.DBs())
	}
	var empty []int
	for i, db := range dbs {
		found, err := checkShardMeta(ctx, db, metaFor(router, i))
		if err != nil {
			return fmt.Errorf("DSN #%d: %w", i, err)
		}
		if !found {
			empty = append(empty, i)
		}
	}
	for _, i := range empty {
		want := metaFor(router, i)
		if _, err := dbs[i].ExecContext(ctx,
			`INSERT IGNORE INTO shard_meta (id, db_index, db_count, slot_count, slots) VALUES (1, ?, ?, ?, ?)`,
			want.DBIndex, want.DBCount, want.SlotCount, formatSlots(want.Slots)); err != nil {
			return fmt.Errorf("DSN #%d: init shard_meta: %w", i, err)
		}
		// 再读一遍：并发启动时留下的可能是别的实例写的那一行。
		found, err := checkShardMeta(ctx, dbs[i], want)
		if err != nil {
			return fmt.Errorf("DSN #%d: %w", i, err)
		}
		if !found {
			return fmt.Errorf("DSN #%d: shard_meta is still empty after init", i)
		}
	}
	return nil
}

// checkShardMeta 读出库里的身份并与 want 比较。表是空的时返回 false。
func checkShardMeta(ctx context.Context, db *sql.DB, want ShardMeta) (found bool, err error) {
	var (
		got   ShardMeta
		slots string
	)
	err = db.QueryRowContext(ctx, `SELECT db_index, db_count, slot_count, slots FROM shard_meta WHERE id = 1`).
		Scan(&got.DBIndex, &got.DBCount, &got.SlotCount, &slots)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read shard_meta: %w", err)
	}
	if got.DBIndex != want.DBIndex || got.DBCount != want.DBCount ||
		got.SlotCount != want.SlotCount || slots != formatSlots(want.Slots) {
		return true, fmt.Errorf("%w: database holds db %d of %d (slots %s of %d), configuration expects db %d of %d (slots %s of %d)",
			ErrShardMetaMismatch, got.DBIndex, got.DBCount, slots, got.SlotCount,
			want.DBIndex, want.DBCount, formatSlots(want.Slots), want.SlotCount)
	}
	return true, nil
}
