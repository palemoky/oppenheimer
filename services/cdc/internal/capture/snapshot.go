package capture

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
)

// 全量（设计 4.7）。
//
// 先读源上的 gtid_executed 记为 G，再按表名、表内按主键分批读。正确性靠两点：
// G 之前提交的状态，之后的分批读一定看得到；G 之后的任何变更都在 binlog 里 G 的后面，增量会再发一遍。
// 全量和增量重叠的部分靠行的版本号在下游去重，所以这里不需要一致性快照，也没有长事务。

// beginSnapshot 读 G，把“全量进行中”的位点写下，返回它。
// 先读 G 再列表：G 之后才建的表，它的全部内容都在 binlog 里；反过来先列表再读 G，
// 两步之间建的表和写进去的行会两边都漏掉。
func (p *Pipeline) beginSnapshot(ctx context.Context) (*cdcv1.Checkpoint, error) {
	qctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	var g string
	if err := p.db.QueryRowContext(qctx, "SELECT @@global.gtid_executed").Scan(&g); err != nil {
		return nil, fmt.Errorf("read gtid_executed: %w", err)
	}
	tables, err := p.listTables(qctx)
	if err != nil {
		return nil, err
	}
	cp := &cdcv1.Checkpoint{GtidSet: normalizeGTIDSet(g), Snapshot: &cdcv1.SnapshotCursor{}}
	if len(tables) > 0 {
		cp.Snapshot.Table = tables[0]
	}
	if err := p.saveCheckpoint(ctx, cp); err != nil {
		return nil, err
	}
	p.log.Info("snapshot started", "gtid_set", cp.GetGtidSet(), "tables", len(tables))
	return cp, nil
}

// listTables 列出 schema 里匹配规则的表，按名字排序。
func (p *Pipeline) listTables(ctx context.Context) ([]string, error) {
	rows, err := p.db.QueryContext(ctx,
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'", p.schema)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan table name: %w", err)
		}
		if _, _, ok := matchRule(p.cfg.Rules, name); ok {
			tables = append(tables, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	slices.Sort(tables)
	return tables, nil
}

// snapshot 从位点里的游标继续全量，读完后清掉游标。stopped 为真表示中途被停止（游标已保存）。
// 每一批发完并确认后才推进游标，所以中断后最多重发一批。
func (p *Pipeline) snapshot(ctx context.Context, cp *cdcv1.Checkpoint) (stopped bool, err error) {
	tables, err := p.listTables(ctx)
	if err != nil {
		return false, err
	}
	cursor := cp.GetSnapshot()
	for _, table := range tables {
		if table < cursor.GetTable() {
			continue
		}
		rule, slot, _ := matchRule(p.cfg.Rules, table)
		last := int64(0)
		if table == cursor.GetTable() {
			last = cursor.GetLastKey()
		}
		for {
			if p.stopping() {
				p.log.Info("snapshot stopped", "table", table, "last_key", last)
				return true, nil
			}
			n, maxKey, err := p.snapshotBatch(ctx, rule, slot, table, last)
			if err != nil {
				return false, err
			}
			if n == 0 {
				break
			}
			if err := p.flush(ctx); err != nil {
				return false, err
			}
			last = maxKey
			cp.Snapshot = &cdcv1.SnapshotCursor{Table: table, LastKey: last}
			if err := p.saveCheckpoint(ctx, cp); err != nil {
				return false, err
			}
			if p.afterSnapshotBatch != nil {
				p.afterSnapshotBatch()
			}
			if n < p.cfg.SnapshotBatch {
				break
			}
		}
		p.log.Info("snapshot table done", "table", table)
	}
	cp.Snapshot = nil
	if err := p.saveCheckpoint(ctx, cp); err != nil {
		return false, err
	}
	p.log.Info("snapshot finished", "gtid_set", cp.GetGtidSet())
	return false, nil
}

// snapshotBatch 读一批主键大于 after 的行，每行发一条 OP_READ，返回行数和这批最大的主键。
func (p *Pipeline) snapshotBatch(ctx context.Context, rule *Rule, slot int32, table string, after int64) (n int, maxKey int64, err error) {
	// 查询有时限；发送用外层的 ctx：Produce 的 ctx 在消息被确认前取消，消息就会失败。
	qctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	key := quoteIdent(rule.KeyColumn)
	// 表名和列名不能作为参数绑定，只能拼进语句；它们来自 information_schema 和配置，并且已转义。
	query := fmt.Sprintf( //nolint:gosec // 见上
		"SELECT * FROM %s WHERE %s > ? ORDER BY %s LIMIT ?", quoteIdent(table), key, key)
	rows, err := p.db.QueryContext(qctx, query, after, p.cfg.SnapshotBatch)
	if err != nil {
		return 0, 0, fmt.Errorf("snapshot %s: %w", table, err)
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return 0, 0, fmt.Errorf("snapshot %s: column types: %w", table, err)
	}
	raw := make([]sql.RawBytes, len(types))
	dest := make([]any, len(types))
	for i := range raw {
		dest[i] = &raw[i]
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return 0, 0, fmt.Errorf("snapshot %s: scan: %w", table, err)
		}
		image := make(map[string]*cdcv1.Value, len(types))
		for i, t := range types {
			v, err := sqlValue(t.DatabaseTypeName(), raw[i])
			if err != nil {
				return 0, 0, fmt.Errorf("snapshot %s: column %s: %w", table, t.Name(), err)
			}
			image[t.Name()] = v
		}
		msg, k, kn, err := p.newChange(rule, slot, cdcv1.Op_OP_READ, nil, image)
		if err != nil {
			return 0, 0, err
		}
		if err := p.produce(ctx, rule.Topic, k, msg); err != nil {
			return 0, 0, err
		}
		p.countEvent(ctx, cdcv1.Op_OP_READ)
		n++
		maxKey = max(maxKey, kn)
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("snapshot %s: %w", table, err)
	}
	return n, maxKey, nil
}

func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
