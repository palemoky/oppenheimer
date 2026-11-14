package capture

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"google.golang.org/protobuf/proto"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
)

// streamState 是增量阶段的进度。只有流水线自己的 goroutine 访问。
type streamState struct {
	// 当前事务的 GTID 和在最初的主库上的提交时间（微秒）。
	gtid         string
	commitMicros int64
	// done 是最后一个完整读完的事务之后的 GTID 集合；saved 是已经写进位点的集合。
	done, saved string
}

// stream 从 gtidSet 之后读 binlog，直到被停止或出错。停止时做最后一次“确认后落位点”。
func (p *Pipeline) stream(ctx context.Context, gtidSet string) error {
	set, err := mysql.ParseMysqlGTIDSet(gtidSet)
	if err != nil {
		return fmt.Errorf("parse checkpoint gtid set %q: %w", gtidSet, err)
	}
	syncer := replication.NewBinlogSyncer(p.binlog)
	defer syncer.Close()
	streamer, err := syncer.StartSyncGTID(set)
	if err != nil {
		return fmt.Errorf("start binlog sync from %q: %w", gtidSet, err)
	}
	p.log.Info("streaming binlog", "gtid_set", gtidSet)

	// 停止信号要能打断阻塞中的 GetEvent。
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-p.stop:
			cancel()
		case <-loopCtx.Done():
		}
	}()

	st := &streamState{done: gtidSet, saved: gtidSet}
	now := time.Now()
	nextCheckpoint := now.Add(p.cfg.CheckpointInterval)
	nextLagReport := now.Add(lagReportInterval)
	for {
		if err := p.failed(); err != nil {
			return err
		}
		wake := nextCheckpoint
		if nextLagReport.Before(wake) {
			wake = nextLagReport
		}
		ectx, ecancel := context.WithDeadline(loopCtx, wake)
		ev, err := streamer.GetEvent(ectx)
		ecancel()
		switch {
		case err == nil:
			if err := p.handleEvent(ctx, ev.Event, st); err != nil {
				return err
			}
		case loopCtx.Err() != nil:
			// 被停止（或必须立刻放弃）：不再读取，确认已发出的消息后落最后一次位点。
			return p.checkpoint(ctx, st)
		case errors.Is(err, context.DeadlineExceeded):
		default:
			return fmt.Errorf("read binlog: %w", err)
		}

		now = time.Now()
		if !now.Before(nextCheckpoint) {
			if err := p.checkpoint(ctx, st); err != nil {
				return err
			}
			nextCheckpoint = now.Add(p.cfg.CheckpointInterval)
		}
		if !now.Before(nextLagReport) {
			p.lag.report(p.log)
			nextLagReport = now.Add(lagReportInterval)
		}
	}
}

// checkpoint 等此前发出的消息全部被确认，再把最后一个完整读完的事务写进位点。没有新事务就不写。
func (p *Pipeline) checkpoint(ctx context.Context, st *streamState) error {
	if st.done == st.saved {
		return p.failed()
	}
	done := st.done
	if err := p.flush(ctx); err != nil {
		return err
	}
	if err := p.saveCheckpoint(ctx, &cdcv1.Checkpoint{GtidSet: done}); err != nil {
		return err
	}
	st.saved = done
	return nil
}

func (p *Pipeline) handleEvent(ctx context.Context, ev replication.Event, st *streamState) error {
	switch e := ev.(type) {
	case *replication.GTIDEvent:
		next, err := e.GTIDNext()
		if err != nil {
			return fmt.Errorf("gtid event: %w", err)
		}
		st.gtid = next.String()
		st.commitMicros = int64(e.OriginalCommitTimestamp) //nolint:gosec // 微秒时间戳远小于 int64 上限
		if st.commitMicros == 0 {
			st.commitMicros = int64(e.ImmediateCommitTimestamp) //nolint:gosec // 同上
		}
	case *replication.RowsEvent:
		return p.handleRows(ctx, e, st)
	case *replication.XIDEvent:
		if e.GSet != nil {
			st.done = e.GSet.String()
		}
	case *replication.QueryEvent:
		query := string(e.Query)
		if query == "BEGIN" {
			return nil
		}
		// 非 BEGIN 的 QueryEvent 是事务的终点：DDL 自成一个事务；非事务引擎的写入以 COMMIT 结束。
		if e.GSet != nil {
			st.done = e.GSet.String()
		}
		if query != "COMMIT" {
			p.log.Info("ddl", "schema", string(e.Schema), "query", query, "gtid", st.gtid)
		}
	case *replication.TransactionPayloadEvent:
		// 开了 binlog_transaction_compression 时，整个事务压缩在一个事件里。
		for _, inner := range e.Events {
			if err := p.handleEvent(ctx, inner.Event, st); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Pipeline) handleRows(ctx context.Context, e *replication.RowsEvent, st *streamState) error {
	if string(e.Table.Schema) != p.schema {
		return nil
	}
	table := string(e.Table.Table)
	rule, slot, ok := matchRule(p.cfg.Rules, table)
	if !ok {
		return nil
	}
	cols, err := columnsOf(e.Table)
	if err != nil {
		return err
	}
	if slices.ContainsFunc(e.SkippedColumns, func(s []int) bool { return len(s) > 0 }) {
		return fmt.Errorf("table %s: row image is not full: binlog_row_image must be FULL on the source", table)
	}

	emit := func(op cdcv1.Op, before, after []any) error {
		var b, a map[string]*cdcv1.Value
		var err error
		if before != nil {
			if b, err = binlogRow(cols, before); err != nil {
				return fmt.Errorf("table %s: %w", table, err)
			}
		}
		if after != nil {
			if a, err = binlogRow(cols, after); err != nil {
				return fmt.Errorf("table %s: %w", table, err)
			}
		}
		msg, key, _, err := p.newChange(rule, slot, op, b, a)
		if err != nil {
			return err
		}
		msg.Gtid = st.gtid
		msg.CommitMicros = st.commitMicros
		if op == cdcv1.Op_OP_UPDATE {
			p.checkVersion(rule, table, key, b, a, st.gtid)
		}
		if err := p.produce(ctx, rule.Topic, key, msg); err != nil {
			return err
		}
		p.lag.observe(time.Now().UnixMicro() - st.commitMicros)
		return nil
	}

	switch e.Type() {
	case replication.EnumRowsEventTypeInsert:
		for _, row := range e.Rows {
			if err := emit(cdcv1.Op_OP_INSERT, nil, row); err != nil {
				return err
			}
		}
	case replication.EnumRowsEventTypeDelete:
		for _, row := range e.Rows {
			if err := emit(cdcv1.Op_OP_DELETE, row, nil); err != nil {
				return err
			}
		}
	case replication.EnumRowsEventTypeUpdate:
		if len(e.Rows)%2 != 0 {
			return fmt.Errorf("table %s: update event has %d row images, want pairs", table, len(e.Rows))
		}
		for pair := range slices.Chunk(e.Rows, 2) {
			if err := emit(cdcv1.Op_OP_UPDATE, pair[0], pair[1]); err != nil {
				return err
			}
		}
	default:
		// 例如 PARTIAL_UPDATE_ROWS_EVENT（binlog_row_value_options=PARTIAL_JSON），只有部分映像，不能当整行发出。
		return fmt.Errorf("table %s: unsupported rows event %s", table, e.Type())
	}
	return nil
}

// checkVersion 检测“更新了行但版本列没变”（设计 4.6）：下游按版本去重，这样的更新会被当成重复而丢掉。
// 只记错误日志和计数，消息照常发。
func (p *Pipeline) checkVersion(rule *Rule, table, key string, before, after map[string]*cdcv1.Value, gtid string) {
	if rule.VersionColumn == "" {
		return
	}
	bv, ok1 := before[rule.VersionColumn]
	av, ok2 := after[rule.VersionColumn]
	if !ok1 || !ok2 || !proto.Equal(bv, av) {
		return
	}
	p.versionAnomalies.Add(1)
	p.log.Error("row updated without bumping version",
		"table", table, "key", key, "version_column", rule.VersionColumn, "gtid", gtid)
}
