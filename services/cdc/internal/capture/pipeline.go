package capture

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	driver "github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// 各类跨进程调用的时限。
const (
	queryTimeout      = 30 * time.Second
	checkpointTimeout = 30 * time.Second
	// flushTimeout 是等待已发出的消息全部被确认的时限。生产者自己的投递时限（main 里配置）应当比它短。
	flushTimeout = 2 * time.Minute
	// lagReportInterval 是延迟汇总日志的间隔（设计 4.10）。
	lagReportInterval = 10 * time.Second
)

// 默认值。
const (
	DefaultCheckpointInterval = time.Second
	DefaultSnapshotBatch      = 500
)

// Config 是一条流水线的配置。
type Config struct {
	// Name 是流水线名，写进消息的 source 字段，也是位点的 key。
	Name string
	// DSN 是源库的地址（go-sql-driver 的格式），库名就是要捕获的 schema。读 binlog 和全量读表都用它。
	DSN string
	// ServerID 是伪装成从库时用的 server-id，在源所在的复制拓扑里必须唯一。
	ServerID uint32
	// Rules 是要捕获的表。
	Rules []Rule
	// Producer 发送行变更，要求 acks=all 且开启幂等（franz-go 默认开启）。
	// 每条流水线独占一个：位点推进前的 Flush 只应等待本流水线的消息。调用方负责关闭。
	Producer *kgo.Client
	// Checkpoints 存位点。
	Checkpoints *CheckpointStore
	// CheckpointInterval 是推进位点的间隔，默认 DefaultCheckpointInterval。
	CheckpointInterval time.Duration
	// SnapshotBatch 是全量每批读取的行数，默认 DefaultSnapshotBatch。
	SnapshotBatch int
	Logger        *slog.Logger
}

// Pipeline 是一条流水线：一个源库，一个 goroutine 串行地读事件、发消息、推进位点。
// 它实现 app.Component。
type Pipeline struct {
	cfg    Config
	log    *slog.Logger
	schema string
	db     *sql.DB
	binlog replication.BinlogSyncerConfig

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}

	// sendErr 记录第一条最终发送失败的消息的错误。出现后流水线退出，不再推进位点。
	sendErr atomic.Pointer[error]
	// versionAnomalies 是“更新了行但版本列没变”的次数（设计 4.6）。
	versionAnomalies atomic.Int64

	lag lagStats

	// eventOpts 是 cdc.events 按 op 的标签，见 newEventOptions。
	eventOpts map[cdcv1.Op]metric.AddOption
	// lastCommitMicros 是最后一个读完的事务在源库的提交时间（微秒），给 cdc.lag_ms 用；还没有时为 0。
	lastCommitMicros atomic.Int64

	// 以下只供测试替换。
	purgedQuery        string
	afterSnapshotBatch func()
}

// New 校验配置并创建流水线。它不连接任何东西，连接在 Start 里建立。
func New(cfg Config) (*Pipeline, error) {
	if cfg.Name == "" || cfg.Producer == nil || cfg.Checkpoints == nil || cfg.Logger == nil {
		return nil, errors.New("capture: name, producer, checkpoints and logger are required")
	}
	if cfg.ServerID == 0 {
		return nil, fmt.Errorf("capture %s: server id must not be 0", cfg.Name)
	}
	if len(cfg.Rules) == 0 {
		return nil, fmt.Errorf("capture %s: no rules", cfg.Name)
	}
	for _, r := range cfg.Rules {
		if err := r.validate(); err != nil {
			return nil, fmt.Errorf("capture %s: %w", cfg.Name, err)
		}
	}
	if cfg.CheckpointInterval <= 0 {
		cfg.CheckpointInterval = DefaultCheckpointInterval
	}
	if cfg.SnapshotBatch <= 0 {
		cfg.SnapshotBatch = DefaultSnapshotBatch
	}

	dsn, err := driver.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("capture %s: parse dsn: %w", cfg.Name, err)
	}
	if dsn.DBName == "" {
		return nil, fmt.Errorf("capture %s: dsn has no database name", cfg.Name)
	}
	if dsn.Net != "tcp" {
		return nil, fmt.Errorf("capture %s: dsn must use tcp, got %q", cfg.Name, dsn.Net)
	}
	host, portStr, err := net.SplitHostPort(dsn.Addr)
	if err != nil {
		return nil, fmt.Errorf("capture %s: dsn address %q: %w", cfg.Name, dsn.Addr, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("capture %s: dsn port %q: %w", cfg.Name, portStr, err)
	}

	// 全量读表用文本协议（客户端插值参数），所有值都以 MySQL 的文本形式返回，和 binlog 路径的映射规则一致；
	// 时间不解析成 time.Time，会话时区固定为 UTC，TIMESTAMP 两边都按 UTC 显示。
	dsn.InterpolateParams = true
	dsn.ParseTime = false
	if dsn.Params == nil {
		dsn.Params = map[string]string{}
	}
	dsn.Params["time_zone"] = "'+00:00'"
	connector, err := driver.NewConnector(dsn)
	if err != nil {
		return nil, fmt.Errorf("capture %s: mysql connector: %w", cfg.Name, err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(2)

	logger := cfg.Logger.With("pipeline", cfg.Name)
	return &Pipeline{
		cfg:    cfg,
		log:    logger,
		schema: dsn.DBName,
		db:     db,
		binlog: replication.BinlogSyncerConfig{
			ServerID:                cfg.ServerID,
			Flavor:                  mysql.MySQLFlavor,
			Host:                    host,
			Port:                    uint16(port),
			User:                    dsn.User,
			Password:                dsn.Passwd,
			TimestampStringLocation: time.UTC,
			HeartbeatPeriod:         5 * time.Second,
			ReadTimeout:             30 * time.Second,
			// 断线后 go-mysql 自己从上一个完整事务重连；连续失败这么多次就报错退出，交给进程重启。
			MaxReconnectAttempts: 10,
			Logger:               goMySQLLogger(logger),
		},
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		purgedQuery: "SELECT @@global.gtid_purged",
		eventOpts:   newEventOptions(cfg.Name),
	}, nil
}

// Name 实现 app.Component。
func (p *Pipeline) Name() string { return "cdc-" + p.cfg.Name }

// VersionAnomalies 返回“更新了行但版本列没变”的累计次数。
func (p *Pipeline) VersionAnomalies() int64 { return p.versionAnomalies.Load() }

// Stop 让 Start 停止读取、做最后一次位点推进后返回，等待到 ctx 到期为止。
func (p *Pipeline) Stop(ctx context.Context) error {
	p.requestStop()
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("stop %s: %w", p.Name(), ctx.Err())
	}
}

func (p *Pipeline) requestStop() { p.stopOnce.Do(func() { close(p.stop) }) }

func (p *Pipeline) stopping() bool {
	select {
	case <-p.stop:
		return true
	default:
		return false
	}
}

// Start 实现 app.Component：加载位点，必要时做全量，然后读 binlog 直到被停止或出错。
// ctx 只在必须立刻放弃时取消；正常的停止走 Stop。
func (p *Pipeline) Start(ctx context.Context) error {
	defer close(p.done)
	defer p.db.Close()
	// 指标登记失败不影响同步，只记一条日志。
	if unregister, err := otelx.ObserveGauge(meter, "cdc.lag_ms", lagDesc, p.lagMillis,
		attribute.String("pipeline", p.cfg.Name)); err != nil {
		p.log.WarnContext(ctx, "register cdc lag gauge", "error", err)
	} else {
		defer unregister()
	}

	if p.stopping() {
		return nil
	}

	cp, err := p.loadCheckpoint(ctx)
	if err != nil {
		return err
	}
	if cp == nil {
		p.log.InfoContext(ctx, "no checkpoint, starting snapshot")
		if cp, err = p.beginSnapshot(ctx); err != nil {
			return err
		}
	} else if err := p.checkResumable(ctx, cp.GetGtidSet()); err != nil {
		// 全量没做完的位点也要查：续上全量之后要从 G 读 binlog，G 之后的日志同样不能缺。
		return err
	}
	if cp.GetSnapshot() != nil {
		stopped, err := p.snapshot(ctx, cp)
		if err != nil || stopped {
			return err
		}
		// 全量可能很久，期间 binlog 可能已经过了保留期，开始读之前再查一次。
		if err := p.checkResumable(ctx, cp.GetGtidSet()); err != nil {
			return err
		}
	}
	return p.stream(ctx, cp.GetGtidSet())
}

func (p *Pipeline) loadCheckpoint(ctx context.Context) (*cdcv1.Checkpoint, error) {
	ctx, cancel := context.WithTimeout(ctx, checkpointTimeout)
	defer cancel()
	cp, err := p.cfg.Checkpoints.Load(ctx, p.cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("load checkpoint: %w", err)
	}
	if cp != nil {
		p.log.InfoContext(ctx, "loaded checkpoint", "gtid_set", cp.GetGtidSet(),
			"snapshot_table", cp.GetSnapshot().GetTable(), "snapshot_last_key", cp.GetSnapshot().GetLastKey())
	}
	return cp, nil
}

func (p *Pipeline) saveCheckpoint(ctx context.Context, cp *cdcv1.Checkpoint) error {
	ctx, cancel := context.WithTimeout(ctx, checkpointTimeout)
	defer cancel()
	return p.cfg.Checkpoints.Save(ctx, p.cfg.Name, cp)
}

// checkResumable 确认位点之后的 binlog 还在源上：已清除的事务（gtid_purged）必须都包含在位点里。
// 否则位点之后有一段日志已经读不到了，续传会悄悄丢变更。这时不自动重做全量（设计 4.5），由人决定。
func (p *Pipeline) checkResumable(ctx context.Context, gtidSet string) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	var purged string
	if err := p.db.QueryRowContext(ctx, p.purgedQuery).Scan(&purged); err != nil {
		return fmt.Errorf("read gtid_purged: %w", err)
	}
	var subset sql.NullInt64
	if err := p.db.QueryRowContext(ctx, "SELECT GTID_SUBSET(?, ?)", purged, gtidSet).Scan(&subset); err != nil {
		return fmt.Errorf("compare gtid_purged with checkpoint: %w", err)
	}
	if !subset.Valid || subset.Int64 != 1 {
		return fmt.Errorf("binlog after checkpoint %q has been purged on the source (gtid_purged %q); "+
			"redo the snapshot with -reset-checkpoint=%s", gtidSet, normalizeGTIDSet(purged), p.cfg.Name)
	}
	return nil
}

// normalizeGTIDSet 去掉 MySQL 在多个 uuid 之间插入的换行和空白。
func normalizeGTIDSet(s string) string {
	return strings.Join(strings.Fields(s), "")
}

// failed 返回第一条最终发送失败的错误。
func (p *Pipeline) failed() error {
	if e := p.sendErr.Load(); e != nil {
		return *e
	}
	return nil
}

// produce 异步发出一条变更。失败由回调记下，流水线在下一个事件或推进位点前发现并退出。
func (p *Pipeline) produce(ctx context.Context, topic string, key string, msg *cdcv1.RowChange) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal row change: %w", err)
	}
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}
	p.cfg.Producer.Produce(ctx, rec, func(r *kgo.Record, err error) {
		if err != nil {
			err = fmt.Errorf("send %s key %s: %w", r.Topic, r.Key, err)
			p.sendErr.CompareAndSwap(nil, &err)
		}
	})
	return nil
}

// flush 等此前发出的所有消息被确认，任何一条失败都返回错误。
func (p *Pipeline) flush(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()
	if err := p.cfg.Producer.Flush(ctx); err != nil {
		return fmt.Errorf("flush producer: %w", err)
	}
	return p.failed()
}

// newChange 组装一条消息并取出它的 key。
func (p *Pipeline) newChange(rule *Rule, slot int32, op cdcv1.Op, before, after map[string]*cdcv1.Value) (*cdcv1.RowChange, string, int64, error) {
	image := after
	if op == cdcv1.Op_OP_DELETE {
		image = before
	}
	kv, ok := image[rule.KeyColumn]
	if !ok {
		return nil, "", 0, fmt.Errorf("table %s: key column %s not found", rule.Table, rule.KeyColumn)
	}
	key, n, err := keyString(kv)
	if err != nil {
		return nil, "", 0, fmt.Errorf("table %s: %w", rule.Table, err)
	}
	return &cdcv1.RowChange{
		Source: p.cfg.Name,
		Table:  rule.Table,
		Slot:   slot,
		Op:     op,
		Before: before,
		After:  after,
	}, key, n, nil
}

// goMySQLLogger 把 go-mysql 的日志接到 slog 上，只保留 Warn 及以上。
func goMySQLLogger(l *slog.Logger) *slog.Logger {
	return slog.New(minLevelHandler{Handler: l.Handler(), min: slog.LevelWarn}).With("lib", "go-mysql")
}

type minLevelHandler struct {
	slog.Handler
	min slog.Level
}

func (h minLevelHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.min && h.Handler.Enabled(ctx, level)
}

func (h minLevelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return minLevelHandler{Handler: h.Handler.WithAttrs(attrs), min: h.min}
}

func (h minLevelHandler) WithGroup(name string) slog.Handler {
	return minLevelHandler{Handler: h.Handler.WithGroup(name), min: h.min}
}
