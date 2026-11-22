package redisx

// 命令耗时的指标（设计文档 3a 的 Redis 盲区）。
//
// redisotel.InstrumentMetrics 只给连接池的统计和 db.client.connections.use_time（毫秒、不分命令），
// 和 MySQL 的 db.client.operation.duration 对不上，排查时看不出慢的是哪种命令。这里用一个 go-redis 的 Hook
// 把每条命令的耗时记到和 MySQL 同名的直方图里，用 db.system.name 区分：
//
//	db.client.operation.duration（秒，桶由 otelx.DurationView 统一设）
//	  db.system.name     固定为 redis
//	  db.operation.name  命令名（go-redis 的 Cmder.Name()，小写，如 get、evalsha）；
//	                     普通管道是 pipeline，事务管道（MULTI/EXEC）是 multi；
//	                     见过的命令名超过 maxOperationNames 种、或名字过长时归到 _OTHER
//	  error.type         成功时不带；失败时是 timeout、canceled、pool_timeout、connection、redis_error、_OTHER 之一
//
// key、脚本内容、地址都不进标签。一个服务连着几个 Redis（库存闸门、缓存、限流）时，这个指标分不出是哪一个：
// 地址不能当标签，要区分得另加一个调用方给的固定名字。
//
// 记在哪一层：Hook 加在 newClient 返回的那个客户端上（新建连接时的握手命令不记，见 isHandshake）。go-redis 的 hook 包住整个处理过程，所以一条命令记一次，
// 耗时包括客户端自己的重试（MaxRetries）和等连接池的时间；Cluster 模式下加在 ClusterClient 上，
// MOVED/ASK 重定向和换节点重试都算在同一次里，不会在节点那一层再记一次（节点的 Client 不继承 ClusterClient 的 hook）。
// Sentinel 模式下是指向主库的 *redis.Client，向 Sentinel 问主库的命令走另一个客户端，不记。

import (
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/semconv/v1.43.0/dbconv"

	"github.com/palemoky/oppenheimer/pkg/otelx"
)

const (
	// maxOperationNames 是记成单独标签值的命令名种数上限。命令名来自我们自己的代码，正常只有十几种；
	// 上限防的是有人用 Do 拼出动态的命令名，让时间序列无限增长。
	maxOperationNames = 64
	// maxOperationNameLen 是命令名的长度上限，更长的不是正常的命令名。
	maxOperationNameLen = 32

	// otherOperation 是超出上限的命令名归入的值，沿用语义约定里“其他”的写法（error.type 的 _OTHER）。
	otherOperation = "_OTHER"
	// pipelineOperation、txPipelineOperation 是管道的命令名：不把里面的命令拼起来，那样组合是无限的。
	pipelineOperation   = "pipeline"
	txPipelineOperation = "multi"
)

// errClass 是 error.type 的取值，下标对应 errClassValues。
type errClass uint8

const (
	errNone errClass = iota // 成功，不带 error.type
	errTimeout
	errCanceled
	errPoolTimeout
	errConnection
	errRedis
	errOther
	numErrClasses
)

var errClassValues = [numErrClasses]string{
	errTimeout:     "timeout",
	errCanceled:    "canceled",
	errPoolTimeout: "pool_timeout",
	errConnection:  "connection",
	errRedis:       "redis_error",
	errOther:       semconv.ErrorTypeOther.Value.AsString(),
}

// classify 把命令的错误收敛成少数几类。
//
// redis.Nil（键不存在）是正常结果。NOSCRIPT 也不算：Script.Run 收到它之后自己改用 EVAL 重发，
// 那一条另外记，真正的失败会体现在 eval 上。
func classify(err error) errClass {
	switch {
	case err == nil, errors.Is(err, redis.Nil):
		return errNone
	case errors.Is(err, redis.ErrPoolTimeout):
		return errPoolTimeout
	case errors.Is(err, context.Canceled):
		return errCanceled
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return errTimeout
	}
	// 用 errors.AsType 而不是 errors.As（和 redis.HasErrorPrefix）：后者的目标变量逃逸，每次分配。
	if rerr, ok := errors.AsType[redis.Error](err); ok {
		if strings.HasPrefix(strings.TrimPrefix(rerr.Error(), "ERR "), "NOSCRIPT") {
			return errNone
		}
		return errRedis
	}
	if nerr, ok := errors.AsType[net.Error](err); ok {
		if nerr.Timeout() {
			return errTimeout
		}
		return errConnection
	}
	if errors.Is(err, redis.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errConnection
	}
	return errOther
}

// opEntry 是一个命令名预先建好的度量选项，按错误类别各一份：记录时不分配。
type opEntry struct {
	opts [numErrClasses][]metric.RecordOption
}

// commandMetrics 是命令耗时的直方图和按命令名缓存的标签组合。所有客户端共用一份（defaultCommandMetrics）。
type commandMetrics struct {
	hist metric.Float64Histogram

	// ops 是命令名到 opEntry 的只读快照：读不加锁，新的命令名在 mu 下复制一份再替换（很少发生）。
	ops   atomic.Pointer[map[string]*opEntry]
	mu    sync.Mutex
	other *opEntry
}

var defaultCommandMetrics = newCommandMetrics(otelx.Meter("github.com/palemoky/oppenheimer/pkg/redisx"))

func newCommandMetrics(m metric.Meter) *commandMetrics {
	name := dbconv.ClientOperationDuration{}.Name()
	c := &commandMetrics{
		hist: otelx.Histogram(m, name, dbconv.ClientOperationDuration{}.Description()),
	}
	ops := map[string]*opEntry{
		pipelineOperation:   newOpEntry(pipelineOperation),
		txPipelineOperation: newOpEntry(txPipelineOperation),
	}
	c.ops.Store(&ops)
	c.other = newOpEntry(otherOperation)
	return c
}

func newOpEntry(op string) *opEntry {
	e := &opEntry{}
	for class := range numErrClasses {
		attrs := []attribute.KeyValue{semconv.DBSystemNameRedis, semconv.DBOperationName(op)}
		if class != errNone {
			attrs = append(attrs, semconv.ErrorTypeKey.String(errClassValues[class]))
		}
		e.opts[class] = []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(attrs...))}
	}
	return e
}

// entry 返回命令名对应的 opEntry。已经见过的名字只是一次 map 查找。
func (c *commandMetrics) entry(op string) *opEntry {
	if e, ok := (*c.ops.Load())[op]; ok {
		return e
	}
	if op == "" || len(op) > maxOperationNameLen {
		return c.other
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := *c.ops.Load()
	if e, ok := cur[op]; ok {
		return e
	}
	// 管道的两个名字是预先放进去的，不占上限。
	if len(cur)-2 >= maxOperationNames {
		return c.other
	}
	op = strings.Clone(op) // op 可能引用命令参数的内存，存进 map 前复制一份
	next := maps.Clone(cur)
	next[op] = newOpEntry(op)
	c.ops.Store(&next)
	return next[op]
}

func (c *commandMetrics) record(ctx context.Context, e *opEntry, start time.Time, err error) {
	c.hist.Record(ctx, time.Since(start).Seconds(), e.opts[classify(err)]...)
}

// metricsHook 实现 redis.Hook。它只计时，错误和返回值原样传回，不重试、不发命令。
type metricsHook struct{ m *commandMetrics }

var _ redis.Hook = metricsHook{}

func (metricsHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h metricsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		name := cmd.Name()
		if isHandshake(name) {
			return next(ctx, cmd)
		}
		start := time.Now()
		err := next(ctx, cmd)
		h.m.record(ctx, h.m.entry(name), start, err)
		return err
	}
}

// isHandshake 判断是不是 go-redis 新建连接时自己发的握手命令。
//
// go-redis 有意让握手（HELLO、AUTH、一个 SELECT/READONLY/CLIENT SETNAME 的管道、CLIENT MAINT_NOTIFICATIONS、
// 一个 CLIENT SETINFO 的管道）也经过 hook。不排除的话，每建一个连接就多记两三条命令和两个 pipeline，
// 而且 CLIENT MAINT_NOTIFICATIONS 在不支持它的 Redis 上每次都失败（库忽略这个错误），会被记成 redis_error，
// 让错误率看起来一直不为零。握手的耗时已经算在触发建连的那条命令里了（它在等连接），不会丢。
// 业务代码不发这几种命令；以后要发的话，它们也不会进这个指标。
func isHandshake(name string) bool {
	switch name {
	case "hello", "auth", "client", "select", "readonly":
		return true
	}
	return false
}

// ProcessPipelineHook 同时用于普通管道和事务管道（go-redis 用同一个接口包两者）。事务管道的命令被包成
// MULTI ... EXEC，第一条是 multi。
//
// 管道的 error.type 只看 hook 链返回的错误：单节点和 Sentinel 下那是网络层的错误，里面某条命令被 Redis
// 拒绝（WRONGTYPE 之类）不算；Cluster 下返回的是第一条失败命令的错误，会算。
func (h metricsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		op := pipelineOperation
		if len(cmds) > 0 {
			switch first := cmds[0].Name(); {
			case first == "multi":
				op = txPipelineOperation
			case isHandshake(first): // 建连接时的握手管道，见 isHandshake
				return next(ctx, cmds)
			}
		}
		start := time.Now()
		err := next(ctx, cmds)
		h.m.record(ctx, h.m.entry(op), start, err)
		return err
	}
}

// addCommandMetrics 给客户端加上命令耗时的 hook。三种模式都加在最外层的客户端上，一条命令记一次。
func addCommandMetrics(client redis.UniversalClient, m *commandMetrics) {
	client.AddHook(metricsHook{m: m})
}
