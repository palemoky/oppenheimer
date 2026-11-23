package kafkax

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"maps"
	"math/rand/v2"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Message 是交给 Handler 的一条消息。
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	// Headers 里同名的 header 只保留最后一个。
	Headers   map[string]string
	Timestamp time.Time
}

// Handler 处理一条消息。返回 nil 表示处理完成，位点可以提交。
//
// 消息按至少一次投递：同一条消息可能被处理多次（重试、再平衡、崩溃重启），
// Handler 必须幂等。返回普通错误会原地重试；返回 Permanent 错误直接进死信。
// ctx 只在消费者被强制停止时取消。
type Handler func(ctx context.Context, msg Message) error

// ConsumerConfig 是消费者的配置。
type ConsumerConfig struct {
	Config
	// Group 是消费者组，必填。
	Group string
	// Topics 是订阅的 topic，必填。每个 topic 对应的死信 topic 必须已存在。
	Topics []string
	// Logger 为空时用 slog.Default()。
	Logger *slog.Logger

	// 失败重试：原地重试，期间该分区不前进。

	// MaxAttempts 是每条消息的最大处理次数（含第一次），默认 5。
	MaxAttempts int
	// RetryInitial 是第一次重试前的等待，之后每次翻倍，默认 100ms。
	RetryInitial time.Duration
	// RetryMax 是重试等待的上限，默认 5s。死信发送失败的重试也用这个上限。
	RetryMax time.Duration

	// DLQSuffix 是死信 topic 的后缀：死信 topic = 原 topic + DLQSuffix，默认 ".dlq"。
	DLQSuffix string
	// SessionTimeout 是 broker 多久收不到心跳就把这个成员踢出组，默认 10 秒。心跳间隔取它的三分之一。
	//
	// 它决定消费者被 kill -9（来不及主动离组）之后，它名下的分区要停多久才会分给别人，
	// 重启的新实例也得等这么久：旧成员没被踢掉之前，重平衡完不成。franz-go 的默认值是 45 秒，
	// 杀进程测试里表现为库存迟迟不归还、受理的订单等不到落库就被回收（设计文档 7.3.4）。
	// 取得太短，一次长的 GC 暂停或网络抖动就会触发不必要的重平衡；不能低于 broker 的
	// group.min.session.timeout.ms（默认 6 秒）。
	SessionTimeout time.Duration

	// Concurrency 是一个分区内同时处理的消息数，默认 1（分区内严格串行）。
	//
	// 大于 1 时分区内开这么多条 lane：消息按 key 的哈希分到 lane，lane 内串行，lane 之间并行。
	// 同一个 key 的消息总在同一条 lane 上，所以“同一个 key 按位点顺序处理”仍然成立；
	// 不同 key 之间不再有先后。只有当处理一条消息碰到的数据都属于它的 key 时才应当打开
	// （设计文档 1d 的 2.2、ADR-0017）。
	//
	// 打开之后提交的是“连续完成的前缀”（见 prefixTracker），前缀之后已经完成的消息在重启或
	// 再平衡后会重复投递，handler 必须幂等。
	//
	// 延迟消费者也可以打开：等到期的是分发的那一个 goroutine，不是 lane，所以“队首没到期，
	// 后面的都不动”照旧成立，并发的只是已经到期的那些（见 NewDelayConsumer）。
	Concurrency int
	// MaxUncommitted 只在 Concurrency > 1 时有意义：一个分区里已分发、位点还不能提交的消息数上限，
	// 默认 1024。到达上限时停止分发，等最前面那条完成。
	//
	// 没有这个上限的话，一条一直重试的消息会让其他 lane 不停地往前处理而位点一动不动，
	// 重启后要重放的范围和内存里的记录都无限增长。有了它，最坏情况是重放这么多条，
	// 之后这个分区停住，和串行时“一条卡住、分区停住”的行为一致。
	MaxUncommitted int
}

const (
	defaultMaxAttempts    = 5
	defaultRetryInitial   = 100 * time.Millisecond
	defaultRetryMax       = 5 * time.Second
	defaultDLQSuffix      = ".dlq"
	defaultSessionTimeout = 10 * time.Second
	defaultMaxUncommitted = 1024

	// maxPollRecords 限制一次拉取的条数，让拉取循环尽快把记录分发出去并允许再平衡。
	maxPollRecords = 500
	// 每个分区的本地队列超过 highWater 条时暂停该分区的拉取，降到 lowWater 条以下再恢复。
	// 用水位而不是“有积压就暂停”，是因为恢复后要等 broker 上在途的 fetch 返回（最长 FetchMaxWait）
	// 才会重新拉这个分区，频繁暂停恢复会拖慢吞吐。
	highWater = 2 * maxPollRecords
	lowWater  = maxPollRecords / 5
	// fetchMaxWait 是 broker 在没有新数据时最多挂起一个 fetch 的时间。
	// 分区恢复拉取要等在途 fetch 返回，这个值决定了恢复的延迟上限，所以比默认的 5s 小。
	fetchMaxWait = time.Second

	// commitInterval 是兜底的提交间隔：分区一直有积压、队列始终不空时也能定期提交。
	commitInterval = time.Second
	commitTimeout  = 10 * time.Second
	// abortGrace 是被强制取消后，最后一次提交和离开消费者组的时限。
	abortGrace     = 2 * time.Second
	dlqSendTimeout = 10 * time.Second
	// maxDLQErrorLen 是 dlq-error header 的最大字节数，避免超长错误信息撑大消息。
	maxDLQErrorLen = 1024
)

// 死信消息附加的 header。
const (
	HeaderDLQError     = "dlq-error"
	HeaderDLQTopic     = "dlq-topic"
	HeaderDLQPartition = "dlq-partition"
	HeaderDLQOffset    = "dlq-offset"
	HeaderDLQGroup     = "dlq-group"
	HeaderDLQAttempts  = "dlq-attempts"
)

// Consumer 是消费者组成员，实现 app.Component。
//
// 投递语义是至少一次：
//   - 关闭自动提交，handler 返回 nil（或消息已成功送入死信）之后才提交它的位点。
//     自动提交按“已拉取”而不是“已处理”提交，进程在处理中途崩溃时，已提交但没处理完的消息就丢了。
//   - 每个分区一个 goroutine，分区内严格按位点串行处理；不同分区并行。
//     Concurrency > 1 时分区内按 key 分成多条 lane 并行，同一个 key 仍然串行，见 ConsumerConfig.Concurrency。
//   - 失败原地重试，不跳过：跳过意味着提交了后面的位点，前面那条就再也不会被投递。
//     并发时重试只停住它所在的 lane，位点停在它前面，直到它成功或进入死信。
//
// 并发结构：
//   - 拉取循环（Start 所在的 goroutine）只负责把记录分发到各分区的 worker 队列。
//   - 开启 BlockRebalanceOnPoll：从拉到非空结果到调用 AllowRebalance 之间，再平衡回调不会执行。
//     分发很快，分发完立刻 AllowRebalance，所以不会长时间阻塞再平衡；
//     换来的保证是：分发出去的记录一定属于当前持有的分区，不会把上一代分配里拉到的旧记录
//     塞给重新分配回来的同一分区（那会跳过中间的位点，造成丢消息）。
//   - 再平衡回调与 worker 并发运行：收回分区时等该分区在途的那条消息处理完，提交位点后才返回。
type Consumer struct {
	cfg     ConsumerConfig
	handler Handler
	log     *slog.Logger
	client  *kgo.Client
	dlq     *Producer
	// batch 非空时按批处理（NewBatchConsumer），handler 只在一批失败后逐条回退时使用。
	batch    BatchHandler
	maxBatch int
	// laneSeed 是把 key 分到 lane 的哈希种子，见 laneOf。
	laneSeed maphash.Seed
	// clock 只在延迟消费者里非空。
	clock Clock

	mu      sync.Mutex
	workers map[topicPartition]*worker
	// hardCancel 取消交给 handler 的 context，表示必须立刻放弃。
	hardCancel context.CancelFunc

	// commitMu 串行化所有位点提交，避免较旧的提交晚于较新的提交到达而把位点回退。
	commitMu sync.Mutex
	commitCh chan struct{}

	started  atomic.Bool
	stopOnce sync.Once
	stopCh   chan struct{}
	done     chan struct{}
}

type topicPartition struct {
	topic     string
	partition int32
}

// NewConsumer 创建消费者并确认 broker 可达、死信 topic 存在。
// 调用 Start 开始消费，Stop 停止；Stop 之后不能再次 Start。
func NewConsumer(ctx context.Context, cfg ConsumerConfig, handler Handler) (*Consumer, error) {
	return newConsumer(ctx, cfg, handler, nil)
}

func newConsumer(ctx context.Context, cfg ConsumerConfig, handler Handler, clock Clock) (*Consumer, error) {
	if cfg.Group == "" {
		return nil, errors.New("kafkax: consumer group is required")
	}
	if len(cfg.Topics) == 0 {
		return nil, errors.New("kafkax: consumer topics are required")
	}
	if handler == nil {
		return nil, errors.New("kafkax: consumer handler is required")
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultMaxAttempts
	}
	if cfg.RetryInitial <= 0 {
		cfg.RetryInitial = defaultRetryInitial
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = defaultRetryMax
	}
	cfg.RetryMax = max(cfg.RetryMax, cfg.RetryInitial)
	if cfg.DLQSuffix == "" {
		cfg.DLQSuffix = defaultDLQSuffix
	}
	if cfg.SessionTimeout <= 0 {
		cfg.SessionTimeout = defaultSessionTimeout
	}
	if cfg.Concurrency < 0 {
		return nil, fmt.Errorf("kafkax: consumer concurrency %d is negative", cfg.Concurrency)
	}
	cfg.Concurrency = max(cfg.Concurrency, 1)
	if cfg.MaxUncommitted <= 0 {
		cfg.MaxUncommitted = defaultMaxUncommitted
	}
	// 窗口比 lane 数还小的话，有的 lane 永远轮不到。
	cfg.MaxUncommitted = max(cfg.MaxUncommitted, cfg.Concurrency)
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	c := &Consumer{
		cfg:      cfg,
		handler:  handler,
		log:      logger.With("group", cfg.Group),
		clock:    clock,
		laneSeed: maphash.MakeSeed(),
		workers:  make(map[topicPartition]*worker),
		commitCh: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}

	opts := []kgo.Opt{
		kgo.ConsumerGroup(cfg.Group),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.HeartbeatInterval(cfg.SessionTimeout / 3),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		// 新的消费者组从最早的位点开始，不会因为消费者晚于生产者启动而漏消息。
		// 这个选项同时决定 broker 丢数据、位点越界时从哪里继续：也选最早，宁可重复不可跳过。
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(fetchMaxWait),
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onLost),
	}
	client, err := NewClient(ctx, cfg.Config, append(opts, producerOpts()...)...)
	if err != nil {
		return nil, err
	}
	c.client = client
	c.dlq = &Producer{client: client}

	if err := c.checkDLQTopics(ctx); err != nil {
		client.Close()
		return nil, err
	}
	return c, nil
}

// dlqCheckWait 是启动时等死信 topic 在元数据里出现的上限。
const dlqCheckWait = 10 * time.Second

// checkDLQTopics 确认每个死信 topic 存在。broker 关闭了自动建 topic，
// 死信 topic 不存在时消息进不了死信，分区会一直卡在死信重试上，所以在启动时就报错。
//
// “查不到”要等一会儿再下结论：topic 刚由 EnsureTopics 建好时，每个 broker 的元数据缓存是各自异步更新的。
// EnsureTopics 等到的是它问的那个 broker 看得见，而这里是另一个客户端，可能问到另一个还没更新的 broker。
// 单 broker 的环境（本机、CI）里不会出现；三个 broker 的集群上，服务因此在启动时退出过（设计文档 2c 第 13 节）。
func (c *Consumer) checkDLQTopics(ctx context.Context) error {
	names := make([]string, len(c.cfg.Topics))
	for i, t := range c.cfg.Topics {
		names[i] = t + c.cfg.DLQSuffix
	}
	adm := kadm.NewClient(c.client)
	deadline := time.Now().Add(dlqCheckWait)
	for {
		details, err := adm.ListTopics(ctx, names...)
		if err != nil {
			return fmt.Errorf("kafkax: list dlq topics: %w", err)
		}
		missing := ""
		for _, name := range names {
			d, ok := details[name]
			switch {
			case !ok, errors.Is(d.Err, kerr.UnknownTopicOrPartition):
				missing = name
			case d.Err != nil:
				return fmt.Errorf("kafkax: dlq topic %s: %w", name, d.Err)
			}
		}
		if missing == "" {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("kafkax: dlq topic %s does not exist", missing)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafkax: dlq topic %s: %w", missing, ctx.Err())
		case <-time.After(topicPollInterval):
		}
	}
}

// Name 实现 app.Component。
func (c *Consumer) Name() string { return "kafka-consumer:" + c.cfg.Group }

// Start 实现 app.Component：阻塞消费直到 Stop 或 ctx 取消。被 Stop 正常停止时返回 nil。
//
// ctx 会传给 handler；它被取消表示必须立刻放弃，在途消息不提交，下次重新投递。
func (c *Consumer) Start(ctx context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return errors.New("kafkax: consumer already started or stopped")
	}
	defer close(c.done)

	// 在 Start 里登记、Start 返回时注销：Stop 等的就是 Start 返回（done 关闭），所以 Stop 返回时已经注销了。
	// 这个 defer 在 close(c.done) 之前执行。登记失败只少一个指标，不影响消费。
	if unregister, err := c.registerOldestPending(); err != nil {
		c.log.WarnContext(ctx, "kafka consumer oldest pending age gauge not registered", "error", err)
	} else {
		defer unregister()
	}

	hardCtx, hardCancel := context.WithCancel(ctx)
	defer hardCancel()
	c.mu.Lock()
	c.hardCancel = hardCancel
	c.mu.Unlock()

	// Stop 或强制取消都会让拉取循环退出。
	pollCtx, pollCancel := context.WithCancel(hardCtx)
	defer pollCancel()
	go func() {
		select {
		case <-c.stopCh:
		case <-hardCtx.Done():
		}
		pollCancel()
	}()

	var bg sync.WaitGroup
	committerDone := make(chan struct{})
	bg.Go(func() { c.runCommitter(hardCtx, committerDone) })

	c.pollLoop(pollCtx, hardCtx)

	// 停止顺序：先让所有 worker 处理完在途的那条消息并退出，再提交到最新，最后离开消费者组。
	// 先提交再离开：离开会触发再平衡，别的成员会从已提交的位点开始，晚于离开的提交可能已经来不及。
	//
	// 被强制取消时 worker 会立刻放弃在途消息，但之前已经处理完的消息仍然值得提交（否则全部重复处理），
	// 离开消费者组也能让别的成员立刻接手而不是等会话超时。这两步只给很短的时限，然后就放弃。
	finalTimeout := commitTimeout
	if hardCtx.Err() != nil {
		finalTimeout = abortGrace
	}
	finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalTimeout)
	defer cancel()
	c.retire(finalCtx, c.allWorkers(), true)
	close(committerDone)
	bg.Wait()

	if err := c.client.LeaveGroupContext(finalCtx); err != nil {
		c.log.WarnContext(ctx, "kafka leave group failed", "error", err)
	}
	c.client.CloseAllowingRebalance()

	select {
	case <-c.stopCh:
		return nil
	default:
		return fmt.Errorf("kafkax: consumer %s: %w", c.cfg.Group, ctx.Err())
	}
}

// Stop 实现 app.Component：停止拉取，等在途的 handler 跑完、提交位点、离开消费者组。
// ctx 到期时取消交给 handler 的 context 并返回错误，Start 随后尽快返回。
func (c *Consumer) Stop(ctx context.Context) error {
	c.stopOnce.Do(func() { close(c.stopCh) })
	// 从未 Start 过：占住 started，防止之后再 Start，直接关闭 client。
	if c.started.CompareAndSwap(false, true) {
		c.client.Close()
		close(c.done)
		return nil
	}
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		if c.hardCancel != nil {
			c.hardCancel()
		}
		c.mu.Unlock()
		return fmt.Errorf("kafkax: stop consumer %s: %w", c.cfg.Group, ctx.Err())
	}
}

// pollLoop 拉取并分发记录，直到 pollCtx 取消或 client 关闭。
func (c *Consumer) pollLoop(pollCtx, hardCtx context.Context) {
	for {
		fetches := c.client.PollRecords(pollCtx, maxPollRecords)
		if pollCtx.Err() != nil || fetches.IsClientClosed() {
			// 停止时手里的记录直接丢弃：它们没被处理，位点也不会提交，下次会重新投递。
			c.client.AllowRebalance()
			return
		}
		var fetchErr bool
		fetches.EachError(func(topic string, partition int32, err error) {
			fetchErr = true
			c.log.WarnContext(pollCtx, "kafka fetch error", "topic", topic, "partition", partition, "error", err)
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			stampPolled(p.Records, time.Now())
			c.workerFor(hardCtx, topicPartition{p.Topic, p.Partition}).enqueue(p.Records)
		})
		c.client.AllowRebalance()

		// 只有错误没有数据时稍等再拉，避免持续出错时空转。
		if fetchErr && fetches.Empty() {
			t := time.NewTimer(time.Second)
			select {
			case <-t.C:
			case <-pollCtx.Done():
				t.Stop()
			}
		}
	}
}

// workerFor 返回分区的 worker，没有就创建。只在拉取循环里、AllowRebalance 之前调用，
// 此时不会有再平衡回调并发执行，拉到的记录一定属于当前持有的分区。
func (c *Consumer) workerFor(hardCtx context.Context, tp topicPartition) *worker {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w, ok := c.workers[tp]; ok {
		return w
	}
	w := &worker{
		c:      c,
		tp:     tp,
		log:    c.log.With("topic", tp.topic, "partition", tp.partition),
		wake:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
		exited: make(chan struct{}),
	}
	if n := c.cfg.Concurrency; n > 1 {
		w.lanes = make([]chan *kgo.Record, n)
		for i := range w.lanes {
			// 容量等于窗口：分发前已经确认未提交的记录数低于窗口，所以往 lane 里放永远不会阻塞。
			w.lanes[i] = make(chan *kgo.Record, c.cfg.MaxUncommitted)
		}
		w.progress = make(chan struct{}, 1)
	}
	c.workers[tp] = w
	go w.run(hardCtx)
	return w
}

// onAssigned 是分到分区时的回调，只记日志。
//
// 进程被 kill -9 后重启，新实例要等旧成员的会话过期才分到分区；有这条日志，
// “启动之后多久才开始消费”就能直接从日志里读出来，不用靠推断。
func (c *Consumer) onAssigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	if len(assigned) > 0 {
		c.log.InfoContext(ctx, "kafka partitions assigned", "partitions", assigned)
	}
}

// onRevoked 是分区被收回时的回调（也在离开组时调用）。
// 等被收回分区的 worker 处理完在途消息，把处理完的位点提交掉，再交出分区。
// 不等的话，新的持有者会从旧位点开始，与本成员同时处理同一分区，顺序和重复都失控。
func (c *Consumer) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	if len(revoked) > 0 {
		c.log.InfoContext(ctx, "kafka partitions revoked", "partitions", revoked)
	}
	c.retire(ctx, c.workersOf(revoked), true)
	// 暂停状态在分区重新分配回来后仍然有效，交出前必须恢复，否则再分配回来会永远不拉。
	c.client.ResumeFetchPartitions(revoked)
}

// onLost 是分区丢失时的回调（会话过期等）。此时提交大概率失败，而且分区可能已经属于别人，
// 所以不提交，只停掉 worker；没提交的消息会被新的持有者重新处理。
func (c *Consumer) onLost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.log.WarnContext(ctx, "kafka partitions lost", "partitions", lost)
	c.retire(ctx, c.workersOf(lost), false)
	c.client.ResumeFetchPartitions(lost)
}

// workersOf 返回指定分区现有的 worker。
func (c *Consumer) workersOf(tps map[string][]int32) []*worker {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ws []*worker
	for t, ps := range tps {
		for _, p := range ps {
			if w, ok := c.workers[topicPartition{t, p}]; ok {
				ws = append(ws, w)
			}
		}
	}
	return ws
}

// allWorkers 返回所有现有的 worker。
func (c *Consumer) allWorkers() []*worker {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Collect(maps.Values(c.workers))
}

// retire 停掉 ws：等在途消息处理完，按需提交，然后移除。
// 之后再拉到这些分区的记录会创建新的 worker。
func (c *Consumer) retire(ctx context.Context, ws []*worker, commit bool) {
	for _, w := range ws {
		w.halt()
	}
	for _, w := range ws {
		<-w.exited
	}

	// 在 commitMu 里提交并移除：定时提交拿不到已移除的 worker，
	// 也就不会在分区交出去之后再替它提交（那可能回退新持有者的位点）。
	c.commitMu.Lock()
	defer c.commitMu.Unlock()
	if commit {
		c.commitLocked(ctx, ws)
	}
	c.mu.Lock()
	for _, w := range ws {
		if c.workers[w.tp] == w {
			delete(c.workers, w.tp)
		}
	}
	c.mu.Unlock()
}

// triggerCommit 请求尽快提交一次，不阻塞。
func (c *Consumer) triggerCommit() {
	select {
	case c.commitCh <- struct{}{}:
	default:
	}
}

// runCommitter 在收到请求或定时器到期时提交所有分区已处理完的位点。
func (c *Consumer) runCommitter(ctx context.Context, quit <-chan struct{}) {
	ticker := time.NewTicker(commitInterval)
	defer ticker.Stop()
	for {
		select {
		case <-quit:
			return
		case <-ctx.Done():
			return
		case <-c.commitCh:
		case <-ticker.C:
		}
		ws := c.allWorkers()
		c.commitMu.Lock()
		c.commitLocked(ctx, ws)
		c.commitMu.Unlock()
	}
}

// commitLocked 同步提交 ws 中已处理完、尚未提交的位点。调用方持有 commitMu。
// 提交失败只记日志：位点停在旧值，最坏是重复处理，不会丢；下次提交会带上最新值。
func (c *Consumer) commitLocked(ctx context.Context, ws []*worker) {
	var (
		recs  []*kgo.Record
		owner []*worker
	)
	for _, w := range ws {
		if rec := w.uncommitted(); rec != nil {
			recs = append(recs, rec)
			owner = append(owner, w)
		}
	}
	if len(recs) == 0 {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, commitTimeout)
	defer cancel()
	// CommitRecords 提交每条记录的 offset+1，内部用 CommitOffsetsSync，不会被再平衡取消。
	if err := c.client.CommitRecords(cctx, recs...); err != nil {
		c.log.WarnContext(ctx, "kafka commit failed", "partitions", len(recs), "error", err)
		return
	}
	for i, w := range owner {
		w.setCommitted(recs[i].Offset + 1)
	}
}

// laneOf 返回 key 所在的 lane。同一个 key 总是得到同一条 lane，这是并发消费仍然保持同 key 有序的全部依据。
//
// 用带随机种子的 maphash，而不是生产者分区用的那个哈希（murmur2）：一个分区里的 key 本来就是
// “murmur2 取模后相等”的那一批，再用同一个哈希对 lane 数取模，lane 数和分区数有公因数时
// 它们会挤进少数几条 lane。种子每个进程不同也没有关系，lane 的分配不需要跨重启稳定。
func (c *Consumer) laneOf(key []byte) int {
	return int(maphash.Bytes(c.laneSeed, key) % uint64(c.cfg.Concurrency)) //nolint:gosec // 取模后小于 Concurrency
}

// worker 处理一个分区的消息：默认串行；并发消费时它是分发者，把消息交给各条 lane。
type worker struct {
	c   *Consumer
	tp  topicPartition
	log *slog.Logger

	mu    sync.Mutex
	queue []*kgo.Record
	// head 是串行（含按批、延迟）消费时已经从 queue 取出、还没处理完的最前面那条：一条消息在重试或等到期时
	// 只在 worker 的局部变量里，queue 和 last 都看不到它。只给 oldest_pending_age 用（见 oldestPending），
	// 并发消费时不设，那时 tracker 里就有。
	head *kgo.Record
	// paused 表示本 worker 暂停了该分区的拉取；waiting 表示正在等延迟消息到期。
	paused  bool
	waiting bool
	// last 是最近一条处理完的记录，committed 是已提交的位点（下一条要消费的 offset）。
	last      *kgo.Record
	committed int64

	wake     chan struct{}
	stopOnce sync.Once
	stop     chan struct{}
	exited   chan struct{}

	// 以下只在并发消费（Concurrency > 1）时使用。
	// lanes[i] 是第 i 条 lane 的队列。tracker 记录已分发、还不能提交的记录，由 mu 保护。
	// progress 在可提交的前缀前进时收到通知，唤醒因为窗口已满而等待的分发循环。
	lanes    []chan *kgo.Record
	tracker  prefixTracker
	progress chan struct{}
}

// enqueue 追加拉到的记录。积压超过高水位时暂停该分区的拉取。
//
// 暂停是安全的：franz-go 在 poll 时会剔除已暂停分区的缓冲数据，且不推进它的拉取位置，
// 恢复后从上一次返回给我们的位置之后继续拉，不会跳过记录。
func (w *worker) enqueue(recs []*kgo.Record) {
	w.mu.Lock()
	w.queue = append(w.queue, recs...)
	if len(w.queue) >= highWater && !w.paused {
		w.setPausedLocked(true)
	}
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *worker) setPausedLocked(paused bool) {
	if w.paused == paused {
		return
	}
	w.paused = paused
	tps := map[string][]int32{w.tp.topic: {w.tp.partition}}
	if paused {
		w.c.client.PauseFetchPartitions(tps)
	} else {
		w.c.client.ResumeFetchPartitions(tps)
	}
}

// maybeResumeLocked 在没有延迟等待、积压降到低水位以下时恢复拉取。
func (w *worker) maybeResumeLocked() {
	if w.paused && !w.waiting && len(w.queue) <= lowWater {
		w.setPausedLocked(false)
	}
}

func (w *worker) setWaiting(waiting bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.waiting = waiting
	if waiting {
		w.setPausedLocked(true)
	} else {
		w.maybeResumeLocked()
	}
}

// halt 让 worker 在当前这条消息的 handler 返回后退出，并打断重试、死信重试和延迟等待。
func (w *worker) halt() { w.stopOnce.Do(func() { close(w.stop) }) }

// stopping 报告是否应当放弃当前消息。
func (w *worker) stopping(ctx context.Context) bool {
	select {
	case <-w.stop:
		return true
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// sleep 等待 d，被 halt 或强制取消打断时返回 false。
func (w *worker) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-w.stop:
		return false
	case <-ctx.Done():
		return false
	}
}

// run 是 worker 的主循环。ctx 交给 handler，只在强制停止时取消。
func (w *worker) run(ctx context.Context) {
	defer close(w.exited)
	if len(w.lanes) > 0 {
		w.runLanes(ctx)
		return
	}
	if w.c.batch != nil {
		w.runBatches(ctx)
		return
	}
	for {
		rec, ok := w.next(ctx)
		if !ok {
			return
		}
		if !w.c.process(ctx, w, rec) {
			// 被打断：这条消息没有处理完，不标记、不提交，下次重新投递。
			return
		}
		w.markDone(rec)
	}
}

// runLanes 是并发消费时的主循环：按位点顺序取记录，登记到 tracker，再交给 key 对应的 lane。
// 返回前等所有 lane 退出，所以 exited 关闭时这个分区已经没有 handler 在运行。
//
// 停止时（halt 或强制取消）不再分发；各 lane 做完手上的那一条就退出，已经在 lane 队列里、
// 还没开始的记录直接放弃。它们留在 tracker 里没有完成，可提交的前缀到不了它们，位点也就不会越过它们。
func (w *worker) runLanes(ctx context.Context) {
	var lanes sync.WaitGroup
	defer lanes.Wait()
	for _, ch := range w.lanes {
		lanes.Go(func() { w.runLane(ctx, ch) })
	}
	for {
		if !w.waitWindow(ctx) {
			return
		}
		rec, ok := w.next(ctx)
		if !ok {
			return
		}
		// 先登记再分发：lane 可能在这个 goroutine 往下走之前就把它处理完了。
		w.mu.Lock()
		w.tracker.add(rec)
		w.mu.Unlock()
		// 延迟消费者：在这里等到期，到期之后才交给 lane。分发是按位点顺序一条一条来的，
		// 所以队首没到期时它后面的一条都分发不出去，已经分发出去的都是到期了的。
		// 等之前就登记：它没有完成，可提交的前缀到不了它；oldestPending 也看得见它（没到期的年龄是 0）。
		// 被打断时它留在 tracker 里，位点不会越过它，下次重新读到。
		if w.c.clock != nil && !w.c.waitDue(ctx, w, rec) {
			return
		}
		select {
		case w.lanes[w.c.laneOf(rec.Key)] <- rec:
		case <-w.stop:
			return
		case <-ctx.Done():
			return
		}
	}
}

// waitWindow 等到未提交的记录数低于窗口。被 halt 或强制取消时返回 false。
func (w *worker) waitWindow(ctx context.Context) bool {
	for {
		w.mu.Lock()
		n := w.tracker.len()
		w.mu.Unlock()
		if n < w.c.cfg.MaxUncommitted {
			return true
		}
		select {
		case <-w.progress:
		case <-w.stop:
			return false
		case <-ctx.Done():
			return false
		}
	}
}

// runLane 串行处理一条 lane 上的记录。
func (w *worker) runLane(ctx context.Context, ch <-chan *kgo.Record) {
	for {
		select {
		case <-w.stop:
			return
		case <-ctx.Done():
			return
		case rec := <-ch:
			// 两个分支同时就绪时 select 随机选一个，所以取到记录之后再确认一次没有被要求停止。
			if w.stopping(ctx) {
				return
			}
			if !w.c.process(ctx, w, rec) {
				// 被打断：这条消息没有处理完，不标记完成，位点不会越过它。
				return
			}
			w.complete(rec)
		}
	}
}

// complete 是并发消费时的 markDone：标记 rec 处理完，可提交的前缀前进时更新 last。
func (w *worker) complete(rec *kgo.Record) {
	w.mu.Lock()
	if last := w.tracker.complete(rec.Offset); last != nil {
		w.last = last
		// 非阻塞：已经有一个通知在等着被取走就够了，分发循环醒来后会自己重新读未提交的数量。
		select {
		case w.progress <- struct{}{}:
		default:
		}
	}
	idle := w.tracker.len() == 0 && len(w.queue) == 0
	w.mu.Unlock()
	if idle {
		w.c.triggerCommit()
	}
}

// next 取队首记录，队列为空时等待；被 halt 时返回 false。
func (w *worker) next(ctx context.Context) (*kgo.Record, bool) {
	for {
		if w.stopping(ctx) {
			return nil, false
		}
		w.mu.Lock()
		if len(w.queue) > 0 {
			rec := w.queue[0]
			w.queue[0] = nil
			w.queue = w.queue[1:]
			if len(w.lanes) == 0 {
				w.head = rec
			}
			w.maybeResumeLocked()
			w.mu.Unlock()
			return rec, true
		}
		w.mu.Unlock()
		select {
		case <-w.wake:
		case <-w.stop:
		case <-ctx.Done():
		}
	}
}

// markDone 记录处理完的消息。一批（本地队列）处理完时请求提交。
func (w *worker) markDone(rec *kgo.Record) {
	w.mu.Lock()
	w.last = rec
	w.head = nil
	drained := len(w.queue) == 0
	w.mu.Unlock()
	if drained {
		w.c.triggerCommit()
	}
}

// setHead 记下串行消费时正在处理的那条，见 head。
func (w *worker) setHead(rec *kgo.Record) {
	w.mu.Lock()
	w.head = rec
	w.mu.Unlock()
}

// oldestPending 返回这个分区已经拉到、还没处理完的消息里位点最小的那条，没有时返回 nil。O(1)。
//
// 并发消费时是 tracker 的第一条：complete 会弹出开头连续完成的一段，所以留在开头的那条一定没做完，
// 它也正是挡住位点提交的那条。串行时是 head。两者都没有时是 queue 的第一条（拉到了、还没轮到）。
func (w *worker) oldestPending() *kgo.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case len(w.lanes) > 0 && w.tracker.len() > 0:
		return w.tracker.pending[0].rec
	case w.head != nil:
		return w.head
	case len(w.queue) > 0:
		return w.queue[0]
	}
	return nil
}

// uncommitted 返回尚未提交的最新已处理记录，没有则返回 nil。
func (w *worker) uncommitted() *kgo.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.last == nil || w.last.Offset+1 <= w.committed {
		return nil
	}
	return w.last
}

func (w *worker) setCommitted(next int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.committed = max(w.committed, next)
}

// process 处理一条消息直到成功、进入死信或被打断。返回 false 表示被打断，消息没有处理完。
func (c *Consumer) process(ctx context.Context, w *worker, rec *kgo.Record) bool {
	log := w.log.With("offset", rec.Offset)
	// 并发消费时到期已经在分发的时候等过了（runLanes），lane 里不再等。
	if c.clock != nil && len(w.lanes) == 0 && !c.waitDue(ctx, w, rec) {
		return false
	}

	msg := toMessage(rec)
	for attempt := 1; ; attempt++ {
		// 每次尝试一个 span（见 startProcess）。handler 拿到的是带着这个 span 的 ctx，
		// 它里面的数据库、Redis、gRPC 调用都接在下面；下面的日志用同一个 ctx，带 trace id。
		start := time.Now()
		spanCtx, span := c.startProcess(ctx, rec, msg, attempt)
		if attempt == 1 {
			c.observeStart(spanCtx, span, rec, start)
		}
		err := c.invoke(spanCtx, msg, log)
		c.endProcess(spanCtx, span, rec.Topic, start, err)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		if IsPermanent(err) || attempt >= c.cfg.MaxAttempts {
			log.ErrorContext(spanCtx, "kafka message sent to dlq", "attempt", attempt, "permanent", IsPermanent(err), "error", err)
			dlqCount.Add(spanCtx, 1, c.metricAttrs(rec.Topic))
			return c.sendDLQ(ctx, w, rec, err, attempt, log)
		}
		backoff := c.backoff(attempt)
		retryCount.Add(spanCtx, 1, c.metricAttrs(rec.Topic))
		log.WarnContext(spanCtx, "kafka handler failed, retrying", "attempt", attempt, "backoff", backoff, "error", err)
		if !w.sleep(ctx, backoff) {
			return false
		}
	}
}

// backoff 返回第 attempt 次失败后的等待：RetryInitial 起每次翻倍，不超过 RetryMax。
func (c *Consumer) backoff(attempt int) time.Duration {
	d := c.cfg.RetryInitial
	for range attempt - 1 {
		if d >= c.cfg.RetryMax {
			break
		}
		d *= 2
	}
	d = min(d, c.cfg.RetryMax)
	// 加抖动：在 [0.8d, 1.2d) 里均匀取。没有抖动时，同一时刻失败的一批消息（下游挂了，各分区、各 lane、
	// 各实例都在重试同一个下游）会在同一时刻再试，下游刚恢复就迎来一个同步的洪峰。
	return d - d/5 + rand.N(2*d/5+1) //nolint:gosec // 抖动不需要密码学强度的随机数
}

// invoke 调用 handler，把 panic 转成普通错误：一条坏消息不应该让整个进程退出，
// 它会和其他错误一样重试，最终进死信。
func (c *Consumer) invoke(ctx context.Context, msg Message, log *slog.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.ErrorContext(ctx, "kafka handler panicked", "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("kafkax: handler panic: %v", r)
		}
	}()
	return c.handler(ctx, msg)
}

// sendDLQ 把消息原样发到死信 topic，一直重试到成功或被打断。
//
// 死信发送失败不能跳过：跳过就要提交这条消息的位点，而它既没处理成功也没进死信，等于丢了。
// 所以宁可让分区卡住（日志里持续报错），也不前进。
func (c *Consumer) sendDLQ(ctx context.Context, w *worker, rec *kgo.Record, cause error, attempts int, log *slog.Logger) bool {
	headers := make([]kgo.RecordHeader, 0, len(rec.Headers)+6)
	headers = append(headers, rec.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: HeaderDLQError, Value: []byte(truncateUTF8(cause.Error(), maxDLQErrorLen))},
		kgo.RecordHeader{Key: HeaderDLQTopic, Value: []byte(rec.Topic)},
		kgo.RecordHeader{Key: HeaderDLQPartition, Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
		kgo.RecordHeader{Key: HeaderDLQOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		kgo.RecordHeader{Key: HeaderDLQGroup, Value: []byte(c.cfg.Group)},
		kgo.RecordHeader{Key: HeaderDLQAttempts, Value: []byte(strconv.Itoa(attempts))},
	)
	dlqTopic := rec.Topic + c.cfg.DLQSuffix

	for try := 1; ; try++ {
		// 每次重发都新建记录：franz-go 会在发送后修改记录的分区、位点等字段。
		out := &kgo.Record{Topic: dlqTopic, Key: rec.Key, Value: rec.Value, Headers: headers}
		sendCtx, cancel := context.WithTimeout(ctx, dlqSendTimeout)
		_, err := c.dlq.sendRaw(sendCtx, []*kgo.Record{out})
		cancel()
		if err == nil {
			return true
		}
		backoff := c.backoff(try)
		log.ErrorContext(ctx, "kafka dlq send failed, retrying", "dlq_topic", dlqTopic, "try", try, "backoff", backoff, "error", err)
		if !w.sleep(ctx, backoff) {
			return false
		}
	}
}

func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	s = s[:n]
	for len(s) > 0 && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

func toMessage(rec *kgo.Record) Message {
	var headers map[string]string
	if len(rec.Headers) > 0 {
		headers = make(map[string]string, len(rec.Headers))
		for _, h := range rec.Headers {
			headers[h.Key] = string(h.Value)
		}
	}
	return Message{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Key:       rec.Key,
		Value:     rec.Value,
		Headers:   headers,
		Timestamp: rec.Timestamp,
	}
}
