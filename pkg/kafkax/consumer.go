package kafkax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kadm"
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
}

const (
	defaultMaxAttempts  = 5
	defaultRetryInitial = 100 * time.Millisecond
	defaultRetryMax     = 5 * time.Second
	defaultDLQSuffix    = ".dlq"

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
//   - 失败原地重试，不跳过：跳过意味着提交了后面的位点，前面那条就再也不会被投递。
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
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	c := &Consumer{
		cfg:      cfg,
		handler:  handler,
		log:      logger.With("group", cfg.Group),
		clock:    clock,
		workers:  make(map[topicPartition]*worker),
		commitCh: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}

	opts := []kgo.Opt{
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		// 新的消费者组从最早的位点开始，不会因为消费者晚于生产者启动而漏消息。
		// 这个选项同时决定 broker 丢数据、位点越界时从哪里继续：也选最早，宁可重复不可跳过。
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(fetchMaxWait),
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

// checkDLQTopics 确认每个死信 topic 存在。broker 关闭了自动建 topic，
// 死信 topic 不存在时消息进不了死信，分区会一直卡在死信重试上，所以在启动时就报错。
func (c *Consumer) checkDLQTopics(ctx context.Context) error {
	names := make([]string, len(c.cfg.Topics))
	for i, t := range c.cfg.Topics {
		names[i] = t + c.cfg.DLQSuffix
	}
	details, err := kadm.NewClient(c.client).ListTopics(ctx, names...)
	if err != nil {
		return fmt.Errorf("kafkax: list dlq topics: %w", err)
	}
	for _, name := range names {
		d, ok := details[name]
		if !ok {
			return fmt.Errorf("kafkax: dlq topic %s does not exist", name)
		}
		if d.Err != nil {
			return fmt.Errorf("kafkax: dlq topic %s: %w", name, d.Err)
		}
	}
	return nil
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
		c.log.Warn("kafka leave group failed", "error", err)
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
			c.log.Warn("kafka fetch error", "topic", topic, "partition", partition, "error", err)
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
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
	c.workers[tp] = w
	go w.run(hardCtx)
	return w
}

// onRevoked 是分区被收回时的回调（也在离开组时调用）。
// 等被收回分区的 worker 处理完在途消息，把处理完的位点提交掉，再交出分区。
// 不等的话，新的持有者会从旧位点开始，与本成员同时处理同一分区，顺序和重复都失控。
func (c *Consumer) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	if len(revoked) > 0 {
		c.log.Info("kafka partitions revoked", "partitions", revoked)
	}
	c.retire(ctx, c.workersOf(revoked), true)
	// 暂停状态在分区重新分配回来后仍然有效，交出前必须恢复，否则再分配回来会永远不拉。
	c.client.ResumeFetchPartitions(revoked)
}

// onLost 是分区丢失时的回调（会话过期等）。此时提交大概率失败，而且分区可能已经属于别人，
// 所以不提交，只停掉 worker；没提交的消息会被新的持有者重新处理。
func (c *Consumer) onLost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.log.Warn("kafka partitions lost", "partitions", lost)
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
		c.log.Warn("kafka commit failed", "partitions", len(recs), "error", err)
		return
	}
	for i, w := range owner {
		w.setCommitted(recs[i].Offset + 1)
	}
}

// worker 串行处理一个分区的消息。
type worker struct {
	c   *Consumer
	tp  topicPartition
	log *slog.Logger

	mu    sync.Mutex
	queue []*kgo.Record
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
	drained := len(w.queue) == 0
	w.mu.Unlock()
	if drained {
		w.c.triggerCommit()
	}
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
	if c.clock != nil && !c.waitDue(ctx, w, rec, log) {
		return false
	}

	msg := toMessage(rec)
	for attempt := 1; ; attempt++ {
		err := c.invoke(ctx, msg, log)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		if IsPermanent(err) || attempt >= c.cfg.MaxAttempts {
			log.Error("kafka message sent to dlq", "attempt", attempt, "permanent", IsPermanent(err), "error", err)
			return c.sendDLQ(ctx, w, rec, err, attempt, log)
		}
		backoff := c.backoff(attempt)
		log.Warn("kafka handler failed, retrying", "attempt", attempt, "backoff", backoff, "error", err)
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
	return min(d, c.cfg.RetryMax)
}

// invoke 调用 handler，把 panic 转成普通错误：一条坏消息不应该让整个进程退出，
// 它会和其他错误一样重试，最终进死信。
func (c *Consumer) invoke(ctx context.Context, msg Message, log *slog.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("kafka handler panicked", "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
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
		log.Error("kafka dlq send failed, retrying", "dlq_topic", dlqTopic, "try", try, "backoff", backoff, "error", err)
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
