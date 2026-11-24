# s014 的答案

## 标准根因

orderindex 的 CDC 消费者在解码一批订单变更时，把这一批按 64 条一块分给几个 goroutine 并行解码（`sync.WaitGroup.Go`），
每个 goroutine 用一条“快速路径”按类型直接取列值。快速路径假设“状态按订单的进度排，`status >= PAID` 的都付过款、带着 `paid_at`”，
对 `paid_at` 做了不带 `, ok` 的类型断言。可是 CANCELLED（4）、CLOSED（5）的数值也大于 PAID（3），而它们没付款，`paid_at` 是 NULL：
一张超时没付款、被关掉的订单的变更一到，`paid.GetKind().(*cdcv1.Value_Str)` 在 nil 接口上断言，panic。

panic 发生在消费者 handler 自己起的 goroutine 里。`pkg/kafkax` 在 handler 外面有 recover（`invoke`、`invokeBatch`，
会把 panic 变成普通错误、原地重试），但 recover 只管同一个 goroutine，接不到这里，于是整个进程退出（退出码 2）。
这一批没处理完，位点没有提交；重启后消费者从同一个位点再拉到同一条 CLOSED 的变更，再 panic：“毒消息”，Pod 进入 CrashLoopBackOff。

代码：`services/orderindex/internal/indexer/decode_fanout.go` 的 `rowDirect`（`paid_at` 那一行）和 `decodeChanges`（起 goroutine 的地方），开关 `f07`。

## 定位必需的关键证据

- orderindex 的两个副本重启次数一直涨，状态在 `CrashLoopBackOff` / `Error` 之间转，上一次退出的码是 2（Go 的未恢复 panic），不是 137（不是 OOM、不是被杀）。
- `kubectl logs --previous` 的最后是一段**不是 JSON** 的 Go 运行时输出：`panic: interface conversion: interface is nil, not *cdcv1.Value_Str`，
  出错的 goroutine 的栈是 `indexer.rowDirect`（`decode_fanout.go`）← `indexer.decodeChangeWith` ← `indexer.decodeChanges.func1`，
  下面是 `created by ... indexer.decodeChanges`，也就是说它不在消费者 handler 的那个 goroutine 上。
- 日志里**没有** kafkax 的 `kafka batch handler panicked` / `kafka handler panicked`：那是 recover 接住 panic 时才打的，它没出现正说明 panic 逃出了 recover 的范围。
- 消费组 `orderindex.cdc-orders` 的已提交位点不动、积压一直涨（Kafka Exporter 的 `kafka_consumergroup_lag`）；CDC 自己是正常的（没有 `CDCLagging`）。
- 崩溃的时刻和“有 CLOSED 订单的变更被拉到”对得上：把那个分区停住的位点上的消息取出来看（`kafka-console-consumer` 指定分区和位点，值是 `cdc.v1.RowChange` 的 protobuf），
  是 `status=5`、`paid_at` 为 NULL、`merchant_id` 有值的 UPDATE（或者归档产生的 DELETE）。待支付和已支付的变更都能正常解码。

## 容易误判成的原因

- OOM 或者资源不够被杀（退出码是 2 不是 137；内存曲线没有往上爬就断了）。
- 存活探针太严、启动太慢被 kubelet 杀掉（事件里是容器自己退出 `Error`，不是 `Liveness probe failed` 后的 Killing）。
- 依赖不可用（MySQL、Kafka 都正常；依赖坏了时消费者是“原地重试、分区停住、进程活着”，不会崩，ADR-0021）。
- CDC 发出了坏消息、表结构被改了（消息是合法的、形状和平时一样；表结构不符时报的是 `ErrSchema`，进程不会退出）。
- 发版或配置变更（镜像和配置都没变；关掉开关之后同一份镜像就好了）。
- 网格的问题（gateway 查商家订单的 503 是因为 orderindex 的 Pod 不在、没有可用的端点，不是边车自己的超时或熔断）。

## 备注

- **触发多快**（按设计推的，没有在集群上量过）：mixed 压测下每秒约 46 单，其中付款的约 11 单/秒，其余约 35 单/秒在 15 分钟后被到期关单，
  所以压测跑过 15 分钟之后每秒都有几十条 CLOSED 的变更。开关打开后 1 秒以内每个副本就会遇到第一条；重启后的新进程在拿到分区、拉到第一批时（几秒内）又崩。
  不是每条都触发：CREATING（商家是 NULL）、PENDING_PAYMENT、PAID 的变更都正常，只有“状态大于 PAID 而没付款”的那几类。
  mixed 里没有取消（k6 的 mixed 不调取消接口），触发的全是到期关单；归档任务删热库里关掉的订单时产生的 DELETE 也会触发。
- **重启后开关还要在**：开关在进程内存里，进程崩了就复位。所以这个场景要走 `FAULT_FILE`（`pkg/fault` 启动时读，列出的开关一登记就是开着的，没有 ttl）。
  只经管理端口打开的话，每个副本崩一次之后新进程里开关是关的，消费者把那条消息正常处理掉，自己就好了（看到的只是每个副本重启一次）。
- **pprof 没有用**：进程活不过几秒，连不上调试端口；证据在上一个容器的 stderr 里。那段 panic 输出不是 JSON，按 JSON 解析日志的管道可能把它丢掉或者当成一行一行的普通文本，
  要直接 `kubectl logs --previous`。orderindex 的指标也可能报不上来（进程活的时间不到一个导出周期），`ServiceMetricsMissing` 可能也会响。
- **连带**：orderindex 的查询接口（`ListMerchantOrders`）在同一个进程里，Pod 崩着时商家订单列表全部失败。它是 mixed 里 5% 的请求（约 28 次/秒），
  gateway 返回 5xx，`IngressGateway5xxHigh`（阈值 1%）大约 5 分钟后会响。下单、支付、查库存、查用户订单不受影响（不经过 orderindex）。
- **`ConsumerBlocked` 不会响**：它看的是“最老的一条在进程里待了多久”，每次进程重启都从 0 开始算，而进程活不过几秒。能看出来的是 `ConsumerNotConsuming`
  （已提交的位点 10 分钟不动再加 for 的 10 分钟，约 20 分钟后响）和 `DerivedDataLagging`（逐行核对变更的发现索引落后）。30 分钟的时长是按这两个的出现时间定的。
- **消息会不会丢**：不会。一批没有处理完就不标记完成、位点不提交；崩溃前处理完但还没来得及提交（提交间隔 1 秒）的批会被重放，按 version 的 upsert 是幂等的。
  `cdc.order.orders` 是压缩保留的（按 key 留最新的一条），即使停很久，丢的也只是同一订单被更新的版本覆盖掉的中间版本，最新状态还在。
- **恢复**：去掉 `FAULT_FILE` 里的 `f07`（或者把整份 Secret 清空），再滚动重启 orderindex。只经管理端口打开的情形，崩一次之后已经自己恢复。
  恢复后消费者从已提交的位点接着消费，按批（一批最多 500 条）把积压追上（30 分钟的积压按设计推几分钟内追完，没有量过），派生数据追上后和订单库一致，不用重建索引。
- 已知的局限：一条 CLOSED 的变更一定会进到某个分区，而这一批解码在写库之前，所以崩之前一条都写不进去；但不同分区的批是并行处理的，
  某个分区可能在别的分区崩之前写完了一批并提交，所以不能说“每个分区一条都没前进”，只是整体基本不动。
