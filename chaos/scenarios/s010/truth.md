# s010 的答案

## 标准根因

inventory 的 `GetStock` 在查库存外面包了一层“记住上一次的数”：缓存的 Redis 和库都查不到时，退回这个 SKU
上一次查到的库存。记录用的是**整个进程共用的一把 `sync.Mutex`** 护着的一张表，而函数一开头就
`Lock()` 加 `defer Unlock()`，于是**整次查询（读缓存 Redis，未命中时再加重建锁、查库、写回缓存，
另一个副本正在重建同一个 key 时还要按 20 毫秒一轮等它）都在锁里做**。锁本来只需要护住读写那张表的
几百纳秒，现在护住的是一次网络往返；所有 SKU 的查询在每个副本上排成一队，一次只过一个。

代码：`services/inventory/internal/service/stock_lastknown.go` 的 `stockOrLastKnown`，开关 `f03`。
只在查库存这条只读路径上：Reserve、Confirm、Release、闸门、对账都不经过 `stock`，也不碰这把锁。

## 定位必需的关键证据

- **CPU 不高而延迟高**：inventory 的 CPU 和打开前一样（甚至略低，排队的 goroutine 不占 CPU），
  `GetStock` 的服务端耗时（gRPC 服务端指标、gateway 的 `GET /api/v1/skus/{id}/stock`）变长，
  而同一个进程里的 Reserve 的耗时不变。
- **链路**：`GetStock` 的服务端 span 变长，但它下面的 Redis（`GET`）和 MySQL 的子 span 跟平时一样快；
  时间花在服务端 span 开始到第一个子 span 开始之间，那一段没有任何 I/O。
- **mutex 剖析**（`DEBUG_MUTEX_FRACTION` 开着时）：Go 把争用记在**放锁的一方**的调用栈上，
  最大的一项是 `sync.(*Mutex).Unlock` ← `service.(*Service).stockOrLastKnown`（defer 那一行）
  ← `(*Service).stock` ← `(*Service).GetStock`。延迟总量大约等于这段时间里所有查询等锁的时间之和。
  平时这个进程里几乎没有锁争用，所以它会排第一，量级差得很远。
- **block 剖析**（`DEBUG_BLOCK_RATE` 开着时）：**等锁的一方**，`sync.(*Mutex).Lock` ←
  `stockOrLastKnown`，在所有的 gRPC 处理 goroutine 上。block 剖析里平时就有不少条目（后台循环和消费者等
  channel、`select`，cache 包等重建时的 `time.After`），要按调用栈里有没有 `GetStock` 筛，不要看总量。
- **goroutine 剖析**：排队严重的时刻能看到多个 goroutine 停在 `sync.runtime_SemacquireMutex`，
  调用栈都在 `stockOrLastKnown`；数量随查库存的速率涨落，关掉开关后归零（不是泄漏）。
- CPU 剖析里**看不到**这个问题：锁里的时间大部分在等网络，不在 CPU 上。

## 容易误判成的原因

- 缓存的 Redis 慢了：Redis 的子 span 和 `redis-cache` 自己的延迟都正常；慢的是“排队等着去读 Redis”。
- MySQL 慢了，或者连接池不够：查库只发生在缓存未命中时，而且整个副本同时最多只有一个查库存的查询在用连接，
  连接池的在用数反而更低。
- `pkg/cache` 的重建锁（Redis 里的 `cache:lock:*`）：那是另一把锁，只在 key 过期的那一刻、跨副本起作用，
  正常时一秒几次；它和这里的进程内锁会叠加（锁里等重建的那 20 毫秒），但不是根因。
- CPU 限流、节点 CPU 高、GC 停顿：CPU 剖析和运行时指标都没有变化。
- 网格的并发上限或摘除：只有在排队严重到 gateway 每个 Pod 对 inventory 的在途请求超过 128 时才会出现
  overflow，那是结果，不是原因。

## 备注

全部是按设计推的，没有在集群上量过。

- **负载**：稳态（k6 mixed，约 570 请求/秒）里查库存约 200 次/秒，分在 2 个副本上，每个副本约 100 次/秒；
  SKU 只有 20 个、Zipf 分布，缓存 TTL 1 秒（±20%）。
- **锁里一次要多久（S）**：缓存命中（约九成）是一次 Redis 往返，集群里按 0.3 到 1 毫秒算；
  未命中（每个 SKU 约每秒一次，两个副本合起来每秒十几到二十次）是读、`SETNX`、再读、查库、写回、解锁，
  按 3 到 6 毫秒算；偶尔撞上另一个副本正在重建同一个 key，要在锁里等一轮 20 毫秒（按每个副本每秒零点几次算）。
  平均 S 约 1 到 2 毫秒。
- **排队的程度**：每个副本的锁被占用的比例 ρ = 100 次/秒 × S ≈ 0.1 到 0.25。所以稳态下的症状是**中等的**：
  p50 只多零点几毫秒，p99 大约翻一到两倍（gateway 上这个接口的 p99 从约 9 毫秒到 20 到 30 毫秒），
  mutex 和 block 剖析里非常明显。打开后几秒内就出现，不随时间累积。
- **触发条件是“锁里的那次 I/O 变慢”**：每个副本查库存的吞吐上限变成 1 / S。缓存 Redis 的往返到 5 毫秒时
  S 约 6 到 8 毫秒，上限 125 到 170 次/秒，ρ 到 0.6 到 0.8，p99 到几百毫秒；往返到 10 毫秒以上时上限低于
  100 次/秒，队列只涨不降，直到 gateway 的 3 秒时限（504）和每个 Pod 128 个在途请求的上限（503，
  `MeshConcurrencyLimitOverflow`）把多出来的请求挡掉。也就是说这个场景和“缓存 Redis 加延迟”叠加时会从
  “慢一点”变成“这个接口基本不可用”。一个副本被杀掉、所有查库存压到另一个副本上时同理（ρ 翻倍）。
- **不连带扣减路径**：Reserve、Confirm、Release 不调用 `stock`，不碰这把锁；查库存的缓存 Redis
  （`CACHE_REDIS_ADDR`）和闸门的 Redis 是两个实例；排队的查询不占 DB 连接（锁外面的不发起查询），
  所以不会挤占 Reserve 的连接池。共享的只有 gRPC 服务端和 inventory 的入站边车：入站边车在网格里没有并发上限，
  gateway 一侧的 128 会先把查库存的在途数挡住，Reserve 来自 order，有自己的额度和摘除统计。
- **恢复**：关掉开关之后新的查询不再进这把锁，已经排着的几毫秒内放完，不用重启。
  进程里那张表关着时不读，大小以 SKU 数为上限，留着无害。
- 测试 `stock_lastknown_test.go` 用给每条 Redis 命令加 15 毫秒的办法钉住了“关着时并行、开着时串行、Reserve 不等”。
