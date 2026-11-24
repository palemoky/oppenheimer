# s011 的答案

## 标准根因

order 查用户订单列表（`ListUserOrders`）时，把每一行转成返回消息的 `toSummaryProto` 会先把币种“规整”成
ISO 4217 的大写代码，而规整用的正则**在每次调用里现场编译**：`canonicalCurrency` 里
`regexp.MustCompile("(?i)^\s*(AED|AFN|…|ZWG)\s*$")`，150 多个代码的不区分大小写的多选一。
编译一次在开发机（M4）上约 0.1 毫秒、约 3300 次小对象分配、约 250 KB；一页 20 行就是约 2 毫秒、6.5 万次分配、5 MB。
结果是对的（库里都是大写的 `CNY`，原样返回），只是每个列表请求都很贵，并且大量短命的小对象把 GC 推得很忙。

开销和这一页的行数成正比：只有一两单的用户几乎不受影响，订单多于一页的用户（Zipf 头部，也是查列表最频繁的那些）
每次都是满页；`page_size=100` 再乘 5。查单个订单（`GetOrder`）、下单不经过这段代码。

代码：`services/order/internal/service/list_currency.go` 的 `canonicalCurrency`（`toSummaryProto` 调它），开关 `f04`。

## 定位必需的关键证据

- order 两个 Pod 的 CPU（`container_cpu_usage_seconds_total`）同时上升，`rpc_server_call_duration` 的请求速率没有变：
  不是流量多了，是每个请求贵了。
- 分配速率（`go_memory_allocated_bytes_total` 的 rate）和 GC 次数成倍上涨，而堆的目标（`go_memory_gc_goal_bytes`）、
  常驻内存基本不变：分配的都是短命对象，不是泄漏。
- CPU 剖析（`/debug/pprof/profile?seconds=30`）：按累计排，`service.(*Service).ListUserOrders` → `newListResponse` →
  `toSummaryProto` → `canonicalCurrency` → `regexp.MustCompile` → `regexp.compile`，下面主要是
  `regexp.compileOnePass` / `makeOnePass` / `mergeRuneSets` 和 `regexp/syntax` 的解析；另一大块是 GC：
  `runtime.gcBgMarkWorker` / `gcDrain`、`runtime.mallocgc`、`growslice`。开发机上量的（只有这段代码在跑）：
  正则编译约占 28%，GC 的后台标记约 21%，其余是调度和系统调用。
- 堆剖析要看 `alloc_space` / `alloc_objects`（不是 `inuse_*`）：分配几乎全在 `canonicalCurrency` 下面的 `regexp/syntax`。
  `inuse_space` 看不出东西。
- 链路：`ListUserOrders` 的 span 比它下面的 MySQL 子 span 长出几毫秒，多出来的时间在查完数据库之后、进程里面；
  数据库的查询数和耗时不变。

## 容易误判成的原因

- 流量上涨或者被压测打过了拐点（请求速率、HPA 都没动；HPA 的上下限都是 2，本来也不会扩）。
- MySQL 慢（列表查询走索引，子 span 和 MySQL 的指标都正常；读名额也不会被占满（没有 `RESOURCE_EXHAUSTED`，
  不是 `docs/runbooks/order-read-busy.md` 的情形），名额只罩着数据库查询那一段，CPU 花在释放名额之后）。
- 内存泄漏（分配速率高，但堆的目标和常驻内存不涨，`inuse_space` 正常）。
- “正则回溯”：Go 的 `regexp` 是 RE2，匹配是线性的，没有灾难性回溯。贵的是**编译**，不是匹配；
  剖析里看到的是 `regexp.compile`，不是 `regexp.(*Regexp).doExecute`。
- 边车或者节点的 CPU 被别人抢了（order 没有设 CPU limit，`container_cpu_cfs_throttled_periods_total` 不涨；
  节点上别的 Pod 的 CPU 没变）。

## 备注

**按设计推的，没有在集群上量过**：稳态压测（k6 mixed，约 570 请求/秒）里查列表占 10%，约 57 次/秒，两个副本各约 28 次/秒，
固定 `limit=20`。开发机上一页 20 行约 2 到 2.6 毫秒；集群的宿主机是 3900X（单核比 M4 慢，按 1.5 到 2 倍算）约 3 到 5 毫秒。
满页的比例取决于库里的数据，按一半算：每个 Pod 直接多花约 0.05 到 0.1 核，加上 GC（每秒上百 MB、上百万次的短命分配，
GC 的次数跟着翻几倍）合计估计每个 Pod 多 0.15 到 0.3 核，相对于平时业务容器约 0.3 核是翻倍左右，开关打开后一个抓取周期内就看得到。
查列表的延迟中位数多几毫秒，p99 多得更多（GC 的辅助标记落在正在分配的请求上）。

**会不会拖慢下单**：会拖慢一点延迟，不会让下单出错。order 进程里 CPU 和 GC 是全进程共享的：下单（`SubmitOrder`）、
落库的消费者、查单个订单在 GC 期间也要做辅助标记，延迟会有可见的上抬（毫秒级）。这条路径没有独立的隔离：
读名额只限数据库查询的并发，不限 CPU；Envoy 入站的并发上限（`http2MaxRequests`）远大于这点在途请求（28 次/秒 × 几毫秒，在途不到 1 个），
不会因此拒绝请求。order 没有 CPU limit，不会被节流；时限都在几百毫秒到秒级，毫秒级的抬升碰不到。
没有写坏任何数据：币种规整只改返回给客户端的那一份，库里都是大写，结果和关着时一样。

**恢复**：关掉开关之后下一个请求起就回到平时，不用重启，堆里不留东西。

**和 s005（order 的 CPU 压力）的区别**：s005 是容器里多出来的 stress 进程，剖析里看不到 Go 代码；这个场景的 CPU 在 order 自己的进程里，
剖析能指到函数。
