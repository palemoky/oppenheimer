# 场景池的内部笔记（主人不读）

和 `docs/design/4b-experiments.md` 配套：那一份主人能读，不写内容；这里写内容。接手的 Agent 先读这里。

## 现在的池（2026-11-24）

| 编号 | 档 | 类别 | 方式 | 目标 | 是什么 | 在集群上跑过 |
|---|---|---|---|---|---|---|
| s001 | 2 | app | fault-switch `f01` | gateway | 查库存的时限用了无缓冲 channel，超时的请求泄漏一个 goroutine | 是（4a，不盲） |
| s002 | 1 | pod | chaos-mesh | inventory | `pod-kill`（`gracePeriod` 默认 0），“没出事”的对照 | 是（4a，不盲） |
| s003 | 1 | network | chaos-mesh | payment | 出站延迟 100 毫秒 | 否 |
| s004 | 1 | pod | chaos-mesh | order | `container-kill`，进程直接被 KILL | 否；这个 action 没有冒烟过 |
| s005 | 1 | stress | chaos-mesh | order | 一个副本的容器里 3 个 CPU worker（没有 CPU limit，会挤节点） | 否 |
| s006 | 1 | stress | chaos-mesh | gateway | 一个副本的容器里要 640 MB，超过 512 Mi 的 limit，OOM | 否；超过 limit 的情形没有冒烟过 |
| s007 | 1 | network | chaos-mesh | inventory | 出站丢包 10% | 否 |
| s008 | 1 | dns | chaos-mesh | orderindex | 数据库的域名解析失败，随连接到期逐渐显现，35 分钟 | 否 |

每个场景自己的“没有核实的”写在它的 `truth.md` 的备注里。B1（e2e 里试）要逐条核对。

## B1 里要试的（设计 4b 第 3 节）

- s004：退出码、事件、边车留不留、Chaos Mesh 的 `container-kill` 要不要 `duration`。
- s006：被杀的是整个容器还是只有 stress 的 worker；容器重启后会不会再注入。
- s008：两条模式能不能挡住 Go 解析器发出的全部写法；e2e 里连接的寿命是 30 分钟，等不起，可以临时把目标 Pod 重启一次看新进程建连接时的错误长什么样。
- `pod-kill` 的 `gracePeriod` 0 和 45 各一次，看进程日志里有没有排空的那几行。主人同意第 2 节第 1 项之后把 s002 改成 45。
- Istio 故障注入的两条路（改现有的 VirtualService；EnvoyFilter）。
- 服务到数据库的分区：NetworkChaos 的 `target` 选别的命名空间要那个命名空间也有注解（脚本只允许 `oppenheimer` 两个），试 `externalTargets` 写域名或者 Pod 的地址行不行。

## 七个故障点打算放在哪（主人确认设计第 2 节第 4 项之后派 Opus 写）

原则：不进资金、库存扣减、订单状态机的路径；选读路径和派生数据；每个要有像真的那样的触发条件。下面是主会话的初步分配，
派任务之前要先读对应的代码确认接缝存在、确认它和写路径不共用会被拖垮的资源（或者共用是有意的）。

| 开关 | 类别 | 服务 | 想法 | 要注意的 |
|---|---|---|---|---|
| f02 | 内存泄漏 | gateway | 一个按“用户加 SKU”做键、没有上限也不淘汰的本地缓存 | Zipf 分布下键的增长是长尾的，涨得慢才像真的；limit 512 Mi，算一下多久到 |
| f03 | 锁竞争 | order 的读路径（查订单） | 一把全局的互斥锁罩住了一段带 I/O 的代码 | 读路径有自己的隔离舱（`OrderReadBulkheadRejecting`），确认拖不到下单；要 mutex、block 采样开着才诊断得了 |
| f04 | CPU 飙高 | gateway | 每个请求里重新编译正则，或者大量小对象引发 GC 压力 | Go 的正则是 RE2，没有回溯，计划里的“正则回溯”在 Go 里造不出来，复盘时讲这一点 |
| f05 | 死锁 | orderindex | 两条路径以相反的顺序拿两把锁，要并发到一定程度才撞上 | 撞上之后消费者停住，派生数据落后，重启恢复；别放在会拖住全部 goroutine 的地方（探针也会挂，变成反复重启，那是另一个场景） |
| f06 | 连接泄漏 | orderindex 的查询 | 某个出错的分支没有关 `rows` | 它的连接池和 inventory、payment 不共用，但连的是同一个 `mysql-main`：算清楚池的上限，不能把主库的 `max_connections` 吃满 |
| f07 | panic 反复重启 | payment 之外的任一个，倾向 orderindex | 某种少见的输入下空指针，没有 recover | 要 `FAULT_FILE` 加 Secret（重启后开关要还在）；消费者里的 panic 会让同一条消息反复触发，像真的“毒消息” |
| f08 | 数据竞争 | orderindex（派生数据） | 并发写一个没有保护的 map 或者计数 | 只在 `oppenheimer-e2e` 里做；不能进 `-race` 的测试，测试单独处理 |

gateway 已经有 f01，再放两个偏多：写之前看看 f02、f04 能不能有一个挪到 order 的读路径上。
