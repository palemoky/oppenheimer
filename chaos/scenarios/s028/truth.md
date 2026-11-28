# s028 的答案

## 标准根因

mysql-main 的 `max_connections` 被在线调小了（`SET GLOBAL max_connections = 30`，脚本造的；配置文件里是 400）。
已经建好的连接不受影响，但新建连接在连接数达到上限时被拒绝：`Error 1040: Too many connections`。
inventory、payment、orderindex 和 order 的冷库都连这个实例，各自的连接池在连接到期（最长 30 分钟、空闲 5 分钟）要换新时建不出来。

## 定位必需的关键证据

- 服务的日志里是 `Error 1040: Too many connections`（MySQL 自己返回的错误，不是超时、不是拒绝连接）。
- `SELECT @@GLOBAL.max_connections` 是 30，而配置文件（chart 的 values）写的是 400；`performance_schema.variables_info` 里这个变量的 `VARIABLE_SOURCE` 是 `DYNAMIC`，有 `SET_TIME` 和 `SET_USER`：能看出是什么时候、谁在线改的。
- `Threads_connected` 贴着上限；`Connection_errors_max_connections` 在涨；`Aborted_connects` 可能跟着涨。
- 症状是逐渐出现的、时有时无：谁的连接先到期谁先失败，抢到名额的服务正常。几个服务同时受影响，而它们唯一的共同点是 mysql-main；订单库 0、1 完全正常。
- 库本身不忙：CPU、慢查询、锁都正常，30 个连接里大部分是空闲的。

## 容易误判成的原因

- 某个服务连接泄漏把库的连接占满了（各服务的池没有超过自己的上限，总数只有 30；是上限变了）。
- mysql-main 挂了（exporter 自己的连接断了之后也连不上，`mysql_up` 变 0、`MySQLDown` 响；但库是好的，从 Pod 里用 root 连得上）。
- 服务的连接池配小了（池的上限没有变，是建连接被拒绝）。
- 网络问题（错误是 MySQL 返回的 1040）。

## 备注

- **全部是推的，没有在集群上跑过；症状多久出现、有多重没有把握**：取决于注入时已有多少连接、各个池的连接什么时候到期。30 这个数是猜的，注入前先看 `Threads_connected` 再定（`CX_MAX_CONNECTIONS`）。脚本在本机验过：改成 30、撤销后回到 400。
- root 有一个额外保留的连接名额，所以撤销时脚本连得上；但只有一个，同一时刻有别的 root 会话占着的话要等。
- 预期 `MySQLConnectionsHigh`（连接数超过上限的八成持续 5 分钟，warning）会响：这是最直接的线索。`MySQLDown` 可能误响。
- 顺带核对了设计 4b 第 1.3 节的一项（连接池的预算，按检索子 Agent 报的数算的，没有逐个核对代码）：mysql-main 上 inventory 2×50、payment 2×50、orderindex 2×2×10、order 的冷库 2×10，合计 260，加上对账、exporter 不到 300，在 400 以内；订单库每个实例 order 2×40 加 CDC、对账，在 200 以内。预算没有问题。
