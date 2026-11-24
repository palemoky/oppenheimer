# s013 的答案

## 标准根因

order 的后台扫表（sweeper 的 `ListExpired`、`ListStaleCreating`）在从库上执行时，改成了先用 `db.Conn(ctx)`
从从库的连接池里拿出一条连接固定下来，`SET SESSION max_execution_time = N` 之后再在同一条连接上 SELECT，
用完由 `handBackScanConn` 还原会话变量并 `conn.Close()` 交还。**扫描结果为空的那条分支提前 return，
没有交还这条连接**：注释说的“Next 返回 false 时结果集已经自己关了”对 `*sql.Rows` 是对的，但 `*sql.Conn`
不会跟着关，也不跟 ctx 走（ctx 到期只关结果集，不还连接），这条连接从此一直算在池子的“在用”里。

每一次结果为空的从库扫描泄漏一条从库连接。从库的池子和主库的池子是分开的两个 `*sql.DB`
（`MYSQL_REPLICA_DSNS` 和 `MYSQL_DSNS`，每个上限都是 `MYSQL_MAX_OPEN_CONNS` = 40），泄漏只吃掉从库的池。
池子用完之后，从库的健康探测（每个库每秒一次 `WAIT_FOR_EXECUTED_GTID_SET`，用的是同一个从库池）拿不到连接，
等满 2 秒（最大延迟 1 秒加 1 秒余量）超时，从库被标记为不健康；之后 sweeper 直接扫主库，泄漏也就停在 40 条。

代码：`services/order/internal/store/refs_session.go` 的 `listReplicaRefs`，开关 `f06`。

## 定位必需的关键证据

- 只有一个 order 副本有问题：持有 sweeper 那把 leader 锁的那个（扫表只在 leader 上跑）。另一个副本的从库是健康的。
- 那个副本的 `GET /degrade`（管理端口 8081）里 `replica_read` 是 degraded，原因是
  `probe failed: wait replica of db N: context deadline exceeded`；日志里有一条 WARN `replica probe failed, reading primary`，
  `error` 是 `context deadline exceeded`（只在从健康变成不健康时记一次，两个库各一条）。
- 从库本身是好的：`MySQLReplicaLagHigh`、`MySQLReplicaSQLThreadStopped` 都不响，mysqld-exporter 的复制延迟正常，
  另一个副本对同一个从库的探测一直通过。“探测失败”的原因是客户端拿不到连接，不是从库慢。
- 在从库上 `SHOW PROCESSLIST`（或 `performance_schema.threads`）：来自 leader 那个 Pod 的 IP 有约 40 条（每个从库）
  `Command = Sleep` 的连接，`Time` 一直涨，超过 300 秒、甚至超过 1800 秒。健康的池子里不会有这样的连接：
  `ConnMaxIdleTime` 是 5 分钟、`ConnMaxLifetime` 是 30 分钟，空闲的连接到时就被池子关掉了；被借走没还的不受这两个限制。
- 连接池的指标（`db.sql.connection.open{status="inuse"}`、`db.sql.connection.wait`、`db.sql.connection.wait_duration`）：
  在用数阶梯式上涨，每 30 秒（sweeper 的间隔）一级，到 40 封顶不再下降；之后等待次数每个库约每 3 秒加一（探测），
  每次约 2 秒。**注意下面“备注”里说的指标重名问题**：这组指标分不出主库池和从库池。
- goroutine 剖析：**没有**“一大堆”停在 `database/sql.(*DB).conn` 的 goroutine，这里的调用方都是串行的后台循环。
  能看到的是每个库一个 `replica-prober` 的 goroutine 停在 `(*DB).conn` ← `store.(*Store).WaitReplicaOfDB`；
  泄漏的连接本身没有 goroutine，在 goroutine、heap 剖析里都看不到。从库被标记不健康之前那一两轮，
  还可能看到 sweeper 停在 `(*DB).conn` ← `(*DB).Conn` ← `store.listReplicaRefs`，这是指向代码位置最直接的一条。
- 代码：`listReplicaRefs` 里 `if !rows.Next() { ...; return nil, nil }` 在 `defer handBackScanConn(ctx, conn)` 之前。

## 容易误判成的原因

- 从库延迟或者复制断了（探测的日志写的是“reading primary”，`replica_read` 降级，最像这个）。复制是好的，
  另一个副本的探测通过，exporter 的延迟正常。
- 主库的连接池耗尽（指标重名时看到的“order_db_0 在用 40”很可能是从库池的数，见备注）。主库的写入、落库、关单都正常，
  落库延迟和 `OrderBackpressureRejecting` 没有变化。
- MySQL 的 `max_connections` 不够、网络问题（从库上的连接是建好了在 Sleep，不是建不上）。
- 读路径的 bulkhead（`OrderReadBulkheadRejecting`）：长期运行那一套是 `ORDER_READ_MODE=primary`，展示读本来就不读从库，
  这个故障对用户请求没有影响。

## 备注

**速度（按设计推的，没有在集群上量过）**：长期运行那一套 `SWEEP_INTERVAL` 是默认的 30 秒，8 个 slot、每个库 4 个，
每个 slot 每轮在从库上做两次扫描。`ListStaleCreating`（落库超过 1 分钟还在 CREATING）在稳态下几乎总是空的，
`ListExpired` 是否为空看延迟关单跟得多紧（k6 mixed 每秒约 46 单、大部分不付款，15 分钟后到期，延迟消息到点就关，
sweeper 扫到的往往是空的）。所以每个库每轮泄漏 4 到 8 条，**开关打开后约 2.5 到 5 分钟**从库池用完、
从库被标记为不健康。这个触发不依赖压测：没有流量时扫描全是空的，泄漏得最快。
`oppenheimer-e2e` 加上 chaos 的覆盖时 `SWEEP_INTERVAL` 是 2 秒，十几秒就用完。

**会不会拖到主库**：不会。两个池是分开的 `*sql.DB`，泄漏只占从库池。从库不健康之后 sweeper 改扫主库：
每轮 16 次走索引的小查询，对 40 条的主库池没有可感知的压力。展示读在 `ORDER_READ_MODE=primary` 下本来就读主库；
在 `gtid`/`window` 模式（e2e 那一套）下会因为从库不健康退回主库，退回的读占展示读的名额
（`READ_MAX_CONCURRENCY` 启动时校验小于连接池），所以写路径仍然有连接可用；那时读主库的量会上去，
可能看到展示读的名额满（`RESOURCE_EXHAUSTED`），这是退回的结果，不是根因。

**关了开关之后**：已经占住的 40 条连接不会回来（`*sql.Conn` 没有终结器，database/sql 永远把它们算在用），
leader 副本上的探测一直失败、从库一直被当成不健康，sweeper 一直扫主库。**要重启 order**（`recover.restart`）。
重启之后 leader 可能换到另一个副本，开关还开着的话新的 leader 会接着泄漏，所以先关开关再重启。
从库那边的 Sleep 连接要等进程退出（连接被关）或者 MySQL 的 `wait_timeout` 到期才消失。

**指标重名**：`pkg/mysqlx` 给连接池指标的属性只有 `db.system.name` 和 `db.namespace`（库名），主库池和从库池的库名
相同（都是 `order_db_N`），两个池的序列属性完全一样。按 OTel Go SDK 的聚合（没有在集群上核对）：计数类
（`wait`、`wait_duration`）是两个池相加，`open` 这类 gauge 是同一次采集里最后观察到的那个（大概率是后注册的从库池）。
所以“哪个池满了”要靠 PROCESSLIST 的来源和 `/degrade` 判断，不能只看指标。

**开关打开时的其余影响**：不为空的从库扫描照常交还连接；开关只改从库上的扫表，展示读、探测、写路径都不经过这段代码。
