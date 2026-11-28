# s026 的答案

## 标准根因

订单库 1 的从库（`mysql-order-1-1`）被设了延迟复制：`CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 180`（脚本造的，对应“有人为了留后悔的窗口给从库配了延迟，没想到下游读的是它”）。
IO 线程照常收 binlog，SQL 线程故意等 180 秒才应用。两个线程都是 Running，没有任何报错，从库的数据稳定地比主库晚 3 分钟。

## 定位必需的关键证据

- 从库上 `SHOW REPLICA STATUS`：`Replica_IO_Running`、`Replica_SQL_Running` 都是 Yes，`SQL_Delay: 180`，`Seconds_Behind_Source` 稳定在 180 上下，`Replica_SQL_Running_State` 是 “Waiting until SOURCE_DELAY seconds after source executed event”。
  `Retrieved_Gtid_Set` 紧跟主库（收到了），`Executed_Gtid_Set` 落后（没应用）。
- 落后是一条平线，不是越拉越大：从库的 CPU、IO 都不忙，没有大事务，主库的写入量没有变。
- 下游读这个从库的都晚 3 分钟：CDC 的订单库 1 流水线（它读的是从库的 binlog）`cdc_lag_ms` 约 180 秒，订单库 0 的流水线正常；商家订单列表里订单库 1 的新订单要 3 分钟后才出现；变更核对报索引落后。
- order 的降级状态里订单库 1 的从库标成不健康（落后超过 1 秒），兜底扫描退回读主库；用户的下单、查订单不受影响（展示读走主库）。
- 主库上什么都看不出来。

## 容易误判成的原因

- 从库性能不够、单线程回放跟不上（那样落后会随写入量波动、越拉越大，SQL 线程一直在忙；这里是固定的 180 秒）。
- 大事务（没有；`SQL_Delay` 直接写着）。
- 复制断了（两个线程都在跑；`MySQLReplicaSQLThreadStopped`、`MySQLReplicaIOThreadStopped` 不响）。
- CDC 自己慢了、Kafka 积压（订单库 0 的流水线正常，积压只在一条流水线上，源头是它读的那个从库）。
- orderindex 消费慢（它没有积压可消费：消息还没有被 CDC 发出来）。

## 备注

- **全部是按代码和配置推的，没有在集群上跑过。** 脚本在本机 compose 的从库上验过：延迟设成 180、SQL 线程在跑，撤销后回到 0。
- 预期的告警：`MySQLReplicaLagHigh`（落后超过 30 秒持续 5 分钟，warning）、`CDCLagging`（warning）、`DerivedDataLagging`（warning）。都是推的。
- **整点前后不要做**：每小时的对账会把索引落后报成差异，`ReconFoundDifferences`（critical）会响（s008 碰到过）。
- 顺带验设计 4b 第 5 节里的一项：变更核对的“从库目标”在从库落后时是不是报 Behind。
- 这个设置存在从库的复制元数据里，重启也还在：所以是第二档，脚本另起了一个到时自己改回去的后台任务。
