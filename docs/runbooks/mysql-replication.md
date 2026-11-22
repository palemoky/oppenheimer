# MySQL 主从复制

覆盖的告警：`MySQLReplicaSQLThreadStopped`（critical）、`MySQLReplicaIOThreadStopped`（critical）、`MySQLReplicaLagHigh`（warning）、`MySQLReplicaMetricsMissing`（warning）

## 这条告警在说什么

只有订单库两套（`mysql-order-0`、`mysql-order-1`）有从库，从库是 Pod `-1`；`mysql-main` 没有。

- `MySQLReplicaSQLThreadStopped`：`mysql_slave_status_replica_sql_running == 0`，2 分钟。SQL 线程不再应用主库的变更。
- `MySQLReplicaIOThreadStopped`：`mysql_slave_status_replica_io_running == 0`，2 分钟。收不到主库的 binlog。
- `MySQLReplicaLagHigh`：`mysql_slave_status_seconds_behind_source > 30`，5 分钟。平时是 0（量到过）。
- `MySQLReplicaMetricsMissing`：清单里的从库（`mysql-order-0-1`、`mysql-order-1-1`）的延迟序列**和**SQL 线程状态序列都没有（`absent`），5 分钟。清单是写死的，加从库要同步改规则。

为什么是四条：SQL 线程停了以后 `Seconds_Behind_Source` 是 `NULL`，延迟序列直接消失，“延迟大”对最严重的情况不响。这是 3a 验证 7 里实际做过的实验（`STOP REPLICA SQL_THREAD`），见[3a 笔记第 11 节](../notes/3a-observability.md)。线程状态在、值是 0 的归前两条；两个都没有的归第四条。出处：[设计 1.3 第 3 条、4.4](../design/3c-slo-alerting.md)。

抑制：`MySQLDown` 抑制同一实例的 `MySQLReplica*`，和同一套里的 `MySQLReplicaIOThreadStopped`（主库连不上时从库的 IO 线程自然停）。

## 影响

- 读从库的流量（订单列表、商家索引的 CDC、对账、备份）读到旧数据。CDC 读的是从库的 binlog，复制停了商家索引就不更新（3a 验证 7 里 `orderindex.cdc-orders` 的消费延迟随之涨）。
- 数据风险：这时主库再坏，从库上缺的那部分就丢了。critical 的两条是现在就看。
- 备份在从库上做，复制线程不都是 `Yes` 时 `backup.sh` 直接失败（宁可没有备份也不要假的新备份），所以复制停久了会连带触发 [`MySQLBackupStale`](mysql-backup.md)。
- 延迟大（warning）：落库不受影响，读到的旧数据变多；继续拉大变成数据风险。

## 先看什么

1. 哪个实例、哪种状态：

   ```
   scripts/cluster/monitoring.sh promql 'mysql_slave_status_replica_sql_running'
   scripts/cluster/monitoring.sh promql 'mysql_slave_status_replica_io_running'
   scripts/cluster/monitoring.sh promql 'mysql_slave_status_seconds_behind_source'
   scripts/cluster/monitoring.sh promql 'mysql_up{instance=~"mysql-order-.-1"}'
   ```
2. 线程为什么停（错误号和信息）：

   ```
   echo "SELECT LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE FROM performance_schema.replication_applier_status_by_worker; SELECT LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE FROM performance_schema.replication_connection_status;" | \
     KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n mysql exec -i mysql-order-0-1 -c mysql -- bash -c '. /scripts/lib.sh; rsql 127.0.0.1 -N -B'
   ```
3. 集群上的核对，有无 `ANOMALY`、落盘方式是不是 2/0：`scripts/cluster/middleware.sh mysql`。
4. 从库 Pod 和日志，最近有没有重启、节点有没有断过电：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n mysql get pods -o wide
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n mysql logs mysql-order-0-1 -c mysql --tail=100
   ```
5. 延迟大的话先确认是不是在压测（`k6` 在跑、PROGRESS 里有没有登记的实验），落库的爆发会让从库回放跟不上。

## 常见的原因和各自的处理

1. **压测把从库打落后。** 3b 第 0 轮：回放要保持主库的提交顺序，近似串行，每秒只能回放约 87 个事务，100 单/秒起就落后，涨到 553 秒，停压后追了约 36 分钟（[round-0](../notes/3b-rounds/round-0-first-bottleneck.md)）。ADR-0032 把从库的提交放松成 `innodb_flush_log_at_trx_commit=2`、`sync_binlog=0` 解决了这一档（[round-1](../notes/3b-rounds/round-1-replica-flush-and-payment.md)）。压测中响 `MySQLReplicaLagHigh` 是预期，计划内的静默用 `scripts/cluster/monitoring.sh silence 'alertname=MySQLReplicaLagHigh' 2h '压测'`。
2. **手工停过复制没恢复。** 3a 验证 7 做过 `STOP REPLICA SQL_THREAD`（2 分钟后恢复，延迟序列回来）。恢复：`START REPLICA SQL_THREAD;`（用上面第 2 步的 `rsql`）。
3. **节点非正常关机后的从库。** 复制可能照常在跑、数据却少了几个事务（不报错的那一种）。不看复制状态，**一律从主库重建**：[切换手册第三节](../infra/mysql-switchover.md)，删 PVC 先问主人。出事后到重建前的备份不可信。理由见 [ADR-0032](../adr/0032-replica-relaxed-durability.md)。
4. **主从切换做了一半或切换之后。** 清单是按序号写死的：切换后 `-1` 成了主库、没有复制状态，`MetricsMissing` 会为它响；而成了从库的 `-0` 不在清单里，没人盯（见规则的问题，报告里写了）。`middleware.sh mysql` 的 `ANOMALY` 行会指出停在哪步，对照[手册](../infra/mysql-switchover.md)“出了岔子怎么看”。
5. **主键冲突、GTID 对不上让 SQL 线程停住**（上一条里“看得见”的那一种）：第 2 步的错误信息里会有，处理也是重建。
6. **exporter 读不到复制状态、复制被拆掉**（`MetricsMissing`，推断，没有遇到过）：先看 `mysql_up`，再看 `middleware.sh mysql` 是否报 `neither primary nor replicating`。

## 怎么确认恢复了

- 前两条：两个线程状态回到 1，2 分钟内告警消失；`MetricsMissing`：序列回来后 5 分钟的窗口过去；`LagHigh`：延迟回到 30 秒以下。
- `scripts/cluster/monitoring.sh promql 'mysql_slave_status_seconds_behind_source'` 回到 0；`scripts/cluster/middleware.sh mysql` 没有 `ANOMALY`。
- 停过一段时间的，等 CDC 的消费延迟回到 0（`kafka_consumergroup_lag`，组名 `orderindex.cdc-orders`，3a 笔记第 11 节用的就是它）。
- 以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%H%M)`。

## 怎么人为触发它

按[设计 5 节第 2 层](../design/3c-slo-alerting.md)：在 `mysql-order-0-1` 上 `STOP REPLICA SQL_THREAD`（3a 做过），2 分钟后 `SQL` 那条响，同时延迟序列消失；`STOP REPLICA IO_THREAD` 触发 IO 那条；5 分钟后 `MetricsMissing` 不响（线程状态序列还在，这正是“互补”的设计，可顺带验证）。`LagHigh` 要么压测，要么临时把阈值改到 0 以下，并说明这只验证了“路是通的”。`MetricsMissing` 的真实触发待定，建议单元测试加在 `rules/tests/middleware_test.yaml`。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
