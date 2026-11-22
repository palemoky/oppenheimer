# MySQL 备份没有成功

覆盖的告警：`MySQLBackupStale`（warning）

## 这条告警在说什么

命名空间 `mysql` 里名字以 `-backup` 结尾的 CronJob（`mysql-order-0-backup`、`mysql-order-1-backup`、`mysql-main-backup`），满足下面两种之一，持续 10 分钟（`for: 10m`）：

- 上次成功（`kube_cronjob_status_last_successful_time`）距今超过 26 小时：每天一次，留 2 小时余量；
- 从来没成功过（没有这个序列）：改用 CronJob 的创建时间（`kube_cronjob_created`）距今超过 26 小时。

暂停的 CronJob（`kube_cronjob_spec_suspend == 1`）不报。排期是 UTC 19:00、19:30、20:00 错开（[2b 笔记第 11 节](../notes/2b-middleware.md)）。出处：[设计 4.4](../design/3c-slo-alerting.md)；2b 笔记里写着“备份失败没有告警，阶段 3 接监控时加”，就是这条。

## 影响

- 用户看不到任何东西，系统照常运行。
- 数据风险：备份是丢数据之后唯一的退路。备份是逻辑备份（`mysqldump`），binlog 没有归档，**只能恢复到备份的那一刻**，之后的数据会丢。主库和从库一起坏时，没有备份就没法恢复。
- 26 小时意味着已经错过了一次，当天处理；越久，能恢复到的时间点越旧。

## 先看什么

1. 哪个 CronJob、多久没成功：

   ```
   scripts/cluster/monitoring.sh promql 'time() - kube_cronjob_status_last_successful_time{namespace="mysql",cronjob=~".*-backup"}'
   scripts/cluster/monitoring.sh promql 'kube_cronjob_spec_suspend{namespace="mysql"}'
   ```
2. 最近的 Job 是失败还是根本没起：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n mysql get cronjob,job,pods -l app.kubernetes.io/name=mysql-backup
   ```
3. 失败 Job 的日志和退出原因（`OOMKilled` 在 `describe` 里）：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n mysql logs job/<失败的 Job 名> --tail=50
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n mysql describe pod <Pod 名> | rg -A6 'Last State|Events'
   ```
4. 从库复制是不是停了（备份脚本会拒绝备份复制断了的从库）：见 [mysql-replication.md](mysql-replication.md)；`scripts/cluster/monitoring.sh promql 'mysql_slave_status_replica_sql_running'`。
5. NAS 上现在有哪些备份：`scripts/cluster/middleware.sh mysql-backup-list`。

## 常见的原因和各自的处理

1. **备份 Job 被 OOMKilled。** 订单库灌数之后，输出文件在 NFS 上的页缓存把内存撑满，Job 被杀；内存提到 1 Gi 后三个实例备份成功（[PROGRESS](../PROGRESS.md) 里 3b 灌数那一段，设计 3b 第 6 节）。数据继续长大后可能再来。处理：加大 Job 的内存（`deploy/charts/mysql/values.yaml` 的备份资源），`scripts/cluster/middleware.sh mysql` 下发。
2. **失败的手工备份 Job 留着没人看。** 2026-11-18 两个手工 Job 失败后留了四天没人知道（`mysql-order-0-backup-manual-261118173325` 等，设计 3c 第 1.1 节）；那就是 3c 要加这条告警的由头之一。失败的 Job 不会自己消失：看完日志再 `kubectl -n mysql delete job <名>`。
3. **从库复制断了，备份脚本拒绝执行。** `backup.sh` 里 `SHOW REPLICA STATUS` 为空或两个线程不是 `Yes` 就直接失败，宁可没有备份也不要一份假的新备份（2b 笔记 11.2）。先修复制，再手工补一份。从节点非正常关机之后没重建的从库备出来的备份不可信（[ADR-0032](../adr/0032-replica-relaxed-durability.md)）。
4. **NAS 掉线或 NFS 写不进去。** 备份写在 NFS 的 PV 上（`deploy/cluster/mysql/backup-pv.yaml`），先写 `.tmp-*` 再改名，半截的不会被当成备份。Job 的日志里会是写文件或挂载失败；`describe pod` 的 Events 里有挂载错误。NAS 本身的恢复不在这里。
5. **CronJob 被暂停但忘了恢复**（推断，没有遇到过）：被暂停的不报警，所以这条不会提醒；第 1 步的第二个查询是 1 就是它。
6. **备份成功了但告警没消**（推断，没有验证过）：手工触发的 Job（`create job --from=cronjob/...`）是否刷新 `kube_cronjob_status_last_successful_time` 没有验证过。恢复之后如果指标没动，等下一次 19:00 的定时备份，或者检查那个 Job 是否带了 CronJob 的 ownerReference。

## 怎么确认恢复了

- 手工补一份并等它完成：`scripts/cluster/middleware.sh mysql-backup [mysql-order-0]`（不带参数是三个实例），输出里有 `backup ok`；`mysql-backup-list` 里多了一份新时间戳。
- `time() - kube_cronjob_status_last_successful_time{namespace="mysql",cronjob=~".*-backup"}` 回到 26 小时以下（见上，第 6 条的限定）；告警在下一个求值周期消失。
- 内容是否完整（退出码为 0 不说明内容完整）：用恢复演练 `scripts/cluster/mysql-restore-drill.sh` 比对。服务在跑时演练会报不一致（PROGRESS 里记过），以对账为准：

  ```
  KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%H%M)
  ```

## 怎么人为触发它

设计 5 节第 2 层没有为这条写办法。真实触发要让备份停 26 小时，不现实（待定）。建议：把规则里的 `26 * 3600` 临时改成比当前“距今”更小的数（比如 `3600`），下发后 10 分钟响，再改回去，并说明这只验证了“路是通的”；“从来没成功过”那半靠单元测试（`rules/tests/middleware_test.yaml`）。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人，不要为了测告警删 NAS 上的备份。

## 历史

（还没有真的响过）
