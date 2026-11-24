# 对账：有差异、没跑成、没在跑、全量对账变慢

覆盖的告警：`ReconFoundDifferences`（critical）、`ReconRunFailed`（warning）、`ReconNotRunning`（warning）、`ReconDailySlow`（warning）

常驻的三个核对各有自己的 runbook：钱 [payment-totals.md](payment-totals.md)，库存 [recon-stock.md](recon-stock.md)，派生数据的变更 [recon-changes.md](recon-changes.md)。

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.recon` 组。设计：[3c-slo-alerting.md](../design/3c-slo-alerting.md) 第 3.7 节、第 8 节“`recon` 的 CronJob”“全量对账的耗时”。原理：[ADR-0022](../adr/0022-reconciliation-hash-tree.md)。

## 这条告警在说什么

`recon` 是比对几份应当一致的数据的工具：订单主库对从库、订单对商家索引、订单对支付单、订单对库存预占（热库加冷库）。集群里有两个 CronJob（命名空间 `oppenheimer`，清单 `deploy/cluster/recon/cronjob.yaml`，`make recon-cron`）：`recon-hourly`（每小时第 7 分钟，比最近 2 小时前到 30 分钟前的订单）和 `recon-daily`（UTC 21:30，全量，读冷库）。退出码 0 无差异、1 有复查后仍存在的差异、2 运行出错（连不上库、超时）。两种失败分开处理（2026-11-23 改，原来是 `backoffLimit: 0`、都不重试、告警分不开）：`backoffLimit: 2` 加 `podFailurePolicy`，退出码 1 走 `FailJob`，不重试，Job 立刻失败，失败原因是 `PodFailurePolicy`；其余的（退出码 2、被杀、panic）重试 2 次，间隔 10 秒、20 秒，耗尽后失败原因是 `BackoffLimitExceeded`。退出码 1 不重试，是因为重跑一次也许就“好了”（在途的状态变了），差异会被盖住；对账只读，别的失败重跑没有副作用。

- **`ReconFoundDifferences`**（critical）：只在“有确认的差异”（退出码 1）时响。某个 CronJob 最近一次这种失败的 Job 的开始时间，晚于它上次成功的时间（`kube_cronjob_status_last_successful_time`），持续 1 分钟。规则按 kube-state-metrics 的 `kube_job_status_failed{reason=...}` 分：Job 失败了（`kube_job_failed{condition="true"}`），并且原因不是 `BackoffLimitExceeded`、`DeadlineExceeded`、`Evicted` 的，归这一条。所以认不出原因的失败也一律按“有差异”报（宁可错报成重的）。不直接看失败的 Job 在不在：失败的 Job 会保留 5 个，那样人不删就一直响，下一次对上了也不恢复。
- **`ReconRunFailed`**（warning）：没跑成：退出码 2（连不上库、超时）重试 2 次仍然失败，也包括撞上 Job 的时限（`DeadlineExceeded`）、被驱逐（`Evicted`）。即最近一次失败的原因是这三种之一，之后还没有成功过，持续 1 分钟。这一次对账没有结论，不代表有差异。重试途中（失败的 Pod 数已经大于 0，但 Job 还没失败）什么都不响。一直跑不成由 `ReconNotRunning`（2.5 小时）接着报。
- **`ReconNotRunning`**：上次成功距今超过 2.5 小时（hourly）或 50 小时（daily），从没成功过时用创建时间顶上，持续 10 分钟。人为暂停的（`kube_cronjob_spec_suspend == 1`）不报。
- **`ReconDailySlow`**：最近开始的那一个 `recon-daily-*` Job 的耗时（完成时间减开始时间）超过 20 分钟，持续 5 分钟。2026-11-22 是 1474.9 万单 1 分 57 秒，约每 100 万单 7.5 秒，时限是 55 分钟，20 分钟就该换成按场景的对账（阶段计划 3e）。

## 影响

`ReconFoundDifferences`：钱、库存、订单三者之一对不上。是最高一级的告警，当天内要查清；差异的明细在失败的 Job 的日志里。`ReconRunFailed`：对账没能跑完，这一个周期没有结论，数据没有被证明不一致，只是没人看着；当天看，修好之后手工复跑一次。`ReconNotRunning`：对账没在跑，这段时间数据不一致不会被发现，当天看。`ReconDailySlow`：还没有影响，全量对账快撞时限了。

## 先看什么

1. 哪个 CronJob、哪个 Job 失败，失败的原因（`PodFailurePolicy` 是有差异，`BackoffLimitExceeded` 等是没跑成）：
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get cronjob,job | rg recon`
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get job <名字> -o jsonpath='{.status.conditions[?(@.type=="Failed")].reason}'`
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer describe job <失败的 Job 名>`
2. 日志里的差异明细（退出码 1）或错误（退出码 2）：
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs job/<名字>`；Loki：`{service_name="recon"} | json | level=~"WARN|ERROR"`（服务名标签没有核对，Job 刚跑完时 Loki 里可能还没有，以 kubectl 为准）。
3. 先排除“还没追上”：消费者停住或从库落后超过 30 秒时，`recon` 会把还没追上的报成差异（设计 8 第 2 点）。
   `scripts/cluster/monitoring.sh promql 'sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup=~"order\\.order-submit|order\\.payment-events|inventory\\.order-events|payment\\.order-events|orderindex\\.cdc-orders"})'`
   `scripts/cluster/monitoring.sh alerts`（看 `MySQLReplicaLagHigh`、`ConsumerBlocked`、`CDCLagging` 有没有在响）。
4. 钱的那一对看实时核对：`scripts/cluster/monitoring.sh promql 'max by (kind) (recon_totals_mismatch{k8s_namespace_name="oppenheimer"})'`（见 [payment-totals.md](payment-totals.md)）。
5. 手工复跑确认差异还在：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`，再 `logs job/<名字> -f`。要只看一对或指定范围用 `make recon RECON_ARGS="-pairs index -since 26h"`。

## 常见的原因和各自的处理

- **还没追上，不是数据坏了。** 积压没清、从库落后、CDC 落后时的差异，等追平再跑一遍（设计 8 第 2 点）。处理：先处理 [consumer-blocked.md](consumer-blocked.md)、[cdc.md](cdc.md) 的告警，再复跑。
- **商家索引的差异。** 这是唯一可以自动修复的一对（ADR-0022 决定 7）。CDC 落后或停过时会有差异（推断，没有遇到过）。看 [cdc.md](cdc.md)。
- **已受理的订单没落库被回收**（round-1 第 6 节的 608 个，`verify` 第 15 条）：账是对的、用户看到订单消失。看 [order-backlog.md](order-backlog.md)。
- **库存预占对不上。** 认领之后消息进死信的占用不会自动归还（1b 记录的已知缺口），看 [consumer-dead-letter.md](consumer-dead-letter.md)。
- **`ReconRunFailed`：退出码 2，连不上库或超时。** 它读四个 MySQL 实例（两个订单主库不只是从库）；库挂了、`recon` 的 Pod 起不来、时限（55 分钟）撞上、Pod 被驱逐。Job 已经重试过 2 次，所以不是“某个库连不上半分钟”，要看是哪个库一直连不上。处理：看 `describe job` 里的事件和日志，修好之后手工复跑一次（见下）。
- **`ReconNotRunning`：CronJob 被暂停后忘了恢复。** `scripts/cluster/services.sh recon-cron-suspend` 之后要 `recon-cron-resume`；规则对暂停的不报，所以这条响的时候多半不是暂停。再看 Job 是不是调度不上、拉不到镜像（`describe job`）。镜像 tag 是装的时候定的，`make deploy` 之后要再 `make recon-cron`，否则一直跑旧的（设计 8 第 3 点）。
- **`ReconDailySlow`：订单量涨了。** 全量耗时线性增长；拐点比事先估的远得多（1 亿单约 13 分钟，线性外推没有实测）。处理：按阶段计划 3e 换成按场景的对账；短期不用动。只看最近的一次：下一次跑快了就恢复（2026-11-23 改的，原来取所有保留的 Job 里最长的）。
- **增量对账的盲区。** 两个 CronJob 只比创建时间在窗口里的订单，看不到很久以前的订单后来被改错（设计 8 第 1 点）；这类差异只有 daily 的全量能发现。
- 对账期间长期被暂停、`oppenheimer-e2e` 空表对照期间对账仍指向正式库：见设计 8 第 4、5 点，正式压测前要先暂停。

## 怎么确认恢复了

- `ReconFoundDifferences`、`ReconRunFailed`：下一次成功的 Job 让 `kube_cronjob_status_last_successful_time` 晚于失败的开始时间，告警恢复（两条都是）。手工用 `--from=cronjob/...` 创建的 Job 成功了也算（2026-11-23 实测：它带着 CronJob 的属主引用，`lastSuccessfulTime` 会更新，一分多钟后告警恢复），不用等下一个整点。
- 失败的那个 Job 留着的话，chart 自带的 `KubeJobFailed`（warning）15 分钟后会响，直到它被删掉或者被 `failedJobsHistoryLimit`（5）挤掉。看完、把日志和退出码记到下面的“历史”里之后，由主人决定删不删（`kubectl -n oppenheimer delete job <名字>`）。
- 手工复跑的窗口上界是“现在往前 30 分钟”：故障刚结束就跑，故障那一段的订单不在窗口里。要盖住故障时段，等故障结束 30 分钟以后再跑。
- 手工跑一遍，退出码 0、差异 0：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs job/<名字>`。全量：`RECON_FULL=1 RECON_SETTLE=30m scripts/cluster/loadtest.sh recon`（要读 1470 万单，约 2 分钟，数据库会有两分钟的聚合负载）。
- `ReconNotRunning`：`scripts/cluster/monitoring.sh promql 'time() - kube_cronjob_status_last_successful_time{namespace="oppenheimer"}'` 小于 2.5 小时（hourly）、50 小时（daily）。
- 这是涉及订单、库存、钱的所有告警的收尾标准：对账差异为 0。

## 怎么人为触发它

设计第 5 节第 2 层：对账差异，在 `oppenheimer-e2e` 里手工改一行数据（不动长期运行的那一套的库），再对着 e2e 的库跑 `recon`，退出码 1。做过一次（2026-11-23，设计第 8 节“第 15 项”）：`CHAOS_DELETE=0 CHAOS_DURATION=30s E2E_KEEP=1 scripts/cluster/chaos.sh` 留下现场，用 root 在 `mysql-main-0` 上改 `e2e_payment_db.payments` 或 `e2e_merchant_index_db_*` 里的一行，再 `kubectl -n oppenheimer-e2e exec <chaos 的 Pod> -c chaos -- recon -pairs payment,stock,replica,index -settle 0 -recheck 5s`；用完 `scripts/cluster/services.sh e2e-down`。这只验证 `recon` 报得出来，告警本身看的是 `oppenheimer` 命名空间的 CronJob，不会响。`ReconRunFailed`（2026-11-23 实测过）：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly <名字> --dry-run=client -o json`，把环境变量 `MYSQL_REPLICA_DSNS` 里的一个端口改成连不上的（3306 改成 3399），再 `kubectl apply`；Job 重试 2 次（约 1 分钟）后失败，再过约 1 分半告警响（`ReconFoundDifferences` 不响）；之后 `create job --from=cronjob/recon-hourly` 跑一次成功的就恢复，再删掉那个失败的 Job。`ReconNotRunning`：待定，建议：暂停 CronJob 时这条不响，所以用 `kubectl -n oppenheimer patch cronjob recon-hourly -p '{"spec":{"schedule":"0 0 31 2 *"}}'` 让它永远不调度（没有验证过），等 2.5 小时，用完改回 `7 * * * *`（清单是 `deploy/cluster/recon/cronjob.yaml`，以它为准，`make recon-cron` 可恢复）。`ReconDailySlow`：待定，建议临时把阈值改到当前耗时（约 2 分钟）以下，只验证“路是通的”。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

- 2026-11-22 19:07 UTC，`ReconFoundDifferences`（critical，发到了 Telegram）：**不是差异，是连不上库**。告警质量检查的第 8 项在 19:06 停了 `mysql-order-0-1` 的 IO 线程，从库的就绪探针失败、被从 `mysql-order-0-replica` 这个 Service 里摘掉，19:07 的 `recon-hourly`（Job `recon-hourly-29922907`）连它被拒绝，退出码 2（`connect order_replica_0: ... connection refused`）。19:12 恢复复制，19:15 手工复跑差异 0，告警一分多钟后恢复。教训两条：故障注入不要压在每小时的第 7 分钟上；这条告警的名字说的是“有差异”，但退出码 2（没跑成）也会让它响，看到它先看退出码（2026-11-23 已拆开：现在没跑成的归 `ReconRunFailed`）。
- 2026-11-23 07:20 UTC，`ReconRunFailed` 第一次真实触发（告警质量检查第 17 项，人为的）：Job `recon-inject17-1795418426` 三个 Pod 都是 `connect order_replica_0: ... i/o timeout`，07:21:23 以 `BackoffLimitExceeded` 失败，07:22 进 `pending`，07:22:52 `firing`，07:23:22 送到 alertsink（warning，没有发 Telegram）；`ReconFoundDifferences` 全程没有进 `pending`。07:24 手工跑的一次差异 0，约 1 分钟后告警恢复，失败的 Job 已删。
