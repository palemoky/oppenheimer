# 对账：有差异、没在跑、全量对账变慢

覆盖的告警：`ReconFoundDifferences`（critical）、`ReconNotRunning`（warning）、`ReconDailySlow`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.recon` 组。设计：[3c-slo-alerting.md](../design/3c-slo-alerting.md) 第 3.7 节、第 8 节“`recon` 的 CronJob”“全量对账的耗时”。原理：[ADR-0022](../adr/0022-reconciliation-hash-tree.md)。

## 这条告警在说什么

`recon` 是比对几份应当一致的数据的工具：订单主库对从库、订单对商家索引、订单对支付单、订单对库存预占（热库加冷库）。集群里有两个 CronJob（命名空间 `oppenheimer`，清单 `deploy/cluster/recon/cronjob.yaml`，`make recon-cron`）：`recon-hourly`（每小时第 7 分钟，比最近 2 小时前到 30 分钟前的订单）和 `recon-daily`（UTC 21:30，全量，读冷库）。退出码 0 无差异、1 有复查后仍存在的差异、2 运行出错（连不上库、超时）。`backoffLimit: 0`，不重试。

- **`ReconFoundDifferences`**：某个 CronJob 最近一次失败的 Job 的开始时间，晚于它上次成功的时间（`kube_cronjob_status_last_successful_time`），持续 1 分钟。不直接写 `kube_job_status_failed > 0`：失败的 Job 会保留 5 个，那样人不删就一直响，下一次对上了也不恢复。退出码 1 和 2 在 kube-state-metrics 里分不开，都是 critical，要看日志分。
- **`ReconNotRunning`**：上次成功距今超过 2.5 小时（hourly）或 50 小时（daily），从没成功过时用创建时间顶上，持续 10 分钟。人为暂停的（`kube_cronjob_spec_suspend == 1`）不报。
- **`ReconDailySlow`**：最近开始的那一个 `recon-daily-*` Job 的耗时（完成时间减开始时间）超过 20 分钟，持续 5 分钟。2026-11-22 是 1474.9 万单 1 分 57 秒，约每 100 万单 7.5 秒，时限是 55 分钟，20 分钟就该换成按场景的对账（阶段计划 3e）。

## 影响

`ReconFoundDifferences`：钱、库存、订单三者之一对不上，或者对账没能跑完。是最高一级的告警，当天内要查清；差异的明细在失败的 Job 的日志里。`ReconNotRunning`：对账没在跑，这段时间数据不一致不会被发现，当天看。`ReconDailySlow`：还没有影响，全量对账快撞时限了。

## 先看什么

1. 哪个 CronJob、哪个 Job 失败，退出码：
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get cronjob,job | rg recon`
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
- **退出码 2：连不上库或超时。** 它读四个 MySQL 实例（两个订单主库不只是从库）；库挂了、`recon` 的 Pod 起不来、时限（55 分钟）撞上。处理：看 `describe job` 里的事件和日志。
- **`ReconNotRunning`：CronJob 被暂停后忘了恢复。** `scripts/cluster/services.sh recon-cron-suspend` 之后要 `recon-cron-resume`；规则对暂停的不报，所以这条响的时候多半不是暂停。再看 Job 是不是调度不上、拉不到镜像（`describe job`）。镜像 tag 是装的时候定的，`make deploy` 之后要再 `make recon-cron`，否则一直跑旧的（设计 8 第 3 点）。
- **`ReconDailySlow`：订单量涨了。** 全量耗时线性增长；拐点比事先估的远得多（1 亿单约 13 分钟，线性外推没有实测）。处理：按阶段计划 3e 换成按场景的对账；短期不用动。只看最近的一次：下一次跑快了就恢复（2026-11-23 改的，原来取所有保留的 Job 里最长的）。
- **增量对账的盲区。** 两个 CronJob 只比创建时间在窗口里的订单，看不到很久以前的订单后来被改错（设计 8 第 1 点）；这类差异只有 daily 的全量能发现。
- 对账期间长期被暂停、`oppenheimer-e2e` 空表对照期间对账仍指向正式库：见设计 8 第 4、5 点，正式压测前要先暂停。

## 怎么确认恢复了

- `ReconFoundDifferences`：下一次成功的 Job 让 `kube_cronjob_status_last_successful_time` 晚于失败的开始时间，告警恢复。**手工用 `--from=cronjob/...` 创建的成功 Job 能不能让它恢复没有核对**，保险的做法是等下一个整点，或确认后看 `scripts/cluster/monitoring.sh promql 'oppenheimer:recon_cronjob:last_failed_start_time'` 是否仍大于 `kube_cronjob_status_last_successful_time{namespace="oppenheimer"}`。
- 手工跑一遍，退出码 0、差异 0：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs job/<名字>`。全量：`RECON_FULL=1 RECON_SETTLE=30m scripts/cluster/loadtest.sh recon`（要读 1470 万单，约 2 分钟，数据库会有两分钟的聚合负载）。
- `ReconNotRunning`：`scripts/cluster/monitoring.sh promql 'time() - kube_cronjob_status_last_successful_time{namespace="oppenheimer"}'` 小于 2.5 小时（hourly）、50 小时（daily）。
- 这是涉及订单、库存、钱的所有告警的收尾标准：对账差异为 0。

## 怎么人为触发它

设计第 5 节第 2 层：对账差异，在 `oppenheimer-e2e` 里手工改一行数据（不动长期运行的那一套的库），再对着 e2e 的库跑 `recon`，退出码 1。`ReconNotRunning`：待定，建议：暂停 CronJob 时这条不响，所以用 `kubectl -n oppenheimer patch cronjob recon-hourly -p '{"spec":{"schedule":"0 0 31 2 *"}}'` 让它永远不调度（没有验证过），等 2.5 小时，用完改回 `7 * * * *`（清单是 `deploy/cluster/recon/cronjob.yaml`，以它为准，`make recon-cron` 可恢复）。`ReconDailySlow`：待定，建议临时把阈值改到当前耗时（约 2 分钟）以下，只验证“路是通的”。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
