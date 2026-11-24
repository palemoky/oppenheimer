# 派生数据的变更核对：对不上、落后、核对停了

覆盖的告警：`DerivedDataMismatch`（critical）、`DerivedDataLagging`（warning）、`DerivedDataCheckStalled`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.recon_changes` 组。设计和规则原理：[3e-reconciliation-by-scenario.md](../design/3e-reconciliation-by-scenario.md) 第 4.3 节、[ADR-0039](../adr/0039-reconciliation-by-scenario.md)；代码 `services/recon/internal/changes`（规则在包注释里）。

## 这条告警在说什么

`recon-changes`（Deployment，命名空间 `oppenheimer`，`make recon-changes` 装）用自己的消费组（`recon.cdc-orders`，和商家索引的消费者读同一个 topic，位点互不影响）读订单的行变更。每条变更提交满 10 秒（`-delay`）之后，去派生端按主键读那一行，用行上递增的 `version` 判定。设变更的版本是 v，派生端那一行的版本是 d：

- `d > v`：派生端已经是更晚的版本，通过（更晚的那个版本自己有一条变更，会被单独核对）。
- `d = v`，内容相同：一致，通过。
- `d = v`，内容不同：**不一致（`mismatch`）**，不会自己好，第一次看到就报。
- `d < v` 或者没有这一行：**还没追上**，每 5 秒复查；第一次核对之后过了 2 分钟（`-lag-after`）仍然如此才计一次**落后（`lagging`）**。

派生端有三个：`index`（商家索引，只看有商家的订单）、`replica`（订单从库）、`archive`（冷库，只看 DELETE）。

- **`DerivedDataMismatch`**：`recon_changes_checked_total{result="mismatch"}` 在 5 分钟里有增加，按 `target` 分，没有 `for`。
- **`DerivedDataLagging`**：`result="lagging"` 在 5 分钟里有增加，按 `target` 分，持续 2 分钟。
- **`DerivedDataCheckStalled`**：`recon_changes_verified_through`（核对到的提交时刻，Unix 秒）落后现在超过 5 分钟，**并且**订单库这段时间有写入，持续 5 分钟；没有 `recon_changes_verified_through` 序列而订单库有写入时也响。

“订单库有写入”用 `order_transitions_total` 的 5 分钟增量近似，**不是 binlog 本身**：只有订单状态流转才计数，所以只有行变更而没有状态流转（比如不改状态的更新）时这条会漏响。没有验证过哪些写入会产生 CDC 变更而不计入它。

指标名是照 `recon_totals_*` 的导出方式类推的（点变下划线、计数器加 `_total`、没有单位后缀），**没有在 Prometheus 上核对过**。

## `lagging` 和 `mismatch` 的区别

- `lagging` 是**时间问题**：派生端的版本比变更的旧，或者还没有这一行。CDC、从库复制、索引的消费者任何一环慢都会有。追上了就没事；2 分钟还没追上才计数。它是 warning，往往和 `CDCLagging`、`MySQLReplicaLagHigh`、`ConsumerBlocked` 一起响，先处理那些。
- `mismatch` 是**数据问题**：版本号相同、内容不同。慢不会造成它，等也等不好。它是 critical。
- 看不到的差别：`mismatch` 的计数只在核对那一刻增加一次，5 分钟的窗口滑过去告警就恢复，**恢复不代表那一行修好了**。响过就要按日志里的订单号去查，不要等它自己好。

## 差异是 0 和核对没在跑，不是一回事

- 差异是 0：`recon_changes_checked_total` 在涨（`result="ok"` 有增长），**并且** `recon_changes_verified_through` 在跟着订单库的写入往前走。
- 核对没在跑：`mismatch` 和 `lagging` 的计数不增加，和“没有差异”看起来一样。分辨它：
  `scripts/cluster/monitoring.sh promql 'time() - max(recon_changes_verified_through{k8s_namespace_name="oppenheimer"})'`（落后现在多少秒；没有结果是没有这个序列）
  `scripts/cluster/monitoring.sh promql 'sum by (target, result) (increase(recon_changes_checked_total{k8s_namespace_name="oppenheimer"}[10m]))'`
  `scripts/cluster/monitoring.sh promql 'max(recon_changes_pending{k8s_namespace_name="oppenheimer"})'`（等着复查的条数；一直很大说明派生端一直没追上）
- 订单库没有写入时 `verified_through` 本来就不动，所以 `DerivedDataCheckStalled` 要求订单库有写入；探针一直在下单，平时总是有。

## 它看不到的三样

这个核对只回答“CDC 里的每一条订单变更，派生端有没有跟上”，所以：

1. **不经过 binlog 的改动**：有人直接改了派生端的一行（比如手工改了商家索引或从库上的数据）。源库没有变更，也就没有要核对的那一条。
2. **CDC 自己漏掉或者搞错的变更**：它和商家索引的消费者读的是同一个 topic，CDC 漏发的变更两边都收不到，都不知道。
3. **不在 CDC 里的表**：只核对 `orders` 这一张表（`SourceTable`），别的表的变更忽略。

这三样靠逐行的树（`recon` 的每小时窗口和每天全量，[recon.md](recon.md)）。另外：等着复查的变更只在内存里，位点在第一次核对之后就提交了，进程重启时正在等的那些不会再被核对，重启前后那一小段也由逐行对账盖住。`DerivedDataMismatch`、`DerivedDataLagging` 不响，不等于派生端和订单库是一致的。

## 影响

`DerivedDataMismatch`：某个派生端上有一行和订单库对不上：`index` 是商家看到的订单列表错了，`replica` 是读从库的接口返回错的数据，`archive` 是冷库里的订单不对。现在就要查，当天内查清。`DerivedDataLagging`：派生端比订单库旧，商家看到的订单、从库读到的订单是旧的，数据没有坏。`DerivedDataCheckStalled`：“没有不一致”不再是证据，每小时的逐行对账还在兜底，当天看。

## 先看什么

1. 哪个派生端、什么结果：
   `scripts/cluster/monitoring.sh promql 'sum by (target, result) (increase(recon_changes_checked_total{k8s_namespace_name="oppenheimer"}[10m]))'`
2. 是哪一行、哪一列：每条不一致和每条落后各打一条日志，带订单号和两边各自的值：
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs deploy/recon-changes --tail=100`
   Loki：`{service_name="recon"} | json | level=~"WARN|ERROR"`（服务名标签没有核对，日志的措辞以代码为准）。
3. 核对本身在跑吗：上一节的三条查询，加 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide | rg recon-changes`；内部端口 `/healthz`、`/readyz` 见下。
4. 上游的告警：
   `scripts/cluster/monitoring.sh alerts`（`CDCLagging`、`CDCLaggingTooLong`、`MySQLReplicaLagHigh`、`MySQLReplicaSQLThreadStopped`、`ConsumerBlocked`、`ReconNotRunning`）。
5. `recon-changes` 自己的消费者在不在、落后多少：
   `scripts/cluster/monitoring.sh promql 'max by (service_name, messaging_consumer_group_name, messaging_destination_name) (kafkax_consumer_oldest_pending_age_seconds{k8s_namespace_name="oppenheimer"})'`
   `scripts/cluster/monitoring.sh promql 'kafka_consumergroup_lag{consumergroup="recon.cdc-orders"}'`（消费组名按 `api/eventx` 的 `GroupReconCDCOrders`，没有前缀；没有核对过 Kafka Exporter 里有没有它）。
6. 开关是不是被关了：`kubectl -n oppenheimer port-forward deploy/recon-changes 8086:8086`，再 `curl -s localhost:8086/degrade`（没有验证过这条命令的输出）。
7. 要定位到行再跑逐行对账：`make recon RECON_ARGS="-pairs index -since 2h"`，或 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）。

## 常见的原因和各自的处理

`DerivedDataMismatch`、`DerivedDataLagging` 2026-11-24 在 `oppenheimer-e2e` 里真实触发过，`DerivedDataCheckStalled` 没有（见最后一节）；下面的原因除标明的外仍是推断。

- **`DerivedDataLagging` 单独响，`target=index`**：CDC 落后，或者 orderindex 的消费者停了、堵了。处理：看 [cdc.md](cdc.md)、[consumer-blocked.md](consumer-blocked.md)。
- **`DerivedDataLagging`，`target=replica`**：从库复制落后或 SQL 线程停了。处理：看 [mysql-replication.md](mysql-replication.md)。
- **`DerivedDataLagging`，`target=archive`**：归档任务比变更慢，或者冷库连不上（连不上时核对停在这一批上，不会计 lagging，见下）。
- **`DerivedDataMismatch`**：同一个版本两边内容不同，不是延迟。可能的来源：商家索引的映射有缺陷、从库上有人写了数据、冷库的归档把行拷坏了、同一个订单有两条版本号相同的变更（版本号的假设被破坏）。处理：按日志里的订单号，同时读订单库、派生端那一行和变更里的映像，判断是哪一边错；`index` 可以用 `recon` 自动修复（ADR-0022 决定 7），其余的不能自动修。
- **派生端读不到（连不上、超时）时核对停在这一批上等它恢复**，不往前走，也不把读不到当成不一致：这时 `verified_through` 不动，是 `DerivedDataCheckStalled`，不是 `lagging`。
- **开关 `changes_check` 被关了**：关着时只消费、不查库，位点照常往前走，`verified_through` 不动，也会让 `DerivedDataCheckStalled` 响。这是有意的：核对被关了，“没有不一致”就不是证据。**计划内关掉要加静默**。
- **`recon-changes` 没有在跑**：Pod 不在、连不上 Kafka、连不上派生端（启动时三个库是必需的，连不上就启动失败），或指标送不出。`ServiceMetricsMissing` 不覆盖 recon，只能靠 `DerivedDataCheckStalled`。
- **CDC 自己停了**：`verified_through` 也不动，`CDCLagging` 会一起响；这条是同一件事的另一面。
- **消息解不开**：计入 `target="none"、result="skipped"`，打日志、跳过，不挡住分区；这类消息商家索引的消费者会报。
- **核对的 Pod 重启后旧序列还留 5 分钟**：规则已用 `max`、`sum by (target)` 处理。
- 镜像 tag 是工作区的，提交之后要再 `make recon-changes`。
- 压测时每条变更几次主键点查加在从库和索引库上的负载、`recon_changes_pending` 能涨到多大，没有验证过。

### 关掉核对的开关

核对每条变更要对从库和索引库做几次主键点查，过载时可以关掉（和 [degrade.md](degrade.md) 里的开关同一套机制，开关名 `changes_check`）。内部端口 8086 没有 Service、没有鉴权，只在 Pod 里和集群里可达：

```
KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer port-forward deploy/recon-changes 8086:8086
curl -X PUT 'localhost:8086/degrade/changes_check?mode=off'     # 关
curl -X PUT 'localhost:8086/degrade/changes_check?mode=auto'    # 开回来
curl localhost:8086/degrade                                       # 看现在的值
```

这几条命令是照 `watch_changes.go` 的说明和 degrade.md 写的，**没有验证过**（`scripts/cluster/degrade.sh` 是给六个业务服务写的，没有验证过它认不认 recon-changes）。开关只在本进程生效、不持久化：重启回到环境变量 `DEGRADE` 的值（清单里没设，所以是 `auto`）。想让它重启后仍然关着，要在 `deploy/cluster/recon/changes.yaml` 里加 `DEGRADE=changes_check=off`，并 `make recon-changes`。关掉之后位点照常前进，所以**开回来之后关着的这段时间里的变更不会被补核对**，那段由逐行对账盖住。关着时加静默：`scripts/cluster/monitoring.sh silence 'alertname=DerivedDataCheckStalled' 2h '<原因>'`。

## 怎么确认恢复了

- `DerivedDataMismatch`、`DerivedDataLagging` 的计数在 5 分钟里不再增加；同时 `recon_changes_checked_total{result="ok"}` 在涨、`recon_changes_verified_through` 回到贴近现在，不然只是核对没在跑。
- `recon_changes_pending` 回到接近 0。
- `DerivedDataMismatch` 自己恢复不算数：确认出问题的那一行（日志里的订单号）两边已经一致，再跑逐行对账。
- 最后以对账差异为 0 收尾：`make recon-check FROM=<RFC3339> TO=<RFC3339>`（2026-11-24 在长期运行的那一套上跑过），或 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`，退出码 0。

## 怎么人为触发它

设计第 5 节第 2 层只列了“对账差异：在 `oppenheimer-e2e` 里手工改一行数据”。**2026-11-24 验证过两条**，下面“待定，建议”的做法有出入，以这一段为准。做法：`E2E_KEEP=1 make cluster-e2e` 起一套，把三个核对的清单改成指向 `e2e_*` 库（`recon-changes` 另加 `KAFKA_PREFIX=e2e-<运行号>.`）装进 `oppenheimer-e2e`；正式的规则只看 `oppenheimer`，所以另下发一份把命名空间换成 `oppenheimer-e2e`、每条加标签 `env: e2e` 的副本，先加静默 `env=e2e`；做完 `services.sh e2e-down`、删掉那份规则。细节在设计 3e 第 9 节“第 6 步：造差异验证”。

  - **怎么让它重新核对一行**：它只在有变更时核对。不用去动源库：把实例缩到 0，删掉它的消费组（`kafka-consumer-groups.sh --delete --group <前缀>recon.cdc-orders`），再起来，它从头重放整个 topic，等于把每个订单核对一遍。
  - `DerivedDataMismatch`：改商家索引里一行的 `status`、`src_version` 不动，重放后日志报 `column status: derived 4, change 5`，`target="index"` 立刻 firing。冷库：归档的 e2e 用例收尾时会删冷库里的行，热库的删除事件还在 topic 里，实例一起来就报了 6 条 `archived row not found in the cold database`，`target="archive"` firing。**从库那一个目标没有造过不一致。**
  - `DerivedDataLagging`：删掉商家索引里的一行，重放后先等着复查，2 分钟（`-lag-after`）后日志报 `derived row not found`，`target="index"` firing。**不要用停从库复制的办法**：从库的实例是和长期运行的那一套共用的。
  - 这两条在最后一次计数之后 5 分钟自己恢复，和那一行修没修无关（上面“已知的不足”）。
  - `DerivedDataCheckStalled`：**没有触发过**。实例缩到 0 之后规则的前一半成立（没有 `verified_through`），但它还要求“订单库有写入”，e2e 的测试跑完后没有流量。登记在 `docs/plan/phase-4-chaos.md` 的“4a 准备”。


- `DerivedDataMismatch`：待定，建议在 `oppenheimer-e2e` 里直接改商家索引（或从库）上某个订单的一列，再让这个订单有一次新的变更（版本号变成和你改的那一行相同的值不容易构造，先试：改列之后在订单库里对同一订单做一次状态流转，派生端若没有被覆盖就会报）。`recon-changes` 要指向 e2e 的库和 topic 前缀（`KAFKA_PREFIX`），现在清单对着正式的，要另起一个实例。
- `DerivedDataLagging`：停掉 orderindex 的消费者（或者在 e2e 里停从库的 SQL 线程），同时订单有写入，等 2 分钟（`-lag-after`）加 2 分钟。
- `DerivedDataCheckStalled`：`kubectl -n oppenheimer scale deploy/recon-changes --replicas=0`，5 分钟加 5 分钟后响（探针一直在下单，所以订单库有写入），用完缩回 1；或者按上一节关掉开关 `changes_check`。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
