# CDC 落后，或管道没有在报延迟

覆盖的告警：`CDCLagging`（warning）、`CDCLaggingTooLong`（critical）、`CDCPipelineMissing`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.pipeline` 组。出处：[ADR-0014](../adr/0014-cdc-merchant-index.md)、[1c 设计](../design/1c-sharding.md) 4.5、4.7、[round-9](../notes/3b-rounds/round-9-soak.md) 3.4 节、[3a 笔记](../notes/3a-observability.md)。

## 这条告警在说什么

cdc 服务（Deployment `cdc`，命名空间 `oppenheimer`，1 个副本）从订单从库读 binlog，把变更发到 Kafka 的 `cdc.order.orders`，`orderindex` 消费它建商家维度的订单索引。每个订单分片一条管道（现在是 `order-0`、`order-1`），位点是 GTID 集合，存在 Kafka 的压缩 topic 里。指标 `cdc_lag_ms` 量的是“最后一条处理的变更距它发生有多久”。

- **`CDCLagging`**：`max by (pipeline) (cdc_lag_ms) / 1000` 超过 60（秒），持续 5 分钟。除以 1000 是为了让通知里的值是秒，写成“落后 6m 5s”这种（原来是“365.4k 毫秒”）。
- **`CDCLaggingTooLong`**：同样按秒，超过 3600（1 小时），持续 5 分钟（critical），通知写“落后 …，binlog 只保留 1 天”。binlog 只留 1 天（2026-11-22 起，round-9 3.4），再落后下去位点对应的 binlog 会被源库清掉，那之后只能重做全量。
- **`CDCPipelineMissing`**：`order-0` 或 `order-1` 的 `cdc_lag_ms` 序列消失，持续 10 分钟（`for`）。通知写“至少 15 分钟没有报延迟”：cdc 停了之后序列还要约 5 分钟才从查询结果里消失（指标是推上来的，Prometheus 对不再更新的序列回看 5 分钟），`absent()` 那时才成立，再加 10 分钟（实测从停掉到通知约 16 分钟）。cdc 挂了、这条管道退出了或指标送不出来。延迟的规则这时没有值不会响，所以按管道清单显式查；订单库加分片时这里要跟着加。

这个指标在源库没有写入时一直涨，直接设阈值会在空闲时误报；探针每秒一单保证了两个分片一直有写入，所以前两条依赖探针（探针停了见 [probe.md](probe.md)）。设计第 2 节第 7 项：没有做源库心跳，因为探针已经解决了它要解决的问题。

## 影响

商家查到的订单列表是旧的，买家一侧不受影响（下单、支付、查单都不走索引）。`CDCLagging` 当天看。`CDCLaggingTooLong`：不处理的话位点被清掉后要停下来重做全量（读全表），要在 binlog 过期之前处理（1 天内）。`CDCPipelineMissing`：索引停在它停下的那一刻。

## 先看什么

1. 哪条管道、落后多少：
   `scripts/cluster/monitoring.sh promql 'max by (pipeline) (cdc_lag_ms{k8s_namespace_name="oppenheimer"})'`
2. cdc 的 Pod 和日志：
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide | rg cdc`
   `{service_name="cdc"} | json | level=~"WARN|ERROR"`
3. 下游有没有积压（`orderindex.cdc-orders`）：
   `scripts/cluster/monitoring.sh promql 'sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup="orderindex.cdc-orders"})'`
   `scripts/cluster/monitoring.sh promql 'max by (service_name, messaging_consumer_group_name, messaging_destination_name) (kafkax_consumer_oldest_pending_age_seconds{k8s_namespace_name="oppenheimer",service_name="orderindex"})'`
4. 源库（订单从库）和复制：`scripts/cluster/monitoring.sh alerts` 里有没有 `MySQLReplicaSQLThreadStopped`、`MySQLReplicaIOThreadStopped`、`MySQLReplicaLagHigh`、`MySQLDown`；cdc 读的就是从库，从库重启时 cdc 会重连几次（round-9 3.4）。
5. 源库有写入吗（空闲会让延迟虚高）：探针在跑吗：`scripts/cluster/monitoring.sh promql 'sum(rate(probe_cycles_total{k8s_namespace_name="oppenheimer"}[5m]))'`。

## 常见的原因和各自的处理

- **一条缺列的消息把 orderindex 的一个分区挡住（已知的设计后果）。** ADR-0014：不跳过，所以所在分区的全部订单停止同步（四分之一的订单），只能从日志和消费组积压发现。注意这时 `cdc_lag_ms` 不涨（cdc 自己是好的），涨的是 orderindex 的积压：看 [consumer-blocked.md](consumer-blocked.md)。
- **orderindex 或 cdc 被停了。** 3a 做过：停 orderindex（缩到 0）加 `STOP REPLICA SQL_THREAD`，2 分钟后恢复，`orderindex.cdc-orders` 的积压在 30 秒内回到 0，探针 0 失败（3a 笔记“验证 7”）。
- **源库没有写入，指标虚高。** 探针停了（`probe-stop`）而没有别的流量时 `cdc_lag_ms` 一直涨。处理：`scripts/cluster/services.sh probe-start`，或按设计的意思静默：`scripts/cluster/monitoring.sh silence 'alertname=CDCLagging' 2h '探针停了，源库没有写入'`。
- **从库复制停了或落后。** cdc 读从库的 binlog，从库没追上，cdc 也追不上。看 [3a 笔记](../notes/3a-observability.md) 的复制三条告警。
- **位点在源库上已被清掉。** `gtid_purged` 里有位点之后的事务时 cdc 拒绝启动并报错，由人决定是否重做全量（1c 设计 4.7；自动重做会把“binlog 保留期配短了”这种问题盖住）。重做全量：`-reset-checkpoint=<管道名>`（给位点写墓碑后退出，下次启动时重做全量，`services/cdc/cmd/cdc/main.go`），下游不用清，重复的映像会被版本号挡掉。全量读要在 binlog 保留期（1 天）内读完。命令的具体调用方式（在集群里怎么执行）没有找到记录，没有验证过。
- **cdc 的 Pod 不在。** `CDCPipelineMissing`，看 Pod 的事件和上次终止的原因。cdc 只有 1 个副本，没有高可用（ADR-0014），挂掉期间索引不更新，重启后从位点追上。
- **指标送不出去。** Collector 的问题时 `ServiceMetricsMissing` 也会响，见 [service-metrics-missing.md](service-metrics-missing.md)。
- 订单库加分片后管道清单没跟着改（规则注释）：新分片的落后不会被 `CDCPipelineMissing` 发现。

## 怎么确认恢复了

- `scripts/cluster/monitoring.sh promql 'max by (pipeline) (cdc_lag_ms{k8s_namespace_name="oppenheimer"})'` 回到几秒到几十秒，两条管道都有。
- `orderindex.cdc-orders` 的积压回到 0。
- 商家索引和订单一致：对账的 `index` 一对差异为 0：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`，或 `make recon RECON_ARGS="-pairs index -since 26h"`（[recon.md](recon.md)）。索引是唯一可以由对账自动修复的一对。

## 怎么人为触发它

设计第 5 节第 2 层：停 cdc（3a 用过）。`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer scale deploy/cdc --replicas=0`：`CDCPipelineMissing` 约 5 分钟序列过期加 10 分钟的 `for` 后响；缩回 1 之后管道追上。`CDCLagging`：`STOP REPLICA SQL_THREAD` 停订单从库的复制（3a 用过，实际语句和账号见 3a 笔记“验证 7”），cdc 读的从库不再前进，延迟涨过 60 秒加 5 分钟后响，做完 `START REPLICA SQL_THREAD`。`CDCLaggingTooLong` 要停 1 小时，建议临时把阈值改到当前值以下。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
