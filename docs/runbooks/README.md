# Runbook

告警响了之后照着做的东西。我们自己写的每条规则（`deploy/cluster/monitoring/rules/`）的 `runbook_url` 指到这里的一个文件，相关的几条共用一份。chart 自带的规则沿用上游的链接（runbooks.prometheus-operator.dev），不在这里。

设计见 [3c-slo-alerting.md](../design/3c-slo-alerting.md) 的 3.8 节。每份的七节是固定的（3d 的诊断 Agent 把它们当输入读）：这条告警在说什么、影响、先看什么、常见的原因和各自的处理、怎么确认恢复了、怎么人为触发它、历史。

## 总则

### 告警从哪来、到哪去

```
规则（Prometheus）→ Alertmanager（分组、抑制、静默）→ alertsink（全部，只打日志）
                                                    └→ Telegram（只有 critical）
```

- 现在有哪些告警：`scripts/cluster/monitoring.sh alerts`（被抑制、被静默的也列出来）。
- 某条告警送到了没有：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring logs deploy/alertsink --since=30m | grep <告警名>`。
- 级别：`critical` 是用户正在受影响或者再不处理会丢数据，现在就看；`warning` 是余量在变少或者某个保护机制在起作用，当天看；`info` 不通知。

### 先止血还是先查因

先止血。顺序：

1. **看是不是只有这一条在响。** 几条一起响时先看最上游的：节点、MySQL、Kafka、Redis 的告警在前，SLO 和业务告警多半是它们的结果。Alertmanager 已经配了一部分抑制（`alertmanager/alertmanager.yaml`），但“节点不在”抑制不了它上面的 Pod 的告警（告警上没有节点的标签），要自己对。
2. **看最近改过什么**：`git log -5`、`helm -n oppenheimer history <服务>`、最近有没有 `make deploy`、有没有压测或故障实验在跑。是刚改出来的就先回滚，再查为什么。
3. **能降级的先降级**（`pkg/degrade` 的开关，[degrade.md](degrade.md)），让下单这条主路先活着。
4. 止住之后再查根因，把现场留下：告警的时间段、相关的大盘截图或查询、日志的查询语句。查完在那份 runbook 的“历史”里记一行。

涉及订单、库存、钱的故障，**以对账差异为 0 收尾**（[recon.md](recon.md)），不以“告警恢复了”收尾。

### 错误预算烧完了怎么办

四个 SLO 各有一周的预算（7 天窗口）。剩余多少看大盘的 SLO 一块，或者查 `slo:<名字>:error_ratio_rate7d` 除以允许的错误比例。

- 预算还有：告警按 runbook 处理就行，实验照做。预算是拿来花的。
- 预算烧完（7 天的错误比例超过了目标允许的）：**停掉会烧预算的事**（压测打过拐点的那几轮、阶段 4 的故障实验），先把烧预算的原因处理掉，等窗口滚过去、预算回来再继续。这个项目没有发布节奏可以冻结，这条规矩约束的是实验。
- 实验本身有意烧预算时（故障注入），事先知道会烧多少，烧完不算事故，但同样要等预算回来再做下一个。

### 静默

计划内的操作（滚动中间件、故障实验、停探针）之前加，有期限，写原因：

```
scripts/cluster/monitoring.sh silence 'alertname=ProbeNoData' 2h '正式压测，探针停了'
scripts/cluster/monitoring.sh silence list
scripts/cluster/monitoring.sh silence expire <id>
```

- 匹配写得越窄越好（带上 `instance`、`service`），不要一条 `severity=warning` 全盖住。
- 压测时不整体静默：过载的几轮里背压、节点 CPU 的告警本来就该响，那是验证的一部分。
- 静默不是修复。同一条告警静默过两次，就该改规则的阈值或者修原因。

### 人为触发告警

每份的“怎么人为触发它”是告警质量检查用的。要动集群的故障注入**先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)**；删 PVC、重置节点、清数据这类破坏性的先问主人。只想验证路由、分组、抑制而不想真的弄坏东西时，可以直接往 Alertmanager 里塞一条合成的告警（几分钟后自己过期）：

```
KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring exec alertmanager-kps-alertmanager-0 -c alertmanager -- \
  amtool --alertmanager.url=http://localhost:9093 alert add alertname=SyntheticTest severity=warning synthetic=true \
  --end="$(date -u -v+4M +%Y-%m-%dT%H:%M:%SZ)" --annotation=summary='"synthetic test"'
```

注意 `severity=critical` 的合成告警会真的发到 Telegram。要避开就先加静默（`monitoring.sh silence 'synthetic=true severity=critical' 7m '<原因>'`），并且让静默比告警活得久：告警还在的时候撤掉静默，它下一次分组刷新就发出去了。

抑制规则有现成的整套检查：`scripts/cluster/monitoring.sh inhibit-test`（核对表 `deploy/cluster/monitoring/alertmanager/tests/inhibit.txt`，自己加静默，不发 Telegram）。

## 索引

### SLO（`rules/slo.yaml`）

| 文件 | 告警 |
|---|---|
| [slo-order-create-availability.md](slo-order-create-availability.md) | `SLOOrderCreateAvailabilityBurn{Fast,Medium,Slow}` |
| [slo-order-create-latency.md](slo-order-create-latency.md) | `SLOOrderCreateLatencyBurn{Fast,Medium,Slow}` |
| [slo-order-persist-delay.md](slo-order-persist-delay.md) | `SLOOrderPersistDelayBurn{Fast,Medium,Slow}` |
| [slo-order-payment-delay.md](slo-order-payment-delay.md) | `SLOOrderPaymentDelayBurn{Fast,Medium,Slow}` |

### 业务（`rules/business.yaml`）

| 文件 | 告警 |
|---|---|
| [probe.md](probe.md) | `ProbeFailing`、`ProbeIncomplete`、`ProbeNoData` |
| [recon.md](recon.md) | `ReconFoundDifferences`、`ReconRunFailed`、`ReconNotRunning`、`ReconDailySlow` |
| [payment-totals.md](payment-totals.md) | `PaymentTotalsMismatch`、`PaymentTotalsCheckStalled`、`PaymentReplyOverdue` |
| [inventory-anomaly.md](inventory-anomaly.md) | `InventoryOversellDetected`、`InventoryGateDrift` |
| [order-gate-memory.md](order-gate-memory.md) | `OrderGateMemoryFull` |
| [order-backlog.md](order-backlog.md) | `OrderBackpressureRejecting`、`OrderQueueNearWaitLimit` |
| [order-admit-rate.md](order-admit-rate.md) | `OrderAdmitRateDropped` |
| [consumer-dead-letter.md](consumer-dead-letter.md) | `OrderSubmitDeadLettered`、`ConsumerDeadLettered` |
| [consumer-blocked.md](consumer-blocked.md) | `ConsumerBlocked`、`ConsumerBlockedTooLong`、`ConsumerNotConsuming` |
| [outbox.md](outbox.md) | `OutboxRelaySlow`、`PaidOrdersNotLanding` |
| [payment-backlog.md](payment-backlog.md) | `PaymentResultSlow` |
| [cdc.md](cdc.md) | `CDCLagging`、`CDCLaggingTooLong`、`CDCPipelineMissing` |
| [breaker.md](breaker.md) | `BreakerOpen`、`OrderLandingBreakerOpen` |
| [degrade.md](degrade.md) | `DegradeSwitchLeftOff` |
| [ratelimit-fallback.md](ratelimit-fallback.md) | `RateLimitFallingBack` |
| [order-read-busy.md](order-read-busy.md) | `OrderReadBulkheadRejecting` |
| [service-metrics-missing.md](service-metrics-missing.md) | `ServiceMetricsMissing` |

### 网格和入口（`rules/mesh.yaml`）

| 文件 | 告警 |
|---|---|
| [ingress-gateway-5xx.md](ingress-gateway-5xx.md) | `IngressGateway5xxHigh` |
| [mesh-overflow.md](mesh-overflow.md) | `IngressRejectingRequests`、`MeshConcurrencyLimitOverflow` |
| [mesh-ejection.md](mesh-ejection.md) | `MeshEndpointEjected` |
| [keda-scaling.md](keda-scaling.md) | `KedaScalerErrors`、`KedaHPAScalingInactive` |

### 中间件（`rules/middleware.yaml`）

| 文件 | 告警 |
|---|---|
| [mysql-down.md](mysql-down.md) | `MySQLDown` |
| [mysql-replication.md](mysql-replication.md) | `MySQLReplicaSQLThreadStopped`、`MySQLReplicaIOThreadStopped`、`MySQLReplicaLagHigh`、`MySQLReplicaMetricsMissing` |
| [mysql-connections.md](mysql-connections.md) | `MySQLConnectionsHigh` |
| [mysql-backup.md](mysql-backup.md) | `MySQLBackupStale` |
| [redis-down.md](redis-down.md) | `RedisDown` |
| [redis-gate-memory.md](redis-gate-memory.md) | `RedisGateMemoryHigh`、`RedisGateMemoryCritical` |
| [redis-replication.md](redis-replication.md) | `RedisReplicaLinkDown` |
| [redis-cluster.md](redis-cluster.md) | `RedisClusterUnhealthy` |
| [redis-sentinel.md](redis-sentinel.md) | `RedisSentinelBelowQuorum`、`RedisSentinelDown` |
| [kafka-partitions.md](kafka-partitions.md) | `KafkaUnderReplicatedPartitions`、`KafkaPartitionsOffline` |
| [kafka-brokers.md](kafka-brokers.md) | `KafkaBrokerMissing` |
| [kafka-operator.md](kafka-operator.md) | `KafkaOperatorNotReconciling`、`KafkaOperatorMetricsMissing` |
| [otel-collector.md](otel-collector.md) | `OtelGatewayDown`、`OtelCollectorRefusing`、`OtelCollectorExportFailing` |

### 节点、Kubernetes、监控自己（`rules/platform.yaml`）

| 文件 | 告警 |
|---|---|
| [node-reboot.md](node-reboot.md) | `NodeRebooted` |
| [node-resources.md](node-resources.md) | `NodeCPUHigh` |
| [node-disk.md](node-disk.md) | `ServerDiskLow`、`ServerDiskCritical`、`AgentDiskLow` |
| [container-memory.md](container-memory.md) | `ContainerOOMKilled`、`ContainerMemoryNearLimit` |
| [ingress-placement.md](ingress-placement.md) | `IngressEnvoyOnServerNode` |
| [alertmanager-notifications.md](alertmanager-notifications.md) | `AlertDeliveryStalled`（chart 自带的 `AlertmanagerFailedToSendAlerts`、`AlertmanagerClusterFailedToSendAlerts` 也看这份） |
