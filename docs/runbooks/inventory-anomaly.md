# 库存异常：发现超卖，闸门和库存账对不上

覆盖的告警：`InventoryOversellDetected`（critical）、`InventoryGateDrift`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.inventory` 组。设计：[3c-slo-alerting.md](../design/3c-slo-alerting.md) 第 4.2 节。背景：[ADR-0018](../adr/0018-async-stock-deduction.md)、[1b 笔记](../notes/1b-high-concurrency.md)。

## 这条告警在说什么

库存有两份：MySQL 里的库存账（唯一的事实来源）和 Redis 闸门里的可售量（只决定放不放进来）。原则是 Redis 允许偏小（少卖）、不允许偏大。ADR-0018 之后 MySQL 不再在预占时检查可售数，闸门是防超卖的唯一关口，MySQL 一侧只在“应用增量会让某个数变负”时事后发现并报错。

- **`InventoryOversellDetected`**：`inventory_oversell_detected_total` 在 15 分钟内有增长（已有序列又涨了，或 15 分钟前还没有、现在有了的新序列）。没有 `for`，第一次发生就响，之后保持 15 分钟。一次都不该有。出处：计划“库存异常”。
- **`InventoryGateDrift`**：`inventory_reconcile_drifts_total` 在 30 分钟内有增长。inventory 的定时核对发现闸门可售量和库存账不一致，并按库存账修了（`inventory_reconcile_repairs_total`），所以是 warning；但偏差本身说明有一条路径在漏，反复出现要查。

计数器平时没有序列，第一次 `Add` 才有，所以规则写成两半（文件头注释第 1 条）；`increase` 从无到有的那一下是 0，不能只用它。

## 影响

`Oversell`：闸门放行的比库存账上有的多，已经受理的订单可能无货可发。涉及订单和钱，马上看。`GateDrift`：偏向多放会超卖，偏向少放是少卖（用户看到“库存不足”而其实有货）。当天看。

## 先看什么

1. 是哪个服务、哪个 Pod 报的，多少次：
   `scripts/cluster/monitoring.sh promql 'sum by (service_name) (increase(inventory_oversell_detected_total{k8s_namespace_name="oppenheimer"}[1h]))'`
   `scripts/cluster/monitoring.sh promql 'sum(increase(inventory_reconcile_drifts_total{k8s_namespace_name="oppenheimer"}[30m]))'`
   `scripts/cluster/monitoring.sh promql 'sum(increase(inventory_reconcile_repairs_total{k8s_namespace_name="oppenheimer"}[30m]))'`
2. inventory 的日志（哪个 SKU、偏了多少、往哪个方向）：
   `{service_name="inventory"} | json | level=~"WARN|ERROR"`；`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs deploy/inventory --tail=200`
3. 同时有没有别的异常，闸门的数据可能丢过：Redis 闸门有没有切主、重启（`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis get pods`，`RedisDown`、`RedisSentinelBelowQuorum` 在不在响，见 [order-gate-memory.md](order-gate-memory.md)），闸门满过（`order_admissions_total` 的 `gate_memory_full`）。
4. 消费者有没有落后（`inventory.order-events`、`order.order-submit` 积压会让预占迟迟不结算）：
   `scripts/cluster/monitoring.sh promql 'sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup=~"inventory\\.order-events|order\\.order-submit"})'`
5. 对账，看订单对预占、库存守恒：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`，再 `logs job/<名字>`（[recon.md](recon.md)）；压测之后用 `scripts/cluster/loadtest.sh verify`（十个 SKU 的可售、预占、已售和闸门里的数要对上）。

## 常见的原因和各自的处理

这两条规则还没有真的响过，下面是文档里记过的、会造成偏差的路径；哪一条是你这次的原因要靠第 2、3 步确认。

- **闸门的 Redis 丢了数据或切主。** 1b 记录：Redis 丢数据后对账的上限不够紧，恢复期里向上修正可能把“已受理、还没在 MySQL 预占”的占用也放出来，会多放进来一批注定在预占时失败的订单，不会超卖（设计 1b 的 6.4）。处理：看 inventory 日志里的修正，等它修完，再对账。
- **认领之后消息进了死信，占用不会自动归还**（1b 已知缺口：不知道 MySQL 里有没有预占，只记 error 日志等人工）。处理：看 [consumer-dead-letter.md](consumer-dead-letter.md)，按死信里的消息手工处理。
- **消费者被杀后分区停摆，库存迟迟不归还。** 1b 记录：会话超时默认 45 秒时，被杀的消费者名下分区整整停摆，释放的库存没回到闸门，表现是少卖不超卖；已改成 10 秒（`SessionTimeout`）。处理：看 [consumer-blocked.md](consumer-blocked.md)。
- **inventory 消费 `order.events` 跟不上**（上限约 105 条/秒，round-1 第 5 节）：预占迟迟不结算，数是对的但慢。
- **宽限期调短后回收与落库竞争。** 1b 杀进程测试预先写下的问题，“回收与落库的竞争”出现且量很大，驱动是会话超时（1b 笔记 430 行附近）。
- **真正的超卖（`Oversell`）**：闸门放行了不该放的。先停掉相关 SKU 的受理（规则描述），再查闸门和库存账是怎么对不上的（上面几条）。3b 里对账过的每一轮都是零超卖，包括过载的几轮（[3b-capacity.md](../notes/3b-capacity.md) 第 1 节）；还没有在这个项目里遇到过。
- 手工改过库存或闸门（推断，没有遇到过）：对账会发现。

## 怎么确认恢复了

- 两个计数器不再增长：`scripts/cluster/monitoring.sh promql 'sum(increase(inventory_oversell_detected_total{k8s_namespace_name="oppenheimer"}[15m]))'`、`sum(increase(inventory_reconcile_drifts_total{k8s_namespace_name="oppenheimer"}[30m]))` 都回到 0（没有数据也算）。告警在最后一次增长之后保持窗口那么久（15 分钟、30 分钟）才自己恢复，不是故障还在。
- 对账：`recon` 差异为 0；压测之后 `verify` 的库存守恒是 `PASS`，预占是 0。
- 订单对预占的那一对无差异，才算涉及库存的收尾完成。

## 怎么人为触发它

设计第 5 节没有专门列。待定，建议：

- `InventoryGateDrift`：在 `oppenheimer-e2e` 里手工改闸门里某个 SKU 的可售量（Redis 的 `gate:` 键，键名见设计 1b 的闸门数据结构），等 inventory 的定时核对发现并修。没有验证过。
- `InventoryOversellDetected`：把闸门的可售量调大于库存账（同样在 e2e 里），再受理订单，直到应用增量让某个数变负。没有验证过，有造成真超卖的风险，只在 e2e 里做。
- 退而求其次：临时把阈值改到当前值以下，只验证“路是通的”。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
