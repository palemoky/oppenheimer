# 库存的计数对流水：对不上、预占卡住、核对停了

覆盖的告警：`StockLedgerMismatch`（critical）、`StockReservationsStuck`（warning）、`StockCheckStalled`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.recon_stock` 组。设计和规则原理：[3e-reconciliation-by-scenario.md](../design/3e-reconciliation-by-scenario.md) 第 4.2 节、[ADR-0039](../adr/0039-reconciliation-by-scenario.md)；代码 `services/recon/internal/stock`（规则在包注释里）。

## 这条告警在说什么

`recon-stock`（Deployment，命名空间 `oppenheimer`，`make recon-stock` 装）在库存库（`inventory_db`，读主库）的一个一致性快照里核对两条等式，按 SKU：

- **预占**：`sku_stock.reserved`（加上 `stock_pending` 里还没应用的增量）等于状态是 `RESERVED` 的预占的数量合计（`reservation_item`）。每分钟一轮（快的一轮），只扫最近 2 小时创建的预占。
- **已售**：`sku_stock.sold`（同样含没应用的增量）等于状态是 `CONFIRMED` 的预占的数量合计。要对全部 `CONFIRMED` 求和，是一次全表扫描，所以不是每轮都做：集群上每 5 分钟做一次（完整的一轮，`-full-every`，程序的默认值是 1 小时），顺带数出卡住的预占。

计数和流水在同一个库里、改状态和记增量在同一个事务里，所以快照里读到的两边是同一时刻，没有在途的量，不设容差，也不复查。它不管总量守恒（`available + reserved + sold = stocked` 由表上的 CHECK 保证），也发现不了流水本身记错了（数量记成别的数，那要和订单的明细比，是 `recon` 的 stock 那一对的事）。

- **`StockLedgerMismatch`**：`max by (check) (recon_stock_mismatch_skus) > 0`，持续 2 分钟（两轮快的）。`check` 是 `reserved` 或 `sold`，值是对不上的 SKU 个数；差的件数是 `recon_stock_mismatch_quantity`。**`sold` 只在完整的一轮（5 分钟一次）更新，两轮之间保持上一次的值**，所以它响了之后最长 5 分钟才能确认恢复。
- **`StockReservationsStuck`**：`max(recon_stock_stuck_reservations) > 0`，持续 10 分钟。卡住的预占是状态还是 `RESERVED`、却比 1 小时（`-stuck-after`）还老的：既没有被确认也没有被释放。这个数也只在完整的一轮更新，**问题解决之后要等下一次完整的一轮（最长 5 分钟）才会自己恢复**。
- **`StockCheckStalled`**：5 分钟内 `recon_stock_runs_total{outcome="ok"}` 的增量小于 1（平时每分钟一轮），持续 5 分钟。

指标名是照 `recon_totals_*` 的导出方式类推的（点变下划线、计数器加 `_total`、没有单位后缀），**没有在 Prometheus 上核对过**。

## 差异是 0 和核对没在跑，不是一回事

- 差异是 0：`recon_stock_mismatch_skus` 有序列、值是 0，**并且** `recon_stock_runs_total{outcome="ok"}` 在涨。
- 核对没在跑：`StockLedgerMismatch` 读的是旧值或者没有值，所以它不响不代表账是平的。还没有成功核对过时核对不报序列（不报 0：0 的意思是“核对过、没有差异”）。看 `StockCheckStalled` 有没有响，或者：
  `scripts/cluster/monitoring.sh promql 'sum by (kind, outcome) (increase(recon_stock_runs_total{k8s_namespace_name="oppenheimer"}[10m]))'`
  `ok` 在涨才算活着；只有 `error` 在涨是连不上库或查询超时。
- `recon_stock_skus` 是上一次核对覆盖的 SKU 数，是 0 或者突然变小说明核对读到的东西不对。

## 影响

`StockLedgerMismatch`：库存的账不再可信。计数多于流水是少卖（占着库存），少于流水有可能超卖。现在就要查，当天内查清。`StockReservationsStuck`：卡住的预占一直占着库存，用户看到的可售量比实际少，不会超卖；当天看。`StockCheckStalled`：账有没有平不知道，每小时的逐行对账（订单对库存预占）还在兜底，但它比的是订单和预占，不是计数和流水，当天看。

## 先看什么

1. 哪条等式、几个 SKU、差多少：
   `scripts/cluster/monitoring.sh promql 'max by (check) (recon_stock_mismatch_skus{k8s_namespace_name="oppenheimer"})'`
   `scripts/cluster/monitoring.sh promql 'max by (check) (recon_stock_mismatch_quantity{k8s_namespace_name="oppenheimer"})'`
   `scripts/cluster/monitoring.sh promql 'max(recon_stock_stuck_reservations{k8s_namespace_name="oppenheimer"})'`
2. 是哪些 SKU：每一轮对有差异的 SKU 打一条 Error 日志（`stock mismatch`，带 `check`、`sku_id`、`counter`、`ledger`、`no_stock_row`，每轮最多 20 个），卡住的预占是一条 Warn（`stuck reservations`，带 `count`、`oldest_order_id`）：
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs deploy/recon-stock --tail=100`
   Loki：`{service_name="recon"} | json | msg=~"stock mismatch|stuck reservations"`（服务名标签和字段名没有核对）。
3. 核对本身在跑吗：上一节的第二条查询，加 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide | rg recon-stock`。
4. 库存一侧的消费者有没有被挡住、有没有进死信：
   `scripts/cluster/monitoring.sh promql 'max by (service_name, messaging_consumer_group_name, messaging_destination_name) (kafkax_consumer_oldest_pending_age_seconds{k8s_namespace_name="oppenheimer"})'`
   `scripts/cluster/monitoring.sh alerts`（看 `ConsumerBlocked`、`ConsumerDeadLettered`、`InventoryOversellDetected` 有没有在响）。
5. 要定位到订单，拿 SKU 去跑逐行对账的 stock 那一对：`make recon RECON_ARGS="-pairs stock -since 26h"`，或 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）。
6. 对账用的是订单库；订单没有的预占、预占没有的订单是另一类差异，不在这里。

## 常见的原因和各自的处理

这三条告警都没有真实触发过，下面除标明的外都是推断，没有验证。

- **同一条增量被应用了两次、丢了一条增量、改了预占的状态而没有记增量**（包注释列的几种）：这些都不破坏总量守恒，所以 CHECK 看不出来，只有这里能发现。真的出现是库存服务的代码缺陷，或者手工改过库。处理：按第 2 步的 SKU，对着 `stock_pending`、`reservation`、`reservation_item` 的行逐条看；修库要先停该 SKU 的受理，修完再让核对跑两轮。
- **预占那条对不上，窗口外有卡住的预占**：快的一轮只看最近 2 小时的预占，窗口之外还是 `RESERVED` 的预占（卡住超过 2 小时）它看不到，却在 `reserved` 计数里，于是快的一轮会报出一个差值，完整的一轮才说明原因（包注释）。所以 `StockLedgerMismatch(check=reserved)` 和 `StockReservationsStuck` 一起响时，先按卡住的预占处理。
- **`StockReservationsStuck`：确认或释放那一步没有发生**：inventory 的 `order.events` 消费者被挡住，或消息进了死信（1b 记录的已知缺口：认领之后进死信的占用不会自动归还），或关单没有触发。处理：看 [consumer-blocked.md](consumer-blocked.md)、[consumer-dead-letter.md](consumer-dead-letter.md)；用 `oldest_order_id` 查那一单现在的状态。
- **核对的快照事务读不到一致的快照**：快的一轮时限 5 秒，完整的一轮 2 分钟（时限同时限制了快照事务开多久）。库存库慢时会超时，计 `outcome="error"`，连续 5 分钟没有成功就是 `StockCheckStalled`。全表聚合随订单总量变慢，完整的一轮超过 2 分钟时每一轮都会接着重试完整的，快的一轮也跟着没有（没有验证过，推断自代码里“失败的完整一轮不更新 lastFull”）。
- **`StockCheckStalled`：recon-stock 没有在跑、连不上库存库、指标送不出**：`ServiceMetricsMissing` 不覆盖 recon，只能靠这条。`recon-stock` 没有“停”的子命令，缩到 0 会让这条响，要加静默：`scripts/cluster/monitoring.sh silence 'alertname=StockCheckStalled' 2h '<原因>'`。
- **核对的 Pod 重启后旧序列还留 5 分钟**：规则已用 `max` 处理。重启后第一轮总是完整的，所以 `sold` 和 `stuck` 马上就有值。
- 镜像 tag 是工作区的，提交之后要再 `make recon-stock`。
- 有压测负载时每一轮的耗时、完整的一轮在大库上要多久，没有验证过。

## 怎么确认恢复了

- `recon_stock_mismatch_skus` 两个 `check` 都回到 0。`reserved` 至少两轮（2 分钟）；`sold` 要等下一次完整的一轮（最长 5 分钟），在那之前它显示的是旧值。同时 `recon_stock_runs_total{outcome="ok"}` 在涨：核对自己失败时瞬时值停在上一次成功的结果上。
- `recon_stock_stuck_reservations` 回到 0，同样要等下一次完整的一轮。`StockReservationsStuck` 自己恢复不算数：确认卡住的那些订单已经确认或释放（用 `oldest_order_id` 查）。
- 最后以对账差异为 0 收尾：`make recon-check FROM=<RFC3339> TO=<RFC3339>`（对 `[FROM, TO)` 读三个常驻核对的指标并跑一次逐行对账，没有验证过），或 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`，退出码 0。

## 怎么人为触发它

设计第 5 节第 2 层只列了“对账差异：在 `oppenheimer-e2e` 里手工改一行数据”。下面都没有验证过，动手前先登记。

- `StockLedgerMismatch`：待定，建议在 `oppenheimer-e2e` 里把某个 SKU 的 `sku_stock.reserved` 手工加 1（CHECK 要求 `available + reserved + sold = stocked`，所以要同时把 `available` 减 1，否则 UPDATE 被拒绝）；`recon-stock` 要指向 e2e 的库，现在清单对着正式库，要另起一个实例。不要在正式库上做。
- `StockReservationsStuck`：待定，建议在 e2e 里停 inventory 的 `order.events` 消费者，下一批预占不会被确认或释放，等 1 小时（`-stuck-after`）再等下一次完整的一轮；或者另起一个实例把 `-stuck-after` 调小。
- `StockCheckStalled`：`kubectl -n oppenheimer scale deploy/recon-stock --replicas=0`，5 分钟加 5 分钟后响，用完缩回 1。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
