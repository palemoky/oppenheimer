# 钱的笔数和金额：两边对不上、核对停了、答复迟迟不到

覆盖的告警：`PaymentTotalsMismatch`（critical）、`PaymentTotalsCheckStalled`（warning）、`PaymentReplyOverdue`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.payment_totals` 组。设计和规则原理：[3c-slo-alerting.md](../design/3c-slo-alerting.md) 第 8 节“钱的笔数和金额”；代码 `services/recon/internal/totals`（规则在包注释里）。

## 这条告警在说什么

`recon-totals`（Deployment，命名空间 `oppenheimer`，`make recon-totals` 装）每 30 秒核对一轮最近 30 分钟：支付一侧“成功且订单已答复”的支付单（`payments` 里 `status = SUCCEEDED AND notified = 1`），对订单一侧“已接受”的支付结果（`payment_results` 里 `succeeded = 1 AND disposition = ACCEPTED`），按支付单号分成一分钟一段，两边各算笔数和金额。两条规则、不带时间容差：**封口的段两边必须相等；没封口的段支付一侧不能比订单一侧多。** 先读支付库、后读订单库，所以不会因为两次查询之间又处理了几笔而误报。

3e 起退款单独算一组，套用同样的两条规则：支付一侧“已退款”（`status = REFUNDED AND notified = 1`）对订单一侧“要求退款”（`succeeded = 1 AND disposition = REFUND_REQUIRED`）。指标多一个标签 `group`（`succeeded`、`refunded`）。同一分钟连续两轮对不上时，`recon-totals` 自己逐笔比一次那一分钟，把是哪几笔支付打进日志（每段只做一次）。**这两样在仓库里，还没有部署**（3e 第 6 步）。

- **`PaymentTotalsMismatch`**：`max by (kind) (recon_totals_mismatch) > 0`，持续 1 分钟（两轮）。`kind` 是 `count`（差几笔）或 `amount`（差多少分）。没有“正常的小差异”，阈值是 0。
- **`PaymentTotalsCheckStalled`**：5 分钟内 `recon_totals_runs_total{outcome="ok"}` 的增量小于 1（平时 30 秒一轮），持续 5 分钟。这时上一条读的是旧值或没有值，“不响”不等于“账是平的”。
- **`PaymentReplyOverdue`**：`recon_totals_oldest_open_age` 超过 20 分钟，持续 2 分钟。支付的时限是 15 分钟，到时限的会被置成失败，所以超过 20 分钟没封口的只能是“成功了、订单的答复一直没到”。核对窗口是 30 分钟，这一笔再过 10 分钟就滑出窗口，**这条会自己恢复而问题还在**，所以 `for` 取得短，响过就要查。

## 影响

`PaymentTotalsMismatch`：钱的账不平。有支付成功而订单没记，或者订单记了而支付一侧不是成功，现在就要查，用户可能付了钱订单没变已支付，或者反过来。`PaymentReplyOverdue`：用户可能已经付了钱而订单还是待支付，15 分钟后关单任务会关掉订单并触发退款，要在那之前恢复。`PaymentTotalsCheckStalled`：账有没有平不知道，逐行的每小时对账还在兜底，当天看。

## 先看什么

1. 差在哪一类、差多少：
   `scripts/cluster/monitoring.sh promql 'max by (kind, group) (recon_totals_mismatch{k8s_namespace_name="oppenheimer"})'`（部署 3e 的版本之前没有 `group`）
   `scripts/cluster/monitoring.sh promql 'max(recon_totals_oldest_open_age{k8s_namespace_name="oppenheimer"})'`
2. 是哪一分钟：recon-totals 每一轮对有差异的段打一条 Error 日志（`payment totals mismatch`，带 `bucket_start`）：
   `{service_name="recon"} | json | level="ERROR"`（服务名标签没核对）；或 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs deploy/recon-totals --tail=100`
3. 核对本身在跑吗：
   `scripts/cluster/monitoring.sh promql 'sum(increase(recon_totals_runs_total{k8s_namespace_name="oppenheimer"}[5m])) by (outcome)'`
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide | rg recon-totals`
4. 支付到订单的链路是否通：order 的 `payment.events` 消费者有没有被挡住、payment 和 order 的 outbox 慢不慢：
   `scripts/cluster/monitoring.sh promql 'max by (service_name, messaging_consumer_group_name, messaging_destination_name) (kafkax_consumer_oldest_pending_age_seconds{k8s_namespace_name="oppenheimer"})'`
   `scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le, service_name, messaging_destination_name) (rate(outbox_relay_delay_seconds_bucket{k8s_namespace_name="oppenheimer"}[5m])))'`
5. 是哪几笔（3e 的版本）：同一段连续两轮对不上之后有一组日志 `payment totals drill-down: differing payment`，每笔一条，带 `payment_id`、`order_id`、`kind`（`payment_only` 支付一侧有而订单一侧没有同组的记录；`order_only` 反过来，并且支付单已经定了；`amount` 金额不同）、`group` 和两边各自的状态；最后一条 `drill-down done` 带总数（最多打 20 笔）。`payment totals drill-down found no differing payment` 是合计对不上了两轮、逐笔却一样，差异在这之间消失了。下钻做没做：`sum by (outcome) (increase(recon_totals_drilldowns_total{k8s_namespace_name="oppenheimer"}[30m]))`（指标名按别的计数器的导出方式推的，没有在 Prometheus 上核对）。
6. 没有那组日志（旧版本，或者下钻失败）时，拿第 2 步的时间范围去跑逐行对账定位到单：`make recon RECON_ARGS="-pairs <对> -since <窗口>"`，或 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）。

## 常见的原因和各自的处理

这条核对是 2026-11-22 才上线的，还没有真的响过，下面除标明的外都是推断。

- **答复在路上，不是差异。** 规则已经把在途精确分开（没封口的段支付一侧只能多不能少），所以 `Mismatch` 响了说明不是在途。
- **订单的 `payment.events` 消费者被挡住，或 order 的 outbox 发不出去** → `PaymentReplyOverdue`。规则描述就是这么写的。处理：看 [consumer-blocked.md](consumer-blocked.md)、[outbox.md](outbox.md)。
- **订单接受了而支付已退款、订单接受了支付库里没有的、缺订单记录、金额不等**：这是测试里造的几种情形（设计 8），真的出现是代码缺陷或手工改库。处理：按第 2 步定位的订单，查两边的 `payments`、`payment_results`，再看 [recon.md](recon.md) 的“订单对支付单”一对。
- **同一段里一笔多一笔少正好抵消**：合计类对账发现不了，逐行对账兜底。
- **recon-totals 没有在跑** → `CheckStalled`：Pod 不在、连不上支付库或订单库、指标送不出（`ServiceMetricsMissing` 不覆盖 recon，只能靠这条）。`recon-totals` 没有“停”的子命令，缩到 0 会让这条响，要加静默：`scripts/cluster/monitoring.sh silence 'alertname=PaymentTotalsCheckStalled' 2h '<原因>'`。
- **核对的 Pod 重启后旧序列还留 5 分钟**：规则已用 `max by (kind)` 处理。
- 镜像 tag 是工作区的，提交之后要再 `make recon-totals`（设计 8）。
- 有压测负载时在途的量和每一轮的耗时没有验证过（设计 8“没有验证的”）。

## 怎么确认恢复了

- `recon_totals_mismatch` 回到 0（两个 `kind` 都是），至少两轮（1 分钟）。核对自己失败时瞬时值停在上一次成功的结果上，所以同时要 `recon_totals_runs_total{outcome="ok"}` 在涨。
- `PaymentReplyOverdue` 自己恢复不算数：确认 `scripts/cluster/monitoring.sh promql 'sum(increase(order_transitions_total{k8s_namespace_name="oppenheimer",from="pending_payment",to="paid"}[10m]))'` 大于 0，且 `kafka_consumergroup_lag{consumergroup="order.payment-events"}` 清空。
- 最后以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`，退出码 0。

## 怎么人为触发它

设计第 5 节第 2 层只列了“对账差异：在 `oppenheimer-e2e` 里手工改一行数据”。`PaymentTotalsMismatch`：待定，建议在 `oppenheimer-e2e` 里把一条 `payment_results` 的金额改掉（recon-totals 要指向 e2e 的库，现在清单对着正式库，要另起一个实例，没有验证过）；`PaymentTotalsCheckStalled`：`kubectl -n oppenheimer scale deploy/recon-totals --replicas=0`，5 分钟加 5 分钟后响，用完缩回 1；`PaymentReplyOverdue`：停 order 的 `payment.events` 消费者（3a 用过停消费者的办法）20 分钟以上，同时有支付成功的订单。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
