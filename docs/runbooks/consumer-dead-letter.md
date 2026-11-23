# 消息进了死信

覆盖的告警：`OrderSubmitDeadLettered`（critical）、`ConsumerDeadLettered`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.pipeline` 组。出处：[ADR-0021](../adr/0021-degradation-and-lifecycle.md)（只有 handler 标成 `Permanent` 的才进死信）、[1b 笔记](../notes/1b-high-concurrency.md)。

## 这条告警在说什么

消费者对普通错误不设重试上限（依赖不可用时让分区停住等它恢复），所以消息进死信只有一种情况：handler 把错误标成 `kafkax.Permanent`，也就是“数据本身不对、重试也不会成功”。死信 topic 是原 topic 加 `.dlq`，保留 7 天，消息带 `dlq-error`、`dlq-topic`、`dlq-partition`、`dlq-offset` 几个 header。

- **`OrderSubmitDeadLettered`**：`kafkax_consumer_dead_lettered_total{messaging_destination_name="order.submit"}` 在 15 分钟内有增长（已有序列又涨，或 15 分钟前没有、现在有了）。没有 `for`。`order.submit` 上的消息是“已经对用户返回 202、但没有落库”的订单。
- **`ConsumerDeadLettered`**：同一个指标在其他 topic 上，按服务和 topic 分组，15 分钟内有增长。数据本身有问题的消息被跳过，那一步没有做（没发的事件、没更新的索引）。

计数器平时没有序列，第一次出现才有，规则写成两半（文件头注释第 1 条）。

## 影响

`OrderSubmitDeadLettered`：用户拿着“已受理”的订单号永远查不到订单。闸门里的占用会被回收任务收回（库存不丢），订单要人工处理。现在就要看。`ConsumerDeadLettered`：该 topic 对应的那一步没做，具体看是哪个 topic：`order.events` 上 inventory 的预占没结算，`payment.events` 上订单没变已支付，`cdc.order.orders` 上商家索引缺一条。当天看。

## 先看什么

1. 哪个服务、哪个 topic、多少条：
   `scripts/cluster/monitoring.sh promql 'sum by (service_name, messaging_destination_name) (increase(kafkax_consumer_dead_lettered_total{k8s_namespace_name="oppenheimer"}[1h]))'`
2. 看死信里的消息和错误原因（header 里的 `dlq-error`）：
   `POD=$(KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get pods -l strimzi.io/cluster=kafka,strimzi.io/broker-role=true -o jsonpath='{.items[0].metadata.name}')`
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka exec $POD -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order.submit.dlq --from-beginning --max-messages 5 --property print.headers=true --property print.key=true --timeout-ms 10000`（topic 换成实际的 `<topic>.dlq`；命令是按 `scripts/cluster/middleware.sh` 里 `kafka-topics.sh` 的调法写的，console-consumer 这一步没有验证过）
3. 对应服务的错误日志，找 handler 判成 `Permanent` 的那条：
   `{service_name="order"} | json | level="ERROR"`（换成第 1 步的 `service_name`）
4. 是不是一个刚发的版本带来的：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods`，镜像 tag 是不是刚变过（滚动发布后消息格式不兼容，推断）。
5. 对账看受影响的订单：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`，看订单对预占、订单对支付单有没有差异（[recon.md](recon.md)）。

## 常见的原因和各自的处理

这两条规则没有真的响过。1b 的杀进程测试最后每次都是死信为零，2c 的集群 e2e 也“没有进死信的消息”；3b 的记录里没有死信的数。文档里记过的、进过死信或会进死信的情形：

- **依赖方重启几秒，重试预算用完，消息进了死信（1b 真遇到过）。** order 落库调 inventory 的 `Reserve`，默认重试 5 次合计约 1.5 秒，短于 inventory 重启的 1 到 3 秒，一次运行有 12 条下单消息进死信；调到 10 次（约 26 秒）。库存服务处理事件的消费者重试调到 60 次（约 5 分钟）。ADR-0021 之后依赖不可用不再进死信（普通错误无限重试）。所以现在看到死信，不应该是依赖抖动，要看 `dlq-error` 是不是数据本身的问题。
- **消息缺字段、格式不对。** CDC 一侧：一条缺列的消息会让所在分区的全部订单停止同步（ADR-0014 的后果，“不跳过”）；其他消费者的 `Permanent` 是 handler 自己判的。处理：按 `dlq-error` 找原因，修数据或代码。
- **认领之后消息进死信的占用不会自动归还**（1b 的已知缺口：不知道 MySQL 里有没有预占，只记 error 日志）。处理：按订单手工查预占是否存在，必要时手工归还，再看 [inventory-anomaly.md](inventory-anomaly.md)。
- **handler 把本该重试的错误标成了 `Permanent`**（推断，没有遇到过）：ADR-0021 的备选里说过漏掉一种就是把一条正常的消息送进死信；涉及资金和库存的消息宁可挡住分区。处理：改代码，把错误改成可重试。
- **重放**：没有找到记录里有现成的重放命令（1b 提到“重放的用户”是指客户端用同一个幂等键重试）。死信里的 `order.submit` 要人工处理：确认订单有没有落库（订单库里查），没有的话占用已被回收，用户需要重新下单，建议通知；重放前先修好原因。没有验证过。

## 怎么确认恢复了

- 计数器不再增长：`scripts/cluster/monitoring.sh promql 'sum(increase(kafkax_consumer_dead_lettered_total{k8s_namespace_name="oppenheimer"}[15m]))'` 为 0。告警在最后一次增长之后保持 15 分钟才自己恢复，恢复不代表那几条消息处理了。
- 死信里的每一条都有结论（重放、手工处理、确认丢弃），记下订单号。
- 最后以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`，退出码 0（[recon.md](recon.md)）；库存对账没有漂移（[inventory-anomaly.md](inventory-anomaly.md)）。

## 怎么人为触发它

设计第 5 节没有列。待定，建议：在 `oppenheimer-e2e` 里往一个 topic 直接发一条格式不对的消息（例如 `order.events` 上缺必填字段），让 handler 判成 `Permanent`；用 `kafka-console-producer.sh` 发（没有验证过，e2e 的 topic 前缀是 `e2e-`，见 `scripts/cluster/e2e-lib.sh`，要发到 e2e 自己的 topic，不要发到正式的）。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
