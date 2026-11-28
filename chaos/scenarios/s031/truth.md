# s031 的答案

## 标准根因

Kafka 上给客户端号 `order` 设了一条拉取配额：`consumer_byte_rate=2048`（每个 broker 每秒 2048 字节；脚本用 `kafka-configs.sh` 设的，对应“有人为了保护 broker 给一个客户端限了速，数填小了”）。
order 的三个消费者（落库 `order.submit`、支付结果 `payment.events`、延迟关单 `order.close.delay`）用的都是这个客户端号，broker 对它们的拉取请求限速：每次拉取被推迟，拉到的量远小于产生的量。
消费者自己没有任何错误，处理也不慢，只是拿不到消息。生产不受影响。

## 定位必需的关键证据

- 三个消费者组（`order.order-submit`、`order.payment-events`、`order.order-close-delay`）的积压（`kafka_consumergroup_lag`）一起涨，而 order 的 CPU 不高、处理一条消息的耗时没有变、没有重试、没有死信、没有重平衡。
- 别的服务的消费者组（inventory、payment、orderindex）正常；order 的生产也正常：问题只在“order 这个客户端号的拉取”。
- `kafka-configs.sh --describe --entity-type clients --entity-name order`（或者 `--entity-type clients` 列出全部）：`consumer_byte_rate=2048.0`。这是决定性的证据。
- broker 的指标里这个客户端号的拉取限速时间（`kafka.server:type=Fetch,client-id=order` 的 `throttle-time`）不是 0；客户端一侧 franz-go 的日志里可能有被限速的记录。
- 下单：受理照常（202），但落库跟不上，等落库的占用越积越多，到背压的线之后受理开始拒绝（429，`BACKLOGGED`）；受理了超过约一分钟没落库的占用被回收，查这些订单是 404。
- 支付：支付结果回不到订单，已经付了钱的订单停在待支付。

## 容易误判成的原因

- order 的落库慢了、订单库慢了（处理耗时没有变，库正常；消息根本没有拉到）。
- order 的消费者卡住了、毒消息（没有重试、最老待处理消息的年龄不涨：本地没有待处理的消息）。
- 重平衡风暴（组的成员没有变）。
- Kafka 的 broker 有问题、ISR 不足（别的消费者组和所有生产都正常）。
- 流量突增（产生的速率没有变）。

## 备注

- **全部是推的，没有在集群上跑过。** 脚本在本机的 Kafka 上验过：配额设得上、查得到、删得掉，重复恢复不报错。
- **数值没有把握**：稳态下落库的消息约每秒 46 条，每条多大没有量过；2048 字节每 broker 每秒够不够“明显限住又不至于一条都拉不到”要试（`CX_FETCH_BYTES`）。一条消息比配额大时仍然能拉到（配额是事后算的）。
- **客户端号不分命名空间**：e2e 那一套的 order 也叫 `order`，在 e2e 里试这个场景会同时限住长期运行的那一套。所以 e2e 里的试只能在没有稳态压测时做，并且时间短。
- 预期的告警是按规则猜的：`OrderBackpressureRejecting`、`OrderQueueNearWaitLimit`（warning），可能还有 `PaymentResultSlow`、`PaidOrdersNotLanding`（critical）、下单可用性的 SLO。`ConsumerBlocked` 多半不响（它看的是本地最老待处理消息的年龄）。仓库里没有按积压条数的告警：这是一个可能的改进项。
- 配额存在 Kafka 的元数据里，broker 重启也还在：第二档，脚本另起了一个到时自己删的后台任务。
- 数据上要验的：受理了没落库的订单被回收后，迟到的落库消息被丢弃；支付结果迟到 15 分钟后订单的状态和支付单对得上。恢复时等逐行对账。
