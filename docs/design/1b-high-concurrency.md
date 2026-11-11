# 1b 设计：高并发改造

本文是阶段 1b 的实现规格。1a 把业务做对了，但所有请求都同步打到 MySQL，跨服务的最终一致靠各服务自己轮询重试。1b 把它改造成能扛峰值的链路，同时不放松 1a 已经建立的任何一条一致性保证。

事件契约以 `api/proto/oppenheimer/event/v1/events.proto` 为准。

## 1. 分四步走

每一步结束时系统都能运行，`make lint test e2e` 通过。后一步建立在前一步之上。

| 步骤 | 内容 | 对外行为 |
|---|---|---|
| 一 | 公共库：`kafkax`、`outbox`、`lock`、`retry`、`worker`、`redisx` | 不变 |
| 二 | 事件化：用 outbox 加 Kafka 替换“同步调用加轮询重试” | 不变 |
| 三 | 削峰：Redis 库存预扣、异步下单、延迟关单 | 下单改为 202 加轮询 |
| 四 | 加固：分布式锁的使用、缓存三大问题、杀进程对账测试 | 不变 |

第三、四步的细节在动工前补到本文（第 6、7 节目前只有方案要点）。

## 2. 步骤二之后的总体结构

```
客户端 ─HTTP─▶ gateway ─gRPC─▶ order ─gRPC(Reserve)─▶ inventory
                  │              │  ▲                     ▲
                  │              │  │ payment.events      │ order.events
                  │        outbox│  │                     │
                  │              ▼  │                     │
                  │            ┌──────── Kafka ───────────┴──┐
                  │            │ order.events  payment.events │
                  │            └──────▲──────────────┬────────┘
                  │              outbox│             │ order.events
                  └─gRPC─▶ payment ────┘◀────────────┘
```

和 1a 相比变了三件事：

| 1a | 1b 步骤二 |
|---|---|
| 订单进入终态后同步调用 `inventory.Confirm/Release`，失败由 sweeper 按 `stock_settled` 补 | 订单状态变更和 `OrderEvent` 在同一个事务里写入，由 outbox 中继发到 Kafka，库存服务消费 |
| 支付服务轮询 `notified = 0` 的支付单，同步调用 `order.HandlePaymentResult` | 支付出结果和 `PaymentEvent` 在同一个事务里写入；订单服务消费后用 `OrderEvent.payment_result_handled` 答复 |
| 每个服务各写一套死锁重试和后台循环 | 收敛到 `pkg/retry` 和 `pkg/worker` |

没有变的：下单时 `order → inventory.Reserve` 仍是同步 gRPC（步骤三才改），支付服务创建支付单时同步查订单。

**为什么是 outbox**（见 ADR-0008）：服务要同时做两件事，改自己的库、通知别人。这两件事不在一个事务里，就一定存在“做了一件、另一件没做”的窗口。1a 的解法是在库里留一个标志位（`stock_settled`、`notified`），后台任务扫描标志位重试。outbox 是同一个思路的通用形式：把“要通知别人”这件事本身作为一行数据，和业务变更写在同一个本地事务里，再由一个独立的中继把它发出去。好处是业务代码不再关心下游是否可达，新增一个订阅者不用改发布方。

## 3. 公共库（步骤一）

所有公共库放在 `pkg/`，用 `go doc` 查看接口。这里说明每个库必须保证的行为。

### 3.1 `pkg/retry`

- `retry.Do(ctx, policy, fn)`：按策略重试 `fn`。策略包含最大次数、初始间隔、最大间隔、抖动比例、判断错误是否可重试的函数。
- 退避是带抖动的指数退避。等待期间 ctx 取消要立即返回。
- `retry.Backoff(policy, attempt)`：只算间隔，给不能阻塞等待的调用方用（例如支付回调的退避）。
- 抖动的随机源可注入，测试里用固定序列。

收敛对象：inventory 的事务死锁重试。

### 3.2 `pkg/worker`

- `worker.Loop`：实现 `app.Component` 的周期任务。按固定间隔调用一个函数；上一轮没跑完不启动下一轮；函数返回错误只记日志不退出；`Stop` 等当前一轮跑完。
- 支持“提前唤醒”：`Loop.Kick()` 让下一轮立即开始（不排队，多次调用合并成一次）。outbox 中继用它在事务提交后立刻发送，而不必等下一个轮询周期。
- 支持可选的“只在持有锁时运行”：传入一个 `Guard`（见 3.5），拿不到锁的这一轮直接跳过。

收敛对象：order 的 sweeper、payment 的 processor。

### 3.3 `pkg/kafkax`

底层是 franz-go（见 ADR-0007）。`NewClient` 和 `EnsureTopics` 已提供。

**生产者** `Producer`：

- `Send(ctx, ...Record) error`：同步发送并等待 broker 确认，`acks=all`，开启幂等生产者。返回 nil 表示全部记录已持久化。
- 部分失败时返回错误，并且能知道哪些记录成功了（outbox 中继只标记成功的）。
- 记录按 key 分区：相同 key 的记录进同一分区。

**消费者** `Consumer`（实现 `app.Component`）：

- 消费者组，手动提交位点。**处理成功后才提交**，所以是至少一次投递。
- 同一分区内的消息串行处理、严格按位点顺序；不同分区并行。
- handler 返回错误时原地重试（带退避），期间该分区不前进。保持顺序比吞吐重要。
- 超过最大重试次数，或 handler 返回 `kafkax.Permanent(err)`（明确表示重试也没用）时，把消息原样发到死信 topic `<topic>.dlq`（附带错误信息、原 topic、分区、位点的 header），记 error 日志，然后继续。死信发送失败则继续重试，不能丢。
- 再平衡时：被收回的分区先把已处理完的位点提交掉，再交出去。
- `Stop` 等在途的 handler 跑完并提交位点。

**延迟消费**（步骤三用，接口先定）：`DelayConsumer` 读一个“延迟 topic”，每条消息带到期时间；队首未到期就暂停该分区直到到期，到期后交给 handler。同一个延迟 topic 里的消息延迟时长相同，所以分区内天然按到期时间有序。

**测试辅助** `pkg/testkafka`：`testkafka.New(t)` 返回 broker 地址和一个建唯一 topic 的函数，测试结束时删除。环境变量 `OPPENHEIMER_TEST_KAFKA_BROKERS` 未设置时跳过（CI 里视为失败），和 `testdb` 的约定一致。

### 3.4 `pkg/outbox`

表结构（各服务在自己的迁移里建，SQL 由本包以常量形式给出）：

```sql
CREATE TABLE outbox (
    id            BIGINT          NOT NULL AUTO_INCREMENT,
    topic         VARCHAR(128)    NOT NULL,
    msg_key       VARBINARY(128)  NOT NULL,
    payload       MEDIUMBLOB      NOT NULL,
    created_at    DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at  DATETIME(6)     NULL,
    PRIMARY KEY (id),
    KEY idx_outbox_pending (published_at, id)
) ENGINE = InnoDB;
```

- `outbox.Add(ctx, tx, Message)`：在调用方的事务里插入一行。只接受 `*sql.Tx`，从类型上保证不会在事务外调用。
- `Relay`（`app.Component`，内部用 `worker.Loop`）：
  1. 读 `published_at IS NULL` 的行，按 `id` 升序，一批最多 N 条。
  2. 用 `Producer.Send` 同步发送。消息带 header `event-id = <service>:<id>`。
  3. 把发送成功的行标记 `published_at = now`。
  4. 定期删除 `published_at` 早于保留期（默认 1 小时）的行，分批删。
- 提交事务后调用 `Relay.Kick()` 立即触发一轮，延迟就不取决于轮询间隔。轮询只是兜底（进程在提交后、Kick 前崩溃）。

必须理解的几个点：

- **为什么按 `published_at IS NULL` 扫，而不是记一个“已发到哪个 id”的游标**。自增 id 在插入时分配，不是在提交时。事务 A 拿到 id 5，事务 B 拿到 id 6，B 先提交。中继这时能看到 6 看不到 5，如果把游标推到 6，A 提交后 5 就永远不会被发出去。按状态扫描不存在这个问题。
- **至少一次**。发送成功但标记失败（进程崩溃）会导致重发。消费者必须幂等。
- **顺序**。同一订单的两次状态变更由订单行的行锁串行，后一个事务一定在前一个提交之后才插入 outbox，所以同一个 key 的事件 id 递增且前者先可见；中继按 id 升序发送，幂等生产者保证分区内不乱序。但失败重发可能让一条旧事件排到新事件后面，所以**消费者不得依赖严格有序**，要以状态机的前置条件判断，而不是假设“先来的一定先发生”。
- **多个中继实例**同时运行是安全的（只会多发），但浪费。可以传入锁让它单实例运行。锁只是优化，正确性不依赖它。
- **事务里不能发 Kafka**。把发送放进事务，事务的时长就取决于网络，而且发送成功后事务仍可能回滚，等于通知了一件没发生的事。

### 3.5 `pkg/lock`

Redis 分布式锁。

- 加锁：`SET key token NX PX ttl`，token 是随机值。
- 解锁：Lua 脚本先比较 token 再删除。不能直接 `DEL`：锁可能已经过期并被别人拿走，直接删会删掉别人的锁。
- 续期（看门狗）：持锁期间按 `ttl/3` 的间隔用 Lua 脚本校验 token 后延长过期时间；续期失败（锁已丢）要通知持有者（一个会被取消的 context）。
- **fencing token**：每次加锁成功返回一个单调递增的序号（`INCR`）。持锁者访问受保护的资源时带上它，资源侧拒绝比已见过的序号小的请求。这是锁过期后仍在运行的“僵尸持有者”的唯一可靠防线。
- `Guard` 接口：`worker.Loop` 用它判断这一轮是否该跑。

要有两个刻意制造失效的测试：持锁方停顿超过 ttl 后锁被别人拿到，旧持有者解锁不影响新持有者；旧持有者带着旧的 fencing token 写入被拒绝。

**不用在库存扣减上**。扣减用单条 SQL 的条件更新（步骤三是 Lua），本身就是原子的。在外面再套一把锁只会把并发变成串行，并且引入“锁过期了但操作还在进行”的新问题。

### 3.6 `pkg/redisx`、`pkg/testredis`

已提供。所有使用 Redis 的代码都要接受 key 前缀，测试用 `testredis.New(t)` 拿到隔离的前缀。

## 4. 事件与 topic（步骤二）

| topic | 发布方 | 消息类型 | key | 消费方（消费者组） |
|---|---|---|---|---|
| `order.events` | order | `OrderEvent` | `order_id` | inventory（`inventory.order-events`）、payment（`payment.order-events`） |
| `payment.events` | payment | `PaymentEvent` | `order_id` | order（`order.payment-events`） |

- 本机每个 topic 4 个分区。服务启动时用 `kafkax.EnsureTopics` 创建自己发布和消费的 topic 及其死信 topic。
- topic 名和消费者组名都带环境变量 `KAFKA_PREFIX` 指定的前缀（默认空），端到端测试用它和本机手动运行的环境隔离。
- 消息体是 proto 二进制。
- 消费者按 `oneof` 分发，不认识或不关心的事件直接确认跳过。

新增配置：

| 变量 | 说明 | 默认 |
|---|---|---|
| `KAFKA_BROKERS` | 逗号分隔的 broker 地址 | `127.0.0.1:9092` |
| `KAFKA_PREFIX` | topic 和消费者组的前缀 | 空 |
| `REDIS_ADDR` | Redis 地址（步骤三起使用） | `127.0.0.1:6379` |
| `REDIS_PREFIX` | Redis key 前缀 | 空 |
| `OUTBOX_INTERVAL` | outbox 中继的兜底轮询间隔 | `100ms` |

## 5. 各服务的改动（步骤二）

通用要求：

- 每个服务新增迁移 `00002_outbox.sql` 建 outbox 表（以及下面列出的列变更）。
- handler 和消费逻辑分开：消费逻辑写成 `func(ctx, *eventv1.XxxEvent) error`，用真实 MySQL 加直接调用来测，不需要 Kafka。Kafka 的行为由 `kafkax` 自己的测试和端到端测试覆盖。
- 每个消费逻辑都要有“同一事件处理两次，结果和副作用与一次相同”的测试，以及与在线路径并发的测试。
- 把各自的死锁重试、后台循环换成 `pkg/retry`、`pkg/worker`。

### 5.1 order

**发布**。以下每个状态转换，在同一个事务里 `outbox.Add` 一条 `OrderEvent`：

| 转换 | 事件 |
|---|---|
| `PENDING_PAYMENT → PAID` | `paid` |
| `PENDING_PAYMENT → CANCELLED` | `cancelled` |
| `PENDING_PAYMENT → CLOSED` | `closed` |
| `CREATING → FAILED`（任何原因，包括创建超时） | `failed` |

事件只在条件更新影响 1 行时写入，这样“事件存在”和“转换由本事务完成”严格等价。

`CREATING → FAILED` 一律发事件，不再区分预占是否存在：Release 是幂等的，预占不存在时留下空回滚标记。1a 里“被拒绝后立即尽力 Release 一次”的同步调用删掉，这条路径上不再有任何对库存的同步写调用。

**删除**：`settle`、sweeper 的“补结清”、`stock_settled` 列和它的索引（迁移里删）。sweeper 保留“超时关单”和“创建超时”两类，它们现在只做状态转换加写事件。

**消费 `payment.events`**。`PaymentCompleted` 的处理就是 1a 的 `HandlePaymentResult`，改动只有：

- 每个分支在同一个事务里 `outbox.Add` 一条 `payment_result_handled` 答复，和 `payment_results` 的记录一起提交。
- 1a 里以错误返回的两种情况改为明确的处置，这样消费者不会因为业务原因卡住：
  - 金额或币种不一致：记录并答复 `REFUND_REQUIRED`，`reason = AMOUNT_MISMATCH`。
  - 订单不存在：答复 `REFUND_REQUIRED`，`reason = ORDER_NOT_FOUND`。
- `payment_results` 里已有该 `payment_id`：什么都不做（答复事件已在当时的事务里写过）。
- 订单被置为 `PAID` 时，同一个事务里还要写 `paid` 事件。一个事务，两条 outbox 记录。

gRPC 的 `HandlePaymentResult` 及其请求、响应消息从 `order.proto` 里删除，`PaymentDisposition` 改用事件 proto 里的定义。只保留一条处理路径。

**并发**：支付结果的消费与取消、关单并发时，仍然靠“都以 `PENDING_PAYMENT` 为前置条件”保证只有一个成功。输给取消或关单的支付得到 `REFUND_REQUIRED`。这一点 1a 已经做对，这里要在消息可能**重复、延迟、乱序**的条件下重新验证：

- 重复：`payment_results` 以 `payment_id` 去重。
- 延迟：支付结果在订单关闭很久之后才到达，走“其他状态 → 退款”分支。
- 乱序：一个订单只有一笔支付单，一笔支付只有一个最终结果，没有“两条消息谁先谁后”的问题。答复事件与 `paid` 事件在同一事务里产生，谁先被消费都不影响结果。

### 5.2 payment

**发布**。processor 的“出结果”一步，在把支付单从 `PENDING` 改为 `SUCCEEDED` 或 `FAILED` 的同一个事务里写 `PaymentEvent.completed`。条件更新影响 0 行就不写。

**删除**：processor 的“回调”一步、对 `order.HandlePaymentResult` 的调用、`notify_attempts` 列。

**消费 `order.events`**，只处理 `payment_result_handled`：

- `ACCEPTED`：`UPDATE payments SET notified = 1 WHERE payment_id = ? AND notified = 0`。
- `REFUND_REQUIRED`：`UPDATE payments SET status = REFUNDED, notified = 1 WHERE payment_id = ? AND status = SUCCEEDED`；若该支付本来就是失败的（没有钱可退），只置 `notified = 1`。原因是 `AMOUNT_MISMATCH` 或 `ORDER_NOT_FOUND` 时记 error 日志，这两种情况说明系统有缺陷。
- 支付单不存在：返回 `kafkax.Permanent`，进死信。

`notified` 的含义从“回调已成功”变为“订单服务已答复”，对外接口不变。

### 5.3 inventory

**消费 `order.events`**：

| 事件 | 动作 |
|---|---|
| `paid` | Confirm |
| `cancelled`、`closed`、`failed` | Release |
| 其他 | 跳过 |

直接调用现有的 Confirm、Release 业务逻辑（不经过 gRPC），它们已经以 `order_id` 幂等。

以下结果说明订单与库存出现了不可能的组合，返回 `kafkax.Permanent` 进死信并记 error 日志，不自作主张修复：

- Confirm 返回 `RESERVATION_RELEASED` 或 `RESERVATION_NOT_FOUND`。
- Release 返回 `RESERVATION_CONFIRMED`。

这替代了 1a 里“保持未结清、等人工介入”的做法：死信 topic 就是待人工处理的队列。

gRPC 的 Confirm、Release 保留（测试和排障用），订单服务不再调用它们。

### 5.4 gateway

步骤二没有改动。

### 5.5 本机运行与测试

- `scripts/run-local.sh` 给各服务传 `KAFKA_BROKERS`；端到端测试设置 `KAFKA_PREFIX`，每次运行用不同的前缀，避免读到上一次运行留在 topic 里的消息（数据库每次都重建，旧消息对不上）。
- `make test` 额外导出 `OPPENHEIMER_TEST_KAFKA_BROKERS` 和 `OPPENHEIMER_TEST_REDIS_ADDR`。
- CI 的 test 和 e2e 任务改为用 `make up` 启动依赖（和本机相同的 compose 文件），不再单独声明 MySQL 服务容器。
- 端到端测试的断言不变：7 个场景都必须通过。库存的最终状态现在是异步到达的，测试里已经是轮询等待。

## 6. 步骤三方案要点（动工前细化）

- **Redis 预扣**：Redis 里保存每个 SKU 的可售数量，作为进入系统的闸门。一段 Lua 脚本原子地完成：幂等检查（同一幂等键返回已有订单号）、各 SKU 库存检查、用户限购检查、扣减、记录本次占用（hold）。MySQL 仍是库存的唯一事实来源，Redis 的数量允许偏小（少卖），不允许偏大（超卖由 MySQL 的条件更新兜底）。
- **占用记录**让归还幂等：归还时先看 hold 是否存在，存在才加回并删除。订单失败、取消、关闭的事件触发归还。
- **预热与同步**：启动或上架时从 MySQL 装载；定期对账，Redis 数量应等于 MySQL 可售数量减去“已占用但尚未在 MySQL 预占”的数量，偏差超过阈值告警并修正。
- **异步下单**：gateway 调 order 的受理接口，预扣成功后把下单请求写入 topic `order.submit` 并立即返回 202 和订单号；order 的消费者落库并走 1a 的“推进”流程；客户端轮询订单状态。受理成功但消息没发出去（进程崩溃）的占用由对账归还。
- **延迟关单**：倾向于固定时长的延迟 topic（`DelayConsumer`），保留低频的 sweeper 扫描兜底。动工时与时间轮、Redis ZSET 对比并写 ADR。

## 7. 步骤四方案要点（动工前细化）

- **分布式锁的使用**：outbox 中继和 sweeper 单实例运行；缓存重建互斥。
- **缓存**：给库存查询和订单查询加缓存，分别处理穿透（缓存空值）、击穿（重建互斥）、雪崩（过期时间加抖动）。
- **杀进程对账测试**：压测过程中随机 `kill -9` 各服务进程，全部恢复并等事件消费完之后，订单、库存、支付三方数据对账零差异。

## 8. 测试重点（步骤一、二）

- `outbox`：事务回滚则消息不存在；提交则一定被发出；发送成功但标记前崩溃会重发；并发事务以乱序提交时不丢消息（构造 id 小的事务后提交）；中继的两个实例并发运行不丢消息。
- `kafkax`：处理成功才提交位点（处理中途停止，重启后重新收到）；handler 失败重试期间该分区不前进；`Permanent` 和超过重试次数进死信；同一分区保持顺序。
- `lock`：互斥；解锁不误删别人的锁；看门狗续期；锁丢失时通知持有者；fencing token 单调递增。
- 各服务：每个消费逻辑的重复投递幂等性；支付结果与取消、关单并发；订单不存在和金额不符的处置。
- 端到端：1a 的 7 个场景不改断言全部通过。
