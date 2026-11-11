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

第四步的细节在动工前补到本文（第 7 节目前只有方案要点）。

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

实现后确认的限制（写 handler 时必须记住）：

- **handler 必须遵守 ctx 并有自己的超时**。分区被收回时要等在途的 handler 返回，卡住超过再平衡超时（默认 60 秒）后本实例会被踢出消费者组，新的持有者开始处理同一分区，两边会并发处理同一条消息。
- **死信发不出去时该分区会一直停住**，不会跳过。这是有意的（不能丢），需要靠积压告警发现。
- **`Send` 返回失败不代表没有写入**（例如 broker 写入后、回复前超时）。调用方重发会产生重复，所以 outbox 中继是至少一次。
- **延迟消费者只看队首**，同一个延迟 topic 里的延迟时长必须相同；到期时间以生产方的时钟为准。
- `Message.Headers` 是 map，同名 header 只保留最后一个。

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

实现后确认的限制（写代码时必须记住）：

- `Guard` 只回答“这一轮能不能跑”，这一轮运行期间丢了锁，任务不会知道。所以“持锁才运行”只能用在**重复执行无害**的任务上（outbox 中继、sweeper 都是），不能用来保护不幂等的操作。
- fencing 计数器的 key 不过期，锁名的数量必须有限。不要按订单号这类无限增长的名字加锁。
- 加锁请求超时的时候，Redis 可能已经加上了锁而调用方不知道，这把无主的锁要等 TTL 才释放。
- 判定丢锁有滞后，最坏要到 TTL 之后再过约 TTL/3。

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

- 发布事件的服务（order、payment）新增迁移 `00002_outbox.sql` 建 outbox 表（以及下面列出的列变更）。inventory 在步骤二只消费不发布，不建表，等它真要发布事件时再加迁移。
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
  - 订单不存在：支付成功的答复 `REFUND_REQUIRED`，`reason = ORDER_NOT_FOUND`；支付失败的答复 `ACCEPTED`（没有收钱，不能让支付服务去退一笔不存在的款）。
  - 订单存在但不可支付（已取消、已关闭、创建失败、创建中、已被付清）：答复 `REFUND_REQUIRED`，`reason = ORDER_NOT_PAYABLE`。
- `payment_results` 里已有该 `payment_id`：什么都不做（答复事件已在当时的事务里写过）。若记录的订单号与事件里的不同，说明事件与已记录的事实矛盾，进死信。
- 订单不存在时没有订单行锁可以排队，同一 `payment_id` 的并发处理会在插入 `payment_results` 时撞主键。主键冲突按可重试处理：重做事务时读到已有记录，走“已处理过”的分支。
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
- `REFUND_REQUIRED`：`UPDATE payments SET status = REFUNDED, notified = 1 WHERE payment_id = ? AND status = SUCCEEDED AND notified = 0`；若该支付本来就是失败的（没有钱可退），只置 `notified = 1`。原因是 `AMOUNT_MISMATCH` 或 `ORDER_NOT_FOUND` 时记 error 日志，这两种情况说明系统有缺陷。
- 支付单不存在、答复里的订单号与支付单不符、不认识的处置：返回 `kafkax.Permanent`，进死信。
- 答复到达时支付单还是 `PENDING`：返回普通错误让消费者重试，不做任何标记。正常不会发生（事件和状态变更同事务提交），但消费者不假设这一点。

**第一次答复为准**（实现时补充，原设计的退款语句没有 `notified = 0`）。退款的前置条件必须带 `notified = 0`：否则一条 `ACCEPTED`（订单已付清）之后再来一条矛盾的 `REFUND_REQUIRED`，就会把已付清订单的钱退掉。订单服务按 `payment_id` 去重，正常不会给出两个不同的答复；但退款是不可逆的资金操作，不能把正确性押在对方没有缺陷上。矛盾的答复记 error 日志后忽略。

已知的缺口：没有任何任务扫描“已出结果但长期 `notified = 0`”的支付单。答复进了死信或订单服务一直没处理时，只能靠死信和积压告警发现。留给 1d 的对账。

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

### 5.6 复核结论：消息重复、延迟、乱序下的并发正确性

步骤二合入后逐行评审了三个服务，结论是 1a 的保证在事件化之后仍然成立。依据如下。

**每个订单至多一条终态事件。** 状态机只有 `CREATING → FAILED | PENDING_PAYMENT` 和 `PENDING_PAYMENT → PAID | CANCELLED | CLOSED`，每个转换都是带前置状态的条件更新，事件只在影响 1 行的那个事务里写。所以支付、取消、关单三方并发时只有一方写出事件，库存服务对一个订单只会看到 `paid` 或者一种释放事件，不会两种都看到。万一看到了（说明有缺陷），Confirm 与 Release 互相拒绝，消息进死信，不会悄悄改库存。

| 情况 | 订单侧（消费 `payment.events`） | 支付侧（消费答复） | 库存侧（消费 `order.events`） |
|---|---|---|---|
| 重复 | 订单行锁下以 `payment_results` 主键去重，第二次什么都不写 | 条件更新带 `notified = 0`，第二次影响 0 行 | Confirm、Release 以 `order_id` 幂等 |
| 延迟 | 订单已不是 `PENDING_PAYMENT`，答复退款 | 无影响 | 无影响 |
| 乱序 | 一笔支付只有一个最终结果，没有先后问题 | 第一次答复为准，矛盾的答复忽略并记 error | `failed` 先于迟到的 Reserve 到达时，空回滚标记挡住它 |

**证据。** 各服务的单元测试覆盖了上表每一格（重复投递比较行数与副作用，并发场景带 `-race`）。端到端的 `TestPayRacingCancelEndsConsistent` 一次运行里 40 个订单 24 个付清、16 个取消，订单服务恰好记了 16 次“需要退款”，两个死信 topic 都是空的。

**仍然存在的缺口**（都不影响上面的结论，记在这里避免以为已经解决）：

- 支付单“已出结果但长期 `notified = 0`”没有任何任务扫描，见 5.2。
- 订单侧不再有库存的兜底：1a 的 `stock_settled` 加补结清没有了，库存是否跟上完全取决于事件被消费。消费卡住（死信发不出去、分区停住）时只能靠积压告警发现，对账在 1d。
- 迁移 `00002` 直接删除 `stock_settled`。升级时库里若还有未结清的终态订单，它们不会再被补结清，也没有事件。本项目只有开发库，没有处理；真实系统要先排空再升级。
- outbox 中继没有配分布式锁，多副本时会重复发送（正确，只是浪费），步骤四处理。
- 库存服务对 Confirm 遇到“预占不存在”直接进死信，前提是“订单进入 `PENDING_PAYMENT` 就说明 Reserve 已提交”。步骤三保持了这个前提（见 6.3）。

## 6. 步骤三：削峰

步骤二之后，下单请求仍然同步完成“写订单库、调库存服务、库存服务写库”才返回。秒杀时绝大多数请求注定买不到，却都要走完这条路才知道。步骤三做三件事：

1. **在入口挡住买不到的请求**：Redis 里的库存闸门，一段 Lua 脚本决定这个请求能不能进。
2. **进来的请求排队慢慢处理**：受理后写入 Kafka 立即返回，订单服务按自己的速度消费落库。
3. **超时关单不再靠扫表**：延迟消息到期触发，扫表降为低频兜底（见 [ADR-0009](../adr/0009-delayed-close.md)）。

### 6.1 总体流程

```
客户端 ─POST /orders─▶ gateway ─gRPC SubmitOrder─▶ order
                                                    │ ① Lua：幂等、库存、限购、扣减、记占用
                                                    ▼
                                                  Redis（库存闸门）
                                                    │ ② 发 order.submit 和 order.close.delay
                                                    ▼
   ◀──────────── 202 + order_id ──────────────── Kafka
                                                    │ ③ order 消费 order.submit
                                                    ▼
                                    认领占用 → 写订单行(CREATING) → Reserve(同步 gRPC) → PENDING_PAYMENT
客户端 ─GET /orders/{id} 轮询─▶ 看到 CREATING → PENDING_PAYMENT（或 FAILED）
```

之后的支付、取消、关单与步骤二相同。订单进入终态后，库存服务消费 `order.events`，先改 MySQL（Confirm 或 Release），再改 Redis（结清或归还占用）。

**MySQL 仍是库存的唯一事实来源。** Redis 的数量只决定“放不放进来”，允许偏小（少放，表现为少卖），不允许偏大。偏大时不会超卖（MySQL 的条件更新兜底），但会放进来一批注定在 Reserve 时失败的订单。所有涉及两边的操作都按“会让 Redis 变小的先做 Redis，会让 Redis 变大的后做 Redis”的顺序：扣减先扣 Redis 再扣 MySQL，归还先还 MySQL 再还 Redis。

### 6.2 库存闸门 `pkg/stockgate`

闸门的 key 约定和 Lua 脚本被两个服务使用（order 受理时扣减，inventory 预热、归还、对账），所以和 `api/eventx` 一样放在一处，避免两边各写各的。所有 key 带 `REDIS_PREFIX`。

| key | 类型 | 内容 |
|---|---|---|
| `gate:sku:<sku_id>` | hash | `avail` 可放行数量；`held` 现有占用的数量之和；`limit` 每个用户的限购数量，0 表示不限 |
| `gate:skus` | set | 已装载进闸门的 sku_id，对账任务遍历它 |
| `gate:bought:<sku_id>` | hash | `user_id → 数量`，该用户占用中和已买到的数量 |
| `gate:hold:<order_id>` | hash | 一笔占用：`user`、`items`（`sku:qty,sku:qty`）、`claimed`、`ts` |
| `gate:holds` | zset | `order_id → 受理时间`，**尚未被认领**的占用，回收任务按时间找旧的 |
| `gate:claimed` | zset | `order_id → 受理时间`，已被认领、订单还没结束的占用，用来发现认领后卡住的 |
| `gate:idem:<user_id>:<幂等键>` | string | `order_id:请求哈希`，带过期时间（默认 24 小时） |

**占用（hold）是整套设计的核心。** 它是一条以订单号为主键的记录，表示“闸门为这个订单扣过这些数量”。它存在，归还才生效；归还时删除它。所以归还天然幂等：同一个订单的归还执行多少次，只有第一次加回数量。占用从受理一直活到订单进入终态，而不是在 MySQL 预占成功后就删除，原因是之后的取消和关单还要靠它做幂等归还和限购回退。

脚本（每个都是一次原子执行）：

| 脚本 | 调用方 | 行为 |
|---|---|---|
| `Hold` | order 受理 | 幂等键已存在：返回已有订单号（请求哈希不同则报幂等键被复用）。否则逐个 SKU 检查已装载、数量够、不超限购，全部通过后扣 `avail`、加 `held`、加用户已购、写占用、写幂等键 |
| `Claim` | order 消费 `order.submit` | 占用存在则置 `claimed = 1`，把它从 `gate:holds` 移到 `gate:claimed`，返回占用内容；不存在返回“没有” |
| `Return` | inventory（Release 之后） | 占用存在：加回 `avail`、减 `held`、减用户已购、删除占用，**保留幂等键**（订单已落库，重放要得到同一个订单）。不存在：什么都不做 |
| `ReturnIfUnclaimed` | order 的回收任务；order 受理后发消息失败 | 只对 `claimed` 不为 1 的占用生效：归还并**删除幂等键**（订单没有落库，用户要能用同一个键重新下单） |
| `Abort` | order 落库时发现幂等键早已有订单 | 归还并删除幂等键，**不看认领状态**。只能由已经认领了这笔占用、且确定不会落库的一方调用 |
| `Settle` | inventory（Confirm 之后） | 占用存在：减 `held`、删除占用。`avail` 和用户已购不动（已经卖掉了） |
| `Load` | inventory 预热 | SKU 未装载时写入 `avail`、`limit`，`held = 0`；已装载时只更新 `limit` |
| `Adjust` | inventory 对账 | 把 `avail` 加上一个增量（可为负），见 6.6 |

`Hold` 的几种拒绝都发生在任何写入之前，所以一个订单的多个 SKU 要么全部扣到，要么都不扣。

限制（写代码时必须记住）：

- 脚本在内部用前缀拼出 key 名，没有全部通过 `KEYS` 声明。这在单实例上没有问题，在 Redis Cluster 上不行（集群要求脚本访问的 key 都在同一个槽）。上集群时要么只支持单 SKU 下单并用 hash tag 把相关 key 放进同一个槽，要么把闸门按 SKU 分片。本项目步骤三只用单实例，这一点记入面试考点。
- 占用没有过期时间。让它自己过期等于不归还就丢掉记录，数量永远少一块。孤儿占用由回收任务处理（6.4），整体偏差由对账处理（6.6）。
- 幂等键过期之后，同一个幂等键会被当作新请求放进来。这时订单库的唯一键 `(user_id, idempotency_key)` 是最后一道防线，见 6.3 的第 3 步。

### 6.3 受理与落库

**受理** `SubmitOrder`（gRPC，替换 `CreateOrder`）：

1. 校验请求，生成订单号，算出支付截止时间 `expire_at = 受理时间 + 支付时限`。
2. 执行 `Hold`。
   - SKU 未装载：调 inventory 的 `WarmUp`（6.5）后重试一次，仍未装载则返回 `SKU_NOT_FOUND`。
   - 数量不够返回 `INSUFFICIENT_STOCK`，超出限购返回 `PURCHASE_LIMIT_EXCEEDED`，幂等键被复用返回 `IDEMPOTENCY_KEY_REUSED`。
   - 幂等键重复：返回已有的订单号，不再发消息。
3. 一次 `Send` 发出两条消息：`order.submit`（下单请求）和 `order.close.delay`（到期时间为 `expire_at` 的延迟消息）。
   - 只有 `order.submit` 发送失败才算失败。延迟消息单独失败时照常受理，关单由 sweeper 兜底。
   - 发送失败：执行 `ReturnIfUnclaimed`，按结果作答。发送失败不代表没写进去（`kafkax` 的已知限制），消息可能已经到了、消费者可能已经认领，所以这里**不能无条件归还**：那会把一个即将在 MySQL 预占成功的订单的占用还掉，还会删掉幂等键让用户重试时下出第二单。
     - 归还成功：本次没有受理，返回 `UNAVAILABLE`。那条可能写进去的消息，消费者 `Claim` 不到就丢弃。
     - 已被认领：消息其实到了，按受理成功作答。
     - 占用已不存在：消息到了且订单已经走完，查一次当前状态作答。
   - `Hold` 本身返回错误（Redis 超时）时结果未知。同样先用 `ReturnIfUnclaimed` 尽力撤销（这个订单号还没发过消息，不可能被认领），再返回 `UNAVAILABLE`。不撤的话，重试会命中幂等键，拿到一个永远不会落库的订单。
4. 返回订单号。网关答复 202。

**为什么这里不用 outbox。** outbox 要求在受理时写一次 MySQL，而受理路径不碰 MySQL 正是削峰的目的。代价是“Redis 扣了、消息没发出去就崩溃”的窗口没有本地事务兜底，留下一笔孤儿占用，由 6.4 的回收任务处理。这是有意的取舍：受理路径只碰内存和顺序写的日志，换来的不一致窗口只会让库存暂时偏小，并且有任务收敛它。

**落库**：订单服务消费 `order.submit`（消费者组 `order.order-submit`），消息体是 `OrderSubmitted`。

1. `Claim`。占用不存在就丢弃消息：它要么已被回收，要么订单早已走到终态（重复投递）。
2. 写订单行，状态 `CREATING`。订单号已存在说明是重复投递，读出来继续。
3. 幂等键唯一键冲突而订单号不同（幂等键在 Redis 里过期后的重放）：这笔订单不成立。先把幂等键补回指向原来的订单，再 `Abort` 这笔占用，丢弃消息。这个顺序保证任何一步失败后重试都能接着做完。这个订单号从未到过库存服务，不存在需要释放的预占。
4. 落库时支付时限已经过了（消费积压超过了支付时限）：不预占，直接转 `FAILED`（原因 `CREATE_TIMEOUT`）并写 `failed` 事件。继续推进的话订单进入待支付又立刻被关闭，用户没有时间支付，库存白占一轮；而且延迟关单的消息早已处理过，没有人会准时来关它。
5. 走 1a 的“推进”：同步调用 `Reserve`，成功则转 `PENDING_PAYMENT`；被拒绝则转 `FAILED` 并写 `failed` 事件（库存服务据此 Release 并归还占用）；结果未知（超时）则返回错误，由消费者原地重试。重试耗尽进死信，订单留在 `CREATING`，由 sweeper 的“创建超时”置为失败。创建超时从落库时刻（`updated_at`）算起，不从受理时刻算：否则积压超过这个时限的订单一落库就会被 sweeper 抢着置为失败。

`Claim` 和回收任务的 `ReturnIfUnclaimed` 在 Redis 里互斥，这是它存在的理由，见 6.4。

**订单进入 `PENDING_PAYMENT` 之前预占一定已经提交**（`Reserve` 仍是同步调用）。所以 5.6 里提到的隐患没有出现：`paid` 不可能先于预占到达，库存服务对“预占不存在”仍然进死信，不用改。

**查询**。受理后、落库前，订单库里还没有这一行。`GetOrder` 查不到订单行时再查占用：占用存在且属于该用户，返回一个状态为 `CREATING` 的订单（只有商品和数量，没有价格）。两边都没有才是 404。

**对外接口**（先改 OpenAPI 规范）：`POST /api/v1/orders` 成功时返回 202 和订单（状态 `CREATING`），带 `Location` 头；客户端轮询 `GET /api/v1/orders/{order_id}` 直到状态离开 `CREATING`。库存不足、超出限购、SKU 不存在仍然同步返回错误，因为闸门当场就能判断。闸门放行后才在 MySQL 失败的订单（只在 Redis 偏大时出现），客户端通过轮询看到 `FAILED`。

### 6.4 孤儿占用的回收

受理成功但消息没发出去（进程在第 2、3 步之间崩溃），占用就没人认领。订单服务的回收任务定期处理：取 `gate:holds` 里早于宽限期（默认 1 分钟）的占用，对每一个执行 `ReturnIfUnclaimed`。

宽限期解决不了的问题是消费积压：消息还在 Kafka 里排队，只是超过了宽限期没被消费。如果回收任务这时把占用还了，稍后消费者又把订单建出来并在 MySQL 预占成功，Redis 就比 MySQL 多出一份，而且不会自己恢复。所以要让“回收”和“落库”二选一：

- 消费者落库前先 `Claim`，回收任务只还未认领的。两个脚本在 Redis 里原子执行，同一笔占用只有一方得手。
- 回收先得手：消费者 `Claim` 失败，丢弃消息。用户看到的是订单消失（404），库存已归还。
- 认领先得手：回收任务跳过它，占用由订单的终态事件归还。

认领时把占用从 `gate:holds` 移到 `gate:claimed`，回收任务只扫前者。原因是已认领的占用要活到订单结束（待支付的订单最长是整个支付时限），留在同一个列表里会一直排在最前面，数量超过回收任务的一批之后，排在后面的孤儿就永远轮不到。这个问题是实现时由子 Agent 用测试复现出来的。

认领之后卡住的占用（认领后消息进了死信）不会被自动归还，因为不知道 MySQL 里有没有预占。回收任务对 `gate:claimed` 里超过支付时限两倍的占用记 error 日志，等人工处理。

仍然存在的缺口：

- **重放的用户会看到订单消失**。幂等键在 Redis 里过期后重放，用户先拿到新订单号的 202，随后这个订单查不到（404），他并不知道原来的订单号。再提交一次就能得到原订单（幂等键已补回）。
- **幂等键重复时不再还原错误**。重放一个已经失败的订单，`SubmitOrder` 返回状态为 `FAILED` 的订单，而不是像 1a 那样返回当初的错误。客户端要看状态。

### 6.5 预热

闸门里的数量从 MySQL 装载。inventory 新增 gRPC `WarmUp(sku_ids)`：对每个 SKU 读出 `available` 和限购数量，执行 `Load`。`Load` 只在未装载时写入数量，所以重复调用、多个实例并发调用都是安全的。

- 按需装载：order 受理时遇到未装载的 SKU 才调用。不做启动时全量装载，SKU 很多时启动会很慢，而且绝大多数用不到。
- 限购数量放在 `sku_stock.purchase_limit`（迁移新增，0 表示不限）。

未装载的 SKU 没有任何占用（占用必须先有 SKU 的 key），所以装载时直接取 MySQL 的 `available` 是准确的。唯一的例外是 Redis 丢了数据，见 6.6。

### 6.6 对账与修正

稳定状态下有一个恒等式：

```
Redis: avail + held  ==  MySQL: available + reserved
```

两边都等于“还没卖掉的数量”。逐个操作看：受理时 `avail` 减、`held` 加，左边不变；MySQL 预占时 `available` 减、`reserved` 加，右边不变；释放和归还同理；支付确认时右边减少（`reserved` 转 `sold`），随后 `Settle` 让左边减少同样的数量。只有“Confirm 已提交、Settle 还没执行”的瞬间两边不等。

inventory 的对账任务（默认每 5 秒）遍历 `gate:skus`，算偏差 `d = (avail + held) - (available + reserved)`：

- `d == 0`：正常。
- `d != 0`：可能只是瞬时的。同一个 SKU 连续 3 轮得到相同的非零 `d` 才认定为真实偏差，记 error 日志并用 `Adjust` 修正。
- 修正用增量（`avail += -d`）而不是把 `avail` 设成某个值：读 MySQL 和读 Redis 不是同一时刻，直接覆盖会抹掉期间发生的扣减。
- 修正后的 `avail` 不超过 MySQL 的 `available`。Redis 比 MySQL 可售还大一定是错的。

最后一条让“Redis 丢了全部数据”也能收敛：重新装载后 `avail = available`、`held = 0`，而 MySQL 里还有丢失了占用的在途订单（`reserved > 0`），`d` 为负。不设上限的话对账会把 `avail` 调高到 `available + reserved`，放进来一批注定失败的订单。设了上限，`avail` 保持等于 `available`；在途订单取消或关闭时 MySQL 的 `available` 增加，对账再跟着调高。

Redis 丢数据的另一个后果：已受理但还没落库的订单，占用没了，`Claim` 失败，订单消失。这是把受理状态只放在 Redis 里的代价，要靠 Redis 的持久化配置（AOF）缩小窗口，放到阶段 4 的故障实验里验证。

实现时补充的规则和确认的缺口：

- **对账必须单实例运行**。修正是增量，两个实例对同一个偏差各修一次就是两倍。对账任务用 `pkg/lock` 的 Leader 做 `Guard`。
- **“连续”指相邻的轮次**。上一轮结束到这一轮开始超过两倍间隔（丢过 leader、进程停顿）时计数清零；偏差的值变了也重新计数；修正之后从头再数。
- **限购数量由对账同步**。订单服务只在 SKU 未装载时才触发装载，之后在 MySQL 里改了限购，由对账任务发现并更新闸门。
- **归还失败不能很快放弃**。库存服务处理事件时先改 MySQL 再改 Redis。Redis 不可用时如果按默认的 5 次重试就把消息送进死信，MySQL 已经释放而占用没有归还，恒等式两边都没变，对账看不出来，闸门永久少放这部分数量。所以库存服务的消费者把重试调到 60 次（约 5 分钟），宁可让分区停住等 Redis 恢复。超过之后仍会进死信，要靠人工重放。**对账只比较汇总量，发现不了“占用该还没还”这一类偏差**，逐单核对占用与预占留给 1d 的对账。
- **上限不够紧**。修正的上限是 MySQL 的 `available`，没有扣掉“已受理、还没在 MySQL 预占”的占用。Redis 丢数据后的恢复期里，向上修正可能把这部分也放出来，多放进来的订单会在预占时失败。不会超卖。
- **“连续 3 轮相同”理论上可能是巧合**。持续有支付流量时，每轮都恰好看到同样数量的订单卡在“已确认、未结清”，会被误修，之后再被反向修回。结果正确，期间少放。
- **Redis 的淘汰策略必须是 `noeviction`**，并开启 AOF。`gate:sku:*` 被淘汰而占用还在时，归还和结清会跳过这个 SKU，数量就丢了。本机的 compose 已经这样配置（`deploy/compose/docker-compose.yml`）；上集群时在清单里同样要写出。
- gRPC 的 Confirm、Release（排障用）和事件消费走同一条路径，也会结清或归还占用。

### 6.7 延迟关单

方案对比和选择见 [ADR-0009](../adr/0009-delayed-close.md)。实现：

- topic `order.close.delay`，消息体 `OrderCloseDue{order_id, user_id, expire_time}`，用 `kafkax.NewDelayRecord` 带上到期时间。同一个 topic 里延迟时长相同（都是支付时限），满足 `DelayConsumer` 的前提。
- 受理时和 `order.submit` 一起发出。这条消息丢了（两条消息不是原子的）也没关系，sweeper 兜底。
- 订单服务用 `DelayConsumer` 消费（消费者组 `order.order-close-delay`）。到期后执行和 sweeper 相同的“关单”：以 `PENDING_PAYMENT` 且已过期为前置条件的条件更新，成功则同事务写 `closed` 事件。
  - 订单已不是 `PENDING_PAYMENT`（已支付、已取消）：什么都不做。
  - 订单还在 `CREATING` 或还不存在：什么都不做，创建超时由 sweeper 处理。
  - 订单是 `PENDING_PAYMENT` 但按本机时钟还没过期（到期时间以发送方的时钟为准，两台机器的时钟有偏差）：返回错误，让消费者稍后重试。
- sweeper 的“超时关单”保留，间隔调大（默认 30 秒）。它处理延迟消息丢失、进死信的情况。

### 6.8 新增的 topic、消费者组和配置

| topic | 发布方 | 消息类型 | key | 消费方（消费者组） |
|---|---|---|---|---|
| `order.submit` | order（受理） | `OrderSubmitted` | `order_id` | order（`order.order-submit`） |
| `order.close.delay` | order（受理） | `OrderCloseDue` | `order_id` | order（`order.order-close-delay`，延迟消费） |

| 变量 | 说明 | 默认 |
|---|---|---|
| `IDEMPOTENCY_TTL` | 幂等键在 Redis 里的保留时长 | `24h` |
| `HOLD_REAP_GRACE` | 占用多久没被认领就回收 | `1m` |
| `HOLD_REAP_INTERVAL` | 回收任务的间隔 | `10s` |
| `GATE_RECONCILE_INTERVAL` | 对账任务的间隔 | `5s` |
| `SWEEP_INTERVAL` | sweeper 的间隔，默认值从 1 秒调到 30 秒 | `30s` |

### 6.9 测试重点

- `stockgate`：并发 `Hold` 不超扣（总量恒定）；多 SKU 订单有一个不够时一个都不扣；限购；幂等键重复返回同一订单号、内容不同报复用；`Return` 执行两次只加回一次；`Claim` 与 `ReturnIfUnclaimed` 并发只有一方得手；`Settle` 之后 `Return` 不加回。
- order：受理后发送失败会归还；消费 `order.submit` 两次只有一个订单；占用被回收后消费者丢弃消息；幂等键过期后的重放不产生第二个订单且占用被归还；落库前 `GetOrder` 返回 `CREATING`；延迟关单的各个分支。
- inventory：Release 之后归还、Confirm 之后结清，各自重复执行结果相同；Redis 失败时事件处理返回错误（重试）而 MySQL 的结果不变；对账的三条规则（瞬时偏差不修、稳定偏差按增量修、不超过 `available`）。
- 端到端：现有场景改为“提交后轮询到 `PENDING_PAYMENT`”；新增限购场景；秒杀场景的断言不变（不超卖，卖出数量等于库存）。

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
