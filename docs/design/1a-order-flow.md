# 1a 设计：单库下的下单主流程

本文是阶段 1a 的实现规格。接口契约以 `api/proto/` 为准，表结构以各服务 `migrations/` 为准，本文说明它们背后的算法和理由。

1a 的目标是**把业务做对**：不引入 Redis 和 Kafka，所有一致性靠 MySQL 本地事务、条件更新、幂等和重试保证。它是之后所有改造的对照基线。

## 1. 总体

```
客户端 ──HTTP──▶ gateway ──gRPC──▶ order ──gRPC──▶ inventory
                    │                ▲
                    └──gRPC──▶ payment ┘（查订单、回调支付结果）
```

- 每个服务一个独立的库（`order_db`、`inventory_db`、`payment_db`），互不访问对方的表。
- 跨服务没有分布式事务。统一的做法是：**先在本地事务里完成自己的状态转换，再调用下游；下游接口全部幂等，调用失败就由后台任务重试到成功**。1b 会把“后台任务重试”升级为 outbox 加 Kafka。

### 一笔订单的生命周期

```
            CreateOrder
                │
                ▼
           ┌─────────┐  预占被拒绝 / 创建超时   ┌────────┐
           │CREATING │──────────────────────▶│ FAILED │
           └────┬────┘                        └────────┘
                │ 预占成功
                ▼
        ┌───────────────┐  用户取消   ┌───────────┐
        │PENDING_PAYMENT│───────────▶│ CANCELLED │
        └───┬───────┬───┘            └───────────┘
   支付成功 │       │ 超时未支付
            ▼       ▼
        ┌──────┐ ┌────────┐
        │ PAID │ │ CLOSED │
        └──────┘ └────────┘
```

所有状态转换都用带前置状态的条件更新完成：`UPDATE ... SET status = 新 WHERE order_id = ? AND status = 旧`，以受影响行数判断是否由本次调用完成。两个互斥的转换（例如支付成功与超时关单）同时发生时，数据库保证只有一个成功。

### 库存的三个量

`available`（可售）、`reserved`（已预占未支付）、`sold`（已售），三者之和恒等于入库总量。

| 订单事件 | 库存操作 | 变化 |
|---|---|---|
| 下单 | Reserve | available −n，reserved +n |
| 支付成功 | Confirm | reserved −n，sold +n |
| 取消、关单、创建失败 | Release | reserved −n，available +n |

## 2. 各服务通用约定

**目录结构**

```
services/<svc>/
  cmd/<svc>/main.go        只做装配：读配置、建依赖、交给 app.Run
  internal/store/          SQL，一个方法一条或一组语句，不含业务判断
  internal/service/        业务逻辑与 gRPC handler
  migrations/*.sql         goose 迁移（已提供）
  migrations.go            package <svc>，用 embed 导出迁移文件
```

**配置**（环境变量）

| 变量 | 说明 | 默认 |
|---|---|---|
| `HTTP_ADDR` | 健康检查 | gateway `:8080`，order `:8081`，inventory `:8082`，payment `:8083` |
| `GRPC_ADDR` | gRPC | order `:9081`，inventory `:9082`，payment `:9083` |
| `MYSQL_DSN` | 本服务的库 | `root:oppenheimer@tcp(127.0.0.1:3306)/<svc>_db` |
| `WORKER_ID` | idgen 节点号 | `0` |
| `ORDER_ADDR`、`INVENTORY_ADDR`、`PAYMENT_ADDR` | 下游地址 | `127.0.0.1:<对应端口>` |

启动时用 `mysqlx.Migrate` 自动应用迁移。

**可直接使用的公共库**（`pkg/`，用 `go doc` 查看接口）

- `app`：生命周期。`app.Run` 管理组件，`app.ListenHTTP`、`app.HealthHandler` 提供健康检查。后台任务实现 `app.Component` 接口。
- `grpcx`：`grpcx.Listen` 建服务端（已带日志、panic 恢复、健康检查），`grpcx.Dial` 建客户端（已带默认超时）。
- `mysqlx`：`Open`、`InTx`、`Migrate`、`IsDuplicateKey`、`IsDeadlock`。
- `errx`：业务错误。`errx.New(code, reason, ...)` 构造，`errx.Is(err, reason)` 判断。原因码常量都在这个包里。
- `idgen`：`Generator.Next(userID)` 生成带分片基因的 ID。
- `testdb`：`testdb.New(t, migrations, dir)` 给每个测试一个干净的库。
- `logx`：日志。

**编码规则**

- 金额是 `int64` 的分。做乘法和求和时检查溢出。
- 事务里只有数据库操作，不发 RPC。
- 下游返回的业务错误（带原因码的）原样透传给上游；其他错误包装后返回，由 `grpcx` 记录。
- 不属于业务错误的内部错误，返回 `codes.Internal`，消息里不带 SQL 或内部细节。
- 入参校验失败返回 `INVALID_ARGUMENT`。
- 使用新版 Go 特性（`wg.Go`、`errors.AsType`、`for range n`、`slices`、`maps`、`t.Context()`、`testing/synctest` 等），lint 的 `modernize` 会检查。
- 时间一律 UTC。需要“当前时间”的地方通过可注入的时钟取得，便于测试。

**测试要求**

- 用 `testdb` 在真实 MySQL 上测，不 mock 数据库。下游服务用实现了 gRPC 客户端接口的假对象。
- 每个幂等接口都要有“重复调用结果相同、副作用只发生一次”的测试。
- 每个状态转换都要有并发测试（`-race` 下），断言互斥的转换只有一个成功。
- `make lint test` 通过。

## 3. inventory

表：`sku_stock`、`reservation`、`reservation_item`。`reservation` 每个订单一行，状态只能 `RESERVED → CONFIRMED` 或 `RESERVED → RELEASED`，它同时是三个操作的幂等记录。

### Reserve

校验：`order_id > 0`，至少一项，`quantity > 0`，`sku_id` 不重复。

一个事务内：

1. `INSERT INTO reservation (order_id, status) VALUES (?, RESERVED)`。
   - 唯一键冲突说明该订单已有记录，读出它：
     - `RELEASED` → `FAILED_PRECONDITION / RESERVATION_RELEASED`。
     - `RESERVED` 或 `CONFIRMED` → 与 `reservation_item` 比较。完全一致则返回已存的结果（幂等成功）；不一致返回 `ALREADY_EXISTS / RESERVATION_MISMATCH`。
2. 把请求项**按 `sku_id` 升序**排列，逐项执行：
   `UPDATE sku_stock SET available = available - ?, reserved = reserved + ? WHERE sku_id = ? AND available >= ?`
   - 受影响 0 行时查这个 SKU 是否存在：不存在 → `NOT_FOUND / SKU_NOT_FOUND`；存在 → `FAILED_PRECONDITION / INSUFFICIENT_STOCK`。返回错误使整个事务回滚，包括第 1 步的记录。
3. 读出各 SKU 的单价，写入 `reservation_item`，返回。

要点：

- **先插入再判断**，而不是先 `SELECT ... FOR UPDATE` 再插入。在可重复读隔离级别下，对不存在的行加锁会得到间隙锁，两个事务都拿到间隙锁再各自插入就会死锁。直接插入让唯一键充当互斥：并发的重复请求会等第一个提交后撞上唯一键冲突。
- **按 `sku_id` 排序加锁**，使所有事务以相同顺序获取 `sku_stock` 的行锁。否则订单 A 买 (1, 2)、订单 B 买 (2, 1) 会死锁。
- **`WHERE available >= ?` 的条件更新**是防超卖的核心：判断和扣减在一条语句里，由行锁保证原子，不存在“先查后改”的窗口。表上的 `CHECK` 约束是最后一道防线。

### Confirm

一个事务内：

1. `SELECT status FROM reservation WHERE order_id = ? FOR UPDATE`。
   - 不存在 → `NOT_FOUND / RESERVATION_NOT_FOUND`
   - `CONFIRMED` → 直接成功（幂等）
   - `RELEASED` → `FAILED_PRECONDITION / RESERVATION_RELEASED`
2. `RESERVED`：把状态改为 `CONFIRMED`，然后按 `sku_id` 升序逐项
   `UPDATE sku_stock SET reserved = reserved - ?, sold = sold + ? WHERE sku_id = ? AND reserved >= ?`
   每条必须恰好影响 1 行，否则说明恒等式已被破坏，返回 `Internal` 并回滚。

这里可以用 `FOR UPDATE`，因为记录应当已经存在；不存在时直接报错，不会接着插入。

### Release

一个事务内：

1. `INSERT INTO reservation (order_id, status) VALUES (?, RELEASED)`。
   - 插入成功：说明 Release 先于 Reserve 到达（或 Reserve 从未成功）。这一行就是“已释放”的标记，直接成功。
   - 唯一键冲突：`SELECT status ... FOR UPDATE`
     - `RELEASED` → 成功（幂等）
     - `CONFIRMED` → `FAILED_PRECONDITION / RESERVATION_CONFIRMED`
     - `RESERVED` → 改为 `RELEASED`，按 `sku_id` 升序逐项
       `UPDATE sku_stock SET reserved = reserved - ?, available = available + ? WHERE sku_id = ? AND reserved >= ?`，每条必须恰好影响 1 行。

要点：订单服务在 Reserve 超时后不知道预占是否成功，只能调 Release 兜底。这时 Release 可能先于仍在网络上的 Reserve 到达。如果 Release 对“不存在的预占”什么都不做，迟到的 Reserve 就会占住一份再也没人释放的库存（**悬挂**）。所以 Release 要留下标记（**空回滚**），让迟到的 Reserve 被第 1 步拒绝。这是 TCC 模式里的经典问题。

### GetStock

读 `sku_stock` 一行。

### 测试重点

- 并发抢购：N 个不同订单并发 Reserve 同一 SKU（库存 M < N），成功数恰为 M，`available = 0`，`reserved = M`，无负数。
- 多 SKU 订单以相反顺序并发 Reserve，不出现死锁错误。
- 多 SKU 订单中一项不足，整单不占任何库存。
- 同一订单并发重复 Reserve，只扣一次。
- Reserve、Confirm、Release 各自重复调用的幂等性；内容不同的重复 Reserve 返回 `RESERVATION_MISMATCH`。
- Release 先于 Reserve：Release 成功，随后的 Reserve 被拒，库存不变。
- 同一订单并发 Confirm 与 Release，恰好一个成功，库存恒等式成立。
- 每个测试结束时断言 `available + reserved + sold` 等于初始总量。

## 4. order

表：`orders`、`order_items`、`payment_results`。

`orders.stock_settled`：订单进入终态后，还要通知库存服务确认或释放。这个标志为 0 表示通知尚未成功，由后台任务重试。它把“订单状态已变”和“库存已跟上”解耦，使状态转换只依赖本地事务。

### CreateOrder

校验：`user_id > 0`，`idempotency_key` 长度 1 到 64，至少一项，`quantity > 0`，`sku_id` 不重复。

`request_hash` = 对按 `sku_id` 排序后的 (sku_id, quantity) 序列计算 SHA-256。

1. 生成 `order_id = idgen.Next(user_id)`。在一个事务里插入 `orders`（状态 `CREATING`，`expire_at = now + 支付时限`，默认 15 分钟）和 `order_items`（单价暂为 0）。
   - `(user_id, idempotency_key)` 唯一键冲突：读出已有订单。
     - `request_hash` 不同 → `ALREADY_EXISTS / IDEMPOTENCY_KEY_REUSED`
     - 否则以已有订单的 `order_id` 继续第 2 步（见下文“推进”）。
2. **推进**（对任意一个已存在的订单都可以安全调用）：
   - 状态不是 `CREATING`：直接按当前状态作答。`FAILED` 返回与 `fail_reason` 对应的错误；其他状态返回订单。
   - 状态是 `CREATING`：调用 `inventory.Reserve(order_id, items)`。
     - 成功：一个事务内 `UPDATE orders SET status = PENDING_PAYMENT, total_amount = ? WHERE order_id = ? AND status = CREATING`，并更新各 `order_items.unit_price`。受影响 0 行说明状态已被别人改变，重新读订单并按当前状态作答。
     - 业务拒绝（`SKU_NOT_FOUND`、`INSUFFICIENT_STOCK`、`RESERVATION_RELEASED`）：`UPDATE orders SET status = FAILED, fail_reason = ?, stock_settled = 1 WHERE order_id = ? AND status = CREATING`，把该错误返回给调用方。这几种情况下库存一定没有被占用，所以直接标记已结清。
     - 其他错误（超时、不可达）：**结果未知**，预占可能成功也可能没有。保持 `CREATING` 不动，返回 `UNAVAILABLE`。客户端用同一个幂等键重试时会再次推进；一直没人重试的由后台任务按超时处理。

总金额 = Σ 数量 × Reserve 返回的单价，检查溢出。同一订单内币种必须一致。

要点：

- 先落订单再预占，而不是反过来。这样任何时候库存里的一份预占都能对应到一条订单记录，后台任务总能找到它并做出处理。反过来的话，预占成功后进程崩溃会留下没有订单的预占，无从清理。
- 推进步骤是幂等的：Reserve 以 `order_id` 幂等，状态更新带前置条件。所以重复请求、并发请求、崩溃后重试都走同一段代码，不需要为“上一个请求还在进行中”设计特殊分支。

### GetOrder

按 `order_id` 和 `user_id` 查询。不属于该用户的订单返回 `NOT_FOUND`，不泄露订单是否存在。

### CancelOrder

`UPDATE orders SET status = CANCELLED, stock_settled = 0 WHERE order_id = ? AND user_id = ? AND status = PENDING_PAYMENT`

- 影响 1 行：取消成功。随后**尽力**立即结清库存（见下文“结清”），失败不影响返回，留给后台任务。
- 影响 0 行：读订单。不存在 → `NOT_FOUND`；已是 `CANCELLED` → 成功（幂等）；其他状态 → `FAILED_PRECONDITION / ORDER_NOT_CANCELLABLE`。

### HandlePaymentResult

一个事务内：

1. `SELECT ... FROM orders WHERE order_id = ? FOR UPDATE`。不存在 → `NOT_FOUND / ORDER_NOT_FOUND`。锁住订单行使同一订单的并发回调串行。
2. 查 `payment_results`：该 `payment_id` 已处理过则返回当时记录的答复（幂等）。
3. 支付失败（`succeeded = false`）：记录答复 `ACCEPTED`，订单状态不变（仍可等到超时关闭）。
4. 支付成功：
   - 订单 `PENDING_PAYMENT`：
     - 金额或币种与订单不一致 → `FAILED_PRECONDITION / AMOUNT_MISMATCH`，不记录答复。
     - 一致 → 改为 `PAID`，写 `payment_id`、`paid_at`，`stock_settled = 0`；记录答复 `ACCEPTED`。
   - 其他任何状态（已取消、已关闭、创建失败、创建中，或已被另一笔支付付清）→ 记录并返回 `REFUND_REQUIRED`。

提交后，若本次把订单改成了 `PAID`，尽力立即结清库存。

要点：用户在超时前一刻付款，而关单任务同时触发，这是真实系统里必然出现的竞争。两边都是以 `PENDING_PAYMENT` 为前置条件的转换，只有一个能成功。关单先成功时，钱已经付了但订单不能要，必须明确告诉支付方退款，而不是悄悄吞掉。

### 结清（settle）

对一个 `stock_settled = 0` 的终态订单：

- `PAID` → `inventory.Confirm(order_id)`
- `CANCELLED`、`CLOSED`、`FAILED` → `inventory.Release(order_id)`

成功后 `UPDATE orders SET stock_settled = 1 WHERE order_id = ? AND stock_settled = 0`。

Confirm 返回 `RESERVATION_RELEASED` 或 `RESERVATION_NOT_FOUND`，或 Release 返回 `RESERVATION_CONFIRMED`，说明订单与库存的状态出现了不可能的组合。这是需要人工介入的不变量破坏：记 error 日志，保持未结清，不要自作主张修复。

### 后台任务（sweeper）

实现为 `app.Component`，按固定间隔（默认 1 秒，可配置）循环，每类每轮最多处理一批（默认 100）：

1. **超时关单**：`status = PENDING_PAYMENT AND expire_at < now` 的订单，逐个 `UPDATE ... SET status = CLOSED, stock_settled = 0 WHERE order_id = ? AND status = PENDING_PAYMENT AND expire_at < ?`，成功的随即结清。
2. **创建超时**：`status = CREATING AND created_at < now - 创建时限`（默认 1 分钟）的订单，`UPDATE ... SET status = FAILED, fail_reason = 'CREATE_TIMEOUT', stock_settled = 0 WHERE order_id = ? AND status = CREATING`，成功的随即结清（Release，由空回滚机制挡住之后迟到的 Reserve）。
3. **补结清**：`stock_settled = 0` 且处于终态、`updated_at < now - 宽限`（默认 5 秒，留给在线路径先完成）的订单，逐个结清。

多个副本同时运行这个任务是安全的：所有转换都是条件更新，所有下游调用都幂等，重复执行只是多做无用功。（1b 会用分布式锁让它单实例运行。）

单个订单处理失败只记日志并继续下一个，不中断整轮。

### 测试重点

- 相同幂等键重复下单返回同一订单，Reserve 只生效一次；相同幂等键不同内容返回 `IDEMPOTENCY_KEY_REUSED`。
- 相同幂等键并发下单，只生成一个订单。
- Reserve 返回各类业务拒绝时订单变为 `FAILED`，重复请求返回同样的错误。
- Reserve 超时后订单保持 `CREATING`；重试成功后变为 `PENDING_PAYMENT`；无人重试则被后台任务置为 `FAILED` 并调用 Release。
- 支付成功回调与取消并发、与超时关单并发：恰好一个转换成功；另一方为支付时得到 `REFUND_REQUIRED`。
- 支付回调重复投递，答复一致，订单只转换一次。
- 金额不一致被拒绝。
- 结清调用失败后由后台任务补上；结清重复执行无害。
- 假的 inventory 客户端要能注入：成功、各类业务错误、超时、以及“实际成功但返回超时”。

## 5. payment

表：`payments`。一个订单只有一笔支付单（`order_id` 唯一）。

### CreatePayment

1. 按 `order_id` 查已有支付单。有则校验 `user_id` 后直接返回（幂等）；`user_id` 不符按 `ORDER_NOT_FOUND` 处理。
2. `order.GetOrder(order_id, user_id)`。`NOT_FOUND` 原样透传；状态不是 `PENDING_PAYMENT` → `FAILED_PRECONDITION / ORDER_NOT_PAYABLE`。
3. `payment_id = idgen.Next(user_id)`，插入 `payments`（状态 `PENDING`，金额取自订单，`outcome` 取自请求，未指定按成功）。唯一键冲突说明并发请求已创建，读出返回。

金额来自订单服务，不接受调用方传入。

### 后台任务（processor）

实现为 `app.Component`，按固定间隔（默认 200 毫秒，可配置）循环，每轮每类最多一批（默认 100）：

1. **出结果**：`status = PENDING` 且创建超过模拟延迟（默认 100 毫秒，可配置）的支付单，按 `outcome` 条件更新为 `SUCCEEDED` 或 `FAILED`（`WHERE status = PENDING`）。
2. **回调**：`notified = 0` 且状态为 `SUCCEEDED` 或 `FAILED` 的支付单，调用 `order.HandlePaymentResult`：
   - 答复 `ACCEPTED` → `notified = 1`
   - 答复 `REFUND_REQUIRED` → 状态改为 `REFUNDED`（`WHERE status = SUCCEEDED`），`notified = 1`
   - 错误 `AMOUNT_MISMATCH` 或 `ORDER_NOT_FOUND` → 不可重试。成功的支付改为 `REFUNDED`，`notified = 1`，并记 error 日志
   - 其他错误 → `notify_attempts + 1`，下次再试。重试间隔指数退避：`min(200ms × 2^attempts, 30s)`，用 `updated_at` 判断是否到期

回调是至少一次投递：处理成功但响应丢失时会再发一次，由订单服务以 `payment_id` 幂等。

### GetPayment

按 `order_id` 查询，`user_id` 不符或不存在 → `NOT_FOUND / PAYMENT_NOT_FOUND`。`REFUNDED` 状态对外可见。

### 测试重点

- 同一订单重复、并发 CreatePayment 只生成一笔支付单。
- 订单不存在、不属于该用户、不在待支付状态时的错误。
- 出结果后回调订单服务；回调失败按退避重试直到成功；`notify_attempts` 递增。
- `REFUND_REQUIRED`、`AMOUNT_MISMATCH` 导致退款，且只退一次。
- 回调“实际成功但返回超时”后重发，最终状态正确。
- 时间相关的逻辑通过可注入时钟测试，不依赖真实等待。

## 6. gateway

对外 HTTP（gin），内部 gRPC。1a 没有真实鉴权，用请求头 `X-User-Id` 代表已登录用户。

| 方法与路径 | 说明 | 下游 |
|---|---|---|
| `POST /api/v1/orders` | 下单。头 `Idempotency-Key` 必填。体 `{"items":[{"sku_id":"1","quantity":2}]}`。成功 201 | `order.CreateOrder` |
| `GET /api/v1/orders/:order_id` | 查询订单 | `order.GetOrder` |
| `POST /api/v1/orders/:order_id/cancel` | 取消订单 | `order.CancelOrder` |
| `POST /api/v1/orders/:order_id/pay` | 发起支付。体可选 `{"outcome":"success"｜"failure"}`。成功 202 | `payment.CreatePayment` |
| `GET /api/v1/orders/:order_id/payment` | 查询支付单 | `payment.GetPayment` |
| `GET /api/v1/skus/:sku_id/stock` | 查询库存 | `inventory.GetStock` |
| `GET /healthz` | 健康检查 | — |

约定：

- **所有 64 位 ID 在 JSON 里用字符串**。JavaScript 的数字是双精度浮点，超过 2^53 的整数会丢精度，订单号正好在这个范围之外。入参同时接受字符串。
- 金额用整数分加币种：`{"amount":1999,"currency":"CNY"}`。
- 枚举用小写字符串：`"pending_payment"`、`"paid"` 等。时间用 RFC 3339。
- 错误响应：`{"error":{"code":"INSUFFICIENT_STOCK","message":"..."}}`，`code` 取自 `errx.Reason`，没有原因码时用 gRPC 状态码的名字。
- gRPC 状态码到 HTTP 的映射：`INVALID_ARGUMENT` 400，`NOT_FOUND` 404，`ALREADY_EXISTS` 409，`FAILED_PRECONDITION` 409（其中 `INSUFFICIENT_STOCK` 也是 409），`UNAVAILABLE` 503，`DEADLINE_EXCEEDED` 504，其余 500。500 的响应不透出内部错误消息。
- 缺少或非法的 `X-User-Id` 返回 401。
- JSON 的编解码用 `encoding/json/v2`，不用 gin 自带的绑定和渲染。
- 每个请求给下游调用设置总超时（默认 5 秒）。
- 请求日志用 `slog`，记录方法、路径模板、状态码、耗时。

### 测试重点

用假的 gRPC 客户端测每个路由：成功响应的 JSON 形状、ID 为字符串、各类错误码的映射、缺少请求头、非法入参。

## 7. 端到端验证（各服务合并后进行）

- `make up` 后启动四个服务，用脚本走通：下单 → 支付 → 订单变为已支付 → 库存 `sold` 增加。
- 并发抢购测试：N 个用户并发抢 M 件库存（N 远大于 M），断言成功订单数恰为 M，库存不为负，三个量之和不变。
- 相同幂等键并发重复提交，只产生一个订单、只扣一次库存。
- 下单后不支付，超时后订单关闭、库存归还。
- 支付与取消并发，最终状态自洽：要么已支付且库存已售，要么已取消且库存归还并退款。

## 8. 1a 不做的事

留给后续子阶段，现在不要提前实现：

- Redis 预扣库存、Kafka 异步下单、outbox（1b）
- 分布式锁（1b）
- 分库分表与主从（1c）
- 限流、熔断、降级（1d）
- OpenTelemetry 埋点（1d）
- 真实鉴权、商品管理
