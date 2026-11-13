# 1c 设计：分库分表与主从

本文是阶段 1c 的实现规格。1b 之后，订单数据仍在一个库的一张表里。1c 先把订单分到多个库、多张表上，再在此之上做主从、非分片键查询、扩容和冷热归档。

分片键和基因法的大方向见 ADR-0002，具体布局见 ADR-0012。

## 1. 分五步走

每一步结束时系统都能运行，`make lint test e2e` 通过。

| 步骤 | 内容 | 对外行为 |
|---|---|---|
| 一 | 分片：`pkg/sharding`、订单表拆成 2 库 × 4 表、order 服务按分片读写 | 不变 |
| 二 | 主从：每个分片一主一从（GTID 复制），读写分离和读己之写 | 不变 |
| 三 | 非分片键查询：Go 实现的 binlog CDC，同步到按商家分片的索引表 | 新增商家维度查询 |
| 四 | 跨分片查询的取舍、造数工具、扩容演练 | 可能新增按用户分页列表 |
| 五 | 冷热分离与归档 | 冷订单仍可查 |

步骤二到五的细节在各自动工前补到本文，第 3 节目前只有要点。

**范围**：只拆 order 服务的数据。inventory 的数据量与 SKU 数相关，不随订单增长；payment 的数据量与订单同阶，但 1c 的学习目标在 order 上都能覆盖，payment 保持单库（理由写进 ADR-0012）。

## 2. 步骤一：分片

### 2.1 布局

- **逻辑分片（slot）8 个**，编号 0 到 7。`slot = user_id & 7`。
- **物理库 2 个**：`order_db_0`、`order_db_1`。`db = slot % 2`，即库 0 放 slot 0、2、4、6，库 1 放 slot 1、3、5、7。
- **每个 slot 一组表**，表名带全局 slot 号：`orders_3`、`order_items_3`、`payment_results_3`。
- **每个库一张 `outbox`**，不按 slot 拆。

几个选择的理由：

1. **为什么按 slot 而不是按“库号 + 库内表号”命名**。表名全局唯一，扩容时一个 slot 的表可以原样从一个库搬到另一个库，不用改名，也不用重新计算行落在哪张表。路由逻辑只认 slot，slot 在哪个库是一张映射。
2. **为什么 `db = slot % 2` 而不是 `slot / 4`**。成倍扩容（2 库变 4 库）时，取模映射下 slot `s` 从 `s % 2` 号库搬到 `s % 4` 号库：旧库 0 的一半 slot（2、6）去新库 2，另一半留下，旧库 1 同理。每个旧库只拆给一个新库，库的编号不变。区间映射（`slot / 4`）下扩容要给旧库重新编号。
3. **为什么 8 个 slot 而基因有 6 位**。基因 6 位最多支持 64 个 slot。现在用 8 个，以后表太大还可以把 slot 数翻倍：`orders_k` 里 `user_id & 15` 为 `k + 8` 的行拆到新表 `orders_{k+8}`，其他行不动。所以 slot 数、库数都必须是 2 的幂，且库数不超过 slot 数，`pkg/sharding` 在构造时校验。
4. **为什么 `payment_results` 也按 slot 拆**。它只在“锁住订单行”的事务里读写（1a 的支付结果处理），必须和订单在同一个库。按库建一张也能满足这一点，但搬 slot 时就要从一张表里挑出属于这个 slot 的行，按 slot 拆则整表搬走。
5. **为什么 `outbox` 按库建**。outbox 的作用是“和业务写同一个本地事务”，它只需要和业务表在同一个库。它的行是短暂的（发布后保留 1 小时），扩容搬 slot 时不需要搬它，只要先把旧库的 outbox 发完。

### 2.2 路由：`pkg/sharding`

纯计算，不碰数据库：

```go
type Config struct {
    Slots int // 逻辑分片数，2 的幂，不超过 64
    DBs   int // 物理库数，2 的幂，不超过 Slots
}

type Router struct{ /* ... */ }

func New(cfg Config) (*Router, error)
func (r *Router) Slots() int
func (r *Router) DBs() int
func (r *Router) ForUser(userID int64) int   // 返回 slot
func (r *Router) ForOrder(orderID int64) int // 返回 slot，依据 idgen.Gene
func (r *Router) DB(slot int) int            // slot 所在的库
func (r *Router) SlotsOf(db int) []int       // 库里有哪些 slot，升序
func Table(base string, slot int) string     // Table("orders", 3) == "orders_3"
```

`ForOrder(id) == ForUser(user)` 对 idgen 生成的订单号恒成立，这是基因法的全部意义。订单号来自外部输入（HTTP 路径参数、Kafka 消息）时，按它算出的 slot 里查不到就是不存在，不用再去别的分片找。

映射暂时只有取模这一种规则，扩容演练（步骤四）需要“部分 slot 已搬走”的中间状态时再改成显式映射表。

### 2.3 表结构与迁移

订单服务的迁移文件改为模板：`migrations/*.sql` 里用 `{{range .Slots}}...{{end}}` 为本库的每个 slot 生成一组表。`mysqlx` 增加一个按模板渲染后再交给 goose 的入口：

```go
// MigrateTemplate 把 fsys 中 dir 下的迁移文件当作 text/template 渲染（数据为 data），再应用到最新版本。
func MigrateTemplate(ctx context.Context, db *sql.DB, fsys fs.FS, dir string, data any) error
```

渲染结果放进内存里的 `fs.FS` 交给 goose，版本号仍按文件名，每个库各自记录。模板数据是 `struct{ Slots []int }`，由 `Router.SlotsOf(db)` 给出。

**迁移历史压缩成一个文件**。原来的 `00001_init.sql`、`00002_outbox.sql` 作用于 `order_db.orders`，新布局下的库名、表名都变了，没有可以原地升级的旧库。新的 `00001_init.sql` 直接建出 1b 结束时的结构（没有 `stock_settled`，有 `outbox`）。旧的 `order_db` 不再使用，本机数据可以丢弃。

迁移只建本库的 slot。扩容时新库要接收的 slot 的表由迁移工具建（步骤四），不由启动迁移建：启动迁移改变不了“一个 slot 只在一个库里”这个约束。

### 2.4 order 服务

**连接**：`MYSQL_DSNS` 取代 `MYSQL_DSN`，逗号分隔，第 i 个是 `order_db_i`。个数必须等于库数，否则启动失败。slot 数和库数暂时写成常量（8 和 2），不开放配置：配置错了数据就找不到了，改它是一次迁移，不是一次重启。

**store**：`store.Store` 持有 `[]*sql.DB` 和 `*sharding.Router`。

- 按订单号的方法（`GetOrder`、`MarkReserved`）用 `ForOrder` 定位；按用户的方法（`GetOrderByIdempotencyKey`）用 `ForUser`；`InsertOrder` 用订单号定位，并校验订单号的基因和 `user_id` 一致，不一致返回错误（这样的订单写进去之后，按用户和按订单号会找到两个不同的分片）。
- 事务绑定在一个 slot 上：`InTx(ctx, orderID, fn)`。`Tx` 记住自己的 slot，`LockOrder`、`MarkPaid` 等方法只操作这个 slot 的表；传入的订单号不属于这个 slot 时返回错误，而不是悄悄去查别的表。这是“不允许跨分片事务”在代码里的体现。
- `AddOutbox` 写入事务所在库的 `outbox`。
- 后台扫描（`ListExpired`、`ListStaleCreating`）带 slot 参数，每次扫一个 slot。它们是明确豁免的全分片扫描：后台任务本来就要看所有数据，按 slot 逐个扫，不需要合并排序。

**outbox 中继**：每个库一个中继，名字分别是 `order-0`、`order-1`，各有自己的 leader 锁（`order:outbox-relay:0`、`order:outbox-relay:1`）。

- **名字必须不同**。event-id 是 `<中继名>:<outbox 行 id>`，行 id 是各库自己的自增列，两个库都会有 id 为 1 的行。名字相同时两条不同的事件 id 相同，按 event-id 去重的下游会丢掉其中一条。
- **同一订单的事件仍然有序**。同一订单的所有状态变更都在它的 slot 所在的库里，经过同一个中继、同一个消息 key，到 Kafka 的同一个分区。
- 事务提交后只踢事务所在库的中继。`service.Config.Relay` 改为按库号取 `Kicker`。

**sweeper、回收任务**：sweeper 每一轮依次扫所有 slot，每个 slot 各取一批。仍然是一把 leader 锁，一个实例在扫。某个库不可用时跳过它的 slot 并记日志，不影响另一个库。回收任务只看 Redis 闸门，不受影响。

**消费的消息**：落库、支付结果、延迟关单三个消费者的消息里都有订单号，按订单号定位。落库消息里的订单号和 `user_id` 基因不一致时当作毒消息处理（记 ERROR、不重试）：正常路径不会出现，出现说明上游有缺陷。

### 2.5 本机运行与测试

- `make up` 在现有的 MySQL 实例里建 `order_db_0`、`order_db_1`，不再建 `order_db`。两个库在同一个实例上，步骤二再拆到各自的主从。
- `scripts/run-local.sh`、`e2e.sh`、`chaos.sh`、`bench.sh` 改为传 `MYSQL_DSNS`，建库时建两个。
- `test/load` 的 `verify` 和 `wait-quiet` 直接读订单表，改为接受 `-order-dsns`（逗号分隔），用 `pkg/sharding` 遍历所有 slot 汇总。这是对账工具，豁免全分片扫描。
- order 服务的测试辅助函数建两个临时库，跑和生产一样的模板迁移。

**测试重点**：

- `pkg/sharding`：订单号和用户落在同一个 slot（对随机用户号和 idgen 生成的订单号）；`SlotsOf` 的并集覆盖全部 slot 且互不相交；非法配置被拒绝。
- 模板迁移：两个库各自只有自己的 4 组表加一张 `outbox`。
- store：订单写进了按用户算出的那张表，别的表里没有；跨 slot 使用 `Tx` 报错；基因不一致的订单插入失败。
- 分布：一批连续的用户号下单后，8 张表的行数相同。
- 原有的并发测试（支付对关单、重复消息、落库重放）在两个库上照常通过。

### 2.6 不在步骤一做的

- 主从和读写分离（步骤二）。
- 按用户的分页列表（步骤四，那时一并讨论跨分片的列表和聚合）。
- 显式 slot 映射表和扩容（步骤四）。
- payment、inventory 分库。

## 3. 后续步骤要点（动工前细化）

**步骤二：主从**。compose 里每个分片一主一从（共 4 个 MySQL 容器，inventory 和 payment 留在现有实例），GTID 加 `SOURCE_AUTO_POSITION`。读写分离按“默认读从、写后读主”：受理或状态变更之后的短时间内同一用户的读走主库，或者带上写入时的 GTID 用 `WAIT_FOR_EXECUTED_GTID_SET` 在从库等待，两种都实现并对比。人为制造延迟用 `SOURCE_DELAY` 或停 SQL 线程。

**步骤三：CDC**。动工时写 ADR，与 Canal、Debezium 对比。商家维度要先在数据模型里有商家（SKU 所属商家），这一点要先改 inventory 的表和下单时的快照。

**步骤四**：跨分片查询先列出哪些不支持；造数工具灌数年跨度的订单；扩容演练按 2.1 的取模映射做一次 2 库到 4 库。

**步骤五**：冷热归档，按订单号里的时间判断冷热，归档库用压缩表起步。
