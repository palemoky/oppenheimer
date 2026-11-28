# s022 的答案

## 标准根因

库存库（mysql-main 的 `inventory_db`）上有一个没有提交的事务：一个 root 会话在事务里对 `sku_stock` 里正在卖的 20 行执行了 `SELECT ... FOR UPDATE`，然后一直开着（脚本造的，对应“有人手工查数据忘了提交”“批处理卡住”）。
它只持有行锁，不改数据。inventory 把库存增量应用到 `sku_stock` 的任务（`apply`，每 20 毫秒一批）要更新这些行，拿不到锁。

## 定位必需的关键证据

- `sys.innodb_lock_waits`（或者 `performance_schema.data_lock_waits`）里，等锁的是 inventory 的 `UPDATE sku_stock SET available = available + ? ...`，挡住它的是一个 root@127.0.0.1 的会话。
- `information_schema.innodb_trx` 里那个会话的事务开始时间就是故障开始的时间，`trx_rows_modified` 是 0、`trx_rows_locked` 约 20；`processlist` 里它在 `SELECT SLEEP(...)`（State 是 User sleep），不是在跑业务语句。
- inventory 的日志里应用增量的一轮反复失败（一批的时限 5 秒，`context deadline exceeded`）；`inventory.stock_pending.backlog` 一直涨，`sku_stock` 的数不再变。
- 预占流水（`reservation`）照常写入：预占不更新 `sku_stock`，只往 `stock_pending` 记增量。
- 没有发布、没有重启；KILL 那个会话之后立刻恢复，积压的增量被追上。

## 容易误判成的原因

- 死锁（没有死锁：是单方面的等待，InnoDB 的死锁计数不涨）。
- inventory 应用增量的代码坏了、或者某个 SKU 卡住（卡住的 SKU 有专门的日志和 `oversell` 计数，这里没有；失败的原因是等锁超时）。
- mysql-main 慢、盘慢（别的库、别的表上的语句是正常的）。
- 超卖、库存数据坏了（`sku_stock` 加上未应用的增量仍然对得上，库存的常驻核对不报不一致）。

## 备注

- **全部是按代码推的，没有在集群上跑过。** 脚本的注入和撤销在本机 compose 的 MySQL 上验过（锁确实持有、KILL 之后放掉）。
- **影响有多大没有把握，e2e 里先看这一件**：应用任务的事务在 `UPDATE sku_stock` 之前已经对 `stock_pending` 的一批加了锁（`ORDER BY id LIMIT 1000 FOR UPDATE`）；
  行数不满一批时扫描到了索引的末尾，可重复读下会锁住“末尾之后”的间隙，新的增量插不进来。平时这个事务几毫秒就提交，看不出来；
  现在它等 `sku_stock` 的行锁，客户端 5 秒后放弃并断开连接，但库里那个线程可能要等满 `innodb_lock_wait_timeout`（50 秒）才发现、才回滚。
  如果是这样，预占（`Reserve`）往 `stock_pending` 插入时也被挡住，落库跟着停，下单大面积被背压拒绝：一个忘了提交的会话拖住整个下单。
  如果不是这样（比如断开连接后线程很快回滚），那就是一个安静的场景：只有积压在涨，查库存因为要合计越来越多的未应用增量而慢慢变慢，要配一句 `report`。
- 脚本锁的是编号最大的 20 个 SKU：稳态压测每一轮用新的、编号更大的 SKU。
- 查库存不受行锁影响（一致性读），但它的语句要合计 `stock_pending` 里这个 SKU 的全部未应用增量，积压越多越慢。
