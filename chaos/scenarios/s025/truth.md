# s025 的答案

## 标准根因

库存库（mysql-main 的 `inventory_db`）上有一个外来的会话在循环开事务（脚本造的，对应“一个批处理任务按相反的顺序加锁”）：先锁 `sku_stock` 的 20 行，隔 0.3 秒再锁 `stock_pending` 最前面的 200 行。
inventory 应用库存增量的事务顺序正好相反：先锁 `stock_pending` 的一批，再更新 `sku_stock`。两边各拿着对方要的锁，InnoDB 检测到死锁，回滚其中一个。
全是 `SELECT ... FOR UPDATE`，不改数据。

## 定位必需的关键证据

- `SHOW ENGINE INNODB STATUS` 的 `LATEST DETECTED DEADLOCK`：两个事务，一个是 inventory 的 `UPDATE sku_stock SET available = available + ? ... WHERE sku_id = ?`（持有 `stock_pending` 上的记录锁，等 `sku_stock` 的），
  另一个是 root@127.0.0.1 的 `SELECT id FROM stock_pending ORDER BY id LIMIT 200 FOR UPDATE`（持有 `sku_stock` 上的记录锁，等 `stock_pending` 的）。读懂这一段、指出两边加锁的顺序相反，是这个场景要练的。
- InnoDB 的死锁计数（`information_schema.innodb_metrics` 的 `lock_deadlocks`）每秒涨一两次，平时不动。
- 被回滚的如果是 inventory 一方：它的汇总日志里死锁次数不是 0，应用增量照常继续（死锁的那一批立刻重来）；被回滚的如果是外来的那一方，inventory 看到的只是每次多等零点几秒。
- 用户基本无感：预占、查库存都正常，`sku_stock` 落后的时间从几十毫秒变成几百毫秒。

## 容易误判成的原因

- inventory 的两个副本互相死锁（代码里按 SKU 升序加锁就是为了避免这个；死锁日志里另一方不是 inventory 的语句，来源是 127.0.0.1 不是 Pod 的地址）。
- 锁等待超时、长事务（每个事务不到一秒就结束，没有长事务）。
- 库存数据要坏了（死锁的事务整个回滚，增量不会应用一半；库存的常驻核对不报不一致）。

## 备注

- **全部是按代码推的，没有在集群上跑过。** 脚本在本机只验了起得来、停得掉（本机的库里没有并发的应用任务，撞不出死锁）。
- **没有把握的**：InnoDB 选哪一方回滚看事务的“重量”（改过的行数加持有的锁数）。外来的事务持有 20 多把锁、没有改行；应用任务平时一批只有几行，所以估计被回滚的多半是应用任务，但没有验。
  e2e 里看：死锁有没有发生、频率、哪一方被回滚、inventory 的日志里怎么体现。一次都撞不上的话把 0.3 秒加长。
- 外来的事务锁 `stock_pending` 时如果扫到了索引的末尾，会短暂挡住新增量的插入（每轮最多零点几秒）；预占的延迟可能有小的抖动。
- 没有告警（仓库里没有死锁的告警），所以带一句 `report`。补不补死锁的告警是复盘时的改进项。
