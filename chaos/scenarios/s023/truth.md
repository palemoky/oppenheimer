# s023 的答案

## 标准根因

订单库 1 的主库上，`orders_1` 这张表的元数据锁被堵住了。两个 root 会话（脚本造的）：
一个在没有提交的事务里读过 `orders_1`，一直占着共享的元数据锁；另一个随后对这张表要排他的元数据锁（`LOCK TABLES orders_1 WRITE`，对应线上最常见的情形：对一张有长事务的表做 DDL），拿不到，在排队。
排他的请求排在队里时，后面所有对这张表的读写都排在它后面。slot 1 的用户（`orders_1`）的每一条语句都挂在 `Waiting for table metadata lock`。

## 定位必需的关键证据

- 订单库 1 主库的 `processlist` 里大量 order 的语句状态是 `Waiting for table metadata lock`，全是 `orders_1` 上的；最早开始等的是那条 `LOCK TABLES orders_1 WRITE`。
- `sys.schema_table_lock_waits`（或者 `performance_schema.metadata_locks`）指出：排队的排他请求被一个持有 `SHARED_READ` 的会话挡着，那个会话在 `SELECT SLEEP(...)`、事务一直开着（`information_schema.innodb_trx`）。
- 只有 `orders_1` 受影响：同一个库的 `orders_3`、`orders_5`、`orders_7` 和订单库 0 起初是正常的。失败的用户集中在一个 slot（八分之一）。
- 库本身是好的：`mysql_up` 1、复制正常、CPU 不高、没有慢查询（语句根本没有开始执行）。
- 连接数一直涨：order 的请求到时限后放弃，库里的线程还挂着，占着连接。涨到 `max_connections`（200）之后新的连接被拒绝（1040），这时订单库 1 的其他 slot 也开始失败。
- 根治是结束占着共享锁的那个事务（不是 KILL 那些挂住的业务语句，也不是只 KILL 排队的那个：只 KILL 排队的，业务语句会恢复，但长事务还在，下一个 DDL 再来一遍）。

## 容易误判成的原因

- 订单库 1 挂了、要主从切换（库是好的；切换会把 CDC 的源也换掉）。
- 行锁等待、死锁（`data_lock_waits` 是空的，等的是表的元数据锁，不是行锁）。
- 慢查询、缺索引（语句没有在执行）。
- order 的连接池配小了、`max_connections` 配小了（都是被挂住的语句占满的，是结果）。
- 网络分区（和 s020 的样子很像：一部分用户失败、库自己正常；区别是这里连接是通的，语句在库里排队，`processlist` 一看就知道）。

## 备注

- **全部是按代码和 MySQL 的行为推的，没有在集群上跑过。** 本机 compose 上验过：另一个会话对 `orders_1` 的查询挂在元数据锁上、`orders_3` 不受影响、撤销之后放行。
- 用 `LOCK TABLES ... WRITE` 而不是真的 `ALTER TABLE`：两者排队时的表现一样，但前者拿到锁也不改任何东西；真的 DDL 一旦执行会经过复制和 CDC。脚本还让排队的那个比长事务早 15 秒超时，它永远拿不到锁。
- **没有把握的**：挂住的语句被客户端放弃之后，库里的线程会不会一直留着（留着的话连接数会顶到 200，影响从一个 slot 扩大到整个订单库 1）；
  落库的消费者里 slot 1 的消息无限重试会不会像 s020 那样拖住别的用户的落库（多半会：是同一个机制）。e2e 里先看前一件，带负载时看后一件。
- 订单库 1 不带节点号的租约，发号不受影响。
- 会响哪些告警没有把握：按 s020 的经验，`SLOOrderCreateAvailabilityBurnFast`、`IngressGateway5xxHigh` 一类的 critical 可能响；连接数顶到上限的话 `MySQLConnectionsHigh` 响，exporter 连不上的话 `MySQLDown` 也会响（那是误导）。
