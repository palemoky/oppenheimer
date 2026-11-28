# s024 的答案

## 标准根因

订单库 0 的主库上有人在反复跑一条很重的统计语句（脚本造的，三个并行的会话轮流扫四张订单表，对应“报表、导出直接连了主库”）：

```sql
SELECT user_id, COUNT(*) AS c, SUM(total_amount) AS s FROM orders_N
 WHERE DATE(created_at) >= CURDATE() - INTERVAL 90 DAY
 GROUP BY user_id ORDER BY s DESC LIMIT 100000, 20
```

三处都是慢的写法：过滤条件把列包在函数里（`DATE(created_at)`，而且 `created_at` 上本来也没有单独的索引），只能全表扫描；按用户分组后对聚合结果排序，要临时表和文件排序；翻到第十万行之后才取 20 行。
它们占着主库的 CPU 和缓冲池，业务语句跟着变慢。只读，不改数据。

## 定位必需的关键证据

- 订单库 0 主库的 `processlist` 里有三个 root@127.0.0.1 的会话一直在跑这条语句，每条几秒到几十秒。
- `performance_schema.events_statements_summary_by_digest`（或者 `sys.statement_analysis`）里这条语句的总耗时、扫描行数（`rows_examined`）、临时表和排序的次数排在最前面，远超业务语句。
- `EXPLAIN`：`type: ALL`、`Using temporary; Using filesort`。
- 订单库 0 主库的 CPU 涨、InnoDB 读的行数涨了几个量级，缓冲池的命中率可能掉；订单库 1 没有变化。落在订单库 0 的用户（偶数 slot）的落库、查订单、列表的延迟上升，订单库 1 的用户不变。
- 慢查询日志是关着的（`slow_query_log` 没有设），所以这次要靠 `performance_schema`；这本身是一个改进项。
- 没有锁等待：这些语句是一致性读，不挡写。

## 容易误判成的原因

- 业务语句缺索引（业务语句的执行计划没有变，是被抢了资源）。
- 锁等待、长事务（没有锁等待）。
- 订单库 0 的盘慢、节点上有别的东西抢 CPU（是库自己的线程在用 CPU，`processlist` 里看得到谁在用）。
- order 的连接池不够（池里的等待是语句变慢的结果）。

## 备注

- **全部是推的，没有在集群上跑过；症状有多重完全取决于表有多大。** 表小的话语句一下就跑完，基本没有症状：注入前先数行数，不够的话这个场景要换写法或者不进池。脚本在本机验过（表几乎是空的，只验了循环起得来、停得掉）。
- 每条语句最多跑 60 秒（`MAX_EXECUTION_TIME`）。
- 订单库 0 带节点号的租约：主库慢到续不上租约的话发号会受影响，所有用户的下单被拒绝（s021 的样子）。按估计到不了这一步，带负载时看。
- 可能没有任何告警，那样要配一句 `report`；试过再定。
