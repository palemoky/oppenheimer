# MySQL 连接数接近上限

覆盖的告警：`MySQLConnectionsHigh`（warning）

## 这条告警在说什么

`mysql_global_status_threads_connected / mysql_global_variables_max_connections > 0.8`，持续 5 分钟。当前连接数超过 `max_connections` 的 80%。每个实例各算各的：订单库两套各 200，`mysql-main` 400（`deploy/cluster/mysql/*.yaml` 的 `maxConnections`）。

阈值来自“连接池的账”：order 每个 Pod 对每个订单库的池是 40，HPA 上限 3 个副本加滚动时多出的 1 个，4 × 40 = 160，`max_connections` 200 里剩 40 给复制、CDC、监控、对账（[round-6 第 4.3 节和第 7 节](../notes/3b-rounds/round-6-scaling-factor.md)，[3b-capacity](../notes/3b-capacity.md)）。80% 正好是 160：order 的池满了就贴在线上，所以这条告警在 order 扩到上限、池又用满时会响，是账的另一面，不是意外。

抑制：`MySQLDown` 在响时这条不通知。

## 影响

- 用户暂时看不到：80% 还有余量。再涨到 100%，新连接报 `Too many connections`（1040），落库、读写失败，下单受理会因为落库健康检查被拒。
- 数据风险没有；失败的写入由幂等键和重试兜，不会丢已确认的订单。
- warning，当天看；如果同时在涨，先看趋势再决定多久处理。

## 先看什么

1. 哪个实例、多少：

   ```
   scripts/cluster/monitoring.sh promql 'mysql_global_status_threads_connected / mysql_global_variables_max_connections'
   scripts/cluster/monitoring.sh promql 'mysql_global_status_threads_connected'
   ```
2. 是在涨还是稳在高位：`scripts/cluster/monitoring.sh promql 'deriv(mysql_global_status_threads_connected[10m])'`。
3. 谁占着（按账号和来源主机）：

   ```
   echo "SELECT USER, SUBSTRING_INDEX(HOST,':',1) AS host, COUNT(*) n FROM information_schema.processlist GROUP BY USER, host ORDER BY n DESC;" | \
     KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n mysql exec -i mysql-order-0-0 -c mysql -- bash -c '. /scripts/lib.sh; rsql 127.0.0.1 -N -B'
   ```
   实例名换成告警里的 `instance`（Pod 名）。
4. order 现在几个副本（每个副本每库 40 个连接）：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get hpa,pods -l app.kubernetes.io/name=order`。
5. 有没有压测、对账、迁移工具在连：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get jobs,pods`。

## 常见的原因和各自的处理

1. **order 扩到 HPA 上限，池用满。** 账见上。连接是 order 池的上限乘副本数，滚动更新时副本数临时多 1。处理：这是预期时只是确认没有继续涨；要彻底解决就按“上限副本数”重算池子，或者调大 `max_connections`（先改 `deploy/cluster/mysql/mysql-order-*.yaml`，要滚动实例，先问主人，MySQL 重启会中断写入）。
2. **池的总数没跟着副本数或库数重算。** 3b round-6 第 4.3 节：池每库 50 时 4 个副本正好等于 200，那一轮没撞上，但“配置上没有任何东西保证不撞”，所以改成了 40。这类问题在本机 compose 上真的撞过：各服务池加起来 164 超过默认 151，报过 1040（[设计 1d](../design/1d-protection.md) 里把 compose 的 `max_connections` 提到 400 的那一条）。改了 HPA 上限或池大小都要重算。
3. **压测打到拐点以上，落库在途数涨。** 落库的在途数是 `SUBMIT_CONCURRENCY` 乘分区数（24 × 4 = 96），由分区数定，不随副本数涨；估下来每库一百四十上下（round-6 第 4.3 节）。压测时 80% 左右是预期。
4. **连接泄漏或慢查询占着连接**（推断，没有遇到过）。第 3 步里某个账号的数量一直涨、不随副本数走，同时 `information_schema.processlist` 里有大量 `Sleep` 很久或长时间运行的语句。处理是找到来源、重启它，必要时 `KILL` 那些连接。
5. **从库、CDC、exporter 的连接**（推断，没有遇到过）：这些固定占几个，不会是涨的原因，但它们在那“40 个余量”里。

## 怎么确认恢复了

- 比值回到 0.8 以下，5 分钟内告警消失：`scripts/cluster/monitoring.sh promql 'mysql_global_status_threads_connected / mysql_global_variables_max_connections'`。
- 已经出过 1040 的，确认服务没有在报连接错误：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs deploy/order --since=15m | rg -i 'too many connections|1040'` 没有输出；跑一次对账，差异为 0：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%H%M)`。

## 怎么人为触发它

设计 5 节第 2 层没有为这条列办法。建议（待定）：在测试实例上用一个临时 Pod 开一批空闲连接到 80% 以上，保持 5 分钟（比如 `mysqlslap` 或脚本开 161 个 `SLEEP(400)` 连接到 `mysql-main`，上限 400 要 321 个）；或者把规则阈值临时改到当前比值以下，响了再改回，并说明这只验证了“路是通的”。不要在长期运行的订单库上真的把连接打满。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
