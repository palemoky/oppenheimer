# s008 的答案

## 标准根因

orderindex 的 Pod 到集群 DNS（kube-dns 的地址 10.43.0.10）的包全部被丢掉（Chaos Mesh 的 NetworkChaos，`partition`，只挡这一个地址）：
这些 Pod 里**任何域名都解析不了**。数据库、Kafka 本身是好的，到它们的网络也是通的。**已经建好的连接不受影响**，只有新建连接时才要解析域名，
所以故障不是一下子出现的：连接池里的连接到了寿命（30 分钟）或者空闲太久（5 分钟）被关掉，补新连接的时候失败，能用的连接越来越少，错误越来越多。

## 定位必需的关键证据

- orderindex 的日志里的错误是**建连接阶段**的解析失败（`lookup <域名>` 加超时或者 `operation not permitted`，
  不是 `connection refused`，不是 SQL 的错误），而且不止数据库一个名字：凡是要新解析的都失败（数据库、Kafka 的 broker、上报指标的 Collector）。
- 错误是逐渐增多的，不是某一刻突然全部失败；数据库连接池里打开的连接数一路往下掉。
- 同样连这个数据库的别的服务（inventory、payment）完全正常；`mysql-main` 自己的指标正常，`MySQLDown` 没有响。
- 集群的 DNS（CoreDNS）没有问题：别的 Pod 解析同样的名字是好的。只有 orderindex 的 Pod 这样，而且两个副本都这样。
- orderindex 自己的指标和链路可能断掉（上报的连接断了之后连不回去），`ServiceMetricsMissing` 可能响：这是同一个原因的另一个表现，容易把人带偏。
- 后果：商家索引的消费者停住（写不进去就原地重试，不进死信），`DerivedDataLagging`、`ConsumerBlocked` 可能响；
  商家订单列表的查询开始失败。订单、支付、库存的主流程不受影响。
- 结束也是自己好的：duration 到了之后新建连接成功，消费者追上，没有重启。

## 容易误判成的原因

- MySQL 主库出了问题或者连接数满了（别的服务连同一个库是好的）。
- orderindex 的连接泄漏、连接池耗尽（池里的连接是在变少，不是被占满；错误是解析失败，不是等连接超时）。
- 集群的 DNS 整体故障（只有这两个 Pod）。
- 到数据库的网络分区（已有的连接一直能用）。
- Kafka 的问题或者消费者的 bug（积压是数据库写不进去造成的后果）。
- 监控坏了（orderindex 的指标没了是结果）。

## 备注

时长取 35 分钟是因为数据库连接的寿命是 30 分钟（`pkg/mysqlx` 的默认值）：要保证每条连接在窗口里至少到期一次。
故障多快显现取决于注入的时刻离各条连接建立的时刻多远，可能头十几分钟什么都看不出来。这正是 DNS 故障在真实系统里的样子。

**原来的写法是 DNSChaos（只让数据库这一个域名解析失败），B1 里试出来不能用**：它要在目标容器里改 `resolv.conf`，
业务镜像是 distroless，chaos-daemon 的辅助程序在里面报 `No such file or directory`，对象一直是 `AllInjected=False`；
改成对边车容器注入也不行（`istio-proxy` 是 init 容器形式的原生边车，选不中）。4a 的冒烟是对 busybox 的探测 Pod 做的，所以当时是通过的。
现在的写法挡的是 Pod 到 10.43.0.10 的全部包，比原来粗：所有域名都解析不了。地址是 k3s 的默认值，集群重建后要核对（`kubectl -n kube-system get svc kube-dns`）。

B1（2026-11-24，`oppenheimer-e2e`）试过：注入成功；在目标 Pod 里用临时的调试容器 `nslookup`，数据库和别的服务的名字都是
`write to '10.43.0.10': Operation not permitted`、超时。目标命名空间里会留下 `PodNetworkChaos` 对象。

没有核实的（要有负载和半小时以上）：连接到期之后错误实际怎么增长；orderindex 的连接池大小和寿命是不是默认值；消费者是不是“停住等”；
查询的失败经 gateway 出去是什么状态码；Go 的解析器收到 `EPERM` 时的错误文字。**第一次在长期运行的那一套上做时由主会话盯着做一次（不盲）更稳妥。**
