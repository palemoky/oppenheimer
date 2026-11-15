# 2d 设计：服务之间的流量治理迁到 Istio

本文是阶段 2d 必做部分的实现规格：把 2c 里手写的 Envoy 边车换成 Istio，1d 实测过的熔断、并发上限、路由超时逐项改写成 Istio 的资源并重新验证。2d 其余几项（NetworkPolicy、cert-manager、Argo CD、灰度发布）是按需的，做不做见第 2 节的第 8 项。

已有的决定，本文不重复理由：保护放在边车上而不是进程里（ADR-0020）；超时逐跳收紧、同步链上不重试（设计文档 1d 的 5.6）；2c 先手写、2d 迁到 Istio（设计文档 2c 的 6.5，主人 2026-11-15 定的）。

状态：主人已确认（2026-11-15），第 2 节的八项都按建议，决定记在 ADR-0029。实现中的发现和量到的数在第 10 节。

## 1. 动工前查到的事实（2026-11-15）

标了“查证”的是当天从官方页面或集群上读到的；标了“没有查证”的是按记忆写的，实现时先对参考页再用。

**版本（查证：istio.io 的支持表、GitHub 的发布页）**

| 版本 | 发布 | 支持到 | 支持的 Kubernetes | Envoy 分支 |
|---|---|---|---|---|
| 1.32.0 | 2026-11-09 | 约 2027-06 | 1.33 到 1.37 | v1.40 |
| 1.31.4 | 2026-11-06（1.31 是 2026-08-27） | 约 2027-03 | 1.32 到 1.37 | v1.39 |
| 1.30.8 | 2026-11-06 | 约 2026-12-16 | 1.32 到 1.36 | v1.38 |

Istio 没有 LTS，每个小版本支持到“后面第二个小版本发布之后 6 周”。集群是 k3s v1.37.2，1.30 不支持它。1.32 用的 Envoy 分支和 1d 实测时的 v1.40.2 是同一个。

**Istio 的默认值和 1d 的三个坑对不上的地方（查证：DestinationRule、VirtualService 的参考页，istiod chart 1.32.0 的 values）**

| 1d 里的东西 | Istio 的默认 | 和 1d 的差别 |
|---|---|---|
| 同步链上不重试（`max_retries: 0`） | **默认重试 2 次**，条件是 `connect-failure,refused-stream,unavailable,cancelled`，退避基数 25 毫秒 | **默认行为直接违反重试规范。** `unavailable` 是 gRPC 状态码的条件：下游返回 `UNAVAILABLE` 的写请求会被边车再发两次。必须关掉（3.4） |
| 路由超时 | 默认不设 | 和手写时一样必须显式写；不写的话慢下游永远不会被摘除（坑 2） |
| `max_ejection_percent: 100` | `maxEjectionPercent` 默认 10% | 一样要改成 100（坑 1 的前一半） |
| `healthy_panic_threshold: 0` | `minHealthPercent` 默认 0，即不启用恐慌模式 | 默认值就是要的（坑 1 的后一半）。文档写的理由正是“每个服务只有几个 Pod 的环境不适用” |
| 不开 `split_external_local_origin_errors` | 默认 false | 默认值就是要的（坑 3） |
| 连续 5 次网关错误摘除 | `consecutive5xxErrors` 默认 5；`consecutiveGatewayErrors` 默认关 | 路由超时产生的 504 也是 5xx，只靠前一个应当就够，要实测 |
| `max_ejection_time: 5s` | **DestinationRule 没有这个字段** | 摘除时间是基数乘以被摘除的次数，上限是 Envoy 的默认（300 秒，1d 查的）。1d 特意固定成 5 秒，这里做不到，除非用 EnvoyFilter 打补丁（第 2 节第 5 项） |
| 检查间隔 1 秒、摘除 5 秒 | `interval` 10 秒、`baseEjectionTime` 30 秒 | 显式写 |
| 并发上限 128 / 512 | `http2MaxRequests` 默认 2^32-1，即没有上限 | 显式写 |
| 入站并发上限（被调用方自己的保护） | DestinationRule 只作用在调用方。入站要用 `Sidecar` 资源的 `inboundConnectionPool`（参考页说它和 DestinationRule 的 `connectionPool` 同构，例子里写的就是 `http2MaxRequests: 1024`） | 能表达，行为要实测 |

1.32 新增了按失败比例摘除（`failurePercentageThreshold`）。1d 记过“没有按错误比例的熔断”，现在有了，但按比例的检测要多个实例互相比较，两个副本下意义不大，这次不用，只记一笔。

**其他查到的**

- **原生 sidecar 从 Istio 1.27 起默认开启**（查证：1.27 的升级说明）。2c 手写时专门解决的启动和停止顺序（设计文档 2c 的 6.1），注入出来的边车同样具备。
- **注入出来的边车默认 requests 是 100m CPU、128 Mi 内存，limits 2 核、1 Gi；istiod 默认 requests 500m、2 Gi**（查证：chart 的 values）。现在手写的边车实际只用 16 到 18 Mi，requests 是 32 Mi。照默认值装，10 个 Pod 的边车加 istiod 要多承诺约 3 Gi，必须改小（第 6 节）。
- **节点的 iptables 是 nf_tables 后端**（查证：agent-1 上 `iptables v1.8.11 (nf_tables)`）。Istio 靠 init 容器在 Pod 的网络命名空间里写重定向规则；chart 有 `global.nativeNftables`（默认 false）。两种写法在这批节点上哪个能用没有试过，是第一个要验证的风险。2b 里这批节点的 AppArmor 拦过容器之间的信号，init 容器要 `NET_ADMIN`，也要看有没有被拦。
- **节点没有区域标签**（查证）。Istio 的文档说配置了异常检测就会启用就近优先的负载均衡；没有标签时应当没有效果，但在 DestinationRule 里把 `localityLbSetting` 显式关掉，不依赖这个推断。
- **中间件的端口名**（查证）：MySQL 是 `mysql`，Redis 是 `redis`、`sentinel`，Kafka 是 `tcp-clients`。Istio 按端口名猜协议。
- **集群内存**（查证，重建之后轻载）：server 48%，agent-1 33%，agent-2 39%；承诺量（requests）分别是 48%、53%、67%。比重建之前宽裕。
- **服务现在都用命名空间的 `default` 账号**，而 Istio 的身份（mTLS 证书里的那个）就是服务账号。不拆开的话五个服务是同一个身份，以后的访问控制分不出谁是谁。Pod 模板里 `automountServiceAccountToken: false`；注入的边车用自己挂的投射令牌，应当不受影响（没有查证）。

## 2. 主人确认的八项（都按建议）

| # | 问题 | 建议 | 主要的另一种做法 |
|---|---|---|---|
| 1 | 数据面的形态 | **边车模式**：和 1d 的拓扑一一对应（出站的保护在调用方的代理上，每个调用方各有额度），面试里问得最多的也是它 | **ambient 模式**（每个节点一个四层的 ztunnel，七层功能靠按命名空间或按服务部署的 waypoint）：省内存，是 Istio 现在主推的方向；但超时、熔断、并发上限都落在被调用方一侧的 waypoint 上，“每个调用方各有额度”要换一种表达，等于重新设计而不是迁移。可以在边车模式做完之后单独做一次对照 |
| 2 | 版本 | **1.32.0**：Envoy 分支和 1d 实测的一致，支持期最长；按规范“没有 LTS 选最新的正式稳定版” | 1.31.4：已经出到第 4 个补丁，更稳；代价是 Envoy 换成 v1.39，1d 的实测结论要多怀疑一层。1.32.0 才发布 6 天 |
| 3 | 中间件的流量（MySQL、Redis、Kafka）过不过边车 | **不过**：出站按端口排除（3306、6379、16379、26379、9092）。这些依赖的熔断在进程里（`pkg/breaker`），中间件一侧没有边车，过了也没有 mTLS；过边车还多一跳延迟，并且长时间空闲的连接池连接会被边车的空闲超时掐掉（要查的坑） | 都过：能在一处看到全部出站流量的指标。等阶段 3 有了监控、觉得缺这部分数据时再放开 |
| 4 | 入站只接管 gRPC 端口，还是全部端口 | **只接管 gRPC 端口**。HTTP 端口（探针、管理端口、gateway 的 8080）不过边车，和 1d、2c 的形态一致。入口的 Envoy 到 gateway 这一跳如果过边车，gateway 看到的对端地址会变成边车的本地地址，`TRUSTED_PROXIES` 不认，`X-Forwarded-For` 被忽略，按 IP 的限流会把所有人算成一个地址 | 全部接管，再给 gateway 配受信任的代理层数。入口到 gateway 这一跳也能有 mTLS（要把入口的 Envoy 也拉进网格），工作量大得多 |
| 5 | 摘除时间不能固定成 5 秒 | **先接受，量了再说**：摘除时间逐次加长（5、10、15 秒…），健康一段时间后次数会降回去。`make cluster-chaos` 里服务被反复删，看恢复时间有没有涨到影响结论 | 用 EnvoyFilter 给每个出站集群补 `max_ejection_time: 5s`。EnvoyFilter 直接改生成出来的 Envoy 配置，升级 Istio 时容易失效，是公认的最后手段 |
| 6 | 本机和 CI 怎么办 | **本机 compose 保留手写的边车**（e2e、chaos、fault 都靠它，CI 上没有集群）。同一个超时于是写在两处，Go 的测试把两处和代码里的常量一起核对（第 5 节） | 本机不再用边车，保护只在集群上测：CI 就测不到熔断了 |
| 7 | 怎么切过去 | **不停机地切**，按“调用方先切”的顺序逐个服务滚动，全程开着探针（第 7 节）。先在 `oppenheimer-e2e` 上把整套验证做完，再动长期运行的那一套 | 整个命名空间删掉重装，中断一两分钟。简单，但“已有的服务怎么不停机地进网格”是真实环境里的问题 |
| 8 | 2d 其余四项做哪些 | **只加 mTLS 严格模式和按身份的访问控制**（网格自带，半天的事，替代了 NetworkPolicy 那一条想防的东西的一大半）；Argo CD、cert-manager、灰度发布这次不做，留到有需要时 | 四项都做。Argo CD 常驻要 1 Gi 上下的内存；灰度发布（Argo Rollouts 配 Istio 的流量拆分）面试价值不低，可以在阶段 3 有了指标之后做，那时才能按指标自动推进或回滚 |

## 3. 设计

### 3.1 总体的样子

```
入口的 Envoy ──HTTP（不过边车）──► gateway:8080
gateway ──gRPC──► [istio-proxy 出站] ══mTLS══► [istio-proxy 入站] ──► order:9081 / inventory:9082 / payment:9083 / orderindex:9084
                   超时、并发上限、摘除（配置来自 istiod）              入站并发上限
order ──► inventory，payment ──► order：同上
各服务 ──► MySQL、Redis、Kafka：直连，不过边车
cdc：不注入
istiod ×1（istio-system）：把 Service、端点和下面的资源翻译成 Envoy 配置，经 xDS 推给每个边车
```

和 2c 相比变了什么：

| | 2c（手写） | 2d（Istio） |
|---|---|---|
| 边车从哪来 | chart 里写的 initContainer，配置是 ConfigMap 里的静态 YAML | istiod 的准入 webhook 在 Pod 创建时注入，配置经 xDS 动态下发 |
| 调用方怎么连下游 | 连自己边车的出站端口（`127.0.0.1:15182`） | 直接写下游的 Service 名（`inventory:9082`）；Pod 里的 iptables 规则把出站连接重定向给边车，进程不知道边车存在 |
| 下游的端点怎么到调用方 | headless Service 加 DNS，`STRICT_DNS` 定期解析 | istiod 监听 EndpointSlice，变化经 EDS 推送 |
| 服务之间的连接 | 明文 | 自动 mTLS，证书由 istiod 签发并轮换 |
| 配置写在哪 | `deploy/envoy/*.yaml`，每个边车一个文件 | `VirtualService`（超时、重试）、`DestinationRule`（并发上限、摘除）、`Sidecar`（入站上限、可见范围） |

第二行是这次迁移里最值得弄明白的原理：2c 是“显式代理”（进程自己把地址指到代理），Istio 是“透明拦截”。透明的代价是出了问题时进程看不出流量去了哪，要会用 `istioctl proxy-config` 看边车实际拿到的配置。

### 3.2 安装

- Helm 装 `base`（CRD）和 `istiod` 两个 chart，版本固定，登记到 `versions.md`。不装 Istio 自己的入口网关（入口仍是 Envoy Gateway，ADR-0025）。新脚本 `scripts/cluster/mesh.sh`，`make mesh-up`，可重复执行，放进一键部署的顺序里（`middleware-up` 之后、`deploy` 之前：webhook 要先于业务 Pod 存在）。
- istiod 1 个副本、关掉它自己的 HPA，requests 改小（第 6 节），和业务服务一样避开 server 节点。**istiod 挂了会怎样**要实测并写进笔记：已经在跑的边车用最后一份配置继续工作，但端点的变化推不下去，新 Pod 注入不了（webhook 的失败策略决定是创建失败还是不带边车起来）。
- 重定向规则用默认的 init 容器（`istio-init`），不装 Istio CNI 的 DaemonSet（多三个常驻 Pod）。先在一个临时 Pod 上验证 nf_tables 后端和 AppArmor 下规则写得进去；写不进去再试 `nativeNftables`，再不行才换 CNI 插件。
- 注入按命名空间打标签（`istio-injection=enabled`）：`oppenheimer` 和 `oppenheimer-e2e`。cdc 在 Pod 上标 `sidecar.istio.io/inject: "false"`。e2e 的测试 Job、压测 Job 也不注入（它们模拟的是集群外的客户端，走入口）。

### 3.3 chart 的改动

- 手写的边车（initContainer、`<服务>-envoy` 的 ConfigMap、headless Service）整体去掉；切换期间用一个 values 开关在两种形态之间选，切完删掉旧分支。
- 普通的 Service 加上 gRPC 端口，端口名 `grpc`。Istio 靠端口名（或 `appProtocol`）认协议，认成 HTTP/2 才会按请求做负载均衡和超时；名字不对会被当成不透明的 TCP，按连接均衡，2c 解决的“长连接不均衡”原样回来，而且不报任何错。这一条要有检查（`istioctl analyze` 进 `make deploy` 的结尾）。
- 每个服务一个 ServiceAccount。
- Pod 模板的注解：入站只接管 gRPC 端口，出站排除中间件的端口，边车的 requests 和 limits。
- 下游地址从 `127.0.0.1:151xx` 改成 `<服务>:<gRPC 端口>`（同命名空间的短名字，两套命名空间不用区分）。
- `DRAIN_DELAY` 先不动（15 秒）。端点改成经 xDS 推送之后传播应当比 DNS 快得多，按 2c 第四步同样的办法重新量，有了数再决定要不要降。

### 3.4 1d 的每一条怎么写

每条调用边仍然是“调用方一侧”的配置。同一个下游被两个调用方用不同的数（order 被 gateway 用 3 秒、512，被 payment 用 1 秒、128），所以规则要按调用方区分：

| 调用边 | 超时 | 并发上限 | 怎么按调用方区分 |
|---|---|---|---|
| gateway → order | 3s | 512 | 超时：`VirtualService` 的路由按 `sourceLabels` 匹配调用方；并发和摘除：`DestinationRule` 的 `workloadSelector` 选调用方的 Pod（没有查证，实现时对参考页；不行就退到每个调用方一个 `exportTo` 受限的规则） |
| gateway → inventory、payment、orderindex | 3s | 128 | |
| order → inventory | 1s | 128 | |
| payment → order | 1s | 128 | |

每条边的 `VirtualService` 路由：`timeout` 写上表的数，**`retries: {attempts: 0}`**。另外在网格配置里把默认的重试策略也设成 0 次（`meshConfig.defaultHttpRetryPolicy`），这样漏写某条路由时的结果是不重试，而不是悄悄重试两次。1.32 新增了入站一侧的默认重试策略（`defaultInboundHttpRetryPolicy`），同样关掉。

每条边的 `DestinationRule`：

```yaml
trafficPolicy:
  connectionPool:
    http: {http2MaxRequests: 128, http1MaxPendingRequests: 128, maxRetries: 0}
  outlierDetection:
    consecutive5xxErrors: 5
    interval: 1s
    baseEjectionTime: 5s
    maxEjectionPercent: 100
    # minHealthPercent、splitExternalLocalOriginErrors 用默认值（0、false），显式写出来并注明理由
  loadBalancer:
    localityLbSetting: {enabled: false}
```

入站：每个被调用的服务一个 `Sidecar` 资源，`inboundConnectionPool` 里 `http2MaxRequests` 写 1024（order）或 512。同一个资源里把 `egress` 的可见范围收到本命名空间加 `istio-system`：默认每个边车会拿到全网格所有 Service 的配置（包括中间件那些 headless Service 的每一个 Pod），内存随集群里的服务数增长，这是 Istio 在大集群里最常见的资源问题，这里顺手做对。

mTLS 和访问控制（第 2 节第 8 项，主人同意才做）：迁移期间 `PeerAuthentication` 用默认的 `PERMISSIVE`（明文和 mTLS 都收，第 7 节的不停机切换依赖它）；全部切完后对 gRPC 端口改成 `STRICT`，再加 `AuthorizationPolicy`：order 的 gRPC 只允许 gateway 和 payment 的身份调，inventory 只允许 gateway 和 order，以此类推。

### 3.5 资源放在哪

`deploy/cluster/mesh/` 下的普通清单文件，不进 chart：它们描述的是调用边，不属于哪一个服务的 release；host 都写短名字，同一份文件对两个命名空间各 `kubectl apply -n` 一次。Go 的测试直接读这些文件（第 5 节）。

## 4. 逐项验证

1d 的故障注入（`scripts/fault.sh`）是对本机进程做的（`kill -STOP`、`kill -9`）。集群上要一份对应的 `scripts/cluster/fault.sh`，在 `oppenheimer-e2e` 上跑，每个场景对着 1d 的结果比：

| 场景 | 集群上怎么造 | 1d 的结果（预期一致，除非注明） |
|---|---|---|
| A 基线 | | 查库存、下单落库正常 |
| B 下游挂住 | 两个 inventory 都挂住。Pod 里没有 shell（distroless），不能 `kill -STOP`：用 Istio 自己的故障注入（`VirtualService` 的 `fault.delay`）还是在节点上对进程发信号，实现时定；前者测不到“超时算不算进摘除”，所以至少要有一次是真的把进程挂住 | 前 5 次各约 3 秒返回 503，之后毫秒级 503；查订单不受影响；恢复后 5 秒左右放回 |
| B' 只挂住一个副本（新场景） | 两个 inventory 里挂住一个 | 1d 没有这个场景。预期：坏的那个被摘除，另一个接走全部流量，摘除之后没有失败 |
| C 下游被杀 | 强制删 Pod | 毫秒级 503，摘除 1 次，新 Pod 就绪后恢复 |
| D 并发上限 | 同时 300 个慢请求 | 每个 gateway Pod 各放过 128 个，其余立刻 503。**两个 gateway 合计 256**（2c 的 6.4 说过） |
| E 业务错误 | 30 次查不存在的 SKU | 都是 404，摘除 0 次 |
| F 不重试（新场景） | 让 inventory 对某个请求返回 `UNAVAILABLE`，数它实际收到几次 | 1 次。去掉 `retries` 那一行作对照：预期 3 次。没有对照就说明不了关重试起了作用 |
| G 重复摘除的时长（第 2 节第 5 项） | 场景 C 连做 5 次 | 记下每次从新 Pod 就绪到恢复的时间 |

三个坑各自要有对照（各改一个值，看失败是否如预期出现）：`maxEjectionPercent` 用默认值时场景 B 不摘除；路由不写 `timeout` 时慢下游不摘除；进程里的时限和路由超时相等时不摘除（这一条 1d 已经证过，机制没变，不重做）。

要弄清楚的一个疑点：**Istio 生成的路由会不会采用调用方带来的 `grpc-timeout` 头**。1d 的坑 2 里，按这个头算出来的超时不计入摘除。gRPC 客户端总是带这个头（进程里的时限是 3.5 秒或 1.5 秒）。如果边车用的是头里的值而不是路由上写的值，那么先到期的仍然是路由的 3 秒，但它算不算“路由超时”要看生成的配置。办法：`istioctl proxy-config routes` 看生成的路由里有没有 `max_stream_duration` 的 `grpc_timeout_header_max`，再用场景 B 实证。

其余验收，和 2c 同一套工具：

- `make cluster-e2e` 19 个用例通过；`make cluster-chaos` 正常删除和强制删除各一次，`verify` 和 `recon` 无差异。
- 滚动发布零报错（`probe.py`，六个服务滚两遍）；重新量端点传播时间。
- gRPC 负载均衡：两个 inventory 各收到一半（2c 第四步的同一个量法）。
- HPA：利用率是按整个 Pod 的 requests 算的，边车的 requests 变了，同样的负载下扩容点会移动。重做 2c 第六步的那次压测，记下差别。
- 延迟：同一个节点上经边车和不经边车（注解关掉注入）各压一轮，对照 1d 的 +0.7 毫秒（p50）。mTLS 的开销在里面。
- 内存：istiod 和每个边车的实际用量，对照手写时的 16 到 18 Mi。

## 5. 同一个超时写在两处

本机和 CI 仍然用 `deploy/envoy/*.yaml`，集群用 `deploy/cluster/mesh/` 的 `VirtualService`。同一跳的预算于是有三处：两份配置和 Go 里的常量（`DownstreamHopBudget` 等）。

现在的测试（gateway、order、payment 各一个）读 Envoy 的 YAML 核对。扩成：同一个测试再读 `deploy/cluster/mesh/` 里对应的 `VirtualService`，断言三处相等、`retries.attempts` 是 0、每条调用边都有路由（缺一条就是没有超时、默认重试两次）。读文件的公共部分放到 `pkg/grpcx` 的测试辅助里还是各服务各写，实现时看重复有多少。

并发上限和摘除的参数没有对应的 Go 常量，靠第 4 节的故障注入守着；两份配置之间不做自动比对，在两边的注释里互相指一下。

## 6. 内存

| | Istio 默认的 requests | 这里设 | 依据 |
|---|---|---|---|
| istiod | 500m / 2 Gi | 100m / 256 Mi，limits 512 Mi | 估的：十几个边车、几十条配置的网格。装上后按 `kubectl top` 改 |
| 每个边车 | 100m / 128 Mi，limits 1 Gi | 50m / 64 Mi，limits 192 Mi | 估的：比手写的（实际 17 Mi）多一个 pilot-agent 进程和整套动态配置。可见范围收窄之后应当在 40 到 60 Mi，官方的数是每秒 1000 请求时约 60 MB（那是 1.24 的基准） |

长期运行的那一套 10 个边车，比现在多承诺约 320 Mi，加 istiod 256 Mi；e2e 那一套跑的时候再多 10 个。两个 agent 节点的承诺量现在是 53% 和 67%，放得下。**server 节点不放任何网格的东西。** 阶段 3 的监控还要 5.5 GB，届时再算总账。

## 7. 不停机地切过去

约束：新旧形态不能在同一个 Pod 里共存（手写边车的入站端口 15001 正好是 Istio 出站拦截用的端口）；但可以在同一个服务的不同 Pod 之间共存，条件是调用方不管连到哪种 Pod 都能通。

1. **准备**（不改变行为）：装 istiod；给 Service 加 gRPC 端口；建好 ServiceAccount。命名空间先不打注入的标签。
2. **调用方先切**，顺序是 gateway、payment、order、inventory、orderindex，每次一个服务滚动：新 Pod 带 Istio 的边车，下游地址是 Service 名。
   - 新调用方连到**还没切**的下游 Pod：对方没有 Istio 边车，自动 mTLS 认出来，发明文，直接到进程的 gRPC 端口。这段时间这条边不经过被调用方的入站上限，可以接受。
   - **还没切**的调用方（手写边车，连 `<下游>-headless:15001`）连到下游：下游此时一定还是旧形态，因为顺序保证了一个服务的所有调用方都先于它切。
   - 所以这个顺序是调用关系的拓扑序：gateway 没有被调用方之外的调用者，先切；order 的调用者是 gateway 和 payment，都切完才轮到它；inventory 的调用者是 gateway 和 order。
3. 每滚一个服务之前先把这条边的 `VirtualService`、`DestinationRule` 建好：规则对还没注入的调用方没有任何作用，先建不会出事；后建的话新 Pod 起来的头几秒是“没有超时、重试两次”的默认行为。
4. 全部切完：删 headless Service 和旧的 ConfigMap；mTLS 改严格模式（如果第 8 项同意）。

全程 `probe.py` 开着，目标 0 失败；先在 `oppenheimer-e2e` 上演一遍。上面的推理有两处没有实证的假设，演的时候盯着：自动 mTLS 在同一个 Service 后面“有的 Pod 有边车、有的没有”时确实逐个端点判断；新调用方的出站在下游规则已存在、下游是旧 Pod 时正常。

**回退**：chart 的开关还在，任何一步出问题把那个服务的 values 改回去再滚一次；回退要按相反的顺序（被调用方先退）。

## 8. 实现顺序和分工

| 步 | 内容 | 谁做 |
|---|---|---|
| 一 | 装 istiod（`mesh.sh`、`make mesh-up`）；在临时命名空间里注入一个测试 Pod，验证重定向规则、AppArmor、原生 sidecar、探针；量 istiod 和空边车的内存 | Sonnet 写脚本，主会话在集群上执行和判断 |
| 二 | chart 的两种形态和开关、ServiceAccount、Service 的 gRPC 端口、注解；`deploy/cluster/mesh/` 的资源 | Opus 写 chart；调用边的资源由 Fable 写（超时和重试是一致性相关的：重试没关掉会重复执行写请求） |
| 三 | Go 的测试扩成核对三处 | Sonnet |
| 四 | `oppenheimer-e2e` 整套换成 Istio 形态：`cluster-e2e`、`cluster-chaos` 通过 | 主会话 |
| 五 | `scripts/cluster/fault.sh`，第 4 节的场景 A 到 G 和三个对照 | Opus 写，主会话执行；结果和 1d 不一致的地方由 Fable 查根因 |
| 六 | 在 `oppenheimer-e2e` 上演不停机切换，再切长期运行的那一套 | 主会话 |
| 七 | 重量：端点传播、负载均衡、HPA 的扩容点、延迟、内存；决定 `DRAIN_DELAY` | 主会话 |
| 八 | （第 8 项同意才做）mTLS 严格模式、`AuthorizationPolicy`，验证没有身份的调用被拒绝 | Fable |
| 九 | 删掉 chart 里手写边车的分支；从空集群重建验证一键部署；ADR、笔记、面试考点 | 主会话，笔记派 Sonnet |

第一步的结果可能推翻后面的安排（规则写不进去就要换 CNI 插件，内存远超估计就要重算），所以第一步做完先停下来看数。

要新增的 ADR：0029（用 Istio 的边车模式；ambient 为什么这次不用），ADR-0020 加一段补充（保护的实现从静态配置换成网格的资源，哪些行为变了）。

## 9. 不在这一步做的

- Istio 的指标、访问日志、链路：阶段 3。边车会自己产生一批指标（标签的基数要看），接入之前先不开额外的遥测。
- 入口的 Envoy 进网格、入口到 gateway 的 mTLS。
- ambient 模式的对照实验（第 2 节第 1 项）。
- 按失败比例的摘除、慢启动（`DestinationRule` 的 `warmup`）。后者正好对着 2c 留下的“扩容出来的新 Pod 一就绪就出 503”，但那个问题的推断原因是冷连接池，属于 order 自己的代码，先按那个方向修；慢启动作为第二手段，修的时候一起评估。
- 灰度发布、Argo CD、cert-manager（第 2 节第 8 项）。

## 10. 实现中的发现和量到的数

### 第一步：装 istiod，验证注入（2026-11-15）

`make mesh-up`（`scripts/cluster/mesh.sh`，values 在 `deploy/cluster/istio/istiod.yaml`）装了 `istio-base` 和 `istiod` 1.32.0，istiod 在 k3s-agent-1 上，28 秒就绪。`make mesh-check` 在临时命名空间里起一对注入了边车的 Pod（安全设置和业务服务相同），发一个请求，看服务端回显的头里有没有 `X-Forwarded-Client-Cert`，跑完删掉命名空间。

第 1 节里担心的几件事，结果都是没有问题：

| 担心的 | 结果 |
|---|---|
| nf_tables 后端下 init 容器写不进重定向规则 | 默认的写法（经 `iptables` 命令）就能用，`nativeNftables` 保持 false，不需要 CNI 插件 |
| AppArmor 拦 `NET_ADMIN` 的 init 容器 | 没有拦。三个节点的内核日志里只有 2b 记过的 `profile="who"` 那一种拒绝 |
| 原生 sidecar | `istio-proxy` 在 `initContainers` 里，`restartPolicy: Always` |
| `automountServiceAccountToken: false` 下边车拿不到令牌 | 拿得到：注入时自己挂了一个投射卷 `istio-token`，和自动挂载无关 |
| Pod 级的 `runAsNonRoot`、`runAsUser: 65532` 和注入的容器冲突 | 不冲突：`istio-init` 自己写了 uid 0，`istio-proxy` 是 1337，容器级的设置优先 |
| mTLS | 回显的头里有 `X-Forwarded-Client-Cert`，身份是 `spiffe://cluster.local/ns/mesh-check/sa/default` |

**顺带弄清了 2b 留下的一个小疑问**：`profile="who"` 读 `/etc/passwd` 被拒绝的记录，时间和从 MacBook ssh 登录节点的时刻一一对应，是登录时系统自己运行的 `who` 触发的，和集群无关。

**量到的内存（空载）**：istiod 44 Mi；每个边车 34 到 36 Mi（手写的是 16 到 18 Mi）。估的 requests（istiod 256 Mi、边车 64 Mi）够用。istiod 等业务服务都进了网格之后再量一次，够低就把 requests 降到 128 Mi。

**顺带看了 Istio 实际生成的 Envoy 配置**（`istioctl proxy-config`，在临时命名空间里建了一条 `VirtualService` 和一条 `DestinationRule` 再删掉），四点和设计时想的不完全一样：

1. **重试确实关掉了。** 网格默认配置里 `attempts: 0` 之后，没有 `VirtualService` 的路由上也没有重试策略；回显的头里 `X-Envoy-Attempt-Count` 是 1。
2. **路由上除了 `timeout: 1s` 还有 `maxGrpcTimeout: 1s`。** 这个字段的含义是“调用方带了 `grpc-timeout` 头就用头里的值，但不超过这个上限”。我们的调用方带的是预算加 500 毫秒，会被压到预算，结果上等于路由超时；但到期的那一刻走的是不是“路由超时”那条路径、计不计入摘除，是第 4 节那个疑点的具体形态，要用场景 B 实证。没有 `VirtualService` 时两个都是 0 秒（不限）。
3. **`DestinationRule` 里写 `maxRetries: 0` 不起作用**：生成出来是 4294967295，Istio 把 0 当成“没写”。它管的是重试的并发预算，重试策略本身已经关了，所以没有后果；清单里不写这一行，免得让人以为它在起作用。
4. **负载均衡的默认策略是 `LEAST_REQUEST`**，不是手写时的轮流。两个副本下差别不大，量负载均衡时留意。

其余和预期一致：`maxEjectionPercent: 100`、恐慌阈值是 0、`consecutive5xx: 5`、检查间隔 1 秒、摘除 5 秒、在途上限 128、集群类型是 EDS。

**实现时的两个小坑**：

- **1.32.0 的 chart 不在官方的 Helm 仓库里。** 仓库的索引只到 1.30.8（2026-11-06 生成），1.31 和 1.32 的正式版都没有，OCI 的地址也没有。改成从 GitHub 的发布包取（`mesh.sh fetch`，核对 sha256，放在 `./bin/istio-1.32.0/`，不入库），里面有 chart 和配套的 istioctl。
- **Helm 按键合并 values**：只写 `limits.memory` 的话默认的 `limits.cpu: 2000m` 还留着，要写 `cpu: null` 才去得掉。

结论：没有推翻后面安排的发现，按第 8 节继续第二步。
