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
- 注入按命名空间打标签（`istio-injection=enabled`）：`oppenheimer` 和 `oppenheimer-e2e`。cdc 在 Pod 上标 `sidecar.istio.io/inject: "false"`。e2e 的测试 Job、压测 Job 也不注入（它们模拟的是集群外的客户端，走入口）。**实现时改了两处**：注入改成按 Pod 的标签（`sidecar.istio.io/inject: "true"`），命名空间不打标签，逐个服务迁移才做得到；删 Pod 测试和压测的 Job 后来也注入了，因为严格模式下它们的 warmup 连不进 inventory（第 10 节的第二到四步、第八步）。

### 3.3 chart 的改动

- 手写的边车（initContainer、`<服务>-envoy` 的 ConfigMap、headless Service）整体去掉；切换期间用一个 values 开关在两种形态之间选，切完删掉旧分支。
- 普通的 Service 加上 gRPC 端口，端口名 `grpc`。Istio 靠端口名（或 `appProtocol`）认协议，认成 HTTP/2 才会按请求做负载均衡和超时；名字不对会被当成不透明的 TCP，按连接均衡，2c 解决的“长连接不均衡”原样回来，而且不报任何错。这一条要有检查（`istioctl analyze` 进 `make deploy` 的结尾）。
- 每个服务一个 ServiceAccount。
- Pod 模板的注解：入站只接管 gRPC 端口，出站排除中间件的端口，边车的 requests 和 limits。
- 下游地址从 `127.0.0.1:151xx` 改成 `<服务>:<gRPC 端口>`（同命名空间的短名字，两套命名空间不用区分）。
- `DRAIN_DELAY` 先不动（15 秒）。端点改成经 xDS 推送之后传播应当比 DNS 快得多，按 2c 第四步同样的办法重新量，有了数再决定要不要降。（量了之后降到 5 秒，第 10 节的第七步。）

### 3.4 1d 的每一条怎么写

每条调用边仍然是“调用方一侧”的配置。同一个下游被两个调用方用不同的数（order 被 gateway 用 3 秒、512，被 payment 用 1 秒、128），所以规则要按调用方区分：

| 调用边 | 超时 | 并发上限 | 怎么按调用方区分 |
|---|---|---|---|
| gateway → order | 3s | 512 | 超时：`VirtualService` 的路由按 `sourceLabels` 匹配调用方；并发和摘除：`DestinationRule` 的 `workloadSelector` 选调用方的 Pod（没有查证，实现时对参考页；不行就退到每个调用方一个 `exportTo` 受限的规则） |
| gateway → inventory、payment、orderindex | 3s | 128 | |
| order → inventory | 1s（`Reserve` 是 3s，2026-11-21 起按路径单独一条路由 `from-order-reserve`，ADR-0020 的补充） | 128 | |
| payment → order | 1s | 128 | |

每条边的 `VirtualService` 路由：`timeout` 写上表的数，**`retries: {attempts: 0}`**。另外在网格配置里把默认的重试策略也设成 0 次（`meshConfig.defaultHttpRetryPolicy`），这样漏写某条路由时的结果是不重试，而不是悄悄重试两次。1.32 新增了入站一侧的默认重试策略（`defaultInboundHttpRetryPolicy`），同样关掉。

每条边的 `DestinationRule`：

```yaml
trafficPolicy:
  connectionPool:
    http: {http2MaxRequests: 128, http1MaxPendingRequests: 128}   # 草稿里还有 maxRetries: 0，实测不起作用，去掉了（第 10 节第一步）
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

mTLS 和访问控制（第 2 节第 8 项，主人同意才做）：迁移期间 `PeerAuthentication` 用默认的 `PERMISSIVE`（明文和 mTLS 都收，第 7 节的不停机切换依赖它）；全部切完后改成 `STRICT`（实现是命名空间级的一条：它只对被边车接管的端口起作用，而接管的只有 gRPC 端口，效果相同），再加 `AuthorizationPolicy`：order 的 gRPC 只允许 gateway 和 payment 的身份调，inventory 只允许 gateway 和 order，以此类推。

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

### 第二到四步：chart 的两种形态、调用边的资源、e2e（2026-11-15）

**做了什么**

- chart 加了 `mesh: envoy | istio`（Opus 写，主会话读过 diff）。默认形态的渲染结果和改动前逐字节相同（六个服务、三种叠加，18 份对比）。Istio 形态：不渲染手写边车的任何东西；**按 Pod 的标签注入**（`sidecar.istio.io/inject`），命名空间不打标签，这样同一个命名空间里的测试 Job 和还没切的服务不受影响；每个服务一个 ServiceAccount；Service 多一个名为 `grpc` 的端口。每个服务在 Istio 形态下的差别放在 `deploy/cluster/services/istio/<服务>.yaml`。
- `scripts/cluster/services.sh` 的 `MESH_ISTIO`：空是全部手写，`all` 是全部 Istio，或者列出服务名（迁移的中间态）。非空时先对命名空间应用 `deploy/cluster/mesh/` 再部署 release；还没切的服务的 Service 自动带上 gRPC 端口（最初是另一个要手工设的变量，Opus 指出漏设会让已经切过去的调用方断流，改成自动）。
- `deploy/cluster/mesh/`（Fable 写）：每个被调用方一个文件，里面是它的 `VirtualService`、每个调用方一条 `DestinationRule`、它自己的 `Sidecar`；`gateway.yaml` 只有 `Sidecar`。
- 三个 Go 测试（Sonnet 写）：`TestHopTimeoutMatchesMesh`（gateway）、`TestInventoryHopTimeoutMatchesMesh`（order）、`TestOrderHopTimeoutMatchesMesh`（payment）。核对调用方的那条路由存在、超时等于 Go 的常量、`retries.attempts` 是 0、兜底路由在最后。改超时、删 `retries`、把兜底挪到最前三种改坏的情况都会失败。它们不核对路由的目标和并发上限，那些靠故障注入守着。

**`MESH_ISTIO=all make cluster-e2e` 第一次就通过**，19 个用例，包括核对链路的那一个。

**边车实际拿到的配置**（`istioctl proxy-config`，在保留的 e2e 现场上看的）：

- 按调用方区分是生效的：gateway 的边车里到 order 的在途上限是 512、路由超时 3 秒；payment 的边车里到 order 是 128、1 秒；order 的边车里到 inventory 是 128、1 秒。没有为它写规则的边（比如 payment 到 inventory）没有上限和摘除，路由落在兜底那条上。
- 所有出站集群都带 mTLS 的设置；路由上都没有重试策略。
- 可见范围收窄生效：order 的边车一共 23 个集群，只有本命名空间的服务和 istiod，没有中间件。
- 入站上限生效：order 的入站集群上限是 1024。
- 内存：边车 32 到 37 Mi，istiod 59 Mi（7 个边车）。

**路由超时计入摘除**（第 4 节的疑点，先手工做了一次；正式的在第五步）。在节点上对 e2e 的 inventory 进程发 `SIGSTOP`，从入口每轮并发 8 个查库存：

| 轮 | 结果 |
|---|---|
| 第 1 轮 | 8 个都在 3.01 秒返回 503 |
| 第 2 轮 | 8 个都在 3.10 秒返回：4 个 503，**4 个 504** |
| 第 3 轮 | 8 个都在 12 到 19 毫秒返回 503；gateway 的边车里这个端点是 `UNHEALTHY`、异常检测 `FAILED` |
| `SIGCONT` 之后 | 又过 5 秒恢复正常 |

两个 gateway 各有各的统计，第 1 轮每个摊到 4 个，不够 5 次，所以第 2 轮才摘除。**和 1d 的结论一致：`maxGrpcTimeout` 没有妨碍路由超时计入摘除。**

**一个和 1d 不一样、还没有查清的现象：超时的请求里有一部分是 504。** 1d 里边车超时调用方看到的一律是 `UNAVAILABLE`（503），504 只在边车不在时出现。推断（没有证实）：Istio 的出站边车会把剩余时间经请求头传给下游，被调用方的入站边车也按它计时，两边差不多同时到期；入站那边先到时返回的是 `DEADLINE_EXCEEDED`，gateway 映射成 504。对保护没有影响（摘除照常发生），但“503 表示边车超时、504 表示边车不在”这条 1d 的判断方法在集群上不成立了。第五步查到了支持这个推断的统计（见第五步的发现三）。

**`istioctl analyze` 查出的问题**：`VirtualService` 的 `destination` 没写端口，Service 有 HTTP 和 gRPC 两个端口时有歧义（IST0112）。已补上端口。连带的一个要知道的后果写在清单的注释里：路由不按端口匹配，网格里的 Pod 访问别的服务的 HTTP 端口也会被送到 gRPC 端口；服务之间没有这样的调用。剩下一条提示：e2e 的 OTLP 接收端的端口名 `otlp-grpc` 不符合 Istio 的命名约定，被当成 TCP 透传，链路的用例通过，没有改。

**子 Agent 指出、没有处理的**：

- Istio 形态的调用方连还没切的被调用方时，绕过了对方手写边车的入站上限（迁移期间，接受）。
- `e2e_inventory_addr` 里 inventory 的端口 9082 是写死的。
- HPA 的分母变了：边车的 CPU requests 从 20m 变成 50m，同样的负载下利用率变低（第七步重量）。

**`MESH_ISTIO=all make cluster-chaos` 两次都通过**（压测 120 秒，每 5 秒随机删一个业务 Pod，静止后 `verify` 和 `recon` 无差异，没有意外的容器重启）：

| | 删掉的 Pod | 受理 | 客户端看到的 5xx | 2c（手写边车）同一项 |
|---|---|---|---|---|
| 正常删除 | 22 个 | 356 单 | 0 | 23 个 Pod，0 |
| 强制删除（`CHAOS_FORCE=1`） | 22 个（其中 order 8 个） | 339 单 | 133 个下单 503；其中 50 次提交最终没拿到订单号，库里也没有这些订单（对得上） | 22 个 Pod，29 个 |

正常删除和 2c 一样是零报错：摘流的那一套（`DRAIN_DELAY`、原生 sidecar 的停止顺序）在 Istio 的边车下照样成立。强制删除的 5xx 是 2c 的四倍多。两次运行删到的服务不同（这次 order 被删了 8 次，下单最依赖它），不能直接比；但也可能和摘除时间逐次加长有关（ADR-0029 的第 5 项）：被强制删掉的 Pod 的地址在调用方的边车里先被摘除，新 Pod 是新地址，按理不受影响。没有查，第五步的场景 G 和第七步重量时一起看。日志在 `tmp/cluster-chaos-logs/1794757258`（正常）和 `1794757535`（强制）。

### 第五步：故障注入（2026-11-16）

`make cluster-fault`（`scripts/cluster/fault.sh`，Opus 写，主会话执行；叠加层 `deploy/cluster/services/fault/`：gateway 和 inventory 各 2 个副本、关限流、打开边车的集群级统计）。在 `oppenheimer-e2e` 上以 Istio 形态部署一套，请求从 MacBook 经入口发。镜像里没有 shell，“挂住”是在节点上按容器号取进程号再发 `SIGSTOP`；存活探针约 30 秒后会重启被挂住的容器，所以每次挂住不超过 15 秒，另有看门狗和退出钩子保证进程被恢复。`FAULT_SCENARIOS` 选场景，`FAULT_SKIP_DEPLOY=1` 复用保留的现场。

第二次运行八个场景全部通过（第一次有两个失败，都是脚本的问题，见后）：

| 场景 | 结果 | 1d（本机、手写边车） |
|---|---|---|
| A 基线 | 查库存 p50 10 毫秒，下单并落库 | p50 1 毫秒（本机没有入口这一跳） |
| B 两个 inventory 都挂住 | 摘除生效之前的 61 个请求都在 3.01 到 3.03 秒返回（503 41 个、504 20 个）；之后在 50 毫秒内返回 503；两个 gateway 的边车里两个端点都是 `FAILED`；期间查订单 20 次全部 200，延迟和基线一样；恢复进程后 0.5 到 1.8 秒查库存恢复（取决于挂了多久：摘除从生效起算 5 秒） | 前 5 次各 3 秒，之后毫秒级；恢复 5.5 秒 |
| B' 只挂住一个副本（新） | 失败 31 个请求（一批 60 个里分到坏副本的那些，各等 3 秒），之后 50 个全部 200、最长 15 毫秒；好的那个副本一直是 `OK` | 没有这个场景 |
| C 两个 inventory 被强制删除 | 40 个请求失败，全部是 503，最长 13 毫秒，没有一个等超时；中断 4.6 秒，新 Pod 就绪就恢复 | 12 次最长 5 毫秒 |
| D 并发上限（同时 400 个，两个 gateway） | 恰好 144 个在 1 秒内被拒绝，256 个在途（2×128），没有超过 5 秒的 | 300 个里恰好 172 个被拒绝、128 个在途 |
| E 业务错误 | 30 次 404，摘除 0 次 | 一样 |
| F 不重试（新） | 912 个请求，gateway 进程交给边车 912 个，边车发往 inventory 637 个、本地拒绝 275 个（637 + 275 = 912，没有多发），`upstream_rq_retry` 增量 0。**对照**（把这条路由改成重试 2 次）：`upstream_rq_retry` 增量 92 | 没有这个场景 |
| G 重复摘除 4 次 | 见下 | |

“坏了的下游不拖垮别的接口、不需要人干预就恢复”在 Istio 的边车上成立，数值和 1d 的手写边车对得上。F 的对照说明量得到重试，所以“增量是 0”说明的是重试确实关了。

**发现一：摘除不是立刻生效的，要晚 0.5 到 1 秒。** 一批请求同时超时（每个端点远超过 5 次）之后，紧接着发的请求仍然被送到坏的端点、各等满 3 秒；0.5 到 1 秒之后才开始被立刻拒绝。单独量了五次（一批超时返回后每 100 毫秒探一次）：+25 毫秒、+500 毫秒的探测还被送过去，+980 毫秒的开始被拒绝，五次都一样。

- 这解释了 1d 里那个“没有解释的现象”（5.13：重启后约 30 毫秒查库存就成功了，而统计里显示还在摘除中）：统计里记了摘除，但负载均衡还没有用上这个结果。
- 把异常检测的 `interval` 临时从 1 秒改成 3 秒再量，延迟仍然是 0.5 到 1 秒，所以不是检查间隔。和 Envoy 集群的 `update_merge_window`（默认 1 秒：只改变端点健康状态的更新会被攒起来，窗口结束时一起下发给负载均衡）吻合；**没有直接证实**（要用 EnvoyFilter 把它改成 0 再量）。
- 后果：下游整体变慢时，等满超时的请求不止“每个调用方 Pod 的前 5 个”，而是“前 5 个加上之后最多 1 秒里到达的全部”。并发上限仍然管着（场景 D），所以在途的不会超过 128 个。

**发现二：摘除时间没有逐次加长。** 场景 G 连续四次“挂住、摘除、恢复”，每次间隔约 10 秒，从摘除生效到恢复都是 5.1 到 5.4 秒。Envoy 的摘除时间是基数乘以“被摘除的次数”，但这个次数在端点健康时每个检查间隔减一；间隔设的是 1 秒，两次故障之间健康几秒就减回去了。ADR-0029 第 5 项担心的“恢复得越来越慢”在这个配置下没有出现，只有端点刚放回来就又坏的情况才会累加。不需要 EnvoyFilter。1d 当时量到的“第二次是 10 秒”是紧接着发生的两次。

**发现三：超时的请求有一部分是 504，原因是被调用方的入站边车也在计时。** 出站边车的统计里，来自上游的响应有 `grpc-status 4`（`DEADLINE_EXCEEDED`）：这个状态码不是出站边车自己超时产生的（它自己超时产生的是 504 映射成的 `UNAVAILABLE`），是 inventory 一侧的边车返回的。出站边车把剩余的时限经请求头传了下去，两边差不多同时到期，谁先到期决定调用方看到 503 还是 504，比例每次不同（0/80、20/61、13/256）。两种都计入摘除。**1d 的“503 是边车超时、504 是边车不在”在集群上不成立了**；gateway 对两种的处理本来就一样（都是失败、都不重试），客户端按幂等键重试，所以没有行为上的后果，但排查时不能再靠这个区分。

**第一次运行失败的两处，都是脚本的问题**：Istio 的统计项名字在集群名和后缀之间多一个分号（`cluster.outbound|9082||inventory...;.upstream_rq_total`），解析不到，所有统计都是 NA；场景 C 把强制删除后进程还没被杀掉时返回的那个 200 当成了“恢复”。改了之后用保存的快照重算，第一次运行的数据同样满足断言。

**顺带的观察**：

- 强制删除后的第一个请求还能成功：API 对象立刻没了，但进程要等 kubelet 去杀。
- 就绪探针比摘除慢：挂住约 4 到 8 秒后端点才被标成 `UNHEALTHY`，摘除是 3 秒超时加不到 1 秒。两者是独立的两条路径，摘除先到。
- Istio 默认只保留很少的统计项，集群级的要在 Pod 上用 `proxy.istio.io/config` 注解的 `proxyStatsMatcher` 打开（chart 加了通用的 `podAnnotations`，只有故障注入的叠加层在用）。阶段 3 接监控时要定保留哪些。

日志在 `tmp/cluster-fault-logs/1794758107-*`。

### 第六步：不停机地切过去（2026-11-16）

**先在 e2e 的命名空间里演了一遍，来回各一趟**：从全部 Istio 形态按“被调用方先退”的顺序逐个退回手写边车（inventory、orderindex、order、payment、gateway），再按“调用方先切”的顺序逐个切回 Istio。十次滚动，探针（`probe.py`，三条同步调用链）19870 个请求，**0 失败**。第 7 节里两处没有实证的假设都成立：同一个 Service 后面有的 Pod 有 Istio 边车、有的没有时，调用方的边车逐个端点决定用不用 mTLS；规则先于 Pod 存在没有副作用。

**演练抓到一个回退时才出现的缺陷，已修。** 第一次演练退回 inventory 时滚动卡在一半：新的 ReplicaSet 建出一个 Pod 之后报 `serviceaccount "inventory" not found`。原因是两件事叠在一起：

- 手写边车形态的模板不写 `serviceAccountName`。但 Pod 模板里还有一个已废弃的同义字段 `serviceAccount`，API 在 `serviceAccountName` 被删掉时不清它，于是新模板实际上仍然指着 `inventory` 这个账号。
- 同一次 `helm upgrade` 把这个 ServiceAccount 删了（手写边车形态不渲染它）。第一个新 Pod 赶在删除之前建了出来，第二个就建不出来了。

探针这期间是 0 失败（旧 Pod 还在服务），但滚动永远完不成。修法：手写边车形态显式写 `serviceAccountName: default`。**教训是回退路径要和前进路径一样演练**：前进的方向一次就通过，问题只在退回时出现。

**长期运行的那一套（命名空间 `oppenheimer`）已经切完。** 先做一次准备性的滚动（新镜像 `c54615c`、所有 Service 加上 gRPC 端口，形态不变），然后 gateway、payment、order、inventory、orderindex 逐个切，最后是 cdc（不注入，只是换成有自己的 ServiceAccount）。七次部署，约 7 分钟，探针 18770 个请求，**0 失败**；延迟 p50 10 到 13 毫秒，p99 15 到 34 毫秒。切完之后 headless Service 和 `<服务>-envoy` 的 ConfigMap 随 release 自动删掉了，`istioctl analyze` 没有问题，十个边车都和 istiod 同步。

**切完之后把默认形态改成了 Istio**：没设 `MESH_ISTIO` 时是 `all`（原来是全部手写边车）。不改的话，一次普通的 `make deploy` 会把十个 Pod 全部退回去。显式设成空（`MESH_ISTIO= make deploy`）才是手写边车。控制面不在时部署脚本直接报错，提示先 `make mesh-up`。默认形态渲染出来的东西和集群上正在跑的没有差异（`kubectl diff` 为空）。

**后来（第九步）开关删掉了**：chart 的 `mesh`、手写边车的模板、`deploy/cluster/services/istio/` 和脚本的 `MESH_ISTIO`、`SERVICE_GRPC_PORT` 都没有了，集群上只有 Istio 一种形态。上面写到这些名字的地方是当时的做法；要回到手写边车只能从 git 历史里恢复（提交 `35efd5d` 及之前）。

### 第八步：mTLS 严格模式和按身份的访问控制（2026-11-16）

`deploy/cluster/mesh/security/policies.yaml`（Fable 写）：一条命名空间级的 `PeerAuthentication`（`STRICT`），每个被调用方一条 `AuthorizationPolicy`，只允许它的调用方的身份（order 允许 gateway 和 payment，inventory 允许 gateway 和 order，payment、orderindex 只允许 gateway）。它们只管被边车接管的端口，也就是 gRPC 端口；探针和 gateway 对外的 HTTP 端口不受影响。

- **身份里带命名空间**（`cluster.local/ns/<命名空间>/sa/<服务>`），所以这份文件是模板，部署脚本把 `__NS__` 换掉。不用后缀通配（`*/sa/gateway`）：那样 e2e 命名空间的 gateway 也能调长期运行的那一套的 order。
- **应用的时机**：只在全部服务都是 Istio 形态时应用，而且在 release 滚动完之后（命名空间里还有手写边车的 Pod 时跳过并提示）；不是 `all` 时反过来在滚动之前先删掉，回退的第一步是放开。
- **测试的发压 Pod 进了网格**。删 Pod 测试和压测的 Job 在 warmup 阶段直接调 inventory 的 gRPC，严格模式下没有证书连不进去。它们现在注入边车（只让到集群内服务的出站过边车：中间件的端口和入口的地址都排除，发压的请求不多过一层代理），用自己的 ServiceAccount `load`，由只存在于 e2e 命名空间的 `inventory-load-test` 放行。**这条策略必须和 `inventory-callers` 一起应用**：单独先建的话，inventory 上只有“允许 load”这一条 ALLOW，gateway 和 order 的调用反而全部被拒绝（ALLOW 的语义：工作负载上只要有 ALLOW 策略，没被任何一条允许的就拒绝）。

验证：

| | 结果 |
|---|---|
| `make cluster-chaos`（严格模式加访问控制下，删 Pod） | 通过，客户端 0 个 5xx，`verify` 和 `recon` 无差异 |
| 名单里的身份（`load` 调 inventory 的 gRPC 端口） | 到了应用（返回 415，gRPC 服务端不认普通的 HTTP 请求） |
| 名单外的身份（`load` 调 order、payment） | `403 Forbidden` |
| 没有边车的 Pod 调 order 的 gRPC 端口 | 连接被重置 |
| 同一个没有边车的 Pod 调 order 的 HTTP 端口 `/healthz` | 正常（不过边车） |
| 应用到长期运行的那一套时 | 探针 3400 个请求 0 失败 |

2c 留下的“任何 Pod 都能直接调任何服务的 gRPC”这个口子关上了。gateway 的 HTTP 入口仍然谁都能连（`TRUSTED_PROXIES` 信整个 Pod 网段的问题还在），那一段不在网格里，要靠 NetworkPolicy，这次没有做。

### 第七步里做了的（2026-11-16）

- **负载均衡**：探针跑 40 秒，两个 inventory 各处理 524 和 496 个 gRPC 请求，两个 order 是 512 和 508。按请求均衡，和手写边车时一样。
- **内存**：十个边车平均 32 Mi，最大 34 Mi；istiod 76 Mi。比手写的边车每个多约 15 Mi，合计约 150 Mi，加 istiod 共 230 Mi 左右。requests（边车 64 Mi、istiod 256 Mi）可以各降一半，没有改。
- **滚动发布零报错**：迁移本身就是七次滚动，0 失败。

- **端点传播时间，`DRAIN_DELAY` 从 15 秒降到 5 秒。** 探针开着，删一个 Pod，看它的日志里最后一个请求离它收到 SIGTERM 有多远。inventory 3 次、gateway 3 次、order 2 次：最后一个请求都落在 SIGTERM 之前 0.5 秒到之后 0.2 秒之间（探针每 0.5 秒发一批，所以分辨率就是 0.5 秒）。2c 里调用方的边车靠 DNS 发现端点，量到的是 1.7 到 9.5 秒。现在端点的变化由 istiod 经 EDS 推给每个边车，入口那一侧本来就是 Envoy Gateway 的 xDS。`DRAIN_DELAY` 改成 5 秒（chart 的默认值；量到的传播时间不超过 0.5 秒，留了十倍的余量），之后带着探针做了一次部署、一次全部重启、四次删 Pod：9570 个请求 0 失败。这一次没有再做 `DRAIN_DELAY=0` 的对照（2c 做过：每删一个 Pod 失败约 15 个）。**退回手写边车时要改回 15 秒。**

**没有做的**：HPA 的扩容点没有重量（边车的 CPU requests 从 20m 变成 50m）；经边车和直连的延迟对比没有做。

### 第九步：删掉手写边车的分支，从空集群重建（2026-11-16）

**删分支**（Opus，主人确认现在删）：chart 的 `mesh`、`sidecar`、`headless`、`service.grpcPort` 和对应的模板，`deploy/cluster/services/istio/`（并进了各服务的基础 values），脚本的 `MESH_ISTIO`、`SERVICE_GRPC_PORT`、渲染和校验手写边车配置的函数。27 个文件，减 419 行、加 78 行。判据：六个服务和 e2e 的五种叠加组合，删之前和删之后的渲染结果去掉注释行后逐字节相同；对集群上正在跑的那一套 `kubectl diff` 为空。本机 compose 和 CI 用的 `deploy/envoy/`、`make envoy-validate`、`make fault` 都没有动。

**从空集群重建**（主人确认后做的）：删掉三台虚拟机，然后 `make cluster-up`、`make middleware-up`、`make mesh-up`、`make mesh-check`、`make deploy`、`make deploy-seed`。

| 步骤 | 耗时 |
|---|---|
| 删三台虚拟机 | 3 秒 |
| `make cluster-up` | **跑了四次才完成**，前三次分别在 114 秒、37 秒、194 秒处失败 |
| `make middleware-up` | 8 分 49 秒 |
| `make mesh-up` | 41 秒 |
| `make mesh-check` | 36 秒 |
| `make deploy` | 1 分 51 秒 |
| `make deploy-seed` | 不到 1 秒 |

- 验收：11 个业务 Pod 全部就绪（10 个带 `istio-proxy`），严格模式和四条访问控制策略都在，整个集群没有一个容器重启过；探针 30 秒 1380 个请求 0 失败；`make cluster-e2e` 19 个用例通过。本机 `make lint`（11 个模块 0 issues）、`make test`、`make e2e`（19 个用例）通过。
- **`cluster-up` 的三次失败都是经代理的下载断了**，和网格无关：两次是节点上下载 k3s 的校验文件（`Download failed`），一次是 Helm 从 Docker Hub 取 Envoy Gateway 的 chart（`EOF`）。每一步都可以重复执行，重跑就接着做；但“一条命令从头跑到尾”这次没有做到。脚本里这几处下载没有重试，是可以补的（k3s 的安装脚本和 `helm upgrade --install` 外面各包一层有上限的重试）。没有改。
- 这次的连续脚本在 `cluster-up` 失败之后没有停下来（写法的问题），所以中间件、Istio 和业务服务是在入口还没装上的情况下装好的，入口最后补上。顺带说明这几样之间没有安装顺序上的依赖（HTTPRoute 用的 CRD 是 k3s 自带的）。
- 重建之后 server 节点的内存是 73%，agent 是 36% 到 38%。2c 重建后轻载时 server 是 48%。网格没有任何东西在 server 上（istiod 在 agent 上，66 Mi），多出来的没有查。

## 11. 还没做的

- 第七步剩下的两项（上面）：HPA 的扩容点、延迟对比；边车和 istiod 的 requests 可以各降一半。
- 一键部署里几处经代理的下载没有重试（第九步）。
- 第 4 节计划的两个对照没有做：`maxEjectionPercent` 用默认值时不摘除、路由不写 `timeout` 时慢下游不摘除。做了的对照只有“把重试打开”（场景 F）和“检查间隔改成 3 秒”。这两条在 1d 的手写边车上证过，Istio 下只确认了生成出来的配置是对的。
- 摘除晚 0.5 到 1 秒生效的原因没有直接证实（用 EnvoyFilter 把 `update_merge_window` 设成 0 再量）。
- 强制删 Pod 时客户端的 5xx 比 2c 多（133 对 29），两次删到的服务不同，没有做可比的对照。场景 C 表明被删的 Pod 是毫秒级失败，摘除时间也不累加，所以大概率只是这次 order 被删得多；没有证实。
- `e2e` 的 OTLP 接收端的端口名不符合 Istio 的约定（被当成 TCP 透传）；`e2e_inventory_addr` 里 inventory 的端口是写死的。
- 手写边车的入站上限在 Istio 下换成了 `Sidecar` 的 `inboundConnectionPool`，配置确认下发了（上限 1024），**没有压到上限验证过**。
- istiod 不在时的行为（已有的边车继续工作、新 Pod 建不出来）没有实测，放进阶段 4。
- server 节点的内存偏高（重建后 73%，2c 重建后是 48%）；网格没有任何东西在它上面，来源没有查。阶段 3 装监控之前要弄清。
