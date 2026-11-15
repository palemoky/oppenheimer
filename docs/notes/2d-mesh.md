# 2d 笔记：把服务之间的边车换成 Istio

这份笔记整理 2d 里值得在面试中讲的点，按主题组织，不按开发步骤。每个主题尽量按“原理（为什么是这样）、这个项目里怎么做的和量到的数、面试时怎么讲”来写。设计全文见 [design/2d-mesh.md](../design/2d-mesh.md)（下文简称“设计”，引用章节号；“设计 10”指第 10 节“实现中的发现和量到的数”），取舍见 [ADR-0029](../adr/0029-istio-sidecar-mesh.md)。对照的前文：1d 的 Envoy 实测和超时重试规范在 [design/1d-protection.md](../design/1d-protection.md) 的 5.3 和 5.6（下文“1d 的坑 1、2、3”指 5.3 的三个坑），2c 手写边车和摘流在 [notes/2c-services.md](2c-services.md) 的第 3、4 节。

2d 做的事：把 2c 里手写的 Envoy 边车换成 Istio 的边车模式（版本 1.32.0）。1d 实测过的熔断、并发上限、路由超时逐项改写成 Istio 的资源并重新验证；在 `oppenheimer-e2e` 上演练过之后，不停机地把长期运行的那一套切了过去；最后加上 mTLS 严格模式和按身份的访问控制。Argo CD、cert-manager、灰度发布、NetworkPolicy 没做。

**关于“通用知识”**：文中标注“（通用知识）”的段落，是为了把原理讲清楚而补的 Istio、Envoy、Linux 常识，设计文档和 ADR 里没有出处，**面试前要对照官方文档核对**。没有标注的内容，事实来自设计文档、ADR、阶段计划或清单文件。设计里标了“没有证实”“推断”“没有查”“没有实测”的地方，这里保持同样的说法，不要在面试时说成确定的结论。数字都是 2026-11-15、2026-11-16 在这个集群上量的，照抄，没有换算。

## 0. 考点与小节的对照

阶段计划“面试考点”里“2d 补充”一共 14 条，对应关系如下：

| # | 计划里的考点（缩写） | 小节 |
|---|---|---|
| 1 | 服务网格解决什么；控制面和数据面；xDS 的几种资源 | 1 |
| 2 | 透明拦截怎么做到的；和显式代理比，排查难在哪 | 2 |
| 3 | 默认重试为什么危险；重试放哪一层；怎么证明重试关了 | 3、6（场景 F） |
| 4 | 熔断是两样东西；默认值哪些要改（`maxEjectionPercent`）；摘除为什么不是立刻生效 | 4、5、6（发现一） |
| 5 | 慢下游怎么被熔断；进程里的超时必须更长 | 5 |
| 6 | `DestinationRule` 作用在调用方；按调用方区分额度 | 4 |
| 7 | 端口名改变行为；认成 TCP 会怎样、为什么不报错 | 7 |
| 8 | `Sidecar` 资源收窄可见范围 | 8 |
| 9 | 不停机进网格；`PERMISSIVE`；按端点判断；调用方先切；回退要演练 | 9 |
| 10 | mTLS 身份；`PeerAuthentication` 与 `AuthorizationPolicy`；ALLOW 语义和应用顺序 | 10 |
| 11 | 删掉 `serviceAccountName` 为什么不生效 | 9.4 |
| 12 | 边车模式和 ambient | 14 |
| 13 | istiod 挂了会怎样 | 13 |
| 14 | 原生 sidecar 之前的老问题 | 15 |

另外：端点传播和 `DRAIN_DELAY` 在 11，资源开销在 12，实现里的小坑在 16，没做的和没弄清的在 17，速记在 18。

---

## 1. 服务网格解决什么，控制面和数据面

**问**：服务网格是什么？为什么需要它？

**（通用知识）要点**：

- 微服务之间的“横切功能”：找下游的实例（服务发现）、按请求做负载均衡、超时、重试、熔断、加密和身份认证、观测。不用网格时这些要么写在每个服务的代码里（每种语言一份），要么像 2c 那样手写代理配置。网格把它们从代码里拿出来，放进每个 Pod 旁边的代理（数据面），由一个控制面统一下发配置。
- **数据面**：Envoy 代理，Pod 里的 `istio-proxy` 容器，真正转发请求、执行超时、熔断、mTLS。
- **控制面**：istiod。它监听 Kubernetes 的 Service、EndpointSlice 和 Istio 自己的资源（`VirtualService`、`DestinationRule` 等），翻译成 Envoy 的配置，通过 xDS 协议推给每个代理；同时作为 CA 给每个代理签发身份证书；它还有注入用的准入 webhook。
- **xDS 各类资源的职责**：LDS（监听器：代理在哪些端口收什么流量）、RDS（路由：请求按什么规则选集群，超时、重试写在这里）、CDS（集群：上游服务的定义，连接池上限、异常检测写在这里）、EDS（端点：一个集群下具体有哪些 Pod 的地址，Pod 增减时推送的主要是它）。面试前对照 Envoy 官方文档核对。

**这个项目里的对应**（设计 3.1）：

| | 2c（手写） | 2d（Istio） |
|---|---|---|
| 边车从哪来 | chart 里写的 initContainer，配置是 ConfigMap 里的静态 YAML | istiod 的准入 webhook 在 Pod 创建时注入，配置经 xDS 动态下发 |
| 调用方怎么连下游 | 连自己边车的出站端口（`127.0.0.1:15182`） | 直接写下游的 Service 名（`inventory:9082`） |
| 下游的端点怎么到调用方 | headless Service 加 DNS，`STRICT_DNS` 定期解析 | istiod 监听 EndpointSlice，变化经 EDS 推送 |
| 服务之间的连接 | 明文 | 自动 mTLS，证书由 istiod 签发并轮换 |
| 配置写在哪 | `deploy/envoy/*.yaml`，每个边车一个文件 | `VirtualService`（超时、重试）、`DestinationRule`（并发上限、摘除）、`Sidecar`（入站上限、可见范围） |

istiod 在这个集群里：1 个副本，关了它自己的 HPA，不上控制面节点（server 节点只有 6 GB，上面有 etcd），requests 100m / 256Mi，limits 512Mi（`deploy/cluster/istio/istiod.yaml`）。

**不变的东西**（ADR-0029）：保护仍然在代理上而不在进程里；每个调用方对每个下游各有额度；同步链上不重试；进程里的时限等于边车的预算加 `grpcx.SidecarMargin`，边车先超时。2d 是“迁移”不是“重新设计”，这也是选边车模式的理由之一（见 14）。

**面试时怎么讲**：先说它解决的问题（把横切功能从业务代码里拿出来），再说两个平面（控制面算配置，数据面执行），再讲你亲手从“手写静态配置”迁到“控制面下发”的经历：端点从 DNS 轮询变成 EDS 推送，量到的变化见 11。

---

## 2. 透明拦截和“显式代理”

**问**：进程怎么“不知道边车存在”？和 2c 的做法比，难在哪？

**原理（通用知识）**：Pod 里所有容器共享一个网络命名空间。注入时多一个 init 容器（`istio-init`，要 `NET_ADMIN` 权限），在这个命名空间里写 iptables 规则：出站的 TCP 连接重定向到边车的 15001 端口，入站的重定向到 15006 端口（端口号是 Istio 的约定，面试前对照官方文档）。边车收到后能还原原本的目的地址（从内核记录的原始目的地址里取），再按自己的配置转发。进程照常连 `inventory:9082`，连接已经被内核改了去向。边车自己出去的连接要打上标记（用特定的用户 id 1337），免得又被规则重定向回来形成回环。

**显式代理（2c）**：进程自己把地址指到代理，调用方连 `127.0.0.1:15182`，代理再连下游。看配置就知道流量去了哪。

**设计怎么说**（设计 3.1）：这是迁移里最值得弄明白的原理。透明的代价是出了问题时进程看不出流量去了哪，要会用 `istioctl proxy-config` 看边车实际拿到的配置。这个项目里确实这样用过：读生成的路由、集群，确认“超时 1s”“上限 512”“没有重试策略”到底有没有下发（见 4、5）。

**这个项目里量到的相关事实**（设计 10 第一步）：

- 节点的 iptables 是 nf_tables 后端（`iptables v1.8.11 (nf_tables)`），设计时担心 init 容器写不进规则。结果：默认写法（经 `iptables` 命令）就能用，`nativeNftables` 保持 false，不需要 Istio CNI 插件。
- 2b 里这批节点的 AppArmor 拦过容器之间的信号，设计时担心也拦 `NET_ADMIN` 的 init 容器。结果没有拦，三个节点的内核日志里只有 2b 记过的 `profile="who"` 那一种拒绝。这个拒绝后来弄清了：时间和从 MacBook ssh 登录节点的时刻一一对应，是登录时系统自己运行的 `who` 触发的，和集群无关。
- Pod 级的 `runAsNonRoot`、`runAsUser: 65532` 和注入的容器不冲突：`istio-init` 自己写了 uid 0，`istio-proxy` 是 1337，容器级的设置优先。
- `automountServiceAccountToken: false` 没有影响边车：注入时它自己挂了一个投射卷 `istio-token`。

**哪些流量不过边车**（chart 的注解，`deploy/charts/service/templates/deployment.yaml`）：

```yaml
traffic.sidecar.istio.io/includeInboundPorts: <gRPC 端口；gateway 没有 gRPC，写空字符串>
traffic.sidecar.istio.io/excludeOutboundPorts: "3306,6379,16379,26379,9092"
sidecar.istio.io/rewriteAppHTTPProbers: "false"
```

- 入站只接管 gRPC 端口。HTTP 端口（探针、管理端口、gateway 的 8080）不过边车。理由（设计 2 第 4 项）：入口的 Envoy 到 gateway 这一跳如果过边车，gateway 看到的对端地址变成边车的本地地址，`TRUSTED_PROXIES` 不认，`X-Forwarded-For` 被忽略，按 IP 的限流会把所有人算成一个地址。不写 `includeInboundPorts` 时默认值是 `"*"`，全部接管，所以 gateway 要显式写空字符串。
- 出站排除中间件的端口（MySQL、Redis、Sentinel、Kafka）。理由（设计 2 第 3 项）：它们的熔断在进程里（`pkg/breaker`），对端没有边车所以没有 mTLS，过边车还多一跳延迟；设计还提到长时间空闲的连接池连接会被边车的空闲超时掐掉，这一条标的是“要查的坑”，**没有实测**。
- 探针不改写。Istio 默认把 HTTP 探针改成先打边车的 15020、由它转给应用，那是为“应用端口被边车接管、kubelet 没有证书连不进来”准备的；这里 HTTP 端口不过边车，kubelet 直接探应用，少依赖一个进程。

**面试时怎么讲**：说清 init 容器加 iptables 的机制，然后说代价：进程不知道边车存在，所以要会看边车实际的配置；再说你为什么把中间件的端口和 HTTP 端口排除在外（对端没有边车、保住客户端地址、少一跳）。

---

## 3. Istio 的默认值和我们的规范冲突在哪

动工前对着官方参考页和 istiod chart 的 values 查了一遍（设计 1，标“查证”），下表是核心的对照。**默认重试是最危险的一条。**

| 1d 的东西 | Istio 的默认 | 怎么处理 |
|---|---|---|
| 同步链上不重试（`max_retries: 0`） | **默认重试 2 次**，条件 `connect-failure,refused-stream,unavailable,cancelled`，退避基数 25 毫秒 | 必须关。网格默认配置里 `defaultHttpRetryPolicy: {attempts: 0}`，每条路由再写 `retries: {attempts: 0}`；1.32 新增的入站默认重试 `defaultInboundHttpRetryPolicy` 也置空 |
| 路由超时 | 默认不设 | 每条路由显式写；不写慢下游永远不会被摘除（坑 2） |
| `max_ejection_percent: 100` | `maxEjectionPercent` 默认 10% | 改成 100（坑 1 的前一半） |
| `healthy_panic_threshold: 0` | `minHealthPercent` 默认 0，即不启用恐慌模式 | 默认值正是要的（坑 1 的后一半），清单里显式写出来防止以后被改 |
| 不开 `split_external_local_origin_errors` | 默认 false | 默认值正是要的（坑 3），同样显式写出 |
| 连续 5 次网关错误摘除 | `consecutive5xxErrors` 默认 5；`consecutiveGatewayErrors` 默认关 | 用前者；路由超时的 504 也算 5xx，实测成立（见 5） |
| `max_ejection_time: 5s` | **DestinationRule 没有这个字段** | 摘除时间是基数乘以被摘除的次数，上限是 Envoy 的默认；见 6 的发现二 |
| 检查间隔 1 秒、摘除 5 秒 | `interval` 10 秒、`baseEjectionTime` 30 秒 | 显式写 1s、5s |
| 并发上限 128 / 512 | `http2MaxRequests` 默认 2^32-1，即没有上限 | 显式写 |
| 就近优先的负载均衡 | 配了异常检测就会启用；节点没有区域标签时应当没有效果（设计里是“推断”） | `localityLbSetting: {enabled: false}` 显式关，不依赖推断 |

**为什么默认重试危险**：

- `unavailable` 是 gRPC 状态码的条件。下游已经执行了一半再返回 `UNAVAILABLE` 的写请求，会被边车再发两次（ADR-0029）。
- 一条调用链上每一跳都这样重试，次数相乘。设计引用的 1d 规则是：每层都重试 3 次，三层就是 27 次，下游刚恢复就被重试的洪水再次打垮。
- 本项目的规则（1d 的 5.6）：只重试幂等的操作；一条调用链上只有一层重试；同步链上不重试，由客户端带幂等键重试，异步的由消费者重试；退避带抖动；有上限。

**怎么证明“重试关了”**（场景 F，设计 10 第五步）：只看“增量是 0”说明不了什么，要有对照。

- 关掉的状态：912 个请求，gateway 进程交给边车 912 个，边车发往 inventory 637 个、本地拒绝 275 个（637 + 275 = 912，没有多发），`upstream_rq_retry` 增量 0。
- 对照：把这条路由改成重试 2 次，`upstream_rq_retry` 增量 92。
- 这个对照说明“量得到重试”，所以“增量是 0”说明的是重试确实关了。这和 2c 笔记第 10 节“没有去掉保护就失败的对照，零失败说明不了什么”是同一个方法。

**网格默认配置里关重试后的实测**（设计 10 第一步）：没有 `VirtualService` 的路由上也没有重试策略；回显的头里 `X-Envoy-Attempt-Count` 是 1。

**面试时怎么讲**：先说“网格的默认值是为通用场景定的，不等于你的规范”，然后用重试举例，讲清危险在哪（状态码条件、写请求、逐跳相乘），再说你怎么关（两层）和怎么证明关了（对照）。

---

## 4. 熔断在 Istio 里是两样东西，每条调用边怎么按调用方区分

### 4.1 两样东西

**（通用知识）**：Envoy 里“熔断”其实是两个机制，Istio 在 `DestinationRule` 的 `trafficPolicy` 里分别暴露：

- **连接池上限**（`connectionPool`，对应 Envoy 的 `circuit_breakers`）：名字叫熔断，实际是上限。限制同时在途的请求数，超过立刻拒绝。它隔离的是“并发”，不看下游对错。
- **异常检测**（`outlierDetection`）：连续 N 次 5xx 把某个实例摘掉一段时间。它看的是下游的失败。

1d 的实测（5.3）：上限 128、发 400 个慢请求，恰好 128 个通过，其余几毫秒内返回 `UNAVAILABLE`；上游连续 5 次返回 `INTERNAL` 或 `UNAVAILABLE` 后被摘除，之后的请求 0.2 毫秒返回；业务错误（连续 50 次 `NOT_FOUND`）不触发摘除。

**和进程里的熔断器相比少了什么**（1d 的 5.3，Istio 同样适用）：按“连续失败次数”判断，没有错误比例和慢调用比例；没有“半开时只放一个探测请求”，到时间就整个放回。1.32 新增了按失败比例摘除（`failurePercentageThreshold`），但按比例的检测要多个实例互相比较，两个副本下意义不大，这次没用，只记了一笔。

### 4.2 每条调用边的清单（摘自 `deploy/cluster/mesh/order.yaml`）

同一个下游被两个调用方用不同的数：order 被 gateway 用 3 秒、512，被 payment 用 1 秒、128。所以规则要按调用方区分，而且规则作用在**调用方的边车**上（`DestinationRule` 只作用在调用方一侧，每个调用方对每个下游各有各的超时、并发额度和摘除统计）。

| 调用边 | 超时 | 并发上限 |
|---|---|---|
| gateway → order | 3s | 512 |
| gateway → inventory、payment、orderindex | 3s | 128 |
| order → inventory | 1s | 128 |
| payment → order | 1s | 128 |

超时按调用方区分：`VirtualService` 的路由用 `sourceLabels` 匹配调用方，从上到下第一条匹配的生效：

```yaml
http:
  - name: from-gateway
    match:
      - sourceLabels: {app.kubernetes.io/name: gateway}
    route:
      - destination: {host: order, port: {number: 9081}}
    timeout: 3s
    retries: {attempts: 0}
  - name: from-payment
    ...
    timeout: 1s
  - name: from-anyone-else      # 兜底，没有 match
    ...
    timeout: 1s
```

最后一条兜底的用意（清单注释）：以后多了一个调用方而忘了加路由时，它拿到的是最紧的预算和不重试，而不是“没有超时”。

并发上限和摘除按调用方区分：`DestinationRule` 的 `workloadSelector` 选调用方的 Pod，这条规则只下发给它的边车：

```yaml
kind: DestinationRule
metadata: {name: gateway-to-order}
spec:
  host: order
  workloadSelector:
    matchLabels: {app.kubernetes.io/name: gateway}
  trafficPolicy:
    connectionPool:
      http: {http2MaxRequests: 512, http1MaxPendingRequests: 512}
    outlierDetection:
      consecutive5xxErrors: 5
      interval: 1s
      baseEjectionTime: 5s
      maxEjectionPercent: 100
      minHealthPercent: 0
      splitExternalLocalOriginErrors: false
    loadBalancer:
      localityLbSetting: {enabled: false}
```

设计 3.4 写 `workloadSelector` 时标的是“没有查证”，实现时验证了：按调用方区分生效（设计 10 第二到四步，`istioctl proxy-config` 看的）。gateway 的边车里到 order 的在途上限是 512、路由超时 3 秒；payment 的边车里到 order 是 128、1 秒；order 的边车里到 inventory 是 128、1 秒。没有为它写规则的边（例如 payment 到 inventory）没有上限和摘除，路由落在兜底那条上。

**“每个调用方各有额度”的数怎么算**：并发上限是每个调用方 Pod 各算各的。场景 D 实测两个 gateway 合计放过 256 个在途（2×128），见 6。

**入站上限**：每个被调用的服务的 `Sidecar` 资源里写 `inboundConnectionPool`（order 是 `http2MaxRequests: 1024`），是被调用方对自己的保护，不管来自谁，比任何一个调用方的出站额度都大，正常情况下先触发的是调用方一侧的上限。配置确认下发了（order 的入站集群上限是 1024），**没有压到上限验证过**。

**实现时看到的几个“和设计时想的不一样”**（设计 10 第一步）：

1. 路由上除了 `timeout: 1s` 还生成了 `maxGrpcTimeout: 1s`。含义是“调用方带了 `grpc-timeout` 头就用头里的值，但不超过这个上限”。我们的调用方带的是预算加 500 毫秒，会被压到预算。这是设计 4 里担心的“边车会不会采用调用方带来的 `grpc-timeout` 头”的具体形态；实测结论见 5。
2. `DestinationRule` 里写 `maxRetries: 0` 不起作用：生成出来是 4294967295，Istio 把 0 当成“没写”。它管的是重试的并发预算，重试策略本身已经关了，所以没有后果；清单里不写这一行，免得让人以为它在起作用。（设计 3.4 的配置片段里还留着 `maxRetries: 0`，是动工前的草稿，以清单文件为准。）
3. 负载均衡的默认策略是 `LEAST_REQUEST`，不是手写时的轮流。两个副本下差别不大。

**面试时怎么讲**：“熔断”要拆成两样说清楚：连接池上限管并发，异常检测管下游失败；然后讲 `DestinationRule` 作用在调用方，所以同一个下游可以对不同调用方给不同额度。再补一句：默认的 `maxEjectionPercent` 10% 对两个副本是 0 个，摘除不生效，所以要改成 100。

---

## 5. 慢下游怎么被熔断，为什么进程的超时要比边车长

**原理**（1d 的坑 2，在 Envoy v1.40.2 上实测，1.32 用的是同一个 Envoy 分支）：

- 只靠调用方自己的截止时间：Envoy 看到的是调用方主动断开，不算上游的错，30 个请求每个都等满 300 毫秒，不摘除。
- 让 Envoy 按调用方的 `grpc-timeout` 头提前一点超时（`max_stream_duration`）：超时生效了，但同样不计入摘除。
- **只有路由上写死的 `timeout` 到期才算一次网关错误**，连续 5 次后摘除，之后快速失败。

**所以进程里的时限必须比边车长**（1d 的 5.6）：进程里的计时器先启动，两个数相等时总是调用方先放手，边车看到的是“调用方断开”，不计入异常检测，慢下游永远不会被摘除。第一版设计写的“两处同一个数”是错的，gateway 的子 Agent 指出的。改成边车的数是预算，进程里的比它长 500 毫秒（`grpcx.SidecarMargin`），只做兜底：边车不在（直连）或边车自己卡住时调用方仍然会放手。

| 调用 | 边车路由超时（预算） | 进程里 |
|---|---|---|
| gateway → 任一下游 | 3 秒 | 3.5 秒 |
| order → inventory | 1 秒 | 1.5 秒 |
| payment → order | 1 秒 | 1.5 秒 |

**同一个超时现在写在三处**（设计 5，ADR-0029）：Go 里的常量、本机 compose 用的 `deploy/envoy/*.yaml`、集群的 `VirtualService`。三个 Go 测试（`TestHopTimeoutMatchesMesh`、`TestInventoryHopTimeoutMatchesMesh`、`TestOrderHopTimeoutMatchesMesh`）核对：调用方的那条路由存在、超时等于 Go 的常量、`retries.attempts` 是 0、兜底路由在最后；改超时、删 `retries`、把兜底挪到最前三种改坏的情况都会失败。它们不核对路由的目标和并发上限，那些靠故障注入守着；并发上限和摘除的参数两边没有自动比对。

**在 Istio 上的实测**（设计 10 第二到四步，手工在节点上对 e2e 的 inventory 进程发 `SIGSTOP`，从入口每轮并发 8 个查库存）：

| 轮 | 结果 |
|---|---|
| 第 1 轮 | 8 个都在 3.01 秒返回 503 |
| 第 2 轮 | 8 个都在 3.10 秒返回：4 个 503，4 个 504 |
| 第 3 轮 | 8 个都在 12 到 19 毫秒返回 503；gateway 的边车里这个端点是 `UNHEALTHY`、异常检测 `FAILED` |
| `SIGCONT` 之后 | 又过 5 秒恢复正常 |

两个 gateway 各有各的统计，第 1 轮每个摊到 4 个，不够 5 次，所以第 2 轮才摘除。**结论：路由超时计入摘除，`maxGrpcTimeout` 没有妨碍它，和 1d 一致。**

**三个坑各自要有对照**（设计 4 的要求）：`maxEjectionPercent` 用默认值时场景 B 不摘除；路由不写 `timeout` 时慢下游不摘除；进程里的时限和路由超时相等时不摘除（这一条 1d 已经证过，机制没变，不重做）。设计 10 的故障注入结果里没有记录前两个对照在 Istio 上单独跑过，所以面试时不要说“三个对照都在 Istio 上重做过”。（第一个对照要改默认值，第二个要去掉 `timeout`；设计 10 的八个场景里只有 F 记了对照。）

**面试时怎么讲**：先讲“为什么慢下游不会自己被摘除”（调用方先放手，代理只看到断开），再讲“只有路由超时才算失败”，最后讲两个超时错开 500 毫秒的设计和测试怎么守住它。

---

## 6. 故障注入：八个场景、结果和三个发现

`make cluster-fault`（`scripts/cluster/fault.sh`）在 `oppenheimer-e2e` 上以 Istio 形态部署一套（gateway 和 inventory 各 2 个副本、关限流、打开边车的集群级统计），请求从 MacBook 经入口发。镜像里没有 shell，“挂住”是在节点上按容器号取进程号再发 `SIGSTOP`；存活探针约 30 秒后会重启被挂住的容器，所以每次挂住不超过 15 秒，另有看门狗和退出钩子保证进程被恢复。

第二次运行八个场景全部通过（第一次有两个失败，都是脚本的问题，见 16）：

| 场景 | 结果 | 1d（本机、手写边车） |
|---|---|---|
| A 基线 | 查库存 p50 10 毫秒，下单并落库 | p50 1 毫秒（本机没有入口这一跳） |
| B 两个 inventory 都挂住 | 摘除生效之前的 61 个请求都在 3.01 到 3.03 秒返回（503 41 个、504 20 个）；之后在 50 毫秒内返回 503；两个 gateway 的边车里两个端点都是 `FAILED`；期间查订单 20 次全部 200，延迟和基线一样；恢复进程后 0.5 到 1.8 秒查库存恢复（取决于挂了多久：摘除从生效起算 5 秒） | 前 5 次各 3 秒，之后毫秒级；恢复 5.5 秒 |
| B' 只挂住一个副本（新） | 失败 31 个请求（一批 60 个里分到坏副本的那些，各等 3 秒），之后 50 个全部 200、最长 15 毫秒；好的那个副本一直是 `OK` | 没有这个场景 |
| C 两个 inventory 被强制删除 | 40 个请求失败，全部是 503，最长 13 毫秒，没有一个等超时；中断 4.6 秒，新 Pod 就绪就恢复 | 12 次最长 5 毫秒 |
| D 并发上限（同时 400 个，两个 gateway） | 恰好 144 个在 1 秒内被拒绝，256 个在途（2×128），没有超过 5 秒的 | 300 个里恰好 172 个被拒绝、128 个在途 |
| E 业务错误 | 30 次 404，摘除 0 次 | 一样 |
| F 不重试（新） | 912 个请求，边车发往 inventory 637 个、本地拒绝 275 个，`upstream_rq_retry` 增量 0；对照（改成重试 2 次）增量 92 | 没有这个场景 |
| G 重复摘除 4 次 | 每次从摘除生效到恢复都是 5.1 到 5.4 秒 | |

结论（设计 10）：“坏了的下游不拖垮别的接口、不需要人干预就恢复”在 Istio 的边车上成立，数值和 1d 的手写边车对得上。

### 发现一：摘除不是立刻生效的，要晚 0.5 到 1 秒

一批请求同时超时（每个端点远超过 5 次）之后，紧接着发的请求仍然被送到坏的端点、各等满 3 秒；0.5 到 1 秒之后才开始被立刻拒绝。单独量了五次（一批超时返回后每 100 毫秒探一次）：+25 毫秒、+500 毫秒的探测还被送过去，+980 毫秒的开始被拒绝，五次都一样。

- 这解释了 1d 里那个“没有解释的现象”（5.13：重启后约 30 毫秒查库存就成功了，而统计里显示还在摘除中）：统计里记了摘除，但负载均衡还没有用上这个结果。ADR-0029 的修正里写的是“手写的边车也是这样，不是 Istio 带来的”。
- 把异常检测的 `interval` 临时从 1 秒改成 3 秒再量，延迟仍然是 0.5 到 1 秒，所以不是检查间隔。
- **原因的限定**：和 Envoy 集群的 `update_merge_window`（默认 1 秒：只改变端点健康状态的更新会被攒起来，窗口结束时一起下发给负载均衡）吻合；**没有直接证实**（要用 EnvoyFilter 把它改成 0 再量）。面试时只说“吻合、没证实”。
- 后果：下游整体变慢时，等满超时的请求不止“每个调用方 Pod 的前 5 个”，而是“前 5 个加上之后最多 1 秒里到达的全部”。并发上限仍然管着（场景 D），所以在途的不会超过 128 个。

### 发现二：摘除时间没有逐次加长

ADR-0029 当初接受了“摘除时间随被摘除的次数加长（5、10、15 秒…），DestinationRule 没有封顶的字段”，担心“被反复重启的服务恢复得越来越慢”。实测场景 G 连续四次“挂住、摘除、恢复”，每次间隔约 10 秒，从摘除生效到恢复都是 5.1 到 5.4 秒。

- 原因（设计 10 的说法）：Envoy 的摘除时间是基数乘以“被摘除的次数”，但这个次数在端点健康时每个检查间隔减一；间隔设的是 1 秒，两次故障之间健康几秒就减回去了。只有端点刚放回来就又坏的情况才会累加。
- 所以不需要 EnvoyFilter，ADR 把这一项改成了“可以当作定论”（限于这个配置和这个间隔）。1d 当时量到的“第二次是 10 秒”是紧接着发生的两次。

### 发现三：超时的请求有一部分是 504，因为被调用方的入站边车也在计时

1d 里边车超时调用方看到的一律是 `UNAVAILABLE`（503），504 只在边车不在时出现。在 Istio 上不成立了：

- 出站边车的统计里，来自上游的响应有 `grpc-status 4`（`DEADLINE_EXCEEDED`）。这个状态码不是出站边车自己超时产生的（它自己超时产生的是 504 映射成的 `UNAVAILABLE`），是 inventory 一侧的边车返回的。
- 出站边车把剩余的时限经请求头传了下去，两边差不多同时到期，谁先到期决定调用方看到 503 还是 504，比例每次不同（0/80、20/61、13/256）。两种都计入摘除。
- 后果：1d 的“503 是边车超时、504 是边车不在”这个判断方法在集群上不再成立；gateway 对两种的处理本来就一样（都是失败、都不重试），客户端按幂等键重试，没有行为上的后果，但排查时不能再靠这个区分。
- 这个“出站边车把剩余时限传下去、入站边车也计时”的机制，设计里写的是依据统计项得出的解释；它为什么会让比例每次不同，设计没有进一步解释。

### 顺带的观察（设计 10 第五步）

- 强制删除后的第一个请求还能成功：API 对象立刻没了，但进程要等 kubelet 去杀。
- 就绪探针比摘除慢：挂住约 4 到 8 秒后端点才被标成 `UNHEALTHY`，摘除是 3 秒超时加不到 1 秒。两者是独立的两条路径，摘除先到。
- Istio 默认只保留很少的统计项，集群级的要在 Pod 上用 `proxy.istio.io/config` 注解的 `proxyStatsMatcher` 打开（chart 加了通用的 `podAnnotations`，只有故障注入的叠加层在用）。阶段 3 接监控时要定保留哪些。

### chaos 的结果（设计 10 第二到四步）

`MESH_ISTIO=all make cluster-chaos` 两次都通过（压测 120 秒，每 5 秒随机删一个业务 Pod，静止后 `verify` 和 `recon` 无差异）：

| | 删掉的 Pod | 受理 | 客户端看到的 5xx | 2c（手写边车）同一项 |
|---|---|---|---|---|
| 正常删除 | 22 个 | 356 单 | 0 | 23 个 Pod，0 |
| 强制删除 | 22 个（其中 order 8 个） | 339 单 | 133 个下单 503；其中 50 次提交最终没拿到订单号，库里也没有这些订单（对得上） | 22 个 Pod，29 个 |

正常删除和 2c 一样是零报错。强制删除的 5xx 比 2c 多（133 对 29），**原因没有查清**：两次运行删到的服务不同（这次 order 被删了 8 次，下单最依赖它），不能直接比；场景 C 表明被删的 Pod 是毫秒级失败、摘除时间也不累加，所以设计里写的是“大概率只是这次 order 被删得多；没有证实”，没有做可比的对照。面试被问到这个数时不要给出一个确定的解释。

**面试时怎么讲**：先说你们把 1d 的故障场景在集群上用同一套判定重做了一遍并新增了两个（B'、F），数值对得上；然后挑一个发现讲透，最推荐发现一（摘除晚生效，以及它解释了 1d 遗留的疑点，同时诚实说原因“吻合、没证实”）。

---

## 7. Service 的端口名为什么会改变行为

**原理（通用知识）**：Istio 不能看到连接就知道上面跑的是什么协议，它按 Service 端口的名字（以 `grpc`、`http` 等开头）或 `appProtocol` 字段来判断协议。认成 HTTP/2 或 gRPC，边车才按请求做七层处理：按请求负载均衡、路由超时、按状态码摘除。名字对不上就当成不透明的 TCP：只能按连接做四层转发。

**为什么不报错**：TCP 是它的合法回退。请求照常通，只是“按连接均衡”，2c 解决过的“gRPC 长连接在 ClusterIP 后面不均衡”原样回来（见 2c 笔记 3.2），而超时、重试这类 `VirtualService` 的七层规则对它也不适用。没有任何错误，只是保护悄悄没了（设计 3.3、ADR-0029 都强调这一点）。

**这个项目里怎么做的**：

- chart 在 Istio 形态下，Service 多一个名为 `grpc` 的端口（设计 3.3、10）。
- `istioctl analyze` 进了 `make deploy` 的结尾作为检查。它查出过一个问题：`VirtualService` 的 `destination` 没写端口，Service 有 HTTP 和 gRPC 两个端口时有歧义（IST0112），已补上端口。连带的后果写在清单注释里：路由不按端口匹配，网格里的 Pod 访问别的服务的 HTTP 端口也会被送到 gRPC 端口；服务之间没有这样的调用，调试时用 `kubectl port-forward`。
- 另一个还没处理的：e2e 的 OTLP 接收端的端口名 `otlp-grpc` 不符合 Istio 的命名约定，被当成 TCP 透传，链路的用例通过，没有改。
- 中间件的端口名（MySQL 是 `mysql`，Redis 是 `redis`、`sentinel`，Kafka 是 `tcp-clients`）Istio 按名字猜协议，但这些流量按端口排除在边车之外，所以不受影响。
- 验证“按请求均衡”（设计 10 第七步）：探针跑 40 秒，两个 inventory 各处理 524 和 496 个 gRPC 请求，两个 order 是 512 和 508。按请求均衡，和手写边车时一样。

**面试时怎么讲**：这是个很好的“配置即行为”的例子：端口名是 Service 里一个看起来无关紧要的字段，写错了请求照通、监控不报错，但七层的保护全没了。接着说你们用 `istioctl analyze` 和负载均衡计数来防它。

---

## 8. `Sidecar` 资源：收窄可见范围

**原理**：默认每个边车会收到全网格所有 Service 的配置（包括中间件那些 headless Service 的每一个 Pod）。配置越多，边车内存和 istiod 推送的量越大，随集群里的服务数增长。设计 3.4 称这是 Istio 在大集群里最常见的资源问题。`Sidecar` 资源的 `egress.hosts` 能把一个工作负载能看到的范围收窄。

**这个项目里怎么做的**（每个服务的 `Sidecar`，摘自 `deploy/cluster/mesh/order.yaml`）：

```yaml
kind: Sidecar
spec:
  workloadSelector:
    labels: {app.kubernetes.io/name: order}
  inboundConnectionPool:
    http: {http2MaxRequests: 1024, http1MaxPendingRequests: 1024}
  egress:
    - hosts: ["./*", "istio-system/*"]
```

一个工作负载只能有一个 `Sidecar` 资源（清单注释），所以入站上限和可见范围写在同一个资源里。

**量到的**（设计 10）：可见范围收窄生效：order 的边车一共 23 个集群，只有本命名空间的服务和 istiod，没有中间件。

**面试时怎么讲**：说清默认是“全网格可见”和它随规模增长的代价，再说你们用 `Sidecar` 收到“本命名空间加 `istio-system`”，并给出 order 的边车只有 23 个集群这个数。规模上的收益（比如不收窄时内存多多少）设计没有量，不要编数字。

---

## 9. 不停机地切过去

### 9.1 约束和推理（设计 7）

- 新旧形态**不能在同一个 Pod 里共存**：手写边车的入站端口 15001 正好是 Istio 出站拦截用的端口。
- 但可以在同一个服务的**不同 Pod 之间**共存，条件是调用方不管连到哪种 Pod 都能通。

**两个机制是这个顺序成立的基础**：

- **`PERMISSIVE` 模式**：迁移期间 `PeerAuthentication` 用默认的 `PERMISSIVE`（明文和 mTLS 都收），全部切完后才改 `STRICT`。
- **自动 mTLS 按端点判断**：新调用方（带 Istio 边车）连到还没切的下游 Pod（没有 Istio 边车）：边车认出对方没有，发明文，直接到进程的 gRPC 端口。（设计 7 把“同一个 Service 后面有的 Pod 有边车、有的没有时，自动 mTLS 确实逐个端点判断”列为没有实证的假设，演练证实了，见 9.3。）

**顺序：调用方先切，即调用关系的拓扑序**，gateway、payment、order、inventory、orderindex，每次一个服务滚动：

- 还没切的调用方（手写边车，连 `<下游>-headless:15001`）连到下游：下游此时一定还是旧形态，因为顺序保证了一个服务的所有调用方都先于它切。
- gateway 没有被调用方之外的调用者，先切；order 的调用者是 gateway 和 payment，都切完才轮到它；inventory 的调用者是 gateway 和 order。
- 每滚一个服务之前先把这条边的 `VirtualService`、`DestinationRule` 建好：规则对还没注入的调用方没有任何作用，先建不会出事；后建的话新 Pod 起来的头几秒是“没有超时、重试两次”的默认行为。

迁移期间的代价（接受的）：新调用方连到还没切的下游 Pod 时，这段时间这条边不经过被调用方的入站上限。

### 9.2 怎么注入

chart 加了 `mesh: envoy | istio`（设计 10）。Istio 形态：不渲染手写边车的任何东西；**按 Pod 的标签注入**（`sidecar.istio.io/inject`），命名空间不打 `istio-injection` 标签，这样同一个命名空间里的测试 Job 和还没切的服务不受影响（设计 3.2 原来写的是按命名空间打标签，实现时改成按 Pod，见 17 的不一致说明）；每个服务一个 ServiceAccount；Service 多一个名为 `grpc` 的端口。`scripts/cluster/services.sh` 的 `MESH_ISTIO` 空表示全部手写，`all` 表示全部 Istio，或者列出服务名（迁移的中间态）。非空时先对命名空间应用 `deploy/cluster/mesh/` 再部署 release。

### 9.3 演练和结果

先在 `oppenheimer-e2e` 里演了一遍，来回各一趟：从全部 Istio 形态按“被调用方先退”的顺序逐个退回手写边车（inventory、orderindex、order、payment、gateway），再按“调用方先切”的顺序逐个切回 Istio。十次滚动，探针（`probe.py`，三条同步调用链）19870 个请求，**0 失败**。设计 7 里两处没有实证的假设都成立（自动 mTLS 逐个端点判断；规则先于 Pod 存在没有副作用）。

长期运行的那一套（命名空间 `oppenheimer`）：先做一次准备性的滚动（新镜像 `c54615c`、所有 Service 加上 gRPC 端口，形态不变），然后 gateway、payment、order、inventory、orderindex 逐个切，最后是 cdc（不注入，只是换成有自己的 ServiceAccount）。七次部署，约 7 分钟，探针 18770 个请求，**0 失败**；延迟 p50 10 到 13 毫秒，p99 15 到 34 毫秒。切完之后 headless Service 和 `<服务>-envoy` 的 ConfigMap 随 release 自动删掉了，十个边车都和 istiod 同步。

切完之后默认形态改成了 Istio（没设 `MESH_ISTIO` 时是 `all`）：不改的话，一次普通的 `make deploy` 会把十个 Pod 全部退回去。显式设成空才是手写边车。控制面不在时部署脚本直接报错，提示先 `make mesh-up`。

### 9.4 演练抓到的回退缺陷：ServiceAccount

第一次演练退回 inventory 时滚动卡在一半：新的 ReplicaSet 建出一个 Pod 之后报 `serviceaccount "inventory" not found`。两件事叠在一起：

- 手写边车形态的模板不写 `serviceAccountName`。但 Pod 模板里还有一个已废弃的同义字段 `serviceAccount`，API 在 `serviceAccountName` 被删掉时不清它，于是新模板实际上仍然指着 `inventory` 这个账号。
- 同一次 `helm upgrade` 把这个 ServiceAccount 删了（手写边车形态不渲染它）。第一个新 Pod 赶在删除之前建了出来，第二个就建不出来了。

探针这期间是 0 失败（旧 Pod 还在服务），但滚动永远完不成。修法：手写边车形态显式写 `serviceAccountName: default`（chart 的模板里有注释）。

**教训：回退路径要和前进路径一样演练**。前进的方向一次就通过，问题只在退回时出现。

**回退的顺序**（设计 7）：chart 的开关还在，任何一步出问题把那个服务的 values 改回去再滚一次；回退要按相反的顺序（被调用方先退）。

**面试时怎么讲**：讲“为什么是这个顺序”的推理（两个机制加一个拓扑序），然后讲数字（两次演练加长期那一套，0 失败），最后讲回退缺陷作为“为什么回退也要演练”的实例。

---

## 10. mTLS 和 AuthorizationPolicy

### 10.1 身份从哪来

**（通用知识）**：Istio 的工作负载身份就是 Pod 的 ServiceAccount。istiod 给每个边车签发一张 X.509 证书，里面的身份是 SPIFFE ID，形如 `spiffe://cluster.local/ns/<命名空间>/sa/<服务账号>`；证书由 istiod 签发并轮换。SPIFFE 是一个“工作负载身份”的规范，面试前对照官方文档核对细节。

**项目里的事实**（设计 10 第一步、`policies.yaml`）：

- 服务原来都用命名空间的 `default` 账号，五个服务是同一个身份，访问控制分不出谁是谁。所以每个服务拆出自己的 ServiceAccount。
- 第一步的验证：回显的头里有 `X-Forwarded-Client-Cert`，身份是 `spiffe://cluster.local/ns/mesh-check/sa/default`。
- `AuthorizationPolicy` 的 `principals` 比的就是证书里的身份（写的时候不带 `spiffe://` 前缀）。

### 10.2 两个资源各管什么

摘自 `deploy/cluster/mesh/security/policies.yaml`：

```yaml
kind: PeerAuthentication
metadata: {name: default}
spec:
  mtls: {mode: STRICT}
---
kind: AuthorizationPolicy
metadata: {name: order-callers}
spec:
  selector:
    matchLabels: {app.kubernetes.io/name: order}
  action: ALLOW
  rules:
    - from:
        - source:
            principals: [cluster.local/ns/__NS__/sa/gateway, cluster.local/ns/__NS__/sa/payment]
```

- **`PeerAuthentication`** 管“连接要不要 mTLS”：`PERMISSIVE` 明文和 mTLS 都收（迁移期间靠它），`STRICT` 只收 mTLS，没有证书的 Pod 连不进来。
- **`AuthorizationPolicy`** 管“这个身份能不能调我”：每个被调用方一条 ALLOW，order 允许 gateway 和 payment，inventory 允许 gateway 和 order，payment、orderindex 只允许 gateway。名单和 `deploy/cluster/mesh/<服务>.yaml` 里的调用边一致，多一个调用方时两处都要加。
- 两者只管被边车接管的端口，也就是 gRPC 端口；探针和 gateway 对外的 HTTP 端口不受影响。

**身份里带命名空间**，所以这份文件是模板，部署脚本把 `__NS__` 换掉。不用后缀通配（`*/sa/gateway`）：那样 e2e 命名空间的 gateway 也能调长期运行的那一套的 order。

### 10.3 ALLOW 的语义和应用顺序的坑

**语义**（清单注释、设计 10）：一个工作负载上只要有 ALLOW 策略，没被任何一条允许的请求就被拒绝（边车返回 403，gRPC 调用方看到的是 `PERMISSION_DENIED`）。

**由此带来的顺序问题**：测试的发压 Pod 要调 inventory，用自己的 ServiceAccount `load`，由只存在于 e2e 命名空间的 `inventory-load-test` 放行。**这条策略必须和 `inventory-callers` 一起应用**：单独先建的话，inventory 上只有“允许 load”这一条 ALLOW，gateway 和 order 的调用反而全部被拒绝。

**应用的时机**（设计 10 第八步）：只在全部服务都是 Istio 形态时应用，而且在 release 滚动完之后（命名空间里还有手写边车的 Pod 时跳过并提示，因为它们发的是明文、没有身份，会被拒绝）；不是 `all` 时反过来在滚动之前先删掉，回退的第一步是放开。

**测试 Job 怎么办**：删 Pod 测试和压测的 Job 在 warmup 阶段直接调 inventory 的 gRPC，严格模式下没有证书连不进去。所以它们现在注入边车，只让到集群内服务的出站过边车：中间件的端口和入口的地址都排除，发压的请求不多过一层代理。（设计 3.2 原来写的是“e2e 的测试 Job、压测 Job 也不注入”，后来改了，见 17。）

### 10.4 验证（设计 10 第八步）

| | 结果 |
|---|---|
| `make cluster-chaos`（严格模式加访问控制下，删 Pod） | 通过，客户端 0 个 5xx，`verify` 和 `recon` 无差异 |
| 名单里的身份（`load` 调 inventory 的 gRPC 端口） | 到了应用（返回 415，gRPC 服务端不认普通的 HTTP 请求） |
| 名单外的身份（`load` 调 order、payment） | `403 Forbidden` |
| 没有边车的 Pod 调 order 的 gRPC 端口 | 连接被重置 |
| 同一个没有边车的 Pod 调 order 的 HTTP 端口 `/healthz` | 正常（不过边车） |
| 应用到长期运行的那一套时 | 探针 3400 个请求 0 失败 |

**没覆盖的**：2c 留下的“任何 Pod 都能直接调任何服务的 gRPC”这个口子关上了；但 gateway 的 HTTP 入口仍然谁都能连（`TRUSTED_PROXIES` 信整个 Pod 网段的问题还在），那一段不在网格里，要靠 NetworkPolicy，这次没有做。

**面试时怎么讲**：先说身份从 ServiceAccount 来、证书由 istiod 签；再说 `PeerAuthentication` 管连接、`AuthorizationPolicy` 管谁能调；最后讲 ALLOW 的语义和“必须一起应用”的坑，这是个很能显示你真的动手配过的细节。

---

## 11. 端点传播从 DNS 换成 xDS：`DRAIN_DELAY` 从 15 秒降到 5 秒

**背景**（2c 笔记第 4 节）：Pod 被删时，SIGTERM 和摘端点同时开始，别处陆续才知道。`DRAIN_DELAY` 是进程收到 SIGTERM 后继续正常服务的时间，必须覆盖“这个 Pod 被摘掉的消息传到每个调用方”的时间。2c 里调用方边车靠 DNS 发现端点，量到的是 1.7 到 9.5 秒，`DRAIN_DELAY` 定 15 秒。

**2d 怎么量的**（设计 10 第七步）：探针开着，删一个 Pod，看它的日志里最后一个请求离它收到 SIGTERM 有多远。inventory 3 次、gateway 3 次、order 2 次：最后一个请求都落在 SIGTERM 之前 0.5 秒到之后 0.2 秒之间（探针每 0.5 秒发一批，所以分辨率就是 0.5 秒）。现在端点的变化由 istiod 经 EDS 推给每个边车，入口那一侧本来就是 Envoy Gateway 的 xDS。

**结论**：`DRAIN_DELAY` 改成 5 秒（chart 的默认值）。设计的说法是“仍然是实测的十倍”，照抄；这个“十倍”具体指什么设计没有展开，面试时不要自己解释。之后带着探针做了一次部署、一次全部重启、四次删 Pod：9570 个请求 0 失败。

**要诚实的几点**：

- 量到的是“最后一个请求落在 SIGTERM 附近”，分辨率 0.5 秒，**不是一个精确的传播时间**。2c 笔记 3.5 说“设计没有给出 xDS 下传播时间的实测数，现在不要编一个数字”：现在有的是上面这个范围，不是传播延迟本身。
- 这一次**没有再做** `DRAIN_DELAY=0` 的对照（2c 做过：每删一个 Pod 失败约 15 个）。
- **退回手写边车时要改回 15 秒**。

**面试时怎么讲**：先说 DNS 靠轮询和缓存、xDS 靠推送，这是机制上的差别；再说 2c 量到 1.7 到 9.5 秒、2d 里最后一个请求落在 SIGTERM 前后不到一秒内；然后说你据此把 `DRAIN_DELAY` 降到 5 秒并验证了 0 失败，同时承认分辨率和缺少对照。

---

## 12. 资源开销

**动工前的估计和默认值**（设计 1、6）：Istio 默认注入的边车 requests 100m / 128Mi、limits 2 核 / 1Gi；istiod 默认 requests 500m / 2Gi。2c 手写的边车实际只用 16 到 18 Mi，requests 是 32 Mi。照默认值装，10 个 Pod 的边车加 istiod 要多承诺约 3 Gi，所以改小：

| | 设的 requests / limits | 来源 |
|---|---|---|
| istiod | 100m / 256Mi，limits 512Mi | 估的，装上后按 `kubectl top` 改 |
| 每个边车 | 50m / 64Mi，limits 192Mi，CPU 不设上限 | 估的；CPU 不设上限和业务容器一致（被节流会造成延迟的长尾） |

**量到的内存**：

| 时点 | istiod | 每个边车 | 手写的边车 |
|---|---|---|---|
| 空载（第一步） | 44 Mi | 34 到 36 Mi | 16 到 18 Mi |
| e2e 上 7 个边车（第二到四步） | 59 Mi | 32 到 37 Mi | |
| 长期运行那一套，十个边车（第七步） | 76 Mi | 平均 32 Mi，最大 34 Mi | |

第七步的小结：比手写的边车每个多约 15 Mi，合计约 150 Mi，加 istiod 共 230 Mi 左右。requests 可以各降一半，没有改。设计 2c 阶段估计的“控制面加 10 个边车 1.5 到 2 Gi”（2c 笔记 3.5）远高于实测，那个估计在 2c 里就标了“没有实测”。

**要知道的后果**：HPA 按整个 Pod 的 requests 算利用率，边车的 CPU requests 从 20m 变成 50m，同样的负载下利用率变低、扩容点会移动。**没有重量**（设计 10、11）。同样**没有做**经边车和直连的延迟对比，也就没有 Istio 版的“多一跳”数字（1d 手写边车是本机 p50 约 0.5 到 1.3 毫秒；设计原来想对照的是 1d 的 +0.7 毫秒）。

**面试时怎么讲**：说默认值是为大网格定的，所以先估后量再改；给出 istiod 76 Mi、边车平均 32 Mi 这组数，说明余量大、requests 没有来得及降；诚实地说 HPA 扩容点和延迟没有重量。

---

## 13. istiod 挂了会怎样

**这一条没有实测**（设计 11 写的是“istiod 不在时的行为（已有的边车继续工作、新 Pod 建不出来）没有实测，放进阶段 4”）。下面是设计和 ADR 里的预期，不是验证过的结论：

- 已经在跑的边车用最后一份配置继续工作，但端点的变化推不下去（Pod 增减后调用方看到的还是旧的）。
- 注入的 webhook 的失败策略是 `Fail`（`istiod.yaml` 注释、ADR-0029 都这样写），业务命名空间里建不出新 Pod：滚动发布、HPA 扩容、删掉重建都会卡住。设计 3.2 在动工前对这点写的是“失败策略决定是创建失败还是不带边车起来”，没有实测确认。
- 只跑一个副本是内存决定的，这个代价要在阶段 4 的故障实验里量（ADR-0029）。

**（通用知识）补充**：数据面和控制面是解耦的，这是网格设计的一个常见论点：控制面短暂不可用不应该影响已有流量。证书到期前没有 istiod 续签会怎样，也是要问的一点，这里不展开，面试前对照官方文档。

**面试时怎么讲**：先说“预期是这样”，再明说“我没有在这个集群上做过 istiod 挂掉的实验，放在阶段 4”。不要把它说成验证过。

---

## 14. 边车模式和 ambient 的取舍

**（通用知识）ambient 的结构**：数据面拆成两层。每个节点一个四层的 ztunnel（DaemonSet），负责 mTLS、四层的身份和访问控制；七层功能（路由、超时、重试等）由按命名空间或按服务部署的 waypoint 代理提供。Pod 里没有注入的边车。具体细节（拦截方式、版本成熟度）面试前对照官方文档核对。

**设计里的取舍**（设计 2 第 1 项，ADR-0029）：

- 选边车模式，因为它和 1d 的拓扑一一对应：出站的保护在调用方的代理上，每个调用方各有额度。这是“迁移”，1d 的每个实测结论都有一个可以直接对照的位置；面试里问服务网格，默认说的也是这个形态。
- ambient 省内存、没有注入，是 Istio 现在主推的方向。没选：超时、熔断、并发上限都落到被调用方一侧的 waypoint 上，“每个调用方各有额度”要换一种表达，等于重新设计而不是迁移。
- ambient 的对照实验不在 2d 里做，可以在边车模式做完之后单独做一次（设计 9）。
- 设计里也提到了 Linkerd（ADR-0029 的备选）：更轻，它的代理不是 Envoy，1d 的实测全部作废，熔断模型也不同（按失败累积）。

**边车模式的已知代价（项目里量到的）**：每个 Pod 多一个 pilot-agent 加整套动态配置，每个边车平均 32 Mi（见 12）；注入影响 Pod 生命周期（init 容器、启动顺序，见 15）；透明拦截让排查依赖 `istioctl`（见 2）。

**面试时怎么讲**：先给选择（边车，理由是迁移成本和与 1d 对得上），再说 ambient 的区别和它的主要卖点，然后说为什么在“按调用方额度”这个具体需求上 ambient 不是直接替换。注意：这里没有在 ambient 上做过任何实验，不要说成对比过性能。

---

## 15. 原生 sidecar 之前，边车和 Job、启停顺序的老问题

**（通用知识）**：在原生 sidecar（`initContainers` 里 `restartPolicy: Always` 的容器）出现之前，边车是普通的第二个容器，有几个老问题（面试前对照官方文档核对）：

- **启动顺序**：两个容器同时启动，应用可能先于边车就绪，启动时的出站调用打在还没监听的端口上。
- **停止顺序**：两个容器同时收到 SIGTERM，边车先退出的话，应用在优雅退出阶段还要处理的在途请求和对下游的调用全部失败。
- **Job**：Job 的主容器结束后边车还活着，Pod 不会结束，Job 一直不完成；需要手工在主容器结束时通知边车退出。

**项目里的事实**：2c 手写边车时专门解决了前两条（2c 笔记 3.1）。Istio 从 1.27 起默认启用原生 sidecar（设计 1，查证：1.27 的升级说明），注入出来的边车同样具备：第一步验证过 `istio-proxy` 在 `initContainers` 里、`restartPolicy: Always`。**摘流在 Istio 下照样成立**：正常删除时 22 个 Pod 零报错（chaos 的结果，见 6），说明原生 sidecar 的停止顺序加 `DRAIN_DELAY` 的那一套没有因为换成 Istio 而失效。

关于 Job 的那一条，设计 2d 里没有专门针对 Istio 边车和 Job 的实验；测试 Job 注入了边车（见 10.3），e2e 能跑通，但设计没有单独记录 Job 结束时边车的行为，面试时不要把它说成项目里量过的结论。

---

## 16. 实现中的小坑

1. **1.32.0 的 chart 不在官方的 Helm 仓库里。** 仓库的索引只到 1.30.8（2026-11-06 生成），1.31 和 1.32 的正式版都没有，OCI 的地址也没有。改成从 GitHub 的发布包取（`mesh.sh fetch`，核对 sha256，放在 `./bin/istio-1.32.0/`，不入库），里面有 chart 和配套的 istioctl。
2. **Helm 按键合并 values。** 只写 `limits.memory` 的话默认的 `limits.cpu: 2000m` 还留着，要写 `cpu: null` 才去得掉（`istiod.yaml` 里有）。
3. **统计项名字里有分号。** Istio 的统计项名字在集群名和后缀之间多一个分号（`cluster.outbound|9082||inventory...;.upstream_rq_total`），故障注入脚本第一次解析不到，所有统计都是 NA。另一个脚本问题：场景 C 把强制删除后进程还没被杀掉时返回的那个 200 当成了“恢复”。改了之后用保存的快照重算，第一次运行的数据同样满足断言。
4. **探针改写要关。** Istio 默认把 HTTP 探针改写成经边车 15020，这里 HTTP 端口不过边车，所以 `rewriteAppHTTPProbers: "false"`（见 2）。
5. **`DestinationRule` 的 `maxRetries: 0` 无效**（生成出来是 4294967295），不要写（见 4.2）。
6. **`VirtualService` 的 destination 要写端口**，否则 `istioctl analyze` 报 IST0112（见 7）。
7. **集群级统计项默认不保留**，要用 `proxy.istio.io/config` 注解的 `proxyStatsMatcher` 打开（见 6）。
8. **`serviceAccountName` 删掉不生效**，因为废弃的同义字段 `serviceAccount` 不被清（见 9.4）。
9. **漏设 gRPC 端口会让已经切过去的调用方断流。** `MESH_ISTIO` 最初靠一个手工设的变量给还没切的服务的 Service 加 gRPC 端口，Opus 指出漏设会让已经切过去的调用方断流，改成自动。
10. **nf_tables 和 AppArmor**：设计时担心，实测都没问题（见 2）。

---

## 17. 还没做的、还没弄清的，以及材料互相对不上的地方

### 17.1 没做的（设计 11、10）

- **第七步剩下的两项**：HPA 的扩容点没有重量；经边车和直连的延迟对比没有做；边车和 istiod 的 requests 可以各降一半，没有改。
- **第九步**：删掉 chart 里手写边车的分支和 `MESH_ISTIO` 的中间态；从空集群重建验证一键部署（顺序里多了 `make mesh-up`）。设计 11 写 ADR-0020 的补充是待办，但 ADR-0020 里已经有这一段（见 17.3）。
- **istiod 不在时的行为没有实测**，放进阶段 4（见 13）。
- **入站上限没有压到上限验证过**：配置确认下发了（order 的入站集群上限 1024），没压过（见 4.2）。
- **Argo CD、cert-manager、灰度发布、NetworkPolicy、ambient 对照实验、Istio 的指标和访问日志和链路**：这次不做（设计 9、2 第 8 项）。入口的 Envoy 进网格、入口到 gateway 的 mTLS 也没做。
- **e2e 的两处小问题**：OTLP 接收端的端口名不符合 Istio 的约定（被当成 TCP 透传）；`e2e_inventory_addr` 里 inventory 的端口 9082 是写死的。
- 设计 10 还记着：server 节点的内存在这一天的测试之后是 76%（重建后轻载时 48%），网格没有任何东西在它上面，来源没有查。
- 中间件流量过边车时“空闲的长连接被掐掉”的问题，设计标的是“要查的坑”，没有查（因为中间件根本没过边车）。

### 17.2 验证过的 与 没验证的，一眼看清

| 已验证（量过、有数） | 没证实或没验证 |
|---|---|
| 重试关了（场景 F，有对照：0 对 92） | 摘除晚 0.5 到 1 秒的原因（和 `update_merge_window` 吻合，**没有直接证实**） |
| 路由超时计入摘除（`maxGrpcTimeout` 没有妨碍） | 强制删 Pod 的 5xx 比 2c 多（133 对 29）的原因没有查清；“大概率只是 order 被删得多”没有证实 |
| 摘除时间不累加（场景 G，5.1 到 5.4 秒） | 出站边车把剩余时限传下去、入站边车也计时这个机制，在设计 10 第二到四步里标的是“推断（没有证实）”，第五步用边车统计里的 `grpc-status 4` 给了支持，请求头本身有没有被直接抓取，设计没有写 |
| 按调用方区分的额度（512、128、1s、3s 都从边车配置里读到了） | 入站上限 1024 没压过 |
| 不停机迁移 0 失败（19870 个和 18770 个请求）；回退缺陷已修 | istiod 不在时的行为 |
| 严格模式加访问控制的六项（10.4） | HPA 扩容点、延迟对比 |
| `DRAIN_DELAY=5` 下 9570 个请求 0 失败 | xDS 的精确传播时间（只有分辨率 0.5 秒的范围）；5 秒下没有 `DRAIN_DELAY=0` 的对照 |

### 17.3 我发现材料之间对不上的地方（写笔记时的处理）

- 设计 3.2 写“注入按命名空间打标签（`istio-injection=enabled`）”；设计 10 和 chart 实际是按 Pod 的标签注入、命名空间不打标签。笔记以后者为准。
- 设计 3.2 写“e2e 的测试 Job、压测 Job 也不注入”；设计 10 第八步测试的发压 Pod 注入了边车（用 ServiceAccount `load`）。笔记以后者为准。
- 设计 3.3 写 `DRAIN_DELAY` “先不动（15 秒）”；设计 10 第七步改成了 5 秒。
- 设计 3.4 的 `DestinationRule` 片段含 `maxRetries: 0`；设计 10 第一步发现它无效，清单里没有。
- 设计 3.4 把 `PeerAuthentication` 描述成“对 gRPC 端口改成 STRICT”；清单是命名空间级的一条 STRICT，只作用于被边车接管的端口，效果等价但写法不同。
- 设计 11 把“ADR-0020 的补充”列为第九步待办，但 `docs/adr/0020-service-protection.md` 里已经有“补充：集群上换成 Istio（2026-11-16）”。
- ADR-0029 “后果”一节仍然写“摘除时间随次数加长……恢复得越来越慢”，同一篇后面的“实测之后的修正”说这一项没有出现；以修正为准。
- 阶段计划“面试考点”里“2d 补充”写“摘除为什么不是立刻生效的”，设计 10 的原因“吻合但没有直接证实”；笔记保留了限定。

### 17.4 我没敢写进笔记的点

- 设计 4 要求的“`maxEjectionPercent` 用默认值时不摘除”“路由不写 `timeout` 时不摘除”两个对照，设计 10 的八个场景结果里没有记录它们在 Istio 上单独做过；笔记（5）只说了设计的要求，没有说做了。
- istiod 证书轮换的周期、证书到期前 istiod 不在的后果：材料里没有。
- 边车和 Job 的老问题在 Istio 下的实测：材料里没有（15）。

---

## 18. 速记：每条考点一句话

- **网格是什么**：把发现、均衡、超时、熔断、mTLS 从代码里拿出来；istiod 算配置经 xDS 推（LDS、RDS、CDS、EDS），边车执行。
- **透明拦截**：init 容器写 iptables，出站重定向到 15001、入站到 15006；进程不知道边车，排查要看 `istioctl proxy-config`。
- **默认值的冲突**：默认重试 2 次且含 `unavailable`（写请求会被再发）、不设超时、`maxEjectionPercent` 10%、没有并发上限；全部显式写，重试在网格默认和每条路由各关一次。
- **证明重试关了**：912 个请求增量 0，对照改成重试 2 次增量 92。
- **熔断两样东西**：`connectionPool` 管并发，`outlierDetection` 管下游失败；作用在调用方，`workloadSelector` 加 `sourceLabels` 按调用方区分。
- **慢下游**：只有路由 `timeout` 到期才算失败；进程里的超时长 500 毫秒；三处同值用测试核对。
- **发现**：摘除晚 0.5 到 1 秒（`update_merge_window` 吻合、没证实）；摘除时间不累加（5.1 到 5.4 秒）；504 的出现是因为入站边车也计时。
- **端口名**：名字不对认成 TCP，按连接均衡，不报错；`istioctl analyze` 加计数防它。
- **`Sidecar` 资源**：默认全网格可见，收窄到本命名空间加 `istio-system`，order 边车 23 个集群。
- **不停机迁移**：`PERMISSIVE`、自动 mTLS 逐端点判断、调用方先切；两次演练加长期一套 0 失败；回退缺陷是废弃字段 `serviceAccount` 不被清，教训是回退也要演练。
- **mTLS**：身份是 ServiceAccount 的 SPIFFE ID；有 ALLOW 就默认拒绝其余，所以名单和 `load` 的策略要一起应用；名单外 403，没边车的 Pod 连接被重置。
- **`DRAIN_DELAY`**：xDS 推送，最后一个请求落在 SIGTERM 前后不到一秒（分辨率 0.5 秒），从 15 降到 5，9570 个请求 0 失败。
- **内存**：边车平均 32 Mi（手写 16 到 18 Mi），istiod 76 Mi，合计约 230 Mi。
- **istiod 挂了**：预期是已有流量继续、新 Pod 建不出来，没有实测。
- **ambient**：ztunnel 加 waypoint；没选是因为“每个调用方各有额度”要重新设计；没做对照。
