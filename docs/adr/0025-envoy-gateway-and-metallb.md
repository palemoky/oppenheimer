# ADR-0025：集群入口用 Envoy Gateway 加 MetalLB，不用 ingress-nginx 和 k3s 自带的入口

- 状态：已采纳
- 日期：2026-11-15

## 背景

阶段 2 的计划原来写的是“ingress-nginx + MetalLB（或 k3s 自带方案）”。动工时查到两件事：

- **ingress-nginx 已经停止维护。** Kubernetes 的 SIG Network 和安全响应委员会在 2025 年 11 月宣布它退役，尽力而为的维护到 2026 年 3 月为止，之后没有新版本，也不修安全漏洞。最后一版是 controller-v1.15.1（2026-03-19）。官方的建议是迁到 Gateway API。
- **Ingress 这套 API 本身也冻结了。** 新功能只进 Gateway API。面试里“流量怎么进集群”这道题，现在要能讲 Gateway API 的三层资源（GatewayClass、Gateway、HTTPRoute）以及它和 Ingress 的区别。

约束：集群在家里的局域网上，没有云厂商的负载均衡；内存是最紧的资源；入口要能撑住阶段 3 从 Mac mini 发起的压测。

## 决定

1. 入口控制器用 **Envoy Gateway**，路由用 Gateway API 的资源（`Gateway`、`HTTPRoute`）声明，不写 `Ingress`。
2. `LoadBalancer` 类型的 Service 由 **MetalLB** 的二层模式分配地址，地址池是 192.168.50.50 到 192.168.50.59。
3. k3s 自带的 Traefik 和 ServiceLB 都禁用（`--disable=traefik --disable=servicelb`）。

## 理由

- **和 1d 是同一套东西。** 服务之间的熔断、并发上限、逐跳时限已经是 Envoy 边车在做（ADR-0020），1d 里把 Envoy 的统计项、摘除的行为、三个默认不生效的坑都跑过一遍。入口也用 Envoy，排查时看的是同一套指标名和同一种访问日志，学到的东西能叠加。
- **是 Gateway API 的参考级实现。** Envoy Gateway 以 Gateway API 为唯一的配置入口，超出标准的能力（限流、重试、熔断）用它自己的策略资源挂在路由上，不靠注解。ingress-nginx 的问题之一正是大量行为藏在注解里。
- **MetalLB 比 ServiceLB 更接近真实的负载均衡。** ServiceLB 是在每个节点上用 hostPort 起一个转发的 Pod，对外的地址就是节点地址。MetalLB 分配独立的虚拟地址，由某一个节点通过 ARP 应答来“持有”，节点挂了由另一个节点接管。阶段 4 关节点时，“入口地址漂移要多久、漂移期间丢多少请求”是值得量的。

## 备选方案

- **k3s 自带的 Traefik 加 ServiceLB**：零安装，Traefik 也支持 Gateway API。没选是因为它和项目里已有的 Envoy 是两套代理、两套指标；ServiceLB 没有地址漂移可练。省下的内存不多。
- **Cilium 的 Gateway API 实现**：CNI 换成 Cilium 后可以不单独装入口控制器，底层也是 Envoy。没选是因为 CNI 还没有定（见 ADR-0026），不想让入口的选择绑在 CNI 上。
- **Istio（含 ambient 模式）**：能同时接管入口和服务之间的流量。没选是因为内存开销大，而且 1d 的边车是手写的 Envoy 配置，换成 Istio 等于把 1d 那一层重做。
- **NGINX Gateway Fabric、Kong、HAProxy**：都能用，和项目已有的东西没有交集。

## 后果

- 多装两个组件（MetalLB、Envoy Gateway），各自有控制器和数据面的 Pod，内存占用 2a 装好后实测并记到设计文档。
- MetalLB 的二层模式同一时刻只有一个节点接收某个地址的流量，入口的带宽上限是单个节点的，不是三个节点之和。压测时如果入口成为瓶颈要先想到这一点。
- gateway 服务自己已经做了限流（ADR-0019）。入口这一层先只做路由和 TLS，不重复限流；要不要把按 IP 的限流前移到 Envoy Gateway，等 2c 服务上了集群再看。
- 1d 的边车在本机是 compose 里的独立容器，上集群后怎么部署（Pod 里的 sidecar 容器）是 2c 的事，不在本篇范围。
- 重新评估的条件：CNI 定为 Cilium 且内存吃紧时，可以考虑用 Cilium 自带的 Gateway API 实现省掉一套控制器。

## 版本

查证于 2026-11-15，引入时登记到 `docs/infra/versions.md`：Envoy Gateway v1.10.0（2026-11-06 发布），MetalLB v0.16.2。Gateway API 用 Envoy Gateway v1.10 的兼容性表里写的 v1.6.1（它支持 Kubernetes v1.34 到 v1.37），不用上游最新的 v1.7.0。Envoy Gateway 的每个版本只支持特定范围的 Gateway API 和 Kubernetes 版本，安装前要对照它的兼容性表。

## 2a 的实测（2026-11-15）

细节在设计文档 [2a-cluster.md](../design/2a-cluster.md) 的第 11、12 节。

- 内存：Envoy Gateway 的控制器 41 Mi，Envoy 38 Mi；MetalLB 的控制器 35 Mi，三个 speaker 各约 22 Mi。合计不到 200 Mi。
- MetalLB 的 chart 默认还会装 BGP 用的 frr-k8s（每个节点约 60 Mi），二层模式用不到，已用 `frrk8s.enabled=false` 关掉。
- **Gateway API 的 CRD 实际用的是 k3s 自带的 v1.6.3（标准通道），不是上面写的 v1.6.1。** k3s v1.37 自己装这套 CRD；Envoy Gateway 的 chart 带的是实验通道的，装上去会被 Gateway API 的准入策略拒绝。所以只装 Envoy Gateway 自己的 CRD。后果：Gateway API 的版本跟着 k3s 走，升级 k3s 时要对照 Envoy Gateway 的兼容性表；实验通道才有的资源用不了。
- 入口的 Service 用 `externalTrafficPolicy: Local`，后端看到的 `X-Forwarded-For` 是真实的客户端地址。
- 入口地址在节点故障时的漂移时间没有量（阶段 4）。

