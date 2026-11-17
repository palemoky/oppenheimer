# 3a 设计：可观测三件套上集群

状态：主人已确认（2026-11-17），第 2 节的十项都按建议。决定和理由见 ADR-0030。

对应阶段计划 [phase-3-observability.md](../plan/phase-3-observability.md) 的 3a。埋点（进程怎么产生链路、指标、日志）在 1d 已经做完（设计文档 1d 第 8 节、ADR-0023），这一步做的是“收、存、看”：Collector、三个后端、exporter、大盘，以及按 QPS 的 HPA。压测是 3b，SLO 和告警是 3c。

## 1. 动工前查到的事实（2026-11-17）

### 1.1 集群现状

- 三个节点都是 Ready，没有不正常的 Pod。内存：server 10 GiB 用了 40%（requests 2.6 GiB），agent-1、agent-2 各 16 GiB 用了 28%、29%（requests 6.5、5.8 GiB）。磁盘：server 剩 52 GB，两个 agent 各剩 138 GB。
- 存储只有 `local-path` 一个 StorageClass（数据在节点本地盘，Pod 因此钉在那个节点上）；NAS 的 NFS 实测同步写只有 20 MB/s（2026-11-17 的会话日志）。
- 集群上的六个服务**现在不导出任何链路和指标**：chart 和 values 里没有 `OTEL_EXPORTER_OTLP_ENDPOINT`，`pkg/otelx` 处于“不导出”状态（span 照常产生，日志里有 trace id）。
- k3s 的 server 已经开了 `etcd-expose-metrics`（2a 时预留的）。k3s 把 apiserver、scheduler、controller-manager、kube-proxy 编在一个进程里，kube-prometheus-stack 默认按 kubeadm 的布局去找这几个组件，要改 values（安装时验证）。
- Istio：`accessLogFile` 是空的（不出访问日志），边车没有配链路追踪，只原样转发 `traceparent`。边车和 istiod 的统计在 15020、15014 端口上，没有人抓。
- 中间件都没有 exporter：MySQL 和两套 Redis 的 chart 里没有，Kafka 的 CR 里没有 `metricsConfig` 和 `kafkaExporter`。
- 服务没有 pprof 端点。
- Mac mini 在线（macOS 27.2，16 GB），没有装 k6（3b 再装）。

### 1.2 本机已经验证过的（1d 步骤六）

本机的 `otel-lgtm` 容器里就是 Collector 加 Tempo、Prometheus、Loki、Grafana。属性名、span 名、指标名都在它上面看过，TraceQL 和 PromQL 的查法已经用来查过两个真实问题（1d 的 8.16、8.18）。集群上的后端和它是同一套，所以这一步的风险在部署和容量，不在埋点。

1d 量到、这里要用的数：一次下单的链路约 70 个 span；1% 采样的开销量不出来，100% 采样时落库吞吐低约 7%；全量采样压一轮，`otel-lgtm` 的内存到 1.3 GiB。

### 1.3 版本

Haiku 子 Agent 2026-11-17 从官方发布页查的。**主会话还没有逐项核对**，引入每个组件时再到发布页确认一次，那时登记到 `versions.md`。

| 组件 | 查到的版本 | 说明 |
|---|---|---|
| kube-prometheus-stack（chart） | 93.3.1 | 它默认带的各组件版本没有查到（chart 包没下载下来），安装时看 |
| Prometheus | LTS 是 3.13（支持到 2027-07-31）；最新是 v3.16.0 | 按规范用 LTS，chart 默认的版本不是它的话在 values 里改 |
| prometheus-operator、Alertmanager、Grafana | v0.95.2、v0.35.0、v13.3.1 | |
| node_exporter、kube-state-metrics | v1.12.1、v2.20.0 | |
| OTel Collector（contrib） | v0.165.0 | 官方 chart `opentelemetry-collector` 0.178.3 默认的是 0.163.2，差两个小版本 |
| Loki | v3.7.10 | 4.0 只有 rc。chart 搬到了 `grafana-community` 仓库，单体模式改叫 Monolithic，仍然支持 |
| Tempo | v3.1.1 | `grafana/tempo` 和 `tempo-distributed` 两个 chart 都标了弃用，单体模式用 `grafana-community` 的 `tempo` |
| Grafana Alloy | v1.22.0 | Promtail 已在 2026-03-02 停止维护，Alloy 是它的后继 |
| mysqld_exporter | v0.21.0 | 文档只写了支持 MySQL 5.6 以上，对 9.x 要实测 |
| redis_exporter | v1.97.0 | |
| Strimzi 自带的 Kafka Exporter | 随 Strimzi 1.3.1 | 仍在文档里，没有弃用标记 |
| KEDA | v2.21.1 | 官方测试的 Kubernetes 是 1.34 到 1.36，我们是 1.37，不在范围里 |
| prometheus-adapter | v0.12.0 | 最近一次发布是 2024-05-17，chart 还在更新 |
| Pyroscope | v2.4.3 | 单体模式要多少内存，文档里没有 |
| k6 | v2.4.0 | `brew install k6`，3b 用 |
| MinIO 社区版 | 已归档（2026-04-25） | 不适合新部署。查到的替代品是 SeaweedFS 4.54；Garage 的最新版本没有查清 |

没有查到的：Collector 的 `filelog` 接收器之外的细节（它是 beta）；Tempo 的 metrics-generator 在单体模式下能不能用；Prometheus 的 OTLP 接收端（`/api/v1/otlp`）和 native histogram 在 3.13 里算不算稳定特性；Loki 单体模式的规模上限。前两项关系到第 2 节的建议，安装那一步先验证。

## 2. 主人确认的十项（都按建议）

| # | 问题 | 建议 | 主要的另一种做法 |
|---|---|---|---|
| 1 | 指标从 Collector 怎么进 Prometheus | **Collector 用 OTLP 直接写进 Prometheus 的 OTLP 接收端**。本机的 `otel-lgtm` 就是这条路，指标名和标签的转换规则因此和本机一致，1d 用过的 PromQL 可以原样用。前提是这个接收端在 3.13 里可用且够稳，没有查到，安装时先验证；不行就退到右边那种 | Collector 开一个 Prometheus 格式的端点，由 Prometheus 来抓：最老、最稳的接法；资源属性的处理和本机不同，Collector 重启时会丢一个抓取周期里攒的数 |
| 2 | Collector 的部署形态 | **两层**：每个节点一个 agent（DaemonSet，只收本节点的容器日志和 kubelet 的指标），加一个 gateway（Deployment，1 个副本，收服务的 OTLP、做尾部采样、分发到三个后端）。服务直接把 OTLP 发给 gateway 的 Service | 只有 DaemonSet：服务发给本节点的 agent。尾部采样要求一条链路的 span 全到同一个实例，而一个订单的 span 来自三个节点上的 Pod，做不了。只有 Deployment：收不了节点上的日志文件 |
| 3 | 日志用什么收 | **OTel Collector 的 `filelog` 接收器**（就是第 2 项的 agent），经 OTLP 写进 Loki。ADR-0004 定的就是“由 Collector 分发三种信号”，少学、少跑一个组件。这个接收器的稳定级别是 beta | Grafana Alloy（Promtail 的后继，Promtail 已停止维护）：Grafana 自己的采集器，和 Loki 配合的文档最多；多一个组件，多一套配置语言 |
| 4 | Prometheus、Loki、Tempo 的数据放哪 | **节点本地盘（`local-path` 的 PVC）**，Loki 和 Tempo 用单体模式加文件系统存储。保留期先定 Prometheus 15 天（另加容量上限）、Loki 7 天、Tempo 3 天，按实测的数据量再改 | NAS 上起一个 S3 兼容的对象存储：数据不跟节点走，更像生产；但 NAS 是机械盘、同步写 20 MB/s，多一个要维护的组件，而且 MinIO 社区版已经归档，要换 SeaweedFS 这类。Prometheus 本身不支持 NFS |
| 5 | 监控这一套放在哪个节点 | **尽量放 k3s-server，并把三台虚拟机的内存从 10、16、16 改成 14、14、14**。阶段 4 要关的是 agent 节点，关的时候不能把“眼睛”一起关掉；server 关了控制面也没了，本来就不在实验里。这一套估计要 4 到 5 GiB（第 4 节），server 现在只剩约 6 GiB | 不指定节点、不改内存：最省事，但阶段 4 关节点时有一半的可能把 Prometheus 一起关掉。或者加第四台虚拟机专门放监控（地址 .43 已预留）：隔离最干净，要从两个 agent 各拿走 4 GiB 和 2 个 vCPU |
| 6 | 采样 | **集群上 SDK 仍按比例做头部采样（平时 100%，压测时调到 10% 上下），Collector 的 gateway 再做尾部采样**：出错的、慢的（先定超过 500 毫秒）全留，其余留 10%。然后按计划做一次头部和尾部的对比（同样的故障，各自留下了多少条出错的链路、各花多少内存） | 只做头部采样：简单、Collector 不用攒数据；出错的那条恰好没采到的问题留着。只做尾部：SDK 全量导出，压测时服务端要付那 7% |
| 7 | Istio 边车的数据收多少 | **只抓边车的标准指标**（`istio_requests_total`、耗时直方图），去掉用不上的标签；**边车不出 span、不开访问日志**。它回答的是“进程以为调用花了 15 毫秒，其中多少在两个边车上”（2d 留下的延迟对比） | 边车也出 span：链路里每一跳多两个 span，能直接看到边车的耗时；数据量翻倍。访问日志在排查网格问题时再临时打开 |
| 8 | 中间件的 exporter 怎么放 | **独立的 Deployment**：一个 mysqld_exporter、一个 redis_exporter，各自用多目标模式抓全部实例；Kafka 用 Strimzi 自己的指标配置加 Kafka Exporter（消费延迟）。MySQL 和 Redis 的 Pod 不用动 | 做成每个实例的边车容器：exporter 和实例同生共死、地址是 localhost；但要改两个 chart，MySQL 和 Redis 全部滚动重启一遍。Kafka 那边无论如何要滚动一次 broker |
| 9 | 持续剖析 | **只开 pprof 端点**（放在各服务已有的内部 HTTP 端口上，不经入口），要看时 `kubectl port-forward` 加 `go tool pprof`。阶段 4 的泄漏类故障和诊断 Agent 的 pprof 工具都只需要这个 | 再部署 Pyroscope 做持续剖析：能回看“出事那一刻”的火焰图；多一个有状态的组件和它的内存。可以等 3b 压测时觉得缺了再加 |
| 10 | 按 QPS 的 HPA 用什么提供指标 | **KEDA**：还在活跃地发版，一个 `ScaledObject` 里直接写 PromQL，以后按 Kafka 消费延迟伸缩消费者也是它。风险是官方没有在 Kubernetes 1.37 上测过，装上先验证 | prometheus-adapter：把 PromQL 映射成自定义指标 API，HPA 本身不变，概念上最贴近“HPA 的自定义指标”这个考点；但最近一次发布是 2024 年 5 月，维护状态存疑 |

## 3. 设计

### 3.1 总体的样子

```
服务（6 个）──OTLP gRPC──▶ Collector gateway ──▶ Tempo（链路，尾部采样之后）
   │ 标准输出的 JSON 日志      │（Deployment）   ──▶ Prometheus（服务的指标）
   ▼                           │                 ──▶ Loki（日志）
节点上的日志文件 ──▶ Collector agent（DaemonSet）──┘

Prometheus ──抓──▶ node-exporter、kube-state-metrics、kubelet、etcd、istiod 和边车、
                    mysqld_exporter、redis_exporter、Kafka、Collector 自己、Tempo、Loki
Tempo metrics-generator ──▶ Prometheus（service graph、span 指标）
Grafana ──查──▶ Prometheus、Loki、Tempo
```

命名空间：`monitoring`（kube-prometheus-stack、Loki、Tempo、Collector、exporter）。Collector 不注入 Istio 边车；服务到 Collector 的 4317 端口加进边车的出站排除名单（和 MySQL、Redis、Kafka 一样，2d 第 2 节第 3 项），否则可观测的流量也要过边车、还要给它写访问控制。

### 3.2 两种数据来源，两条路

- **服务自己的指标和链路：推。** 服务用 OTLP 推给 Collector（ADR-0004）。chart 里加 `OTEL_EXPORTER_OTLP_ENDPOINT`，values 里一个开关；`service.instance.id` 现在是主机名加进程号，在集群上就是 Pod 名，够用。再用 Downward API 把命名空间、Pod 名、节点名放进 `OTEL_RESOURCE_ATTRIBUTES`，不靠 Collector 按来源地址去猜。
- **别人的指标：拉。** 节点、Kubernetes 对象、中间件、Istio 都是 Prometheus 格式的端点，由 Prometheus 按 `ServiceMonitor`、`PodMonitor` 直接抓，不绕 Collector。

两条路的差别本身是考点：推的一方挂了，指标只是不再更新，要靠“这个序列多久没有新点”来发现；拉的一方有 `up`。

### 3.3 Collector

gateway 的管线：

- 接收 OTLP（gRPC 4317）。
- `memory_limiter` 放在最前面（超过上限就拒收，SDK 的队列满了丢 span，不会把 Collector 撑死），`batch` 放在最后。
- 链路：`tail_sampling`（第 6 项的三条策略，决定前等 10 秒）→ Tempo。
- 指标：→ Prometheus（方式见第 1 项）。
- 日志：agent 转过来的 → Loki。

尾部采样只有 1 个副本时才成立。要多副本就得在前面加一层按 trace id 分流的 Collector（`loadbalancing` exporter）；这里不做，记成考点。1 个副本的代价是它重启时丢几秒数据，可以接受。

agent 的管线：`filelog` 读 `/var/log/pods/` 下的容器日志，解析 containerd 的行格式，再解析我们的 JSON，取出级别、`trace_id`、`span_id`。原来还计划用 `kubeletstats` 抓本节点 Pod 的资源用量，实现时去掉了（kube-prometheus-stack 已经在抓 kubelet，见第 8 节第 4 步）。

**Collector 自己的监控**：它在 8888 端口出自己的指标（收了多少、拒了多少、丢了多少、队列多长、采样决定各多少），由 Prometheus 抓，进“Collector”大盘。3c 里给“拒收”和“导出失败”配告警。

### 3.4 日志

- 服务的日志已经是标准输出上的 JSON，带 `trace_id`（1d 的 8.9）。不改代码。
- Loki 的标签只放低基数的：命名空间、Pod、容器、服务名、级别。**`trace_id` 不做标签**，放在结构化元数据里（按它查仍然快，不产生新的流）。和指标标签的规则是同一个道理。
- 中间件和系统组件的日志一起收，不解析内容。

### 3.5 链路

- Tempo 单体模式，接收 OTLP。
- 打开 metrics-generator：service graph（谁调谁、各边的请求数和耗时）和 span 指标，写进 Prometheus。它是按 span 现算的，放在尾部采样**之后**的话只能看到被留下的那部分，数会偏；所以 gateway 里把 span 复制一份、在采样**之前**用 Collector 的 `spanmetrics`、`servicegraph` 连接器算，还是接受偏差只用 Tempo 的，实现时比较后定。
- 一个订单是三条链路（1d 的 8.15），靠 `app.order_id` 关联；Grafana 里做一个按订单号搜的入口。

### 3.6 互相跳转

- 日志 → 链路：Loki 数据源配“从 `trace_id` 字段跳到 Tempo”。
- 链路 → 日志：Tempo 数据源配“按 trace id 和时间范围查 Loki”。
- 指标 → 链路：exemplar。SDK 的直方图在被采样的请求上带 trace id，Prometheus 要开 exemplar 存储，Grafana 的图上点一个点跳到链路。1d 没有做（8.13），这里补；SDK 一侧要不要改代码，实现时先验证。
- 链路 → 指标：Tempo 数据源配“从 span 跳到这个服务的 RED 面板”。

验收就是阶段计划的那一句：任意一个慢请求，从大盘下钻到链路再到日志。

### 3.7 大盘

大盘的 JSON 放在仓库里（`deploy/cluster/monitoring/dashboards/`），以 ConfigMap 下发，Grafana 的 sidecar 自动加载；不在界面上手工建了不留痕迹。

| 大盘 | 内容 |
|---|---|
| 业务总览 | 下单的受理和落库结果（按原因码）、支付结果、背压和闸门、限流、降级开关和熔断器的状态、CDC 和商家索引的延迟 |
| 单服务（一个模板，变量选服务） | RED：HTTP 和 gRPC 的请求量、错误率、延迟分位数；Go 运行时；MySQL 和 Redis 连接池；消费者的延迟、重试、死信 |
| 中间件 | MySQL（QPS、连接、主从延迟、InnoDB）、Redis（内存、命中、主从）、Kafka（broker、分区、消费延迟） |
| 集群资源 | kube-prometheus-stack 自带的节点和 Pod 大盘，不重做 |
| 网格 | 每条调用边的请求量、5xx、摘除次数；边车加出来的延迟 |
| Collector | 3.3 |

Grafana 经入口暴露（`Host: grafana.oppenheimer.test`，和业务入口同一个地址 .51）；管理员密码由脚本在集群里随机生成成 Secret，不入库。Prometheus、Alertmanager 的界面不暴露，用 `kubectl port-forward`。

### 3.8 按 QPS 的 HPA

gateway 和 order 现在按 CPU 伸缩。CPU 对 order 是个凑合的信号（落库卡在下游时 CPU 是低的）。改成按每个副本的请求速率，指标来自 Prometheus；具体组件见第 10 项。放在 3a 的最后做，因为扩容点要对着 3b 的压测数据定，这里先把链路接通、用现有的 CPU 目标反推一个初值。

2c 留下的“新 order Pod 一就绪就有十几个 503”要在这一步之前查清并修掉（它会污染扩容实验的结论），见第 6 节第 1 条。

## 4. 内存和磁盘（估算，装完实测后改）

| 组件 | requests | 说明 |
|---|---|---|
| Prometheus | 1.5 Gi | 序列数先估 10 到 20 万；Istio 边车的直方图是大头，所以第 7 项要裁标签 |
| Grafana | 256 Mi | |
| Alertmanager、operator、kube-state-metrics | 约 250 Mi | |
| node-exporter × 3 | 约 100 Mi | |
| Loki（单体） | 1 Gi | |
| Tempo（单体，带 metrics-generator） | 1 Gi | |
| Collector gateway | 512 Mi | 尾部采样要把 10 秒内的 span 攒在内存里；压测时每秒几千到上万个 span |
| Collector agent × 3 | 约 400 Mi | |
| exporter（MySQL、Redis、Kafka） | 约 250 Mi | |
| 合计 | 约 5.3 Gi | 其中约 4.5 Gi 是要钉在一个节点上的 |

磁盘：server 的系统盘剩 52 GB。Prometheus 给 20 Gi（15 天加容量上限 15 GB）、Loki 10 Gi、Tempo 10 Gi。

要量的：装完空转一天、压测一轮之后各组件的实际内存和磁盘增长，据此定保留期和 limits，结果写进 `hardware.md` 的预算表。etcd 和 Prometheus 在同一个节点上抢磁盘：2b 记过 etcd 的 fsync p99 已经在 8 到 16 毫秒，装完之后这是第一个要看的图。

## 5. 验证

每一项都要有“看到了”的证据，不只是 Pod 在跑：

1. 六个服务的指标在 Prometheus 里查得到，名字和标签与本机的一致；Pod 重启后旧序列停止、新序列出现。
2. 经入口下一单并支付，按 `X-Trace-Id` 在 Tempo 里找到三条链路，span 的种类和 e2e 的 `TestTraceSpansAllServices` 断言的一致。
3. 从这条链路跳到它的日志，从日志跳回链路，从延迟的图上点 exemplar 跳到链路。
4. 尾部采样：制造一批出错的请求和一批慢请求，确认全部留下；正常请求留下的比例接近设定值。和头部采样的对比写成表。
5. 停掉 Collector gateway：服务照常、内存不涨（1d 的 8.17 没有长时间验证过的那一条）；它回来之后数据恢复。
6. `make cluster-fault` 抽两三个场景，看熔断器、开关、摘除的指标和实际发生的事对得上（1d 的 8.17 没有做的那一条）。
7. 中间件：停一个 MySQL 从库的复制、让一个消费者停住，主从延迟和消费延迟的图有反应。
8. `make cluster-e2e` 和探针在监控装上之后仍然通过；从空集群重建的一键顺序里加上 `make monitoring-up`，重建一次。

## 6. 实现顺序和分工

1. **2c 留下的“新 order Pod 一就绪就有十几个 503”不先修，等第 4 步之后用监控重现再定**（Fable）。2c 推断的原因是闸门 Redis 的连接池是冷的，但读代码对不上：`redisx.OpenLazy` 默认把 `MinIdleConns` 设成连接池大小，并在返回前等连接建满（最多 2 秒），就绪之前池子已经是热的。当时只有时间上的吻合，没有别的证据。可能的原因还有：预热在 2 秒内没有完成（Sentinel 模式要先问主库地址）、新 Pod 启动时 CPU 被消费组加入和迁移检查占着、读闸门的 300 毫秒时限在边车刚就绪时不够。这正好是第一个“靠监控查原因”的案例：连接池的等待次数、Pod 的 CPU 节流、出错请求的链路（尾部采样会全留）三样一起看。它必须在第 7 步（HPA）和 3b 之前解决。
2. kube-prometheus-stack（Sonnet 写 values 和脚本，主会话验收）：`scripts/cluster/monitoring.sh`、`make monitoring-up`，k3s 的组件端点，Grafana 的入口。
3. Loki、Tempo（Sonnet）。
4. Collector 的 gateway 和 agent（主会话定管线，Sonnet 写 chart 的 values）；service chart 加 OTLP 的环境变量和出站排除；六个服务滚动一遍。到这里做验证 1 到 3。
5. 并行：中间件的 exporter（Sonnet）；Istio 的 `PodMonitor` 和标签裁剪（Sonnet）；pprof 端点（Opus，`pkg/app`）；大盘（Sonnet，一个大盘一个任务）。
6. 尾部采样和对比实验（主会话）。
7. 按 QPS 的 HPA（主会话定，Sonnet 写清单）。
8. 实测内存和磁盘，定保留期；重建一次；写 ADR、笔记、面试考点。

## 7. 不在这一步做的

- 告警规则、SLO、Alertmanager 的路由：3c。kube-prometheus-stack 自带的那批规则先留着，3c 再筛。
- k6 和压测：3b。
- 监控数据的高可用（Prometheus 双副本、Thanos、Loki 和 Tempo 的分布式模式）：内存不够，也不是这个项目要学的重点；讲得清为什么单副本、丢了什么就行。
- 日志经 OTLP 从 SDK 直接上报：维持“标准输出加采集”（1d 的 8.9）。
- CDC 源库心跳（1c 挪过来的）：属于 3c 的“主从延迟、CDC 延迟”告警，那时一起做。

## 8. 实现中的发现和量到的数

### 第 0 步：挪内存（2026-11-17）

三台虚拟机改成各 14 GiB（`cluster.env`、`vm.sh resize`），按 agent-1、agent-2、server 的顺序一次一台。每台开机后 22 到 38 秒集群全部恢复，入口照常应答。重启 agent 时 Sentinel 又切了一次，**闸门 Redis 的主库现在是 `redis-gate-1`**。

### 第 2 步：kube-prometheus-stack（2026-11-17）

`make monitoring-up`（`scripts/cluster/monitoring.sh`），values 在 `deploy/cluster/monitoring/kube-prometheus-stack.yaml`。第一次安装 3 分 10 秒。

- **版本核对过**：chart 93.3.1 默认带的是 Prometheus v3.16.0、operator v0.95.2、Grafana 13.3.1、Alertmanager v0.35.0、node-exporter v1.12.1、kube-state-metrics v2.20.0。Prometheus 换成了 LTS 的 v3.13.5（quay.io 上 3.13 线最新的补丁），其余用 chart 默认的。
- **第 1 项的前提成立**：3.13.5 有 `--web.enable-otlp-receiver`，chart 里是 `enableOTLPReceiver` 一个开关，不在实验特性的名单里。已打开，还没有东西往里写（第 4 步验证）。
- **k3s 的三个组件抓不到，先关了**：scheduler（10259）、controller-manager（10257）、kube-proxy（10249）的指标端口只监听 127.0.0.1（在 server 上 `ss -ltnp` 确认）。要抓得给 k3s 加启动参数并重启所有节点，眼下没有要靠它们回答的问题；对应的监控对象和告警规则一起关掉。apiserver、kubelet、CoreDNS、etcd（server 节点的 2381）都抓到了。
- **全部落在 k3s-server 上**（“优先”的亲和性起了作用），三个卷已绑定在那里。装完之后没有不健康的抓取目标。
- **序列数比估的多**：什么业务指标都还没有接，已经是 13.2 万条，其中 kubelet 7.2 万、apiserver 4.7 万。第 4 节估的“总共 10 到 20 万”偏低，Istio 和服务接上之后再看要不要裁 apiserver 和 kubelet 的直方图。
- 内存（刚装完几分钟，不代表稳态）：Prometheus 332 Mi、Grafana 424 Mi（启动时在加载大盘，requests 给的是 256 Mi）、其余合计约 100 Mi。server 节点从 21% 到 33%。
- **etcd 的 WAL fsync p99 是 29.6 毫秒，后端提交 p99 是 19.5 毫秒**（装完几分钟后量的两分钟窗口）。2b 在空集群上记的是 8 到 16 毫秒。这个数量的时候 server 刚重启过、Prometheus 刚开始写盘、节点在拉镜像，不能直接归到监控头上；但它正是 ADR-0030 的后果里担心的那一项，**空转一天之后要再量**。
- 写脚本时的一个坑：`tr -dc ... </dev/urandom | head -c 24` 在 `set -o pipefail` 下会让脚本无声地退出（`head` 拿够就走，`tr` 收到 SIGPIPE，管道的退出码是 141）。改用 `openssl rand`。
- Grafana：`http://192.168.50.51/`，`Host: grafana.oppenheimer.test`（浏览器要在 `/etc/hosts` 里加一行），密码用 `scripts/cluster/monitoring.sh password` 看。`scripts/cluster/monitoring.sh promql '<查询>'` 可以不开端口转发直接查。

### 第 3 步：Loki 和 Tempo（2026-11-17）

`scripts/cluster/monitoring.sh loki|tempo|datasources`，`make monitoring-up` 现在依次装三样。values 在 `deploy/cluster/monitoring/loki.yaml`、`tempo.yaml`，数据源在 `datasources.yaml`。Sonnet 写，主会话读过并在集群上执行。

- **版本核对过**：chart 在 `https://grafana-community.github.io/helm-charts`（旧仓库里的 `tempo`、`tempo-distributed` 已标弃用，停在 Tempo 2.9）。Loki 3.7.10（chart 21.3.1）、Tempo 3.1.1（chart 3.2.1），都是 chart 默认的应用版本，也是 GitHub 上最新的正式版。
- **Tempo 的 metrics-generator 在单体模式下能用**：Tempo 3.x 的单体模式不要 Kafka，分发器直接把数据交给 metrics-generator。两处 chart 不替你做：处理器（`service-graphs`、`span-metrics`）要在 `overrides.defaults` 里按租户打开；它的 WAL 默认在 `/tmp`，挪到了卷上。它用 remote write 写 Prometheus，所以 Prometheus 多开了 `enableRemoteWriteReceiver`。**还没有链路进来，生成的指标没有验证过**（第 4 步）。
- 两个 Pod 和卷都在 k3s-server 上。Grafana 里三个数据源的健康检查都通过；Prometheus 数据源上 exemplar 到 Tempo 的跳转已配。
- 互相跳转是按“Collector 把 `trace_id` 存成 Loki 的结构化元数据、`service.name` 成为标签 `service_name`”配的，**第 4 步接上 Collector 之后才能验证**；对不上就改 `datasources.yaml`。配置里给 Grafana 的占位符要写成 `$$`（Grafana 加载数据源文件时会展开 `$VAR`）。
- **验收时发现的一个错**：子 Agent 在 Loki 的存储配置里写了 `admin_api_directory`，那是企业版才有的字段，Loki 启动时解析配置失败、Pod 反复重启。`helm template` 查不出这类错（chart 只是把它原样放进配置文件），要真的起一次才知道。已删掉。
- **`local-path` 不限制卷的大小**：PVC 上写的 10 Gi、20 Gi 只是个标签，没有东西拦着它们写满节点的盘。真正起作用的是保留期（和 Prometheus 的容量上限）。server 的系统盘只有 60 GB，3c 的磁盘告警要盖住它。
- 空载时的内存：Loki 39 Mi、Tempo 19 Mi、Prometheus 560 Mi（还在涨）、Grafana 422 Mi。server 节点 39%。
- 没有处理的：Tempo 的 jaeger 接收端关不掉（chart 的模板直接引用它，删掉渲染失败），多开了几个没人用的监听；Loki 的 chart 即使关了缓存也会建一个 `loki-memcached` 的 ServiceAccount。

### 第 4 步：Collector 和服务接入（2026-11-17）

`scripts/cluster/monitoring.sh collector`，`make monitoring-up` 现在依次装四样。values 在 `deploy/cluster/monitoring/otel-gateway.yaml`、`otel-agent.yaml`。管线由主会话定，Sonnet 写 values 并在本机用真的镜像校验（`validate` 加一次喂假日志的实测），主会话读过后在集群上安装。

- **版本核对过**：Collector contrib v0.165.0 是 GitHub 上最新的正式版（2026-11-07）；chart `opentelemetry-collector` 0.178.3 默认的是 0.163.2，用 `--set image.tag` 覆盖。
- **gateway**（Deployment，1 个副本，在 k3s-server 上）：收 OTLP，`memory_limiter`、`batch`，三条管线分别到 Tempo、Prometheus 的 OTLP 接收端、Loki 的 OTLP 接收端。**尾部采样、`spanmetrics`、`servicegraph` 这一步没有加**，留给第 6 步：先有一段“全量、不采样”的数据作对照，也让下面第 1 条的排查能看到全部链路。
- **agent**（DaemonSet，三个节点各一个）：只读本节点的容器日志发给 gateway。日志正文是 JSON 时取 `level` 作级别、`trace_id` 和 `span_id` 作日志记录自己的链路字段，正文保持原样，其余字段不提成属性；资源上没有 `service.name` 时用容器名补上（业务 Pod 的容器名就是服务名）。
- **和 3.3 的设计不一致，改了设计**：agent 不开 `kubeletstats`。kube-prometheus-stack 已经在抓 kubelet 和 cAdvisor（7.2 万条序列），再收一份是重复的。agent 因此不需要访问 Kubernetes API，没有 ClusterRole。
- chart 的几个行为（子 Agent 报的，渲染结果里核对过）：它把导出器和接收器的名字改写成新的（`otlp_grpc`、`otlp_http`、`file_log`），查日志和配置时按新名字找；values 里的列表是整体覆盖的，所以 JSON 的处理放在 `transform` 处理器里，没有去改预设生成的 `filelog` 操作符；开了检查点（重启后接着上次的位置读）之后 agent 以 root 运行并挂宿主机的 `/var/lib/otelcol`。
- **service chart**：`config` 的默认值里加了 `OTEL_EXPORTER_OTLP_ENDPOINT`（`http://otel-gateway.monitoring:4317`）和 `OTEL_METRIC_EXPORT_INTERVAL`（15 秒，SDK 默认 60 秒）；e2e 那一套的叠加层本来就覆盖这个地址，chaos、bench 本来就置空，都不受影响。Downward API 把命名空间、Pod 名、节点名放进 `OTEL_RESOURCE_ATTRIBUTES`，`service.version` 用镜像的 tag（镜像构建时不带 `.git`，进程自己读不到提交号）。出站排除名单加了 4317。
- 六个服务带着探针滚动了一遍：11120 个请求 0 失败。

**验证 1 到 3 的结果：**

1. 指标：六个服务都有，共约 4600 条序列（order 1682、inventory 944、orderindex 652、payment 536、gateway 400、cdc 15）。名字和本机一致（`rpc_server_call_duration_seconds_*`、`order_admissions_total`、`db_client_*`、`go_*` 等），每条带 `service_name`、`service_instance_id`、`k8s_pod_name`、`k8s_namespace_name`、`k8s_node_name`。滚动之后只有新 Pod 的序列在更新。
2. 链路：经入口下一单并支付，按 `X-Trace-Id` 在 Tempo 里查到下单链路 52 个 span（gateway、order、inventory，含 PRODUCER 和 CONSUMER）、支付链路 20 个 span（gateway、order、payment）；按订单号查到第三条 `payment.complete`。**订单号是整数属性，TraceQL 里不能加引号**：`{ span.app.order_id = 910471714539307015 }`，加了引号查不到也不报错。Tempo 的搜索接口带 `start`、`end` 时查不到刚写进去的数据，不带就有（原因没有查）。
3. 互相跳转：按 `trace_id` 在 Loki 里查到这条下单链路在 gateway、order、inventory 三个服务的 5 行日志（`{service_name=~".+"} | trace_id="..."`），Loki 的标签只有 `service_name`、`k8s_namespace_name`、`k8s_pod_name`、`k8s_container_name` 四个，`trace_id` 在结构化元数据里。Prometheus 里查得到 exemplar：SDK 的直方图上标签名是 `trace_id`，和数据源里配的一致；metrics-generator 生成的直方图上是 `traceID`，Grafana 里从这些图跳不过去（没有改）。**这三项是用接口查的，Grafana 界面上点一遍还没有做。**

metrics-generator 生成的指标验证了（第 3 步留下的）：`traces_service_graph_*`、`traces_spanmetrics_*` 都有，共约 1900 条序列。service graph 里把每个 MySQL 库、每个 Redis 节点都算成了一个“服务”。

**靠后端查出的第一个缺陷，已修：三分之一的 MySQL span 是假错误。** span 指标里 `mysql INSERT`、`mysql SELECT`、`mysql UPDATE` 各有三分之一是 `STATUS_CODE_ERROR`，错误信息是 `driver: skip fast-path; continue as if unimplemented`。原因：驱动没开 `interpolateParams` 时，带参数的语句先返回 `driver.ErrSkip`，`database/sql` 再改走“预处理、执行”；otelsql 默认把这个返回值记成了错误。1d 在本机看链路时没有按状态筛过，所以没发现。`pkg/mysqlx` 里打开了 `DisableErrSkip` 和 `WithDisableSkipErrMeasurement`，加了一个用例（修之前失败）。重新构建部署之后（镜像 tag `69030ff-dirty-20261117101712`，滚动期间探针 23140 个请求 0 失败），两分钟里 6.7 万个 MySQL span 没有一个是 Error，`target_info` 上有了 `service_version`。没有处理的：这样的语句仍然是三个 span（被跳过的那次、预处理、执行），otelsql 没有只去掉第一个的开关。

类似但没有改的：go-redis 跑 Lua 脚本先发 `EVALSHA`，节点上没有这个脚本时返回 `NOSCRIPT` 再改发 `EVAL`，那个 `evalsha` span 被标成 Error。每个脚本在每个 Redis 节点上只出现一次（这次滚动之后共 4 个）。

**量到的数**（服务接上之后约 10 分钟，只有探针的流量，不代表稳态）：

- 序列总数 16.5 万（接之前 13.2 万）：业务服务 4600、metrics-generator 1900、Collector 自己 200，其余的增量来自新 Pod。
- 内存：Prometheus 690 Mi、Grafana 434 Mi、Tempo 118 Mi、Loki 110 Mi、gateway 84 Mi、agent 71 到 80 Mi。server 节点 36%，两个 agent 节点 27%、29%。
- etcd 的 WAL fsync p99 是 15.9 毫秒，回到了 2b 记的范围（装完时的 29.6 毫秒是启动期间的）。仍然按原计划空转一天后再量一次。
- Collector 自己的计数：拒收 0、发送失败 0。

没有处理的：agent 的 Pod 名是 `otel-agent-agent-*`（chart 在名字后面加模式名）；Loki 的结构化元数据里有 `k8s_pod_uid`、`k8s_container_restart_count` 这类用不上的；agent 的检查点在 gateway 不可用时是否真的不前进，没有验证（第 5 节的验证 5 一起做）；滚动前旧的一个 gateway Pod 显示重启过 26 次（最近一次在滚动前 73 分钟），Pod 已经换掉，原因没有查。

### 第 1 条：“新 order Pod 一就绪就 503”查清了原因，没有改代码，等主人定（2026-11-17）

复现：`scripts/cluster/probe.py --workers 120 --rate 3 --duration 240`（和 2c 同样的压法，811 请求/秒）。gateway 和 order 都从 2 扩到 4。195240 个请求里 2 个失败，都是查订单返回 `503 STOCK_GATE_UNAVAILABLE`，在 10:38:20.28；第 4 个 order Pod 是 10:38:18 就绪的。现象和 2c 一致（2c 是 14 个）。

**2c 的“冷连接池”推断不成立。** 证据：

- 两条失败的链路都落在新 Pod 上，order 一侧只用了 3.5 和 10.9 毫秒就答了 `FAILED_PRECONDITION`，链路里**没有读闸门的 Redis span**，只有读缓存和读订单行。也就是根本没去读闸门，谈不上“撞上 300 毫秒时限”。
- 新 Pod 的两个 Redis 连接池：等待 1 次，超时 0 次。闸门的熔断器在四个 Pod 上都没有打开过。order 容器的 CPU 每个 Pod 约 0.2 核，没有被挤。

**实际的原因是三件事叠在一起：**

1. 展示读读闸门有并发名额（`READ_GATE_MAX_CONCURRENCY`，默认 16，1d 的 8.18 为了不让查询占满闸门的连接池加的）。名额满了就不读闸门，只读订单行。
2. 名额满了、订单行又不在库里时，`fetch` 分不清订单是在排队还是不存在，答 `503 STOCK_GATE_UNAVAILABLE`（`errHoldPending`）。这是 1d 有意的行为，有用例（`TestDisplayReadGateLimitDoesNotTripBreaker`）。
3. 名额被占满**不是新 Pod 才有的**：这一轮里四个 Pod 都有（`order_bulkhead_rejected_total{name="read_gate"}`：11、6、4、6，共 27 次），最早的 3 次在 10:36:15，那时还没有开始扩容。27 次里只有 2 次变成了 503，因为其余的订单已经落库，读订单行就答了。扩容之后的几秒里变成 503 的机会多：消费组重新分配分区，落库会晚一点。两个失败的订单从受理到开始落库隔了 0.6 秒和 1.0 秒（其中一个的落库正是新 Pod 在 10:38:19.45 拿到分区之后做的），查询正好落在这段时间里。

所以它不是“新 Pod 冷”，而是“查询的突发超过 16 个名额”加“扩容时落库晚了一秒”。没有量到的：16 个名额为什么会满。一次读闸门不到 1 毫秒，按平均到达率远远占不满；推断是探针的 120 个 worker 按同一个节拍发请求，一个 Pod 在同一毫秒里收到二三十个查询（`order_bulkhead_in_flight` 15 秒采一次，采到过 13）。真实的客户端不会这么整齐，3b 用 k6 压的时候要再看。

**可选的做法（没有做，涉及 1d 定过的行为，由主人定）：**

- 名额满了、订单行又不在时答 `429 READ_BUSY`，而不是 `503 STOCK_GATE_UNAVAILABLE`。这时闸门没有坏，降级也没有发生，是查询太多；`READ_BUSY` 本来就是“展示读拿不到名额”的原因码，订单库的名额满了答的就是它。改动在 `fetch` 的一个分支，加上改那个用例。**建议做这个**：现在的原因码会让人去查 Redis，这一次就查错了方向。
- 把 `READ_GATE_MAX_CONCURRENCY` 调大（比如 32 或 64，连接池是 128）。读闸门是亚毫秒的操作，16 对突发偏紧。要不要调、调到多少，等 3b 有了不那么整齐的流量再定。
- 不做的：就绪前预热连接池（已经是热的）、Envoy 的慢启动（新 Pod 并不慢）。

对第 7 步（按 QPS 的 HPA）和 3b 的影响：这不是扩容本身的缺陷，不挡这两步；但压测时会看到少量这样的 503，要认得它。

**主人定了按建议改（2026-11-17），已改并验证。** 名额满了、订单行又不在时 `fetch` 答 `429 READ_BUSY`；`TestDisplayReadGateLimitDoesNotTripBreaker` 和接口规范里 `READ_BUSY` 的说明跟着改了。部署后同样的压法再跑一次：194760 个请求 0 失败，gateway 和 order 同样从 2 扩到 4；期间名额仍然被占满过（新的四个 Pod 上 14、11、3、0 次），现在这些要么读订单行答了，要么是 429。名额的大小没有动。

### 第 5 步的前两项：中间件的 exporter 和 pprof（2026-11-17）

**exporter**（Sonnet 写清单和脚本、在本机用 docker 实测，主会话读过后安装）：`scripts/cluster/monitoring.sh exporters`（已并入 `make monitoring-up`），Kafka 的部分在 `scripts/cluster/middleware.sh kafka`。

- 版本核对过：mysqld_exporter v0.21.0、redis_exporter v1.97.0，都是 GitHub 上最新的正式版。
- MySQL、Redis 各一个 Deployment，多目标模式，清单是手写的（`deploy/cluster/monitoring/mysqld-exporter.yaml`、`redis-exporter.yaml`），抓取对象用 operator 的 `ScrapeConfig`。目标是每个 Pod（5 个 MySQL、6 个 Redis Cluster 节点、2 个闸门 Redis、3 个 Sentinel），`instance` 标签是 Pod 名，`cluster` 标签是实例名。**目标写死在清单里，加实例时要加一行。**
- MySQL 的监控账号 `exporter` 由脚本在三个主库上建（复制到从库），密码随机生成放在 Secret `mysql-exporter-auth`；**只给了 `REPLICATION CLIENT`**，没有给官方文档建议的 `PROCESS` 和 `SELECT ON *.*`：现在开的三个采集器（全局状态、全局变量、复制状态）用不到，而后者能读全部业务数据。
- **mysqld_exporter 在 MySQL 9.7.4 上能用**（1.3 节没有查证的那一项）。指标的前缀还是 `mysql_slave_status_`，但字段是新名字：延迟是 `mysql_slave_status_seconds_behind_source`，线程是 `replica_io_running`、`replica_sql_running`。写大盘和告警时按这个名字。
- **exporter 连不上数据库时 `up` 仍然是 1**（它自己活着），要看 `mysql_up`、`redis_up`。`monitoring.sh status` 加了这一项，3c 的告警也要按它写。
- Kafka：CR 里加了 `metricsConfig`（JMX 的规则在 `deploy/cluster/kafka/kafka-metrics.yaml`，取自 Strimzi 1.3.1 的官方例子，去掉了两条标签值不是有限枚举的）和 `kafkaExporter`。消费延迟是 `kafka_consumergroup_lag`。`order.order-close-delay` 这个组的延迟有 1.7 万，是延迟关单的机制本身（消息到期前不提交位点，ADR-0009），不是积压；大盘和告警要把它单独对待。
- 序列：三样合计 1.5 万条（Kafka 4700）。
- **装的时候出的两件事：**
  - redis_exporter 第一次起不来：清单里的探针打 `/healthz`，这个版本返回 404，容器被反复重启。改成只探端口。子 Agent 在本机只测了 `/scrape`，没有测探针的路径。
  - **滚动重启 broker 期间有 31 个下单失败**（探针 90 请求/秒，3 个 broker 一次一个，共约 2 分钟；29 个 `503 upstream request timeout`、2 个 504，分布在 59 秒里，下单的 p99 到了 3 秒）。查询和查库存没有受影响。原因没有查：看起来是分区的 leader 换人时生产者的发送等过了这一跳的时限。2b 滚动 broker 时上面还没有服务，所以这是第一次看到。**记给阶段 4（“停掉 broker”的实验）**；以后改 JMX 规则也会触发同样的滚动。

**pprof**（Opus 在独立的 worktree 里写，主会话读过 diff 后合入，提交 `c79f81c`）：

- `pkg/app` 加了 `ListenDebug`、`DebugMux`、`WithDebug`。环境变量 `DEBUG_ADDR` 设了才监听，chart 的默认值是 `127.0.0.1:6060`。**六个服务统一用一个单独的调试端口，不挂在健康检查的端口上**：那个端口在 Service 里，集群里任何 Pod 都连得到，而 `profile`、`trace` 会让进程按调用方要求的时长采样；gateway 的 8080 更是对外的。只监听回环地址，所以只能 `kubectl port-forward`。
- 常开而不是要用时再加：改环境变量会重启 Pod，想看的泄漏现场就没了。
- 调试端口最后停、而且停的时候直接断开连接：停止卡住时还能取 goroutine 的现场；一个正在跑的 30 秒 profile 不会拖住停止。
- `ListenHTTP` 现在拒绝空的 handler（空的会落到 `http.DefaultServeMux`，而导入 `net/http/pprof` 就会往那上面注册）。
- 集群上验证过：`port-forward` 到 order 和 gateway 的 6060 取到了 goroutine 的现场；经入口访问 `/debug/pprof/` 是 404；集群里别的 Pod 访问 order 的 Service 是 404、直接连 Pod 的 6060 连不上。部署期间探针 13004 个请求 0 失败。
- 没有做的：block 和 mutex 的剖析默认不采样，要用得改代码或加一个运行时的开关。

全量 `make lint test` 通过（`tmp/lint-test-3a-step5.log`）。集群上六个服务的镜像 tag 是 `c79f81c-dirty-20261117110013`。
