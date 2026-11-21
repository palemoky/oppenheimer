# ADR-0030：可观测后端的部署：两层 Collector、单体后端、本地盘、放在 server 节点

- 状态：已采纳
- 日期：2026-11-17

## 背景

埋点在 1d 做完了（ADR-0023）：服务用 OTLP 推链路和指标，日志是标准输出上带 trace id 的 JSON。后端选型也定了：Prometheus、Loki、Tempo、Grafana（ADR-0004、ADR-0024）。没有定的是它们在集群上怎么部署：Collector 的形态、数据放哪、放在哪个节点、采样、收多少。集群只有三个节点，内存和节点数都决定了不能照搬“每个组件都高可用”的生产布局。

设计和调研在设计文档 [3a-observability.md](../design/3a-observability.md)。

## 决定

主人 2026-11-17 确认，十项都按设计文档第 2 节的建议：

1. **服务的指标经 Collector 用 OTLP 写进 Prometheus 的 OTLP 接收端**，和本机的 `otel-lgtm` 同一条路。接收端在选定的 Prometheus 版本里不可用或不稳时，退到“Collector 开端点、Prometheus 来抓”。节点、Kubernetes 对象、中间件、Istio 的指标由 Prometheus 直接抓，不绕 Collector。
2. **Collector 分两层**：每个节点一个 agent（DaemonSet，收容器日志和 kubelet 的指标），一个 gateway（Deployment，1 个副本，收服务的 OTLP、做尾部采样、分发）。
3. **日志用 Collector 的 `filelog` 接收器收**，经 OTLP 写进 Loki。不部署 Alloy。
4. **Prometheus、Loki、Tempo 的数据放节点本地盘**（`local-path`），Loki 和 Tempo 用单体模式加文件系统存储。保留期先定 15 天、7 天、3 天。
5. **这一套尽量放在 k3s-server 上，三台虚拟机的内存从 10、16、16 改成 14、14、14 GiB。**
6. **采样两级**：SDK 头部按比例（平时 100%，压测时约 10%），gateway 尾部采样（出错的、超过 500 毫秒的全留，其余留 10%）。
7. **Istio 边车只抓标准指标并裁标签**，不出 span，不开访问日志。
8. **中间件的 exporter 是独立的 Deployment**（mysqld_exporter、redis_exporter 各一个，多目标模式）；Kafka 用 Strimzi 的指标配置加自带的 Kafka Exporter。
9. **只开 pprof 端点**，不部署 Pyroscope。
10. **按 QPS 的 HPA 用 KEDA。**

## 理由

- **尾部采样决定了 gateway 只能有一个副本。** “这条链路要不要留”得等它的 span 到齐才能判断，一个订单的 span 来自三个节点上的 Pod，必须汇到同一个进程。多副本要在前面再加一层按 trace id 分流的 Collector，这个规模不值得。代价是 gateway 重启时丢几秒数据。
- **日志必须在节点上收，OTLP 可以集中收**，所以是两层而不是一层：agent 只做离不开节点的事，其余都在 gateway。
- **监控不能和被它看的东西一起倒。** 阶段 4 要关 agent 节点。server 关了控制面也没了，本来就不在实验里，所以监控放在它上面。它原来只有 10 GiB、剩约 6 GiB，这一套估计要 4 到 5 GiB，因此挪内存。
- **本地盘**：三样数据都是可以丢的（丢了只是看不到历史），NAS 的同步写只有 20 MB/s，Prometheus 不支持 NFS，MinIO 社区版已经归档。为“数据不跟节点走”多维护一个对象存储不值。
- **指标走 OTLP 直写**：指标名和标签的转换与本机一致，1d 写过的查询原样可用；少一个“Collector 攒数、Prometheus 来抓”的中间状态。
- **头部加尾部**：只做尾部要 SDK 全量导出，1d 量过压测时那是约 7% 的吞吐；只做头部则出错的那条可能恰好没采到。两级叠起来，平时靠尾部保证出错和慢的都在，压测时靠头部压住数据量。
- **exporter 独立部署**：做成边车要改两个 chart 并把 MySQL、Redis 全部滚动一遍，而 exporter 自己出问题时不该牵连数据库的 Pod。
- **KEDA 而不是 prometheus-adapter**：后者最近一次发布是 2024 年 5 月。KEDA 以后还能按 Kafka 的消费延迟伸缩消费者。

## 放弃了什么

- 监控数据的高可用和长期保存（双副本的 Prometheus、Thanos、分布式模式的 Loki 和 Tempo、对象存储）。server 节点的盘坏了，历史数据就没了。
- 边车视角的链路和访问日志：边车加出来的延迟只能从指标看，看不到单个请求在边车上花了多久。
- 持续剖析：出事那一刻的火焰图要靠当时手工抓，事后没有。
- 两个 agent 节点各少 2 GiB 内存。

## 备选方案

每一项的另一种做法在设计文档第 2 节的表里。几个差点选了的：

- **加第四台虚拟机专门放监控**：隔离最干净。要从两个 agent 各拿走 4 GiB 和 2 个 vCPU，压测时被测的一方变小。
- **Grafana Alloy 收日志**：和 Loki 配合的文档最多。多一个组件和一套配置语言，而 ADR-0004 定的就是由 Collector 分发三种信号。
- **prometheus-adapter**：概念上最贴近“HPA 的自定义指标 API”，维护状态存疑。

## 后果

- 改虚拟机内存要逐台重启，每台约一分钟（2026-11-17 做过一次）。`hardware.md` 的虚拟机表和预算表要跟着改。
- **etcd 和 Prometheus、Loki、Tempo 在同一个节点上抢磁盘。** 2b 记过空集群上 etcd 的 fsync p99 已经在 8 到 16 毫秒。装完之后这是第一个要看的图；变坏了就要把写得最多的那个挪走。
- server 节点成了“控制面加监控”的单点。它不可用时既不能操作集群也看不到指标，只剩节点上的日志文件。
- 几项建议依赖没有查证的事实，安装时先验证，不成立就按上面写的退路走并在这里补记：Prometheus 的 OTLP 接收端在选定版本里的状态；Tempo 的 metrics-generator 在单体模式下能不能用；KEDA 没有在 Kubernetes 1.37 上测过；mysqld_exporter 对 MySQL 9.x 的支持。
- 推上来的指标没有 `up`。服务挂了，它的序列只是不再更新，告警要按“多久没有新点”来写（3c）。
- Collector 成了三种信号共同的故障点，它自己的指标要被抓、要有大盘，3c 里要有告警；也是阶段 4 的一个实验对象。

## 补充：KEDA 装上之后（2026-11-17）

- KEDA 2.21.1 在 Kubernetes 1.37.2 上可用，没有走退路（prometheus-adapter）。
- **请求速率和 CPU 两个触发器并存，不是只留请求速率。** HPA 对多个指标取最大的期望副本数。留 CPU 有两个理由：请求速率数不到异步的工作（order 的落库）；Prometheus 连不上时请求速率这一项取不到数，CPU 这一项还能扩容（实测）。代价是扩容的原因要看 HPA 的状态才知道是哪一项。
- **指标后端不可用时的行为是“不缩、取得到的还能扩”，没有配 `fallback`。** Prometheus 是单副本，这一条等于把“监控挂了”对伸缩的影响限制在“暂时不缩容”。
- 多了一个要按顺序装的依赖：KEDA 要先于业务服务（chart 里有它的 CRD 对象）。
- 实测的数和从旧 HPA 切换的坑在设计文档第 8 节“第 7 步”。

## 补充：和实现不一致的两处（2026-11-17）

- 第 2 项写 agent 收“容器日志和 kubelet 的指标”。实现时 agent 只读容器日志，没有开 `kubeletstats`：kubelet 和 cAdvisor 的指标 kube-prometheus-stack 已经在抓，再收一份是重复的（设计文档第 8 节“第 4 步”）。
- 尾部采样（第 6 项）有一个当时没有想到的代价：指标上的 exemplar 大半指向被采样丢掉的链路（实测 40 个里查得到 14 个）。慢的和出错的不受影响。见设计文档第 8 节最后一段。
