# 3a 笔记：把可观测的后端装上集群，六个服务接上去

这份笔记整理 3a 里值得在面试中讲的点，按主题组织，不按开发步骤。每个主题尽量按“原理（为什么是这样）、这个项目里怎么做的和量到的数、面试时怎么讲”来写。设计全文见 [design/3a-observability.md](../design/3a-observability.md)（下文简称“设计”，引用章节号；“设计 8”指第 8 节“实现中的发现和量到的数”，其中按步骤分的小节写成“第 6 步”这样），取舍见 [ADR-0030](../adr/0030-observability-backends.md)。埋点本身（进程怎么产生链路、指标、日志）是 1d 做的，在 [design/1d-protection.md](../design/1d-protection.md) 第 8 节和 ADR-0023，这份笔记只讲“收、存、看”。前文：网格的指标和边车见 [notes/2d-mesh.md](2d-mesh.md)。

3a 做的事：在 k3s 集群上装 kube-prometheus-stack、Loki、Tempo；OpenTelemetry Collector 分 agent 和 gateway 两层；六个服务经 OTLP 接上去；MySQL、Redis、Kafka 的 exporter；Istio 边车的指标；Grafana 大盘；gateway 上的尾部采样；gateway 和 order 按请求速率伸缩（KEDA）；以及停 Collector、停复制、停消费者三个故障注入的验证。压测是 3b，告警和 SLO 是 3c，诊断 Agent 是 3d，不在这份笔记里。

**关于“通用知识”**：文中标注“（通用知识）”的段落，是为了把原理讲清楚而补的 Prometheus、OpenTelemetry、Kubernetes、Loki 的常识，设计文档和 ADR 里没有出处，**面试前要对照官方文档核对**。没有标注的内容，事实来自设计、ADR、阶段计划或清单文件。材料里标了“没有查”“推断”“没有验证”“没有实测”的地方，这里保持同样的说法，不要在面试时说成确定的结论。数字都是 2026-11-17 在这个集群上量的，照抄，没有换算；“探针”指 `scripts/cluster/probe.py`，经入口持续发请求。

## 0. 考点与小节的对照

阶段计划“面试考点”里和 3a 有关的一共 19 条（后面“指标、日志、链路各自解决什么问题”以及 Prometheus 的数据模型、PromQL 那几条是通用考点，放在 4 和 8 里顺带讲）。对应关系如下：

| # | 计划里的考点（缩写） | 小节 |
|---|---|---|
| 1 | OpenTelemetry 的架构（API、SDK、Collector、OTLP）与上下文传播 | 1 |
| 2 | Collector 的 agent 加 gateway 两层各自做什么；`memory_limiter` 为什么放最前 | 1 |
| 3 | 推和拉两种采集方式怎么发现“对方挂了” | 2 |
| 4 | 多目标模式的 exporter 里 `up` 和 `mysql_up` 的区别 | 2 |
| 5 | 日志的 `trace_id` 为什么不做 Loki 的标签（结构化元数据） | 3、8 |
| 6 | pprof 端点为什么不能挂在对外或 Service 暴露的端口上 | 13 |
| 7 | 原因码答错会把排查引向哪里（`READ_BUSY`） | 6 |
| 8 | 头部采样和尾部采样各自在哪里做决定、各自的代价 | 5 |
| 9 | 尾部采样之后从 span 算出来的指标为什么是偏的 | 5 |
| 10 | 网格指标的标签在源头裁和在 Prometheus 里裁的区别 | 7 |
| 11 | 埋点把正常的返回值记成错误（`driver.ErrSkip`） | 6 |
| 12 | Collector 停机时三种信号各丢什么、为什么 | 10 |
| 13 | `batch` 为什么切断反压、批次没有上限撞上哪些限额（Loki 的突发量、gRPC 的 4 MiB） | 10 |
| 14 | 复制停了时 `Seconds_Behind_Source` 是 `NULL`，按延迟写的告警不响 | 11 |
| 15 | HPA 的自定义指标怎么取到（Resource、Pods、External，聚合 API，KEDA 只提供数） | 9 |
| 16 | HPA 多个指标取最大；一个取不到时不缩容但还能扩 | 9 |
| 17 | 按请求速率伸缩为什么不比按 CPU 快、好在哪里 | 9 |
| 18 | 指标后端挂了伸缩会怎样、怎么发现（`READY` 看不出来）；同一个 Deployment 不能有两个 HPA | 9 |
| 19 | 指标、日志、链路各自解决什么问题；互相跳转 | 4 |

另外：对数和 exemplar 在 14a；基数和序列数在 8，保留期和磁盘、入口访问日志在 12，单副本丢了什么在 13，从空集群重建时的 `--set` 坑在 14，没做的和没弄清的在 15，材料互相对不上的地方在 16，速记在 17。

---

## 1. OpenTelemetry 的架构，Collector 的两层

**问**：OpenTelemetry 是什么、由哪几部分组成？Collector 为什么分两层？`memory_limiter` 为什么放最前？

**原理（通用知识）**：

- **API**：埋点代码依赖的接口（建 span、记指标、写日志），本身不做任何事；没有装 SDK 时调用是空操作。**SDK**：API 的实现，负责采样、攒批、加资源属性（`service.name` 等）、导出。把两者分开，是为了让库的作者只依赖 API，由应用决定装不装 SDK、导出到哪。
- **OTLP**：OpenTelemetry 自己的传输协议，有 gRPC 和 HTTP 两种承载，链路、指标、日志三种信号同一套。
- **Collector**：独立进程，由接收器（receiver）、处理器（processor）、导出器（exporter）按管线（pipeline）串起来，每种信号一条或几条管线。它把“发给谁、采不采样、批多大”从应用里拿出来，应用只需要知道一个地址。
- **上下文传播**：跨进程时靠请求头（W3C 的 `traceparent`）带 trace id 和 span id。这个项目里，Istio 边车不做链路，只原样转发 `traceparent`（设计 1.1），所以链路是进程自己接起来的。

**这个项目里怎么做的**（设计 2 第 2 项、3.3，ADR-0030 第 2 项）：

| | agent | gateway |
|---|---|---|
| 形态 | DaemonSet，三个节点各一个 | Deployment，1 个副本，在 k3s-server 上 |
| 收什么 | 本节点 `/var/log/pods/` 下的容器日志（`filelog` 接收器） | 服务发来的 OTLP（4317），agent 转来的日志 |
| 做什么 | 解析 containerd 的行格式，再解析我们的 JSON，取出级别、`trace_id`、`span_id`；资源上没有 `service.name` 时用容器名补 | `memory_limiter`、尾部采样、`batch`，分发到 Tempo、Prometheus 的 OTLP 接收端、Loki 的 OTLP 接收端 |
| 为什么在这一层 | 日志文件在节点上，只有节点上的进程读得到 | 尾部采样要一条链路的全部 span 到同一个进程；一个订单的 span 来自三个节点上的 Pod |

要点是“agent 只做离不开节点的事，其余都放 gateway”（ADR-0030 理由）。只用 DaemonSet 做不了尾部采样，只用 Deployment 收不了节点上的日志文件。

- **没有 Istio 边车，也不过边车**：Collector 不注入边车；服务到 Collector 的 4317 端口加进了出站排除名单，和 MySQL、Redis、Kafka 一样（设计 3.1），否则可观测的流量也要过边车并要给它写访问控制。
- **gateway 的管线顺序**（`deploy/cluster/monitoring/otel-gateway.yaml`）：链路是 `memory_limiter`、`tail_sampling`、`batch`；指标是 `memory_limiter`、`batch`；日志是 `memory_limiter`、`batch/logs`（单独的 batch，每批最多 2000 条，原因见 10）。`memory_limiter` 配的是 `check_interval: 1s`、`limit_percentage: 80`、`spike_limit_percentage: 20`。
- **尾部采样放在 `memory_limiter` 之后、`batch` 之前**：它要看到一条链路的全部 span，而 `batch` 会把它们拆进不同的批（清单注释）。
- agent 没有开 `kubeletstats`：设计 3.3 原来计划用它抓本节点 Pod 的用量，实现时去掉了，因为 kube-prometheus-stack 已经在抓 kubelet 和 cAdvisor（7.2 万条序列），再收一份是重复的；agent 因此不需要访问 Kubernetes API，没有 ClusterRole（设计 8 第 4 步）。
- 版本：Collector contrib v0.165.0；官方 chart `opentelemetry-collector` 0.178.3 默认带的是 0.163.2，用 `--set image.tag` 覆盖（设计 8 第 4 步）。`filelog` 接收器的稳定级别是 beta（设计 2 第 3 项）。

**`memory_limiter` 为什么放最前（通用知识）**：它是保命的闸门。超过内存上限时它直接拒收新数据，上游（SDK 的队列、agent 的队列）因此背压或丢弃，而不是让 Collector 被撑到 OOM。它放在最前，是为了在数据还没有被后面的处理器复制、攒批、放进采样窗口之前就拒收；放在后面的话，前面的处理器已经把内存占了。设计 3.3 的说法是“超过上限就拒收，SDK 的队列满了丢 span，不会把 Collector 撑死”。

**量到的和限定**：

- gateway 的内存：尾部采样的那一轮峰值 375 Mi，不采样 277 Mi（设计 8 第 6 步）；requests 512 Mi、limits 1024 Mi。尾部采样要把 10 秒窗口里的 span 攒在内存里，405 请求/秒时约 4 万个 span，所以内存和吞吐成正比，3b 压到几千请求/秒时要再量。
- Collector 0.165 没有 `memory_limiter` 的拒绝数指标，只能从接收端的拒收间接看（设计 8 大盘一节）。
- 没有验证 `memory_limiter` 真的在压力下拒收（没有压到那个量级），上面的行为是设计和清单注释里的预期。

**面试时怎么讲**：API 是接口、SDK 是实现、OTLP 是线上的协议、Collector 是把采样和分发从应用里拿出来的中间层。我们分两层，是因为日志只能在节点上读、尾部采样只能在汇集的地方做；`memory_limiter` 放最前，是让它在数据占内存之前就能拒收。然后说限定：gateway 只有一个副本，多副本的尾部采样要在前面加按 trace id 分流的一层，我们没做。

---

## 2. 推和拉，`up` 和 `mysql_up`

**问**：指标采集的推和拉有什么区别？对方挂了怎么发现？

**这个项目里的分工**（设计 3.2）：

- **服务自己的指标和链路：推。** 服务用 OTLP 推给 Collector（ADR-0004）；Collector 再用 OTLP 直接写进 Prometheus 的 OTLP 接收端（ADR-0030 第 1 项），和本机的 `otel-lgtm` 同一条路，所以指标名和标签的转换与本机一致，1d 写过的 PromQL 原样可用。
- **别人的指标：拉。** 节点、Kubernetes 对象、中间件、Istio 都是 Prometheus 格式的端点，由 Prometheus 按 `ServiceMonitor`、`PodMonitor`、`ScrapeConfig` 直接抓，不绕 Collector。
- 前提“Prometheus 的 OTLP 接收端够用”在设计里原本没有查证；设计 8 第 2 步确认：Prometheus 换成 LTS 的 v3.13.5，它有 `--web.enable-otlp-receiver`，chart 里是 `enableOTLPReceiver` 一个开关，不在实验特性的名单里。

**原理**：

- 拉的一方有 `up`：每次抓取 Prometheus 自己记一条 `up`，目标挂了就是 0（通用知识）。
- **推上来的指标没有 `up`**（ADR-0030 后果）。服务挂了，它的序列只是不再更新，要靠“这个序列多久没有新点”来发现，告警要按这个写（3c 做）。
- 推的另一个好处与代价（通用知识）：推可以穿过网络边界、不需要对方开端点，代价是接收端不知道“应该有哪些发送者”，缺失只能从“没有来”推断。
- SDK 导出的是累计值，导出间隔配成 15 秒（`OTEL_METRIC_EXPORT_INTERVAL`，SDK 默认 60 秒，设计 8 第 4 步）。

**滚动之后旧序列怎么办**：验证 1 里滚动重启之后只有新 Pod 的序列在更新，旧 Pod 的序列停止（设计 8 第 4 步）。这正是推送模型下“发现消失”的方式：没有新点。

**多目标 exporter 的 `up` 和 `mysql_up`**（设计 8 第 5 步前两项）：

- MySQL、Redis 各一个 Deployment（`mysqld_exporter` v0.21.0、`redis_exporter` v1.97.0），多目标模式：一个 exporter 实例用 `ScrapeConfig` 抓所有目标（5 个 MySQL Pod、6 个 Redis Cluster 节点、2 个闸门 Redis、3 个 Sentinel），`instance` 标签是 Pod 名，`cluster` 标签是实例名。**目标写死在清单里，加实例要加一行。**
- **exporter 连不上数据库时 `up` 仍然是 1**，因为 `up` 只表示“Prometheus 抓到了 exporter”，exporter 自己是活的。要看 `mysql_up`、`redis_up`（exporter 报告的“我连得上目标”）。所以 3c 的告警要按后者写；`monitoring.sh status` 已经加了这一项。
- 采用独立 Deployment 而不是边车容器，理由：做成边车要改两个 chart、MySQL 和 Redis 全部滚动一遍，而 exporter 出问题时不该牵连数据库的 Pod（ADR-0030）。代价是上面“目标写死”。
- 监控账号 `exporter` 只给了 `REPLICATION CLIENT`，没有给官方文档建议的 `PROCESS` 和 `SELECT ON *.*`：现在开的三个采集器（全局状态、全局变量、复制状态）用不到，而后者能读全部业务数据。
- mysqld_exporter v0.21.0 在 MySQL 9.7.4 上能用（设计 1.3 原来没有查证这一项）；指标前缀还是 `mysql_slave_status_`，但字段是新名字：`mysql_slave_status_seconds_behind_source`、`replica_io_running`、`replica_sql_running`。
- Kafka 不用独立 exporter：Strimzi 的 CR 里加了 `metricsConfig`（JMX 规则取自 Strimzi 1.3.1 的官方例子，去掉了两条标签值不是有限枚举的）和 `kafkaExporter`；消费延迟是 `kafka_consumergroup_lag`。

**装的时候出的一件事**：redis_exporter 第一次起不来，清单里的探针打 `/healthz`，这个版本返回 404，容器被反复重启，改成只探端口。子 Agent 在本机只测了 `/scrape`，没有测探针的路径（设计 8 第 5 步）。教训：验收要测“清单里写的每一条路径”，不只是主功能。

**面试时怎么讲**：拉有 `up`，推没有，推的发现靠“序列多久没新点”，所以推的指标告警要写成缺失检测。多目标 exporter 的 `up` 说的是“exporter 活着”，`mysql_up` 才说“数据库连得上”，只看 `up` 会在数据库挂了的时候一切正常。

---

## 3. 日志：标准输出加采集，`trace_id` 不做标签

**问**：日志为什么这样收？Loki 的标签里为什么没有 `trace_id`？

**为什么是“标准输出加采集”**（设计 7、设计 3.4，1d 的 8.9）：

- 服务的日志已经是标准输出上的 JSON，带 `trace_id`，不改代码。设计明确不做“日志经 OTLP 从 SDK 直接上报”（设计 7）。
- 通用知识补充：写标准输出让进程不依赖日志后端，后端挂了进程照常；日志落在节点磁盘上，采集器读文件，可以停下来等、从检查点接着读。这一点在 10 里是关键：日志三种信号里是唯一“收集端挂了数据还在磁盘上”的。
- agent 用 `filelog` 读 `/var/log/pods/`，开了检查点（重启后接着上次的位置读），因此 agent 以 root 运行并挂宿主机的 `/var/lib/otelcol`（设计 8 第 4 步）。

**Loki 的标签只放低基数的**（设计 3.4）：命名空间、Pod、容器、服务名、级别。实测（验证 3）Loki 里的标签只有 `service_name`、`k8s_namespace_name`、`k8s_pod_name`、`k8s_container_name` 四个；`trace_id` 在结构化元数据里。

**原理（通用知识）**：Loki 按标签组合划分“流”，每个流单独攒块、单独建索引条目；标签值越多，流越多，索引膨胀、块变小、查询变慢。`trace_id` 每个请求一个值，做标签等于每个请求一个流。结构化元数据是随日志行存的键值对，不进索引的标签集合、不产生新流，但仍然可以过滤（本项目查法：`{service_name=~".+"} | trace_id="..."`）。这和指标标签的规则是同一个道理（设计 3.4，见 8）。

**这个项目里量到的**：

- 按 `trace_id` 在 Loki 里查到一条下单链路在 gateway、order、inventory 三个服务的 5 行日志（设计 8 第 4 步验证 3）。
- 没处理的：结构化元数据里有 `k8s_pod_uid`、`k8s_container_restart_count` 这类用不上的（设计 8 第 4 步）。
- 日志量：4 小时里入口 Envoy 的访问日志占 60%，见 12。

**面试时怎么讲**：Loki 的标签决定流的个数，所以只放有限的几个维度；`trace_id` 这种每请求一个值的东西放结构化元数据，按它查仍然快，但不产生新的流。和 Prometheus 的“标签只能是有限枚举”是同一条规则。

---

## 4. 指标、日志、链路怎么互相跳转，exemplar

**问**：三种信号各自解决什么？怎么从一个慢请求下钻？

**各自解决什么（通用知识）**：指标回答“出了没有、多严重”，数据量小、可聚合，但看不到单个请求；链路回答“这个请求的时间花在哪一跳”，是单个请求的因果；日志回答“这一跳里进程当时说了什么”，信息最全、体量最大。排查是从指标发现、用链路定位、用日志看细节。验收就是阶段计划那一句：任意一个慢请求，从大盘下钻到链路再到日志（设计 3.6）。

**这个项目里的四条跳转**（设计 3.6）：

| 方向 | 怎么跳 |
|---|---|
| 日志 → 链路 | Loki 数据源配“从 `trace_id` 字段跳到 Tempo” |
| 链路 → 日志 | Tempo 数据源配“按 trace id 和时间范围查 Loki” |
| 指标 → 链路 | exemplar |
| 链路 → 指标 | Tempo 数据源配“从 span 跳到这个服务的 RED 面板” |

**exemplar 是什么（通用知识）**：直方图的某个桶旁边附带一个样本点，带着当时那个请求的 trace id。图上点一个点，就能跳到一个落在这个桶里的具体请求的链路。Prometheus 要开 exemplar 存储才存。

**这个项目里的细节**：

- SDK 的直方图上，exemplar 的标签名是 `trace_id`，和数据源里配的一致；业务总览有 10 条、单服务有 17 条延迟查询开了 exemplar（设计 8 第 4 步、大盘一节）。
- **metrics-generator 生成的直方图上标签名是 `traceID`，和数据源里配的不一致，Grafana 里从这些图跳不过去（没有改）**（设计 8 第 4 步）。
- 互相跳转最初是按“Collector 把 `trace_id` 存成结构化元数据、`service.name` 成为标签 `service_name`”配的，设计 8 第 3 步写明要接上 Collector 之后才能验证。
- 验证的程度：验证 3 是用接口查的，设计 8 第 4 步写“界面上点一遍还没有做”；阶段计划里写的是五个大盘和“大盘 → 链路 → 日志”主人在浏览器里看过、没有发现问题。两处记录的时间点不同，以较晚的计划文件为准，但面试时不要说成“每条跳转都在界面上逐条点过”。
- Tempo 数据源上“从 span 跳到 RED 指标”的链接因尾部采样而偏，见 5。
- 一个订单是三条链路（1d 的 8.15：下单、支付、`payment.complete`），靠 `app.order_id` 关联；这是整数属性，**TraceQL 里不能加引号**：`{ span.app.order_id = 910471714539307015 }`，加了引号查不到也不报错（设计 8 第 4 步）。Tempo 的搜索接口带 `start`、`end` 时查不到刚写进去的数据，不带就有（原因没有查）。

**面试时怎么讲**：指标发现、链路定位、日志看细节；三者靠 trace id 串起来：日志里带 trace id、链路能按 trace id 查日志、直方图上挂 exemplar。exemplar 是桶旁边的一个样本点，带着那个请求的 trace id。限定要说：我们的两处 exemplar 标签名不一致（`trace_id` 和 `traceID`），生成的指标那一侧跳不过去。

---

## 5. 头部采样和尾部采样

**问**：两种采样各在哪里做决定？各自的代价？尾部采样之后从 span 算出来的指标为什么偏？

**原理**：

- **头部采样**：请求开始时（入口）掷骰子，决定这条链路留不留，之后每一跳跟着父 span 走。便宜，但决定时还不知道请求会不会出错、会不会慢，所以出错的那条和别的请求一样只有 10% 的机会留下来。
- **尾部采样**：等一条链路的 span 到齐再决定，可以按“出错”“慢”留。代价是（1）要有一个汇集全部 span 的地方（这里是 gateway，只能 1 个副本）；（2）SDK 要全量导出；（3）Collector 要把等待窗口里的 span 放在内存里。

**这个项目里的配置**（设计 2 第 6 项，`otel-gateway.yaml`）：SDK 仍然按比例做头部采样（平时 100%，压测时调到 10% 上下），gateway 再做尾部采样：`decision_wait: 10s`，三条策略是“或”的关系：出错的全留、超过 500 毫秒的全留、其余留 10%。`num_traces` 5 万、`expected_new_traces_per_sec` 1000。晚于 `decision_wait` 才到的 span（例如延迟关单的消费隔了 15 分钟）赶不上这次决定；我们的异步链路是用 link 关联的另一条链路，各自决定，不受影响（清单注释）。

**对比实验**（设计 8 第 6 步）：`probe.py --workers 60 --rate 3 --duration 150`（405 请求/秒），第 60 秒把 inventory 缩到 0 个副本、20 秒后恢复，三轮。头部 10% 那一轮只改 gateway 的 `OTEL_TRACES_SAMPLER=parentbased_traceidratio`、`OTEL_TRACES_SAMPLER_ARG=0.1`。

| | 不采样 | 头部 10% | 尾部（错的和慢的全留，其余 10%） |
|---|---|---|---|
| 探针看到的失败请求 | 5165 | 5682 | 5572 |
| Tempo 里带 503 的链路 | 5164 | 567 | 5572 |
| **出错的链路留下的比例** | 100% | **10%** | **100%** |
| Tempo 里 gateway 的链路总数 | 61019 | 6062 | 11336 |
| 服务发给 Collector 的 span | 72.0 万 | 9.0 万 | 68.4 万 |
| Collector 发给 Tempo 的 span | 72.1 万 | 9.0 万 | 9.0 万 |
| Collector gateway 的内存峰值 | 277 Mi | 275 Mi | 375 Mi |

**读法**：

1. 看“出错的链路留下的比例”这一行：头部 10% 留下的错误就是 10%，尾部一条没丢。这是实验要回答的问题。
2. 看“服务发给 Collector”和“Collector 发给 Tempo”两行：存进 Tempo 的量两种做法差不多（都是 9 万个 span）；尾部多花的是服务端仍然全量导出（68.4 万对 9.0 万）和 Collector 多约 100 Mi 内存。
3. 正常请求里尾部留下 5764 条，是 55448 条的 10.4%，和设定的 10% 对得上。
4. 对比表里的数是分母不同的，不能横着直接比总数：不采样那一轮的 61019 条链路里 5164 条是错的，尾部那一轮 11336 = 5572 条错的 + 5764 条正常的。
5. 内存和吞吐成正比，3b 压到几千请求/秒时要再量。1d 量过全量采样让落库吞吐低约 7%（设计 1.2），这是尾部采样让服务端付的代价。

**限定**：

- **“慢的全留”这一条没有验证到**：三轮里没有一条链路超过 500 毫秒（inventory 不在时请求是快速失败，不是变慢）；阶段 4 注入延迟时再看。
- Tempo 的内存在三轮里是 446、1114、1240 Mi，一路在涨，和采样方式对不上（不采样那轮最低）；没有查。后来的实测见 12（内存跟着写入量走）。

**尾部采样之后，从 span 算出来的指标为什么偏**（设计 3.5、8 第 6 步）：

- Tempo 的 metrics-generator（`traces_service_graph_*`、`traces_spanmetrics_*`，约 1900 条序列）按它收到的 span 现算。尾部采样把正常的只留十分之一、错的和慢的全留，所以它们里出错和慢的占比偏高，**不能再用它们算错误率和请求量**，只用来看拓扑。这是个选样偏差：留下的 span 不是总体的随机样本。
- 设计的决定是“先接受偏差，不在 Collector 里另算”。RED 一律用 SDK 的指标，大盘就是这样做的。要准确的 span 指标，得在 Collector 里、采样**之前**用 `spanmetrics` 和 `servicegraph` 连接器算，多一份内存和序列；现在没有要靠它回答的问题。
- 一个结论的后果：Tempo 数据源上“从 span 跳到 RED 面板”的链接因此是偏的，没有改。
- 另外 service graph 里把每个 MySQL 库、每个 Redis 节点都算成了一个“服务”（设计 8 第 4 步）。
- metrics-generator 在单体模式下能用，要在 `overrides.defaults` 里按租户打开处理器，WAL 默认在 `/tmp` 要挪到卷上，写 Prometheus 用 remote write 所以 Prometheus 开了 `enableRemoteWriteReceiver`（设计 8 第 3 步）。

**面试时怎么讲**：头部是请求一开始就掷骰子，便宜但出错的只留 10%；尾部等 span 到齐再决定，我们实测出错的 100% 留下、正常的 10.4%，代价是服务全量导出加 Collector 多 100 Mi 内存、gateway 只能一个副本。尾部之后 span 指标偏是因为留下的不是随机样本，所以 RED 用 SDK 的指标。要说限定：“慢的全留”这一条没有验证到。

---

## 6. 埋点的两个教训：把正常返回值记成错误，原因码答错

### 6.1 `driver.ErrSkip`：三分之一的 MySQL span 是假错误

**现象**（设计 8 第 4 步）：靠后端查出来的第一个缺陷。span 指标里 `mysql INSERT`、`mysql SELECT`、`mysql UPDATE` 各有三分之一是 `STATUS_CODE_ERROR`，错误信息是 `driver: skip fast-path; continue as if unimplemented`。

**原因**：驱动没开 `interpolateParams` 时，带参数的语句先返回 `driver.ErrSkip`，`database/sql` 看到它再改走“预处理、执行”；这是标准库里一个**正常的内部协议**，不是错误。otelsql 默认把这个返回值记成了错误。1d 在本机看链路时没有按状态筛过，所以没发现。

**修法**：`pkg/mysqlx` 里打开了 `DisableErrSkip` 和 `WithDisableSkipErrMeasurement`，加了一个用例（修之前失败）。重新构建部署后，两分钟里 6.7 万个 MySQL span 没有一个是 Error。

**没处理的**：这样的语句仍然是三个 span（被跳过的那次、预处理、执行），otelsql 没有只去掉第一个的开关。**类似但没有改的**：go-redis 跑 Lua 脚本先发 `EVALSHA`，节点上没有这个脚本时返回 `NOSCRIPT` 再改发 `EVAL`，那个 `evalsha` span 被标成 Error；每个脚本在每个 Redis 节点上只出现一次（这次滚动之后共 4 个）。

**为什么这是个考点**：项目规范“只有服务端故障才把 span 标成 Error，库存不足、限流、降级是正常结果”（AGENTS.md）。库和框架里的内部控制流返回值（`ErrSkip`、`NOSCRIPT`、`io.EOF` 这一类，通用知识）要在埋点那一层区分清楚，否则错误率的图被噪声占满，尾部采样的“出错的全留”也会把它们全留下来。这一条是“靠后端查出来”的：按状态筛过才发现。

**面试时怎么讲**：我在 Tempo 里按状态筛，发现三分之一的 MySQL span 是错误，错误信息是 `ErrSkip`。它是 `database/sql` 的快速路径协议，不是失败；埋点库默认把它记成错误。我关掉这个度量加了回归用例，修后两分钟 6.7 万个 span 没有 Error。同类的 `NOSCRIPT` 我知道有但没处理，因为每个脚本每个节点只出现一次。

### 6.2 `READ_BUSY`：原因码答错，排查就被引到错的方向

这件事是 3a 里“用监控查原因”的第一个案例（设计 6 第 1 条、8“第 1 条”）。

**背景**：2c 留下一个现象：新 order Pod 一就绪就有十几个 `503 STOCK_GATE_UNAVAILABLE`。2c 推断原因是闸门 Redis 的连接池是冷的。设计 6 第 1 条写明读代码对不上（`redisx.OpenLazy` 把 `MinIdleConns` 设成连接池大小，返回前等连接建满，最多 2 秒），当时只有时间上的吻合。

**复现**：`probe.py --workers 120 --rate 3 --duration 240`（811 请求/秒），gateway 和 order 都从 2 扩到 4，195240 个请求里 2 个失败，都是查订单返回 `503 STOCK_GATE_UNAVAILABLE`。

**“冷连接池”不成立的证据**：

- 两条失败的链路都落在新 Pod 上，order 一侧只用了 3.5 和 10.9 毫秒就答了 `FAILED_PRECONDITION`，链路里**没有读闸门的 Redis span**，根本没去读闸门，谈不上撞上 300 毫秒时限。
- 新 Pod 的两个 Redis 连接池：等待 1 次，超时 0 次；闸门的熔断器在四个 Pod 上都没打开过；order 容器的 CPU 每个 Pod 约 0.2 核，没有被挤。

**实际原因是三件事叠在一起**：

1. 展示读读闸门有并发名额（`READ_GATE_MAX_CONCURRENCY`，默认 16），名额满了就不读闸门，只读订单行。
2. 名额满了、订单行又不在库里时，`fetch` 分不清订单是在排队还是不存在，答 `503 STOCK_GATE_UNAVAILABLE`（1d 有意的行为，有用例）。
3. 名额被占满不是新 Pod 才有的：这一轮四个 Pod 上 `order_bulkhead_rejected_total{name="read_gate"}` 是 11、6、4、6，共 27 次，最早的 3 次在 10:36:15，那时还没开始扩容；27 次里只有 2 次变成 503，因为其余的订单已经落库。扩容之后消费组重新分配分区，落库晚一点，才有机会变成 503。两个失败订单从受理到开始落库隔了 0.6 秒和 1.0 秒。

**没量到的**：16 个名额为什么会满。一次读闸门不到 1 毫秒，按平均到达率远远占不满；推断是探针的 120 个 worker 按同一个节拍发请求，同一毫秒里收到二三十个查询（`order_bulkhead_in_flight` 15 秒采一次，采到过 13）。这是推断。真实的客户端不会这么整齐，3b 用 k6 压的时候要再看。

**原因码答错的后果**：`503 STOCK_GATE_UNAVAILABLE` 会让人去查 Redis 和闸门，而这一次闸门没有坏、降级也没有发生，是查询太多。设计 8 写的是“现在的原因码会让人去查 Redis，这一次就查错了方向”。`READ_BUSY` 本来就是“展示读拿不到名额”的原因码。

**处理**：主人定了按建议改（2026-11-17），名额满了、订单行又不在时 `fetch` 答 `429 READ_BUSY`，用例和接口规范的说明跟着改。部署后同样的压法：194760 个请求 0 失败，名额仍然被占满过（新的四个 Pod 上 14、11、3、0 次），名额的大小没有动。

**这与项目规范的关系**（AGENTS.md）：“自己是好的、依赖坏了或功能被关掉”才返回降级（`errx.Degraded`）；`503` 被调用方的边车算成服务失败。这里是反例：服务自己没有问题，答的是 503 加“闸门不可用”。

**限定**：为什么 16 个名额满了是推断；3b 的真实流量下要再看，名额要不要调大（32 或 64，连接池是 128）等 3b 再定。

**面试时怎么讲**：一个看似“新 Pod 冷”的 503，我用三样东西（连接池等待、CPU、出错链路）证伪了冷连接池：出错的链路里根本没有 Redis span。真正的原因是读闸门的并发名额被突发占满、加上扩容时落库晚了一秒。修的不是名额，而是原因码：该答“忙”（429 `READ_BUSY`）的答成“闸门不可用”（503），会把排查带到 Redis。教训是原因码本身是排查的入口，答错的代价比答不出更大。

---

## 7. Istio 的指标：在源头裁标签，经边车多花多少时间

**问**：网格指标的标签在哪里裁？边车加了多少延迟？

**为什么在源头裁**（`deploy/cluster/istio/telemetry.yaml`，设计 8 第 5 步的 Istio 部分）：用全网格的 `Telemetry` 资源的 `tagOverrides` 去掉，不在 Prometheus 里用 `labeldrop`。

| | 在边车里去掉（`tagOverrides`） | Prometheus 里 `labeldrop` |
|---|---|---|
| 边车内存和 `/stats/prometheus` 的传输量 | 省（Envoy 里每个标签组合是一个独立的统计项，清单注释） | 不省，只省存储 |
| 去掉标签后同一次抓取里出现重复序列 | 没有这个问题 | 整次抓取失败 |
| 代价 | 已记过的旧标签组合在边车重启前还留在 `/stats/prometheus` 里（Envoy 的统计项不回收），所以要先提交配置，再滚动重启业务 Pod | 无 |

**怎么裁的**：一条 `istio_requests_total` 的标签从 25 个减到 9 个：`reporter`、`source_workload`、`destination_workload`、`destination_service_name`、`request_protocol`、`response_code`、`grpc_response_status`、`response_flags`、`connection_security_policy`；请求和响应大小的直方图、gRPC 消息数整个关掉。**两个目的标签都留着**：请求在选出上游之前就失败（`UH`、`UO`）时，调用方一侧的 `destination_workload` 是 `unknown`，`destination_service_name` 来自路由一直有，而那正是要看的情况。

同时 PodMonitor 用 `keep` 只留 Istio 的标准指标和几项 Envoy 统计（摘除和并发上限：`outlier_detection.ejections_*`、`upstream_rq_pending_overflow`、`upstream_cx_overflow`），不留的话每个边车有约 600 条 `envoy_*`。这几项统计的开启要靠 `istiod.yaml` 的 `proxyStatsMatcher`，它只在注入时读，所以五个带边车的服务重启了一遍（探针 16660 个请求 0 失败）。

**量到的**：实际新增 532 条序列（子 Agent 估的是 1000 上下），序列总数 19.0 万。

**经边车多花多少时间**（2d 留下的问题，设计 8 第 5 步 Istio 部分；gateway 到 order，90 请求/秒，两分钟的平均值）：

| 量的位置 | 平均耗时 |
|---|---|
| gateway 进程（gRPC 客户端） | 6.46 毫秒 |
| gateway 的边车 | 5.33 毫秒 |
| order 的边车 | 4.23 毫秒 |
| order 进程（gRPC 服务端） | 3.79 毫秒 |

一次调用 6.5 毫秒里，order 自己只用了 3.8 毫秒；其余 2.7 毫秒在路上：进程到自己的边车 1.1、两个边车之间（网络加 mTLS）1.1、边车到 order 进程 0.4。

**限定**：这是低负载下的平均值；分位数因为 Istio 的桶太粗（5、10、25 毫秒……）读不准；和直连的对比仍然没有做；3b 压测时再看它随负载怎么变。网格大盘 `opp-mesh` 里 Istio 和 Envoy 的那些查询只核对过指标名，没有在界面上看过。全网格的 `proxyStatsMatcher` 和 Pod 注解是合并还是覆盖，没有验证。

**面试时怎么讲**：每个标签组合在 Envoy 里是一个统计项，所以在源头去掉能同时省边车内存、传输量和存储；在 Prometheus 里 `labeldrop` 只省存储，还可能因为重复序列让整次抓取失败。我把标签从 25 个减到 9 个，保留了两个目的标签，因为选不到上游时其中一个会是 `unknown`。经边车的开销我量到的是平均 2.7 毫秒，但没做直连对比，也没读到分位数。

---

## 8. 基数和序列数：哪些东西占了序列，标签为什么只能是有限的枚举

**问**：Prometheus 里一个标签有什么代价？你们的序列都花在哪了？

**原理**：规则在 AGENTS.md：指标的标签只能是有限的枚举（结果、原因码、规则名、topic）；订单号、用户号、SKU、IP、请求路径原文一律不进标签，每种标签组合是一条独立的时间序列，它们放在 span 上（`otelx.SetOrder`、`otelx.SetUser`）。（通用知识）序列数决定 Prometheus 的内存（头块里每条活跃序列有常驻开销）、抓取和查询的开销；高基数的标签在这里线性乘出去。同样的道理用在 Loki 的标签（3）和 Istio 的标签（7）上。

**这个项目里序列的数**（都按时间顺序）：

| 时间点 | 序列数 | 说明 |
|---|---|---|
| 刚装完 kube-prometheus-stack（设计 8 第 2 步） | 13.2 万 | 什么业务指标都没接；kubelet 7.2 万、apiserver 4.7 万。设计 4 的估计是“10 到 20 万”，偏低 |
| 六个服务和 metrics-generator 接上（第 4 步） | 16.5 万 | 业务服务 4600（order 1682、inventory 944、orderindex 652、payment 536、gateway 400、cdc 15）、metrics-generator 1900、Collector 自己 200，其余的增量来自新 Pod |
| exporter（第 5 步） | 三样合计 1.5 万 | 其中 Kafka 4700 |
| Istio（第 5 步） | 19.0 万 | 新增 532 条 |
| 压测几轮后（第 8 步前一半） | 活跃 21.8 万，头块里 26.8 万 | kubelet 9.9 万、apiserver 6.3 万，两样占四分之三；业务服务加中间件不到 3 万 |
| 从空集群重建后 | 活跃 22.0 万 | |

**读法**：占序列的主要是 Kubernetes 自己的组件（kubelet、apiserver 的直方图和 cAdvisor），不是业务指标。业务服务加中间件不到 3 万。增长也不全来自“业务多了”：扩容出来的 Pod 和 KEDA 带来的新资源类型都会加序列，Pod 的新旧交替让头块里有消失的 Pod 留下的序列（26.8 万对 21.8 万）。

**没裁，要裁怎么裁**：Prometheus 的内存还有余量（工作集 951 Mi，峰值 1000 Mi，requests 1536 Mi，limits 3072 Mi），没有裁；要裁先裁 apiserver 的直方图和 cAdvisor 里用不到的指标（设计 8 第 8 步前一半）。

**限定**：内存与序列数的比例关系没有专门量（只有两个时间点的数），不要在面试里说成线性公式。

**面试时怎么讲**：每个标签组合是一条独立的序列，所以标签必须是有限枚举；订单号、用户号放 span 上，不放标签。我们的序列主要被 kubelet 和 apiserver 的直方图占了四分之三，业务加中间件不到 3 万。我先量、再决定裁不裁，现在内存有余量，没裁。

---

## 9. 按请求速率伸缩：KEDA、External 指标、取最大、取不到时

这一节是 3a 里实测最多的一块，设计 8“第 7 步”，ADR-0030 的补充。

### 9.1 HPA 怎么取到自定义指标，KEDA 做什么

**（通用知识）**：HPA 的指标有几类：Resource（CPU、内存，来自 metrics-server）、Pods（每个 Pod 的自定义指标）、External（集群外或和 Pod 无关的指标）。后两类要有一个实现了聚合 API（`custom.metrics.k8s.io`、`external.metrics.k8s.io`）的服务，HPA 控制器通过 API server 的聚合层向它取数。面试前对照官方文档核对。

**这个项目里**：

- 选 KEDA，而不是 prometheus-adapter（ADR-0030 第 10 项）：KEDA 还在活跃地发版（v2.21.1）；一个 `ScaledObject` 里直接写 PromQL；以后按 Kafka 消费延迟伸缩消费者也是它。prometheus-adapter 概念上最贴近“HPA 的自定义指标 API”，但最近一次发布是 2024-05-17，维护状态存疑。
- **KEDA 不是另一套伸缩器**：它照着 `ScaledObject` 建一个普通的 HPA（`keda-hpa-gateway`、`keda-hpa-order`），请求速率在里面是 `External` 类型的指标；HPA 控制器每 15 秒经聚合 API 向 KEDA 取数，KEDA 当场去 Prometheus 执行查询。算副本数、稳定窗口、改 Deployment 的仍然是 HPA 控制器（`scaledobject.yaml` 的注释、设计 8 第 7 步）。KEDA 只提供数。
- 风险：官方测试的 Kubernetes 是 1.34 到 1.36，我们是 1.37。**实测可用**：聚合 API `external.metrics.k8s.io` 可用，`prometheus`、`cpu` 两种触发器扩缩都正常；Kubernetes 是 1.37.2（ADR 补充）。内存：operator 46 Mi、metrics-apiserver 35 Mi、webhook 12 Mi。
- operator 第一次启动会自己重启一次（`Secrets have been updated; exiting so pod can be restarted`）：它生成了 webhook 和聚合 API 用的证书，退出好让新证书被加载，不是故障。

### 9.2 两个触发器，取最大

| 服务 | 请求速率的查询（整个服务合计） | 每个副本的阈值 | CPU |
|---|---|---|---|
| gateway | `sum(rate(http_server_request_duration_seconds_count{...service_name="gateway"}[1m]))` | 180 请求/秒 | 70% |
| order | `sum(rate(rpc_server_call_duration_seconds_count{...service_name="order"}[1m]))` | 100 请求/秒 | 70% |

- `metricType: AverageValue`：查询返回整个服务的速率，期望副本数 = ceil(结果 ÷ 阈值)，除以副本数这一步是 HPA 做的（清单注释）。
- **HPA 对多个指标各算一个期望副本数，取最大的**。留着 CPU 的理由（ADR 补充）：请求速率只数了同步的请求，order 消费 Kafka 落库那一半不在这个数里；Prometheus 连不上时请求速率这一项取不到，CPU 这一项还能扩（实测）。代价：扩容的原因要看 HPA 的状态才知道是哪一项。
- **阈值是用 CPU 目标反推的初值**：`probe.py --workers 60 --rate 3`（405 请求/秒），各 2 个副本，两分钟平均。gateway 请求速率 404.6/秒、CPU 0.249 + 0.124 核（服务加边车）、每个请求每秒 0.92 毫核、requests 250m、70% 对应 190/秒；order 221.9/秒、CPU 0.387 + 0.118 核、每个请求 2.3 毫核、requests 350m、70% 对应 108/秒。各取小一点的整数（180、100），让请求速率比 CPU 早一步触发。限定：order 的数依赖请求的组合（受理比查询贵得多，还带着落库），组合变了就不准；3b 压测后重定。
- 顺带看到的：405 请求/秒时两个服务的 CPU 已经到 71% 到 75%，而 2c 压到约 810 请求/秒才扩容；中间多了 Istio 边车（gateway 的 CPU 里三分之一是边车）、全量的链路导出和 15 秒一次的指标导出，**没有逐项拆**，3b 做基线时要拆开看。

### 9.3 为什么不比按 CPU 快

扩缩实验（`probe.py --workers 75 --rate 3 --duration 240`，507 请求/秒，每 5 秒记一次 HPA 状态）：

| | gateway | order |
|---|---|---|
| 发压到期望副本数变成 3 | 48 秒 | 79 秒 |
| 那一刻的指标 | 速率 194.5（阈值 180），CPU 78% | 速率 111.2（阈值 100），**CPU 65%** |
| 新 Pod 加入后稳定值 | 速率 169，CPU 72% | 速率 94，CPU 65% |
| 停压到缩回 2 | 5 分 19 秒 | 5 分 19 秒 |

- 122025 个请求 0 失败，包括两个新 Pod 加入的时候；order 的那次扩容是请求速率触发的（CPU 当时只有 65%，按 CPU 不会扩）。
- **请求速率并不比 CPU 快**：`rate(...[1m])` 在负载突然上来之后要 60 秒才爬到真实值（记录里 gateway 每个副本的速率是 25、105、195 这样爬上去的，真实值是 253）；metrics-server 的 CPU 在 33 秒时就到了 63%。2c 按 CPU 约 55 秒扩容，现在 48 秒，差别在误差里。窗口缩短能快一些，但指标 15 秒才导出一次，30 秒的窗口里只有两个点，少一个点就没有结果；没有改。
- **换成请求速率的理由是信号对不对，不是快不快**：它不受 requests 写错的影响，和“这个服务一个副本能扛多少”这个容量结论直接对应；CPU 对 order 是个凑合的信号（落库卡在下游时 CPU 是低的，设计 3.8）。
- 缩容的 5 分钟是 HPA 默认的稳定窗口（过去 5 分钟里算出的最大期望值），加上速率窗口退空的一分钟里有一部分重叠。扩到 4 个副本的情况这次没压到。

### 9.4 Prometheus 连不上时

在临时命名空间里验证的（做完已删）：一个 3 副本、CPU 是 0 的 Deployment，`ScaledObject` 的 `serverAddress` 指向不存在的地址。

- HPA 的条件变成 `ScalingActive=False FailedGetExternalMetric`，**7 分半没有缩容**（已经超过 5 分钟的稳定窗口；按 CPU 它该缩到 1）。HPA 的规则是：有任何一个指标取不到就不缩，因为不知道那一项要不要更多副本。
- 同一个 Deployment 改成 1 个副本并烧 CPU：30 秒内从 1 扩到 4。**取得到的指标仍然能扩容**。
- **`ScaledObject` 的 `READY` 这时仍然是 `True`**（它只表示定义合法），看它发现不了问题。能发现的是 KEDA 的 `keda_scaler_detail_errors_total`（这个临时对象上涨到了 3，gateway、order 是 0）和 HPA 的 `ScalingActive` 条件。3c 要加这条告警：Prometheus 是单副本，它不在的时候伸缩是悄悄停掉的。
- 没有配 KEDA 的 `fallback`（取数连续失败几次后按一个固定的副本数算）；`fallback` 和 `cpu` 触发器能不能一起用没有查证。ADR 补充的说法是：Prometheus 是单副本，这一条等于把“监控挂了”对伸缩的影响限制在“暂时不缩容”。
- KEDA 不在时（聚合 API 不可用）的行为没有实测，推断和 Prometheus 连不上一样；另外聚合 API 不可用时删命名空间可能卡住（`make cluster-e2e` 每次都删命名空间），阶段 4 做。

### 9.5 从旧 HPA 切换的坑，同一个 Deployment 不能有两个 HPA

旧的 HPA 还在时建 `ScaledObject` 会被 KEDA 的准入 webhook 拒绝（`the workload 'gateway' of type 'apps/v1.Deployment' is already managed by the hpa 'gateway'`），而 `helm upgrade` 是先建新资源、后删旧资源，直接升级会失败。这次是先 `kubectl -n oppenheimer delete hpa gateway order` 再 `services.sh deploy gateway order`（镜像 tag 不变，Pod 没有滚动；中间二十几秒没有 HPA，副本数不动）。脚本里没有加这一步：从空集群部署不会遇到。反方向（把 `query` 清空退回 CPU 的 HPA）没有试，推断是两个 HPA 短暂并存再由 Helm 删掉 `ScaledObject`。KEDA 有接管现有 HPA 的注解（`scaledobject.keda.sh/transfer-hpa-ownership`），没有用：接管之后 Helm 仍然会把那个 HPA 当成自己不再要的资源删掉。

相关的部署顺序：chart 里有 `ScaledObject`，CRD 不在时 Helm 直接失败，所以 `make monitoring-up` 要在 `make deploy` 之前；这个顺序在重建时走通了（见 14）。

**没做的**：按消费延迟伸缩 order（KEDA 的 `kafka` 触发器）；落库卡在下游时请求速率和 CPU 都不涨，现在的两个触发器都看不到。

**面试时怎么讲**：HPA 的自定义指标走聚合 API，KEDA 实现它：把 PromQL 的结果作为 External 指标交给 HPA，算副本数的仍然是 HPA 控制器。我们留了请求速率和 CPU 两个触发器，HPA 取大的。实测过 Prometheus 连不上：不缩容，但 CPU 还能扩；`ScaledObject` 的 `READY` 仍然是 `True`，所以要用 KEDA 的错误计数和 HPA 的 `ScalingActive` 去告警。还有一点要诚实：请求速率并不比 CPU 快，`rate` 的窗口让它要 60 秒才爬到真实值，选它是因为信号更对。

---

## 10. Collector 停机：三种信号各丢什么，日志丢了三处

**问**：Collector 挂了会怎样？指标、日志、链路各丢什么？为什么 `batch` 会切断反压？

**实验**（设计 8“验证 5”）：`probe.py --workers 15 --rate 3`（约 100 请求/秒）压 9 到 10 分钟，第 1 分钟把 `otel-gateway` 副本数改成 0，5 分钟后改回 1。做了三轮：第一轮查出日志在两处被丢，改了配置；第二轮又查出一处；第三轮没有再丢。

**业务不受影响**（三轮一致）：第一、二轮 60855、60795 个请求 0 失败，延迟没有变化；六个服务的内存在停掉期间是平的。SDK 的导出失败只在日志里留下 `telemetry error`（`otelx` 做了合并，每 10 秒一条）。1d 的 8.17 没有长时间验证过的那一条成立。

**三种信号各自丢了什么**：

| 信号 | 停掉的 5 分钟 | 为什么 |
|---|---|---|
| 指标 | 图上断 5 分钟，**累计的数没有丢**：`increase` 算出来 61293 个请求，探针发了 60855 个 | SDK 导出的是累计值（从进程启动到现在的总数），中间几次没送到，下一次送到的数里已经包含了 |
| 链路 | **丢了**。停掉期间 Tempo 里是 0 条 | SDK 的队列只有 2048 个 span，满了就丢新的；链路没有“下次补上”这回事 |
| 日志 | 改配置之前丢了约 6 万行，改之后**不丢，延迟到 gateway 回来之后补齐** | 日志文件在磁盘上，agent 可以停下来等 |

注：`increase` 算出 61293、探针发了 60855，两个数对不上一模一样，设计没有解释差额。`increase` 本身有外推（通用知识），差额不超过 1%，面试里只说“累计值补得回来、数量级对得上”。

### 日志丢在三个地方，都是批次没有上限引出来的

1. **agent 自己丢**（约 4700 行）。原来的配置注释写的是“不会丢，只是延迟”，不成立：重试默认最多 300 秒，超过就丢这一批（`Exporting failed. Dropping data`，每个 agent 正好 10 批，等于并发发送的个数）；队列满了之后新数据被拒绝，而 **`batch` 处理器是异步的**，这个错误传不回 `filelog`，数据就在 `batch` 那里没了（`otelcol_exporter_enqueue_failed_log_records` 涨了 3881）。**这就是“`batch` 切断反压”**：反压本来要一路堵回读文件的接收器，让它停下来；`batch` 把上下游解耦，拒绝的错误回不去。修法：`retry_on_failure.max_elapsed_time: 0` 加 `sending_queue.block_on_overflow: true`，队列满了就一路堵回 `filelog`，它停止读文件。
2. **gateway 发给 Loki 时被丢**（7 批、约 5.7 万行，大头）。agent 把积压的日志一起发过来，gateway 的 `batch` 把它们合成 8000 多条、6.9 MB 的一批；Loki 的写入限额是每秒 4 MB、突发 6 MB，**突发量同时是单次请求的上限，比它大的请求永远是 429**，重试多少次都一样，5 分钟后被丢（`no more retries left ... ingestion rate limit exceeded`）。修法两处：gateway 的日志管线单独用每批最多 2000 条的 `batch/logs`（约 1.7 MB）；Loki 的限额放宽到每秒 16 MB、突发 32 MB。
3. **agent 发给 gateway 时被丢**（1 批、7309 行）。队列改成堵住之后，gateway 一回来 `filelog` 一口气追读，agent 的 `batch` 合出一批超过 gateway 的 gRPC 接收上限（4 MiB），这是不可重试的错误，整批丢掉。agent 的 `batch` 也加了每批最多 2000 条。

**第三轮**：三个服务的日志在停掉的 5 分钟里每分钟的行数和平时一样，`Dropping data` 0 次，`enqueue_failed`、`send_failed` 都是 0。Loki 仍记了 1.8 万行 `rate_limited`，那是补发时被 429 之后重试成功的，没有丢。

**这三处在压测时同样会出现，不只是 Collector 停机时**：日志量一上来，`batch` 攒满 8192 条的时候一批就超过 6 MB。之前几轮 500 请求/秒的压测里 Loki 没有丢弃的记录（查过），但 3b 的量会大得多。

**留下的问题**：

- agent 的内存在队列堵满时到过 204 Mi，上限是 256 Mi，余量不大；队列是 1000 批，一批最多 2000 条。3b 之前看一下要不要把队列改小或把上限加到 384 Mi。
- gateway 到 Loki 这一段仍是默认的重试 300 秒：Loki 停超过 5 分钟 gateway 会丢，而且它前面的 `batch` 同样不把错误传回 agent；没有改，阶段 4 停 Loki 的实验里看。
- gateway 长时间不在、日志文件被 kubelet 轮转掉的部分还是会丢（没有实测）。
- 链路在 Collector 停机时必丢。要不丢就得在每个节点上放一个收 OTLP 的 agent 并落盘排队，没有做，也不打算做：链路本来就是采样的。
- 第三轮有 15 个查库存的请求同时超时（恢复后约 55 秒，同一时刻发出的一批）。入口 Envoy 和 gateway 的访问日志里都没有这 15 个请求，Tempo 里也没有超过 1 秒的链路，所以它们没有到达入口；**卡在 MacBook 到入口之间的哪里没有查到**，和这个实验有没有关系说不清，前两轮没有出现。
- agent 的检查点在 gateway 不可用时是否真的不前进，设计 8 第 4 步写“没有验证”；验证 5 的结果（日志补齐、没有丢）间接支持，但没有单独看检查点。

**面试时怎么讲**：指标是累计值，下一次送到的数里已经包含了中间丢的，所以图上断了但总数没丢；链路在 SDK 队列里，满了就丢，补不回来；日志在磁盘上，agent 能停下来等。我第一次实测时发现日志丢了六万行，原因有三：`batch` 是异步的，队列满的错误传不回读文件的接收器，反压就断了；批次没有上限，合出来的一批撞上 Loki 的突发上限（超过它的请求永远 429）和 gRPC 的 4 MiB。修法是堵住队列和限制批次大小。

---

## 11. 复制停了的时候，延迟指标是断掉的

**实验**（设计 8“验证 7”）：探针约 100 请求/秒压着，在 `mysql-order-0-1` 上 `STOP REPLICA SQL_THREAD`，同时把 `orderindex` 缩到 0 个副本，2 分钟后都恢复。探针 40535 个请求 0 失败。每 30 秒一个点：

| 指标 | 故障前 | 故障的 2 分钟 | 恢复后 |
|---|---|---|---|
| `mysql_slave_status_replica_sql_running` | 1 | 0 | 1 |
| `mysql_slave_status_seconds_behind_source` | 0 | **没有数据** | 0 |
| `kafka_consumergroup_lag`（`orderindex.cdc-orders`） | 0 | 271、581、901、1211 | 30 秒内回到 0 |
| `kafka_consumergroup_members` | 2 | 0 | 2 |

- 消费延迟的图有反应，涨的速度和写入的速度对得上。
- **主从延迟的图在复制停掉时不是变大，而是断掉。** SQL 线程不在运行时 MySQL 的 `Seconds_Behind_Source` 是 `NULL`，exporter 就不出这个指标。所以“延迟大于若干秒”的告警对“复制停了”这种最严重的情况恰恰不响（通用知识补充：缺失的序列不满足 `> N` 的比较，规则不会触发）。3c 的主从告警要写成三条：`replica_sql_running == 0`、`replica_io_running == 0`、延迟超过阈值；中间件大盘上运行状态的那一格已经有了。
- 另外 `Seconds_Behind_Source` 量的是“从库正在执行的事务比主库晚多少”，从库空闲时是 0，IO 线程断了它也可能是 0（设计 8 的说法）。要准的延迟得靠心跳表，这是 3c 里“CDC 源库心跳”那一项。
- 有关消费延迟的一处注意：`order.order-close-delay` 这个组的延迟有 1.7 万，是延迟关单的机制本身（消息到期前不提交位点，ADR-0009），不是积压；大盘和告警要把它单独对待（设计 8 第 5 步）。

**限定**：验证 6（`make cluster-fault` 抽几个场景对指标）没有做。Kafka 消费者的重试和死信两个计数器从没触发过，名字是按代码推的（设计 8 大盘一节）。

**面试时怎么讲**：按“延迟大于阈值”写的告警在复制停了的时候不响，因为这时 `Seconds_Behind_Source` 是 `NULL`，指标直接消失。要同时告警线程状态（`replica_sql_running`、`replica_io_running`）。而且“延迟是 0”也不一定健康：IO 线程断了它也可能是 0，要靠心跳表。

---

## 12. 保留期和磁盘：盘是被压测写满的，入口访问日志占 60%

**测量的时机和限定**（设计 8“第 8 步的前一半”）：装上约 3 个半小时后量，期间做过七八轮压测（每轮 400 到 500 请求/秒、两到四分钟）。**不是空转一天的数**，空转后的那一次还没量（etcd 的 fsync 也是）；重建把监控数据清空了，所以“空转一天”要从重建算起。阶段计划里这一项 checkbox 也没有勾。

**内存**（工作集，Mi）：

| 组件 | 现在（空闲） | 4 小时里的峰值 | requests | limits |
|---|---|---|---|---|
| Prometheus | 951 | 1000 | 1536 | 3072 |
| Tempo | 197 | **1553** | 512 | 2048 |
| Loki | 330 | 507 | 512 | 1536 |
| Collector gateway | 161 | 261（这个 Pod；上一个到过 375） | 512 | 1024 |
| Collector agent × 3 | 70 上下 | 103 | 128 | 256 |
| Grafana | 449 | 357（只算主容器） | 256 | 512 |

- Tempo 的内存不是泄漏：每 20 分钟一个点是 28、32、32、148、521、268、205、244、376、319、121，跟着写入量走，停压后回落；峰值出现在不采样的那几轮（每轮 72 万个 span 全写进去），尾部采样常开之后写入量是十分之一。上限 2 Gi 不动，但**3b 不要关采样压测**，要关就先调它。没有重启过。（这是对 5 里“Tempo 内存一路在涨”的后续。）
- Grafana 主容器已经超过 requests，3b 之前把 requests 调到 384。
- server 节点用了 6.4 GiB（51%），其中监控约 2.5 GiB。

**磁盘**（都在 server 的系统盘上，节点上 `du` 量的；`local-path` 不限卷的大小）：

| | 现在 | 写入了多少 | 落盘的密度 | 保留期 |
|---|---|---|---|---|
| Prometheus | 477 MB | 约 7300 个样本/秒 | 没有量（还没有压缩过的块） | 15 天，另有 15 GB 的容量上限 |
| Tempo | 1362 MB | 807 万个 span，2545 MB | 约 170 字节/span | 3 天 |
| Loki | 234 MB | 284 万行，1631 MB | 压缩约 7 比 1 | 7 天 |

- **`local-path` 不限制卷的大小**：PVC 上写的 10 Gi、20 Gi 只是个标签，没有东西拦着它们写满节点的盘；真正起作用的是保留期和 Prometheus 的容量上限（设计 8 第 3 步）。PVC 的用量指标对 `local-path` 没有意义，五个卷报的都是整块盘的数。server 的系统盘只有 60 GB（58 GB，用了 12 GB，其中镜像 6.3 GB、etcd 0.5 GB，剩 46 GB），3c 的磁盘告警要盖住它。
- **保留期不改**（15 天加 15 GB 上限、7 天、3 天）。空闲时三样加起来一天不到 2 GB（Prometheus 估 1 到 1.3 GB/天，按每个样本 1.5 到 2 字节算，是估计；Loki 空闲时每秒 17 行，可忽略；Tempo 空闲时每秒约 200 个 span，约 3 GB/天会被 3 天的保留期截在 9 GB 上下）。Prometheus 有容量上限兜底，另外两样没有。
- **盘是被压测写满的，不是被保留期**：按这几轮的数折算，每个请求大约落 1.2 个 span（尾部采样后）和 1.4 KB 的日志。2000 请求/秒压一小时：Tempo 约 1.5 GB，Loki 约 1.4 GB（压缩后）。3b 的长稳测试压几个小时就是十几到二十 GB，盘装得下，但和 etcd 在同一块盘上。3b 开始前先定：长稳测试期间日志和链路要不要再降。
- 和 etcd 抢盘（ADR-0030 后果；设计 4）：2b 记过空集群上 etcd 的 fsync p99 8 到 16 毫秒。装完时是 29.6 毫秒（后续解释是 server 刚重启、Prometheus 刚开始写盘、节点在拉镜像）；服务接上后 15.9 毫秒；压测几轮后 19.1 毫秒。**仍然高于 10 毫秒的参考线，仍然没有查原因。**

**入口访问日志占 60%**：4 小时里 776 MB 的日志，入口 Envoy 的访问日志占 60%（其中 gateway 229、order 127、inventory 125，其余可以忽略；这几个数的归属原文就是这样列的，设计没有说明这三项各是什么，更像是按服务名列的日志量）。每个请求在入口记一行、在 gateway 又记一行，内容是重复的。**主人同意后改成只留出错的**（`deploy/cluster/envoy-gateway/gateway.yaml` 的 `telemetry.accessLog`）：条件 `response.code >= 500 || response.flags != 0`，这两类正好是 gateway 的日志里没有的。实测正常请求不再记、`Host` 不存在的请求（404、`NR`）记了；改动经 xDS 下发，Envoy 没有重启。重建之后 30 分钟里入口只有 64 行，gateway 5468 行（设计 8 第 8 步），过滤在新集群上生效。

**一个坑：Envoy Gateway 的 `matches` 实际是“与”。** CRD 的文档说多条 `matches` 满足一条就记（“或”），Envoy Gateway v1.10.0 实际生成的是 `and_filter`，所以“或”要写在**一条** CEL 表达式里（上面的 `||`）。清单里 `matches` 只有一项，就是这个原因。

**面试时怎么讲**：保留期不是磁盘的主要约束，压测才是：每个请求大约 1.2 个 span 和 1.4 KB 日志，长稳测试几个小时就是十几到二十 GB，而且和 etcd 在同一块盘上。`local-path` 的 PVC 大小只是标签，不限制。日志里 60% 是入口的访问日志，和 gateway 的重复，所以只留出错的；顺便说一个坑：Envoy Gateway 的多条 `matches` 是“与”，要“或”得写进一条 CEL。限定：这是压测后 3.5 小时的数，不是空转一天。

---

## 13. 单副本的监控丢了什么，pprof 为什么只开在回环上

### 13.1 单副本

**取舍**（设计 7、ADR-0030“放弃了什么”和后果）：监控数据的高可用和长期保存（双副本的 Prometheus、Thanos、分布式模式的 Loki 和 Tempo、对象存储）不做：内存不够，也不是这个项目要学的重点；讲得清为什么单副本、丢了什么就行。

**丢了什么**：

- server 节点的盘坏了，历史数据就没了（三样数据都是可以丢的，丢了只是看不到历史，所以用本地盘；NAS 的同步写只有 20 MB/s，Prometheus 不支持 NFS，MinIO 社区版已归档）。
- server 节点成了“控制面加监控”的单点：它不可用时既不能操作集群也看不到指标，只剩节点上的日志文件。
- gateway 重启时丢几秒数据（尾部采样只能一个副本）。
- 边车视角的链路和访问日志；出事那一刻的火焰图（没有持续剖析，Pyroscope 不部署）。
- 两个 agent 节点各少 2 GiB 内存（三台虚拟机内存从 10、16、16 改成 14、14、14 GiB；设计 8 第 0 步：每台开机后 22 到 38 秒集群全部恢复，agent 重启时 Sentinel 又切了一次）。

**为什么可以接受**：这是练习环境，监控不是业务路径；ADR 的论证是“监控不能和被它看的东西一起倒”：阶段 4 要关 agent 节点，server 关了控制面也没了，本来就不在实验里，所以监控放在它上面。伸缩这一块也把风险限制了（9.4）：监控挂了只是暂时不缩容。要说清楚的是限定：server 节点同时是 etcd、控制面、监控的单点，这是明确接受的，不是没想到。

### 13.2 pprof 端点

**做法**（设计 8 第 5 步 pprof；`pkg/app` 的 `ListenDebug`、`DebugMux`、`WithDebug`）：环境变量 `DEBUG_ADDR` 设了才监听，chart 的默认值是 `127.0.0.1:6060`。**六个服务统一用一个单独的调试端口，不挂在健康检查的端口上，也不挂在对外的端口上**：

- 健康检查的端口在 Service 里，集群里任何 Pod 都连得到；而 `profile`、`trace` 会让进程按调用方要求的时长采样（通用知识补充：这是一个可以被滥用的拒绝服务面，也会暴露内存里的数据）；gateway 的 8080 更是对外的。
- 只监听回环地址，所以只能 `kubectl port-forward` 进去。
- 常开而不是要用时再加：改环境变量会重启 Pod，想看的泄漏现场就没了。
- 调试端口最后停、而且停的时候直接断开连接：停止卡住时还能取 goroutine 的现场；一个正在跑的 30 秒 profile 不会拖住停止。
- `ListenHTTP` 现在拒绝空的 handler：空的会落到 `http.DefaultServeMux`，而导入 `net/http/pprof` 就会往那上面注册。

**验证过**：`port-forward` 到 order 和 gateway 的 6060 取到了 goroutine 的现场；经入口访问 `/debug/pprof/` 是 404；集群里别的 Pod 访问 order 的 Service 是 404、直接连 Pod 的 6060 连不上（设计 8 第 5 步 pprof）。部署期间探针 13004 个请求 0 失败。

**没做的**：block 和 mutex 的剖析默认不采样，要用得改代码或加运行时的开关。持续剖析（Pyroscope）不部署，可以等 3b 压测时觉得缺了再加（设计 2 第 9 项）。

**面试时怎么讲**：pprof 能让进程按要求的时长采样，所以不能放在 Service 能连到的端口上，更不能放在对外的端口上。我们给它单独一个端口，只监听 127.0.0.1，用 `kubectl port-forward` 进去，并验证了入口、同集群其他 Pod 都连不到。常开是因为泄漏的现场重启就没了。

---

## 14. 从空集群重建：`helm --set` 把全数字的 tag 当成整数

**重建**（设计 8 第 8 步）：删掉三台虚拟机，按新的顺序一步步执行。

| 步骤 | 耗时 |
|---|---|
| `make cluster-up` | 7 分 53 秒 |
| `make middleware-up` | 11 分 23 秒 |
| `make mesh-up`、`make mesh-check` | 29 秒、50 秒 |
| `make monitoring-up` | 12 分 30 秒 |
| `make deploy` | 第一次失败，改了脚本后重跑 1 分 28 秒 |
| `make deploy-seed` | 不到 1 秒 |

不算失败的那一次约 35 分钟（2d 不含监控是 17 分半）。`make monitoring-up` 排在 `make deploy` 之前这个顺序走通了：KEDA 先于业务服务装好，`ScaledObject` 一次建成。

**`--set` 的坑**：镜像 tag 是提交号的前 7 位，这次的提交号 `7390796` 恰好全是数字，`helm --set image.tag=7390796` 把它当成整数，模板里的 `printf "%s"` 渲染出 `%!s(int64=7390796)`，Pod 报 `InvalidImageName`，Helm 等了 10 分钟超时。`services.sh` 改成 `--set-string`。7 位十六进制全是数字的概率约 3.7%（(10/16)^7），所以之前几十次部署都没有碰上。

**教训**：“偶发的、取决于数据的错”要靠重建这种全新环境才暴露；`--set` 做类型推断，凡是“看起来像别的类型的字符串”（纯数字、`true`、`null`）都要用 `--set-string`（后一半是通用知识）。

**验收**：没有不正常的 Pod；除了 KEDA operator 那一次预期内的重启，没有容器重启过；探针 5448 个请求 0 失败；`make cluster-e2e` 通过；没有不健康的抓取目标，`mysql_up`、`redis_up` 都是 1。重建后三个节点内存 29%、30%、40%（server 5.1 GiB）。

**面试时怎么讲**：提交号前 7 位刚好全是数字，`--set` 把它当成整数，渲染出 `%!s(int64=...)`。概率约 3.7%，所以几十次部署都没碰上，重建才撞上。修法是 `--set-string`。我会用这件事说明为什么要定期从空集群重建一遍。

---

## 14a. 故障注入时拿监控对数，exemplar 和尾部采样（主会话补）

这一节是笔记写完之后补的，事实来自设计 8 的“验证 6”和最后一段。

**对数的做法**：`FAULT_TELEMETRY=1 E2E_KEEP=1 make cluster-fault`，让故障注入那一套把指标导出到 Collector，跑完保留现场，等一次抓取，再拿三个来源的数对：脚本自己数的、Istio 的指标、服务自己的指标。400 个并发里被并发上限拒绝的 144 个、等到超时的 314 个、摘除之后被拒绝的 11 个，三边都对得上，合计只差 1 个。

**两个值得讲的发现**：

- 同样是 503，服务自己的指标里全是 `UNAVAILABLE`，只有 Istio 指标的响应标志分得出原因：`UO` 是并发上限，`UT` 是超时，`UH` 是没有健康的端点。这是留着网格指标的理由。
- 大盘上“并发上限溢出”那一格画错了统计项。gRPC 是 HTTP/2，超的是在途请求数，记在 `upstream_rq_active_overflow` 上；一开始留的和画的是 `pending` 和 `cx` 两项，真的溢出时它们是 0。**没有拿一次真实的故障对过的图，不能算验证过**：之前只核对了指标名存在。

**exemplar 和尾部采样**：exemplar 是 SDK 记指标时带上的 trace id，那时不知道这条链路会不会被尾部采样留下。实测 40 个里 Tempo 查得到 14 个。慢的和出错的全留，所以高延迟桶上的 exemplar 跳得过去；普通请求的大半跳不过去。没有改。

**面试时怎么讲**：监控装好之后我做了一次对数，用故障注入制造已知数量的失败，再看监控上是不是同样的数。数是对的，但查出大盘上有一格用错了统计项，真出事时是空的。另外尾部采样有个容易忽略的副作用：exemplar 是在采样决定之前记的，所以大半指向已经被丢掉的链路。

## 15. 还没做的、还没弄清的

### 15.1 没做的

- **空转一天后的数**：内存、磁盘、etcd 的 fsync，都要从重建（2026-11-17 14:40 UTC）之后算（设计 8；阶段计划的这一项没有勾）。
- Prometheus 压缩之后每个样本的实际字节数；磁盘告警（3c）。
- 多副本的尾部采样（要在前面加按 trace id 分流的 `loadbalancing` exporter），设计 3.3 记成考点没做。
- 在 Collector 里、采样之前用 `spanmetrics`、`servicegraph` 算准确的 span 指标（设计 8 第 6 步）。
- 验证 6：`make cluster-fault` 抽几个场景，看熔断器、开关、摘除的指标和实际发生的事对得上（设计 5、8“验证 7”最后一行）。
- 阶段 4 要补的：
  - 停 broker 时下单失败的原因：滚动重启 broker 期间有 31 个下单失败（探针 90 请求/秒，29 个 `503 upstream request timeout`、2 个 504，分布在 59 秒里，下单的 p99 到了 3 秒；查询和查库存没受影响），**原因没有查**，看起来是分区的 leader 换人时生产者的发送等过了这一跳的时限（设计 8 第 5 步）。
  - Pod 重启换地址时 Redis 复制不会自己恢复：`redis-cache-5` 复制断了约 3 小时（连着 `redis-cache-1` 重启之前的旧 Pod 地址）；业务没受影响，修法是 `cluster replicate`，**为什么集群总线学到了新地址而复制没有跟着换没有查**；它记下的主库仍是一个 Pod 地址，主库下次换地址多半还会断。这件事是看大盘（对着 `master_link_down_since_seconds` 约 10260）发现的真故障，现在没有告警，3c 要加 `redis_master_link_up == 0`（设计 8 第 5 步大盘一节）。
  - 注入延迟时验证“慢的全留”；停 Loki 时看 gateway 到 Loki 的重试；KEDA 不在时的行为；istiod 不在时的行为是 2d 的事。
- 3b 要看的：多少请求/秒时 Collector gateway 的内存、Tempo 的内存、agent 的队列；HPA 阈值重定；读闸门名额；k6 流量下 `READ_BUSY` 的出现。

### 15.2 没弄清的

- Tempo 搜索接口带 `start`、`end` 时查不到刚写进去的数据，不带就有（原因没有查）。
- 滚动前旧的一个 gateway Pod 显示重启过 26 次（最近一次在滚动前 73 分钟），Pod 已换掉，原因没有查（设计 8 第 4 步）。
- 第三轮 Collector 实验里 15 个同时超时的请求卡在哪里没有查到。
- etcd 的 WAL fsync p99 在 15.9 到 29.6 毫秒之间，高于 10 毫秒的参考线，没有查原因。
- Tempo 的内存在三轮对比里一路涨，与采样方式对不上；后来的解释是跟着写入量走（12），但对比那三轮本身没有单独查。
- 405 请求/秒时两个服务 CPU 已到 71% 到 75% 的原因没有逐项拆（边车、全量链路导出、15 秒一次的指标导出）。
- Redis 没有命令耗时的指标（`db_client_operation_duration_seconds` 只有 MySQL 的），Redis 只有连接池的指标；同一类量的单位不统一（秒、毫秒、纳秒）；两个 Redis 连接池的名字一个是地址、一个是 `FailoverClient`；没有商家索引端到端落后多少的指标；`cdc_lag_ms` 在源库没有写入时会一直涨；受理成功率的分母里有“库存不足”这类业务上的拒绝，售罄时会掉，3c 定 SLI 时要分开（设计 8 大盘一节）。
- Kafka 的 JMX 规则里没有请求耗时（`RequestMetrics`），要看 produce、fetch 的 p99 得加规则（会再滚动一次 broker）。
- 订单缓存只看到 `miss`：探针对每一单只查一次，没有重复查询所以没有命中；没有去核实别的原因。
- 没处理的小问题：Tempo 的 jaeger 接收端关不掉；Loki 的 chart 即使关了缓存也会建一个 `loki-memcached` 的 ServiceAccount；`deploy/cluster/services/fault/gateway.yaml` 里用 Pod 注解打开统计项的那一段大半是重复的；k3s 的 scheduler、controller-manager、kube-proxy 指标端口只监听 127.0.0.1，没有抓。

### 15.3 没敢写成结论的

- 设计 1.3 的版本表是子 Agent 查的、主会话当时没核对；后面各步骤“版本核对过”的是实际装上的那些（Prometheus、Loki、Tempo、Collector、mysqld_exporter、redis_exporter、KEDA）；其余项（例如 Pyroscope、k6、Alloy）只在版本表里，没有被装上。
- “验证 1 到 3”的互相跳转在接口上验证，界面上点一遍的记录两处不一致（见 4）。
- `memory_limiter` 的拒收在压力下的行为，没有专门压过。

---

## 16. 材料互相对不上的地方（写笔记时的处理）

写笔记时发现的几处，主会话已经回头改了材料：ADR-0030 补记了 agent 没有开 `kubeletstats`；设计 8 里日志量的单位和 `increase` 的差额补了说明；停复制的 `mysql-order-0-1` 就是 `mysql-order-0` 的从库。仍然留着的：

- 设计 8 第 4 步和大盘一节写“界面上还没有人看过”，那是当时的状态；主人后来在浏览器里看过五个大盘和“大盘 → 链路 → 日志”，没有发现问题（阶段计划）。
- 设计 3.7 的大盘表列了六项，实际是五个大盘（`opp-business`、`opp-service`、`opp-middleware`、`opp-collector`、`opp-mesh`），集群资源用 kube-prometheus-stack 自带的。
- 设计 4 估的序列数（10 到 20 万）偏低，实测 22 万。

---

## 17. 速记

- **OTel**：API（接口）、SDK（实现、采样、导出）、OTLP（协议）、Collector（接收、处理、导出）。上下文靠 `traceparent`。
- **两层 Collector**：agent（DaemonSet）读节点日志；gateway（Deployment，1 个副本）收 OTLP、尾部采样、分发。尾部采样要 span 到同一个进程，所以 gateway 只能一个副本。
- **`memory_limiter` 放最前**：数据占内存之前就能拒收，不然 Collector 被撑死。
- **推和拉**：拉有 `up`；推没有，靠“多久没新点”。多目标 exporter 的 `up` 是 exporter 活着，`mysql_up`、`redis_up` 才是数据库连得上。
- **日志**：标准输出加采集，日志在磁盘上采集器可以等。Loki 标签只放有限的几个，`trace_id` 放结构化元数据。
- **跳转**：日志里带 trace id、Tempo 按 trace id 查 Loki、直方图 exemplar。我们的 exemplar 标签名 `trace_id` 对、metrics-generator 的 `traceID` 不对。
- **采样对比（405 请求/秒，inventory 停 20 秒）**：出错的链路头部留 10%、尾部留 100%；正常的尾部留 10.4%；尾部多花服务端全量导出和约 100 Mi（277 到 375 Mi）。“慢的全留”没验证到。
- **尾部之后 span 指标偏**：留下的不是随机样本；RED 用 SDK 指标，span 指标只看拓扑。
- **`ErrSkip`**：三分之一的 MySQL span 是假错误，`database/sql` 的快速路径协议被 otelsql 记成错误；修后 6.7 万个 span 没有 Error；`NOSCRIPT` 同类没处理。
- **`READ_BUSY`**：新 Pod 的 503 不是冷连接池（出错链路里没有 Redis span），是读闸门名额（16）被突发占满加扩容时落库晚了 0.6 到 1.0 秒；原因码改成 429 `READ_BUSY`，因为“闸门不可用”会把人引去查 Redis。名额为什么满是推断。
- **Istio 指标**：在源头（`Telemetry` 的 `tagOverrides`）裁，25 个标签减到 9 个，新增 532 条序列；`labeldrop` 只省存储还可能让整次抓取失败。经边车：6.5 毫秒的调用里路上 2.7 毫秒；没做直连对比。
- **序列数**：22 万活跃，kubelet 9.9 万加 apiserver 6.3 万占四分之三，业务加中间件不到 3 万；标签只能是有限枚举，订单号用户号放 span。
- **HPA**：KEDA 照 `ScaledObject` 建普通 HPA，请求速率是 External 指标，HPA 控制器仍然算副本数；两个触发器取最大（gateway 180/秒、order 100/秒，加 CPU 70%）。
- **不比 CPU 快**：`rate[1m]` 要 60 秒才爬到真实值；选它因为信号更对。order 那次扩容是速率触发的（CPU 65%）。
- **指标后端挂了**：不缩，但 CPU 还能扩（30 秒 1 到 4）；`ScaledObject` 的 `READY` 仍是 `True`，要看 `keda_scaler_detail_errors_total` 和 HPA 的 `ScalingActive`。
- **旧 HPA 切换**：旧 HPA 在时 `ScaledObject` 被准入 webhook 拒绝，要先手工删一次。
- **Collector 停 5 分钟**：指标累计值补得回来（图断、总数没丢）；链路必丢（SDK 队列 2048）；日志改后不丢。业务 0 失败、内存是平的。
- **日志丢三处**：`batch` 异步切断反压（要 `block_on_overflow: true` 加 `max_elapsed_time: 0`）；批次没上限撞 Loki 突发 6 MB（比它大永远 429）；撞 gRPC 4 MiB。修法：`batch/logs` 每批 2000 条，Loki 限额 16 MB/秒、32 MB 突发。
- **复制停了**：`Seconds_Behind_Source` 是 `NULL`，指标消失，延迟告警不响；要加 `replica_sql_running == 0`、`replica_io_running == 0`，延迟为 0 也不一定健康，要心跳表。
- **磁盘**：`local-path` 不限大小；盘是被压测写满的（每请求约 1.2 个 span、1.4 KB 日志）；入口访问日志占 60%，改成只记出错的；Envoy Gateway 多条 `matches` 是“与”，“或”写进一条 CEL。
- **单副本**：盘坏了历史没了，server 是控制面加监控的单点，接受。pprof 只听 127.0.0.1:6060，因为能让进程按要求采样，不放 Service 能连到的端口。
- **重建**：提交号全是数字被 `--set` 当成整数，概率约 3.7%，改 `--set-string`；全程约 35 分钟。
- **诚实的限定**：没有空转一天的数；etcd fsync p99 15.9 到 29.6 毫秒没查原因；停 broker 时 31 个下单失败没查原因；Redis 复制在 Pod 换地址后不恢复没查原因。
