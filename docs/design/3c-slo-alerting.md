# 3c 设计：SLO 与告警

状态：主人已确认（2026-11-22），第 2 节的十项都按建议；通知渠道用 Telegram；两个失败的手工备份 Job 已删。决定和理由动工后写进 ADR。

对应阶段计划 [phase-3-observability.md](../plan/phase-3-observability.md) 的 3c。3a 把数据收上来了，3b 量出了系统正常时的样子和极限（[3b-capacity.md](../notes/3b-capacity.md)），这一步回答“什么算坏了、坏了谁知道、知道了先看什么”。

## 1. 动工前查到的事实（2026-11-22）

### 1.1 集群上现在有什么

- Prometheus 3.13.5、Alertmanager 0.35.0（kube-prometheus-stack 93.3.1 带的）。Prometheus 保留 15 天。
- **告警规则 149 条，全部是 chart 自带的**，没有一条是我们自己写的。
- **Alertmanager 只有一个 `null` 接收器**：告警算得出来，发不到任何地方。
- 现在在响的 4 条：
  - `Watchdog`：设计如此，一直响，用来证明告警这条路是通的。
  - `KubeCPUOvercommit`、`KubeMemoryOvercommit`：3b 把 requests 按实测写满之后的预期结果（两个工作节点的 requests 合计 5.75、6.10 核，可分配 7.5，挂一个节点另一个放不下）。
  - `KubeJobFailed` 两条：11 月 18 日两个手工备份 Job 失败后留着没删（`mysql-order-0-backup-manual-261118173325`、`mysql-order-1-backup-manual-261118173344`，就是 3b 设计第 6 节记的那次备份 Job 被 OOMKilled）。响了四天没有人知道，正好说明现状。
- `docs/runbooks/` 不存在。

### 1.2 SLI 的原料

指标清单是 Haiku 子 Agent 从代码里检索的，下面用到的几条主会话在 Prometheus 上或代码里核对过；没有核对的在第 4 节标了“实现时核对”。

| 要量的 | 现有的指标 | 够不够 |
|---|---|---|
| 下单的请求数、状态码、耗时 | gateway 的 `http_server_request_duration_seconds`，标签有方法、路由模板、状态码；桶边界里有 0.2 秒 | 够 |
| 下单的业务结局 | order 的 `order_admissions_total{outcome}`（`admitted`、`insufficient_stock`、`backlogged`、各种 `*_unavailable`） | 够，但它看不到没走到 order 的请求（限流、gateway 自己的错） |
| 受理到落库 | `order_persist_delay_seconds`（桶到 30 分钟，有 5、10、20、30 秒几档） | 够 |
| 支付出结果到订单变成已支付 | **没有**。`payment_result_delay_seconds` 量的是支付单创建到出结果（模拟渠道的那一段）；订单一侧只有通用的消费者直方图，而且不含 outbox 里等的时间 | 不够，见第 2 节第 4 项 |
| outbox 里等了多久 | **没有**（`outbox.Pending` 有函数没人调） | 不够 |
| 消费者落后多少 | Kafka Exporter 的 `kafka_consumergroup_lag`（条数）；`kafkax_consumer_delivery_duration_seconds`（每条消息从写入到开始处理，只有消费在进行时才有数） | 分区被挡住时两个都答不了“最老的那条等了多久” |
| 对账差异 | **没有**。`recon` 是一次性命令（退出码 0 无差异、1 有差异、2 运行出错），集群里没有定时跑它的东西，只在压测和 chaos 之后手工跑 | 不够 |
| 库存异常 | `inventory_oversell_detected_total`、`inventory_reconcile_drifts_total`、`inventory_reconcile_repairs_total`、`order_gate_unclaimed_holds` | 够 |
| 熔断、降级、限流退回 | `breaker_state`、`degrade_switch_off`、`gateway_ratelimit_fallbacks_total` | 够 |

最近两天（含 3b 的几轮过载）`POST /api/v1/orders` 的状态码：202 是 388 万，**409 是 115 万（库存不足）**，429 是 14.8 万，503 是 3275，499 是 33。两件事由此定：

- 409 是业务上的正常答复，算进“坏的”的话售罄时可用性会掉到七成（3a 大盘那一节已经记过）。
- 5xx 占 0.063%，其中绝大部分出在有意打过拐点的几轮里。99.9% 的目标在平时够得着，过载时一定烧预算，这正是想要的。

### 1.3 三个会让告警悄悄失效的地方

都是前面的阶段撞到过的，规则要专门对付：

1. **没有流量时没有数。** 这个集群只有压测时才有请求。今天重新部署之后没有流量，落库延迟、支付延迟的直方图在 Prometheus 里查不到序列（计数器要等第一次记录才出现）。按比例写的规则这时是 0 除以 0，不响；有一两个请求时一次失败就是 50% 的错误率，乱响。
2. **推上来的指标没有 `up`。** 服务不导出了，序列只是停在最后一个值上然后消失（ADR-0030）。Collector 挂了，六个服务的指标一起没有，按阈值写的规则全部不响。
3. **坏到一定程度指标反而没了。** 复制停了 `Seconds_Behind_Source` 是 `NULL`，延迟的序列断掉（3a 验证 7）；exporter 连不上数据库时 `up` 还是 1（要看 `mysql_up`、`redis_up`）；`cdc_lag_ms` 在源库没有写入时一直涨，直接设阈值会在空闲时误报。

### 1.4 前面各阶段记给 3c 的告警

散在设计、ADR 和 3b 的各轮记录里，收在第 4 节的清单里，每条标了出处。

### 1.5 版本

不引入新的常驻组件。可能新增的只有 `promtool`（规则的单元测试用，版本跟 Prometheus 3.13.5 走），引入时到发布页核对并登记到 `versions.md`。

## 2. 主人确认的十项（都按建议）

| # | 问题 | 建议 | 主要的另一种做法 |
|---|---|---|---|
| 1 | 平时没有流量，SLO 拿什么算 | **在集群里常驻一个低速的探针**（约每秒 1 单，带支付和查询，用压测的号段和专门的 SKU，定期补库存）。它给 SLI 提供连续的数据，让“下单这条路断了”在没人压测时也能发现，也让 `cdc_lag_ms` 这类“要有写入才有意义”的指标一直有意义。代价：每天约 8.6 万单留在库里（按 3b 量到的每单占用，一天 0.1 GiB 的量级）；正式压测前要不要停它，实现时量一次它对结果的影响再定 | 不放背景流量：规则加“最近 5 分钟至少若干个请求”的门槛，平时不出数也不报。省事，但平时等于没有 SLO，告警只在压测时活着；阶段 4 的故障实验每次都要先起负载 |
| 2 | 可用性里 429 算什么 | **5xx 算坏，2xx、409 和其他 4xx 算好，499 不计入，429 也算好**。429 有两种：限流（客户发得太快）和背压（`BACKLOGGED`，系统有意拒绝超出落库能力的量）。容量结论说的就是“持续 125 到 150 单/秒，过载由背压拒绝”，拒绝是承诺的一部分。背压另外配一条业务告警（拒绝的比例持续偏高），不进 SLO | 背压的 429 算坏：SLO 更贴近“用户想下单下不了”。但 gateway 上分不出两种 429（都是同一个状态码），要改成用 order 一侧的 `order_admissions_total` 算，而它看不到没走到 order 的失败 |
| 3 | SLO 的目标值和窗口 | 窗口 **7 天**（Prometheus 只留 15 天，30 天的窗口算不出“还剩多少预算”；一周一个预算也正好对上阶段 4 每周做几次故障实验的节奏）。目标：**下单可用性 99.9%**；**下单延迟：99% 的受理在 200 毫秒内**（`GOALS.md` 的数；基线是 125 单/秒时 p99 45 毫秒）；**落库：99% 在 10 秒内**（基线 p99 2.4 到 3.2 秒，背压的 `ADMIT_MAX_WAIT` 是它的上界）；支付那一条的阈值等第 4 项的指标有了基线再定，暂定 99% 在 5 秒内 | 30 天窗口加把 Prometheus 的保留期延到 35 天：更像教科书，server 的系统盘只剩 22 GiB，放不下。目标定松一档（99.5%）：预算多三倍，告警少，练不到“预算烧完了怎么办” |
| 4 | “支付回调处理时延”没有指标 | **在 order 里加一个直方图**：订单从待支付变成已支付时，记“现在减去支付事件里出结果的时刻”。它量的是 outbox 等待、Kafka、消费三段加起来，也就是用户付完钱到订单显示已支付等了多久。这是订单状态的代码，Fable 写；只加一次 `Record`，不改行为。事件里有没有这个时刻、要不要加字段，实现时核对 | 不改代码，用 `kafkax_consumer_delivery_duration_seconds` 加处理耗时凑：少了 outbox 那一段，而 3b 第 1 轮查出的正是支付这条路在 outbox 上串行排队 |
| 5 | 另外三个缺的指标补不补 | **都补，都不为指标新增查询**：(a) `pkg/outbox` 的中继在发出每条消息时记“在表里等了多久”的直方图；(b) `pkg/kafkax` 的消费者报一个“正在处理和重试的消息里最老的那条有多老”的 gauge，分区被一条消息挡住时它会一直涨，这就是计划里要的“最老的未消费消息的年龄”；(c) 消费者整个不在了的情况靠 Kafka Exporter：有积压而已提交的位点若干分钟没有前进 | 只用 Kafka Exporter 的积压条数：不改代码。但条数换算不成时间（速率是零时除不了），而保留期是按时间算的，24 小时内要有人处理 |
| 6 | 对账差异怎么变成告警 | **`recon` 做成 CronJob**：每小时比最近 2 小时的，每天比最近 26 小时的（具体的范围和时刻量过一次耗时再定）。有差异时退出码是 1，Job 失败，按 Job 的失败和“上次成功距今多久”两条规则报。对账差异是最高一级的告警：它说的是钱、库存、订单三者对不上 | 让 `recon` 自己用 OTLP 报一个“差异数”的指标：能在大盘上画出来，但一次性的进程要保证退出前把数推出去，且“没跑”和“差异为零”在指标上分不清，还是得有 Job 那两条规则 |
| 7 | CDC 源库心跳（从 1c 一路挪过来的）做不做 | **第 1 项定了放探针的话，不做**，记进计划变更。心跳要解决的是“源库空闲时延迟的指标不可信”，探针让两个订单分片一直有写入，`cdc_lag_ms` 就可信了；“探针停了”自己有告警。主从延迟的告警按 3a 定的三条写（SQL 线程、IO 线程、延迟），不依赖心跳 | 做心跳表（cdc 定期往每个源库写一行）：不依赖探针，还能给出准的主从延迟；要给 cdc 写权限、加表、改流水线的过滤，是一块不小的改动 |
| 8 | SLO 的规则手写还是用工具生成 | **手写**。多窗口燃烧率一共十几条规则，自己推一遍阈值是这一步最值得学的东西；写成 `PrometheusRule` 清单入库，配 `promtool test rules` 的单元测试（CI 里加一项） | Sloth 或 Pyrra：写一份 SLO 的声明，规则自动生成，Pyrra 还带一个预算的界面。多一个组件，生成的规则是黑盒 |
| 9 | 通知发到哪 | **两个去向**：所有告警发到集群里一个只打日志的 webhook 接收端（几十行 Go，3d 的诊断 Agent 做出来之后换成它；人为触发告警时靠它确认“确实送到了”）；`critical` 一级另外发给主人。**主人定了用 Telegram**（Alertmanager 自带 Telegram 的接收端；机器人的 token 和会话号放 Secret 不入库；从集群出去到 Telegram 的接口要过 R2S 的代理，通不通实现时先验证） | 只在 Grafana 里看：不用配渠道，但没有“响了有人知道”这一环，告警疲劳、分组、抑制这些也就练不到 |
| 10 | chart 自带的 149 条怎么处理、runbook 写到什么范围 | 自带的**逐条过一遍分成三类**（留、改阈值、关），结果写成表入库；已经知道要关的是 `KubeCPUOvercommit`、`KubeMemoryOvercommit`（有意为之，原因写在 values 的注释里）。**两个失败的手工备份 Job 删掉**（Job 对象，不是数据；主人确认后 2026-11-22 已删，`KubeJobFailed` 随之消失要等规则下一次求值，实现第 5 步时核对）。runbook：**我们自己写的每条规则一份**（估计 35 到 40 条，相关的几条可以共用一份，例如主从的三条），放 `docs/runbooks/`，规则的 `runbook_url` 指过去；自带的规则沿用上游的 runbook 链接，只给验证时真的响过的几条补自己的 | 自带的全部关掉只留自己写的：干净，但节点、kubelet、etcd、Prometheus 自己的那批规则是现成的好东西，重写一遍没有意义 |

## 3. 设计

### 3.1 总体的样子

```
规则（PrometheusRule，入库）
  ├─ SLO：记录规则（各窗口的错误比例）→ 燃烧率告警
  ├─ 业务告警、基础设施告警、“监控自己”的告警
  └─ chart 自带的（筛过）
        │
        ▼
Prometheus ──▶ Alertmanager ──分组、抑制──┬─▶ webhook 接收端（全部；3d 之后是诊断 Agent）
                                          └─▶ 主人的渠道（只有 critical）
每条规则 ──runbook_url──▶ docs/runbooks/<告警名>.md
常驻探针 ──▶ 入口 ──▶ 六个服务（给 SLI 提供连续的数据）
recon CronJob ──▶ Job 成功或失败 ──▶ kube-state-metrics ──▶ 规则
```

文件放在哪：

- 规则：`deploy/cluster/monitoring/rules/*.yaml`，按来源分文件（`slo.yaml`、`business.yaml`、`middleware.yaml`、`platform.yaml`、`meta.yaml`），`monitoring.sh` 加 `rules` 子命令下发。
- 规则的单元测试：`deploy/cluster/monitoring/rules/tests/`。
- Alertmanager 的配置：`deploy/cluster/monitoring/alertmanager.yaml`（`AlertmanagerConfig` 或 values，实现时看哪个能表达全局的抑制规则）；渠道的密钥由脚本从本机的环境变量生成 Secret。
- 探针、`recon` 的 CronJob、webhook 接收端：清单各自入库。

### 3.2 SLI 和 SLO

| SLO | 好的事件 ÷ 有效的事件 | 目标（7 天） | 预算是多少 |
|---|---|---|---|
| 下单可用性 | `POST /api/v1/orders` 上不是 5xx 的 ÷ 全部（不含 499） | 99.9% | 按探针的每秒 1 单，一周约 600 个失败；按 125 单/秒压一小时，是 450 个 |
| 下单延迟 | 受理成功（202）且在 200 毫秒内的 ÷ 受理成功的 | 99% | |
| 落库时延 | 落库延迟在 10 秒内的 ÷ 落库的 | 99% | |
| 支付处理时延 | 出结果到订单变成已支付在阈值内的 ÷ 变成已支付的 | 99%，阈值量过再定 | |

几个取舍：

- **量在 gateway 上，不量在入口上。** 入口的 Envoy 的指标按上游集群分，不按路由分，分不出下单和查询。代价是 gateway 整个不在时这个 SLI 没有数据（不是 100% 失败），所以另配两条规则盖住它：入口上到 gateway 的 5xx 比例、探针自己报的失败。
- **延迟的 SLI 只数成功的请求。** 失败的请求已经在可用性里扣过一次；快速失败不该让延迟变好看。
- **延迟用“落在阈值内的比例”，不用 p99。** 分位数不能跨实例、跨时间窗口相加，比例可以；直方图的桶边界里要正好有阈值这一档（0.2 秒、10 秒都有）。
- **落库时延是异步的 SLO**，量的是“受理之后多久真的成了订单”。它和背压的参数是一对：队列上限 ÷ 落库速率就是最坏的等待，目标不能定得比它短。
- 查询订单的延迟不单独设 SLO（计划里没有），在业务大盘上有；以后要加，结构是一样的。

### 3.3 错误预算和多窗口燃烧率

燃烧率 = 实际的错误比例 ÷ 预算允许的错误比例（99.9% 的目标允许 0.1%）。燃烧率 1 表示正好在窗口结束时用完预算。

每个 SLO 三档，每档两个窗口同时超过才响（长窗口保证“烧掉的量值得叫人”，短窗口保证“现在还在烧”，故障一停告警很快就停）：

| 档 | 长窗口、短窗口 | 燃烧率 | 这个速度烧下去 | 级别 |
|---|---|---|---|---|
| 快 | 1 小时、5 分钟 | 14.4 | 一周的预算约 12 小时烧完 | critical |
| 中 | 6 小时、30 分钟 | 6 | 约 28 小时 | critical |
| 慢 | 1 天、2 小时 | 3 | 约 2.3 天 | warning |

- 14.4 和 6 沿用 Google SRE 手册里 30 天窗口的那一组：换成错误比例是 1.44% 和 0.6%，这个灵敏度和窗口多长没有关系，决定的是“多大的故障多久能发现”。窗口换成 7 天之后，同样的燃烧率烧掉的预算占比是原来的四倍多，所以慢的一档从“3 天、6 小时、1 倍”收紧成上表的样子。这一组数实现时用单元测试把“百分之几的错误持续多久会响、多久恢复”逐条算出来，和这里对一遍，对不上以算出来的为准。
- **记录规则**先把各个窗口的错误比例算好（`slo:order_create_availability:error_ratio_rate5m` 这一类），告警规则只做比较。长窗口的查询贵，算一次大家用；大盘上的“剩余预算”也读它。
- **流量太少时的保护**：每条燃烧率规则带一个“短窗口里至少有若干个有效事件”的条件。有探针时平时每 5 分钟约 300 单，一次失败是 0.33%，够不到 1.44%，不会因为单个请求响。
- 预算烧完之后的规矩（写进 runbook 的总则，不是技术手段）：停止做会烧预算的实验，先处理原因。这个项目里没有发布节奏可以冻结，这一条主要是为了面试时讲得清“预算是拿来花的，花完了要有后果”。

### 3.4 告警的分级和每条规则都要带的东西

| 级别 | 意思 | 去向 |
|---|---|---|
| `critical` | 用户正在受影响，或者再不处理会丢数据。现在就要有人看 | webhook 加主人的渠道 |
| `warning` | 余量在变少，或者某个保护机制在起作用。当天看 | webhook |
| `info` | 只给别的告警当上下文（例如“压测正在跑”） | 不通知 |

原则：

- **按症状叫人，按原因给线索。** `critical` 尽量只给 SLO 的燃烧率和“会丢数据”的那几条；节点 CPU 高、熔断器打开这类原因一级的是 `warning`，它们在 `critical` 响的时候出现在同一组里，告诉人先往哪看。
- 每条规则带：`severity`、`service` 或 `component`、`summary`（一句话，带当前的值）、`description`（影响是什么）、`runbook_url`、`dashboard`（大盘的链接）。标签只放有限的枚举，和指标标签的规则相同。
- 每条规则写明 `for`：瞬时的抖动不响。3b 量到过节点 95% 时 p99 自己能晃一倍，延迟类的 `for` 不短于 5 分钟。
- **“序列没了”单独写规则**，不指望阈值规则顺带发现（1.3 节的三件事）。

### 3.5 路由、分组、抑制、静默

- **分组**：按 `alertname` 加 `namespace`（或 `service`）。同一个原因引出的十个 Pod 的同名告警是一条通知。`group_wait` 30 秒、`group_interval` 5 分钟、`repeat_interval` 按级别（`critical` 1 小时、`warning` 12 小时），实现时看实际的吵闹程度再调。
- **抑制**（上游坏了，下游的不用再报）：
  - 节点不在 → 抑制这个节点上的 Pod、中间件实例的告警。
  - MySQL 实例连不上 → 抑制它的主从、连接数告警。
  - Prometheus 取不到服务的指标（Collector 挂了）→ 抑制六个服务的“没有数据”。
  - SLO 快的一档在响 → 抑制同一个 SLO 慢的一档。
  - 同名告警 `critical` 抑制 `warning`（chart 自带的那条留着）。
- **静默**是人手动加的、有期限的；计划内的操作（滚动中间件、故障实验）之前加，写清原因。`monitoring.sh` 加一个 `silence` 子命令，免得每次去界面上点。**压测时不自动静默**：过载的几轮里背压、节点 CPU 的告警本来就该响，那是验证的一部分。
- **`Watchdog`** 一直在响，路由到 webhook 接收端，接收端记下最后一次收到的时刻。集群外没有东西盯着它（谁来监控监控），这一点只记成考点，不做（要一个集群外的服务，见第 7 节）。

### 3.6 常驻探针（第 2 节第 1 项）

- 形态：集群里一个 Deployment，1 个副本，从入口的地址进来（和真实用户同一条路，经过入口的 Envoy 和限流）。
- 做什么：每秒约 1 次“下单 → 轮询到落库 → 支付 → 轮询到已支付”，穿插查询；一小部分走取消。用户号在压测的号段里（对账把它们当普通订单一样对），SKU 用一段专门的号，库存够用几周，快用完时由探针自己或者一个 CronJob 补。
- 用什么写：现有的 `scripts/cluster/probe.py` 只覆盖下单和查询、不支付；k6 的冒烟脚本覆盖全了但 k6 是为短时间的压测设计的。倾向用 Go 写一个小的常驻程序（复用 `test/e2e` 里手写的客户端），它自己报“每一步成功了没有、花了多久”的指标。实现时定。
- 它自己的告警：探针连续失败（`critical`，这是“下单这条路断了”最直接的信号）；探针没有数据了（`warning`）。
- 对按地址的限流：每秒几个请求，远低于 200。

### 3.7 对账（第 2 节第 6 项）

- CronJob 跑长期运行的那一套的 `recon` 镜像（`chaos.Dockerfile` 的注释里早就给它留了这个用途）。
- 两个频率：每小时一次比最近的（发现得快），每天一次比更长的范围（盖住每小时那次因为“还在途中”跳过的）。`recon` 发现差异后会隔一段时间复查（`-recheck`），在途的订单不会被当成差异。
- 规则：最近一次 Job 失败（`critical`）；上次成功距今超过两个周期（`warning`，说明对账自己没在跑）。退出码 1（有差异）和 2（运行出错）要分开，kube-state-metrics 有没有容器退出码的指标实现时核对，没有的话两种都按 `critical` 报、在 runbook 里教人看日志分。
- 失败的 Job 要留着日志（`failedJobsHistoryLimit`），差异的明细在日志里，Loki 里查得到。
- 它要读四个 MySQL 实例。耗时和对数据库的压力先量一次（3b 长稳之后对 180 万单跑过，`verify` 要 6 Gi 内存是另一个工具；`recon -since` 的耗时在记录里）；正式压测时要不要暂停它，和探针一起定。

### 3.8 runbook

一个告警一个文件（相关的几条共用时文件名取那一组的名字），固定的几节，3d 的诊断 Agent 会把它当输入读，所以结构要稳定：

1. **这条告警在说什么**：规则的表达式用人话讲一遍，阈值是怎么来的（指到 3b 的哪条记录）。
2. **影响**：用户看到什么，数据有没有风险，多久之内要处理。
3. **先看什么**：按顺序的三到五步，每步给现成的 PromQL、LogQL 或 kubectl 命令和大盘链接。
4. **常见的原因和各自的处理**：这个项目里真的遇到过的写在前面（例如“限流的 Redis 超时：先看节点 CPU，3b round-4”）。
5. **怎么确认恢复了**。
6. **怎么人为触发它**（告警质量检查用的那个办法）。
7. **历史**：每次真的响了记一行。

另有一份 `docs/runbooks/README.md`：总则（先止血还是先查因、预算烧完了怎么办、怎么加静默）和索引。

## 4. 告警清单（初稿）

阈值是按 3b 的记录拟的初值，实现时逐条在 Prometheus 上对着历史数据试一遍（两天的压测数据还在）。chart 自带的规则已经覆盖的不重写，表里标“自带”。

### 4.1 SLO

| 告警 | 条件 | 级别 |
|---|---|---|
| 下单可用性、下单延迟、落库时延、支付处理时延，各三档燃烧率 | 3.3 节 | 快、中 `critical`，慢 `warning` |
| 入口到 gateway 的 5xx 比例高 | 入口的 Envoy 上到 gateway 这个上游的 5xx 超过 1% 持续 5 分钟（gateway 整个不在时 SLI 没有数据，靠它） | `critical` |
| 探针连续失败 | 3.6 节 | `critical` |

### 4.2 业务

| 告警 | 条件 | 级别 | 出处 |
|---|---|---|---|
| 对账有差异 | `recon` 的 Job 失败 | `critical` | 计划；ADR-0022 |
| 对账没有在跑 | 上次成功距今超过两个周期 | `warning` | |
| 发现超卖 | `inventory_oversell_detected_total` 有增长 | `critical` | 计划“库存异常” |
| 闸门和库存账对不上 | `inventory_reconcile_drifts_total` 有增长 | `warning` | 计划“库存异常” |
| 下单成功率骤降 | `admitted` 占受理请求的比例比一小时前的同一个量低一半以上，且不是因为 `insufficient_stock` 变多（售罄是正常的） | `warning` | 计划；3a 大盘一节 |
| 背压在拒绝 | `backlogged` 占受理请求的比例超过 5% 持续 10 分钟 | `warning` | 第 2 节第 2 项 |
| 受理的队列快要超过等待上限 | `order_gate_unclaimed_holds` ÷ 落库速率超过 `ADMIT_MAX_WAIT` 的八成 | `warning` | ADR-0011、round-3 |
| 闸门内存到水位，受理被拒 | `order_admissions_total{outcome="gate_memory_full"}` 有增长 | `critical` | ADR-0034 |
| 有消息进了死信 | `kafkax_consumer_dead_lettered_total` 有增长 | `warning`；`order.submit` 上的是 `critical`（已受理的订单没有落库） | ADR-0021 |
| 消费者被挡住 | 最老的在处理的消息超过 10 分钟（`warning`）、超过 2 小时（`critical`；保留期是 24 小时，过了就丢消息） | | round-9 3.4 节 |
| 消费者没有在消费 | 有积压且已提交的位点 10 分钟没有前进（`order.close.delay` 这个组除外，它的积压是延迟关单的机制本身） | `warning` | 3a 第 5 步 |
| outbox 发不出去 | 中继发出的消息在表里等的时间 p99 超过 30 秒持续 5 分钟；以及“有支付出了结果而订单 5 分钟没有变成已支付” | `warning` | 3b round-1 |
| 支付单积压 | `payment_result_delay_seconds` 的 p99 超过 60 秒持续 5 分钟（支付超时是 15 分钟） | `warning` | round-1 |
| CDC 落后 | `cdc_lag_ms` 超过 60 秒持续 5 分钟（依赖探针保证源库有写入） | `warning`；超过 1 小时 `critical`（binlog 只留 1 天，超过要重做全量） | 3a 第 7 节、round-9 |
| 熔断器打开 | `breaker_state == 1` 持续 2 分钟（落库健康的那个单独一条，`critical`） | `warning` | round-3 |
| 降级开关被人关着 | `degrade_switch_off == 1` 持续 1 小时（防止实验做完忘了开回来） | `warning` | 1d |
| 限流退回进程内 | `gateway_ratelimit_fallbacks_total` 的速率大于每秒 1 次持续 5 分钟（退回时限额变宽） | `warning` | ADR-0019、round-4、round-8 |
| 展示读的名额在拒绝 | `order_bulkhead_rejected_total` 的速率持续偏高（长稳时平时每分钟十来次，阈值要高于它） | `warning` | round-9 |
| 某个服务没有指标了 | 六个服务各选一个一定存在的运行时指标，10 分钟没有新点 | `warning` | ADR-0030 |

### 4.3 网格和入口

| 告警 | 条件 | 级别 | 出处 |
|---|---|---|---|
| 入口在拒绝请求 | 入口到 gateway 的排队溢出、在途上限溢出的速率大于 0 持续 2 分钟 | `warning` | round-3 2.2、round-6 第 7 节 |
| 服务之间的并发上限溢出 | 边车的 `upstream_rq_active_overflow`（实现时核对裁剪后的指标名）速率大于 0 持续 2 分钟 | `warning` | round-4 |
| 实例被摘除 | 边车的摘除次数有增长 | `warning` | round-3 |
| KEDA 取不到数 | `keda_scaler_detail_errors_total` 有增长，或者 HPA 的 `ScalingActive` 不是 `True` | `warning` | 3a 第 7 步 |

### 4.4 中间件

| 告警 | 条件 | 级别 | 出处 |
|---|---|---|---|
| MySQL、Redis 连不上 | `mysql_up == 0`、`redis_up == 0`（不是 `up`） | `critical` | 3a 第 5 步 |
| 复制停了 | `replica_sql_running == 0`、`replica_io_running == 0` 各一条 | `critical` | 3a 验证 7 |
| 主从延迟大 | `seconds_behind_source` 超过 30 秒持续 5 分钟 | `warning` | |
| 该有延迟指标的从库没有了 | 清单里的从库 5 分钟没有延迟的序列，而 SQL 线程的状态也没有 | `warning` | 1.3 节第 3 条 |
| MySQL 连接数接近上限 | 超过 `max_connections` 的八成 | `warning` | round-6 4.3 |
| 闸门 Redis 内存 | 超过上限的 70%（`warning`）、80%（`critical`；85% 是停止受理的水位，策略是 `noeviction`） | | round-9 2.2、ADR-0033、ADR-0034 |
| Redis 的从库断了 | `redis_master_link_up == 0` 持续 2 分钟 | `warning` | 3a（`redis-cache-5` 断了 3 小时那次） |
| 缓存 Redis 集群不正常 | `redis_cluster_state == 0`，或者有槽位不是 `ok` | `critical` | |
| Sentinel 不够数 | 能看到的 Sentinel 少于法定数 | `warning` | |
| `Kafka` 资源不是 `Ready` | Strimzi 的自定义资源的条件（kube-state-metrics 要配自定义资源的状态，或者用 Strimzi operator 自己的指标，实现时定） | `warning` | round-8 6.3 |
| Kafka 有分区副本不足、没有 leader | Kafka Exporter 的两个指标 | `warning`、`critical` | |
| broker 不在 | `kafka_brokers` 少于 3 | `critical` | |
| Collector 在拒收、导出失败 | 它自己的 `refused`、`send_failed` 几个计数有增长 | `warning` | 3a 的 3.3 |
| MySQL 的备份没有成功 | 备份的 CronJob 上次成功距今超过 26 小时 | `warning` | |

### 4.5 节点和 Kubernetes

| 告警 | 条件 | 级别 | 出处 |
|---|---|---|---|
| 节点不在、kubelet 不在、Pod 反复重启、Deployment 副本不足、Job 失败 | 自带 | | |
| 节点重启过 | 节点的启动时间变了（从库的刷盘是放松的，节点掉电之后从库要重建，ADR-0032） | `warning` | ADR-0032 |
| 容器被 OOMKilled | 最近一次终止的原因是 `OOMKilled` 且重启次数有增长 | `warning` | 3b 设计第 6 节第 4 步 |
| 容器内存接近上限 | 工作集超过 limit 的 90% 持续 10 分钟 | `warning` | 同上 |
| 节点 CPU 持续高 | 超过 85% 持续 10 分钟（混合读写的拐点在九成上下） | `warning` | round-4 |
| 磁盘空间 | 自带的那两条按“预计多久写满”报，核对它们盖住了三台虚拟机的数据盘；另加绝对值：server 的系统盘剩余低于 15 GiB、agent 的低于 30 GiB（`local-path` 不限制卷的大小，PVC 的用量指标没有意义） | `warning`；server 低于 8 GiB `critical` | 3a 第 3 步、round-9 第 3 节 |
| etcd 的刷盘慢 | 自带；2b、3a 记过 fsync p99 一直在 8 到 16 毫秒，看它现在响不响、阈值合不合适 | | 3a 笔记 |
| 入口的 Envoy 不在工作节点上 | 入口的 Pod 调度到了 k3s-server（round-8 遇到过，手工挪回去的） | `warning` | round-8 |

### 4.6 监控自己

| 告警 | 条件 | 级别 |
|---|---|---|
| Prometheus、Alertmanager 自己的（规则求值失败、通知发不出去、配置重载失败） | 自带 | |
| 抓取目标不在 | 自带的 `TargetDown` | |
| 通知一直发不出去 | Alertmanager 到某个接收端的失败数持续增长 | `warning`（发到 webhook 接收端；主人的渠道坏了的话这条恰恰送不到，写进考点） |

## 5. 验证

计划里的“告警质量检查：人为触发每条告警，确认能响且不误报”。三层：

1. **规则的单元测试**（`promtool test rules`）：给一段构造的序列，断言哪一刻响、哪一刻不响。重点测三类：燃烧率的三档（多大的错误比例、持续多久）；没有流量和只有零星流量时不响；序列消失时该响的那几条会响。进 CI。
2. **在集群上真的触发一遍。** 每条自己写的规则触发一次，确认：Prometheus 里从 `pending` 到 `firing` 的时间和 `for` 对得上；webhook 接收端收到了，分组和抑制符合预期；runbook 的“先看什么”照着做能走到原因；故障撤掉之后告警恢复。触发的办法大多是前面阶段用过的故障注入，**动手之前逐条登记到阶段计划里**（AGENTS.md 的规矩），下面是打算用的：
   - 可用性、延迟的燃烧率：把 inventory 缩到 0 一段时间（3a 用过）；`make cluster-fault` 的场景；用压测打过拐点。
   - 落库时延、背压、队列：压测打过写入的拐点（3b 的普通下单阶梯）。
   - 消费者被挡住、outbox、支付处理时延：把 orderindex、order 的消费者停掉（3a 用过）；停 payment。
   - 主从、CDC：`STOP REPLICA SQL_THREAD`、停 cdc（3a 用过）。
   - 闸门内存：临时把 `maxmemory` 调低而不是真的灌满。
   - 磁盘、内存这类不方便真做的：临时把阈值改到当前值以下，响了再改回来，并说明这只验证了“路是通的”。
   - 对账差异：在 `oppenheimer-e2e` 里手工改一行数据（不动长期运行的那一套的库）。
   - `Kafka` 资源不是 `Ready`：这个要想一个不伤 broker 的办法，实现时定，定不出来就只做单元测试并写明。
3. **放一周看误报。** 所有规则上线后空转（只有探针的流量）几天，记下每一次响：是真的、是阈值不对、还是规则写错了。阶段 4 开工前按这份记录调一轮。

验收对着计划的那一句：每条告警都被真实触发验证过，并有对应 runbook。第 2 层里只做了“改阈值”或者只有单元测试的，在结果里单列，不算“真实触发”。

## 6. 实现顺序和分工

1. **补指标**（第 2 节第 4、5 项）。`pkg/outbox`、`pkg/kafkax` 是公共库，在主线上串行做完再分出去：order 的支付处理时延由 Fable 写；outbox 和 kafkax 的两个派 Opus，Fable 评审。`make lint test`，部署，量一次基线，定支付那一条 SLO 的阈值。
2. **探针**（Opus 写程序，Sonnet 写清单）和 **`recon` 的 CronJob**（Sonnet）。可以并行。
3. **SLO 的规则和单元测试**（主会话写规则，这是这一步的核心；Sonnet 按主会话给的用例表写 `promtool` 的测试和 CI 的那一项）。
4. **其余的规则**：业务的由主会话写；中间件、节点的派 Sonnet（给它第 4 节的表和每个指标的准确名字），主会话在 Prometheus 上逐条对着历史数据验收。
5. **筛 chart 自带的 149 条**（Haiku 列出每条的表达式和现在的状态，主会话定去留）。
6. **Alertmanager 的路由、抑制，webhook 接收端，主人的渠道**（主会话定配置，Sonnet 写接收端和清单）。
7. **runbook**（Sonnet 按 3.8 的模板写，一批五六份；主会话给每份的“常见原因”指到具体的记录，逐份验收）。
8. **第 5 节的验证**（主会话；要动集群，逐条登记）。
9. 大盘上加一块 SLO（剩余预算、各窗口的燃烧率）；ADR（SLO 怎么定的、为什么 7 天、429 算什么、为什么手写）；阶段笔记；面试考点。

第 8 步里有几项要发压，发压时本机不跑测试和构建。

## 7. 不在这一步做的

- 诊断 Agent：3d。这里只留好接口（webhook 的格式、runbook 的结构）。
- 集群之外盯着 `Watchdog` 的东西（dead man's switch）：要一个集群外的服务，现成的是第三方的托管服务。只讲清为什么需要。
- 值班轮换、升级策略、事故复盘的流程：一个人的项目，记成考点。
- Prometheus 的长期存储（30 天以上的 SLO 窗口）、告警的高可用（Alertmanager 多副本的去重）。
- 给查询接口单独设 SLO；按用户或商家分的 SLO。
- 把 `gateway` 的堆缓慢上涨、`read_db_1` 的舱壁这几件 3b 没查原因的小事查清：它们会在第 5 节放一周的时候自己出现或不出现，到时再说。

## 8. 实现中的发现和量到的数

### 第 1 步：补指标（2026-11-22，进行中）

- **支付处理时延：`order.payment.delay`**（Prometheus 里是 `order_payment_delay_seconds`），没有标签。`PaymentEvent` 里本来就有 `occur_time`，是支付服务写结果的那个事务用的时刻（`processor.go` 的 `event`），不用加字段。order 在把订单置为已支付、事务提交之后记“现在减去它”。重复投递、要求退款、事件里没有这个时刻的都不记；负数截成 0（两个进程的时钟）。用的是到 30 分钟的那组桶（名字以 `.delay` 结尾），有 1、2、5、10 秒几档，SLO 的阈值只能取桶的边界。
- **outbox 里等了多久：`outbox.relay.delay`**（`outbox_relay_delay_seconds`），标签是 topic。中继每发出一条被确认的消息记一次。年龄是在取待发行的那条 SELECT 里用数据库自己的时钟算的（`TIMESTAMPDIFF(MICROSECOND, created_at, CURRENT_TIMESTAMP(6))`），再加上从取出到 `Send` 返回在进程里过的时间；没有新增查询。不把 `created_at` 读回来和进程的时钟相减：`DATETIME` 没有时区。它的盲区：中继整个停住时一条都不记，直方图没有新点，所以“outbox 发不出去”的告警不能只看它的分位数，还要有“支付出了结果而订单没有变成已支付”这一类从两头对数的规则。Opus 子 Agent 写的，主会话读过 diff。
- **消费者手里最老的消息：`kafkax.consumer.oldest_pending_age`**（秒，标签是 topic 和消费组，每个 topic 取各分区的最大值），做完了。子 Agent 读代码后停下来问了一件事：串行的消费者（默认）取出一条消息之后，它只在处理它的 goroutine 的局部变量里，已有的数据结构给不出“正在处理的是哪一条”；而“队列里的第一条”是它后面那条，一条消息把分区挡住时队列是空的，读出来正好是 0。定了给 worker 加一个字段记正在处理的那条。这个指标回答的是“有没有一条消息把分区挡住了、挡了多久”，是下界：没拉到进程里的消息它看不到，整体落后多少仍然看 Kafka Exporter。写规则时要注意的：同一个消费组的几个实例各报各的，要对实例取 `max`；延迟消费者从到期时刻算起，没到期的是 0；分区被收回、位点还没提交完的最多 10 秒里被挡住的那条仍然算数；阈值要对着“处理一条消息该花多久”定，不是对着整体的落后量。按批消费退回逐条处理时要重新指一次“正在处理的那条”，是子 Agent 实现时发现并补上的。它写的 lane 模式的测试第一次在完整的测试里失败了：key 到 lane 的哈希种子每个进程随机，测试却等所有别的 key 做完，而和被挡住的 key 同一条 lane 的出不来；改成只等位点在前面的那个 key。
- 三个指标合在一起 `make lint test` 通过（11 个模块 0 issues，56 个包）。集成测试直接用 `go test` 跑、不带 Makefile 的环境变量时会全部被跳过并显示 `ok`，第一次就是这样误以为通过了。

### 第 3 步的前一半：SLO 的规则（2026-11-22）

- `deploy/cluster/monitoring/rules/slo.yaml`：四个 SLO，各六条记录规则（5 分钟到 1 天的错误比例）、一条有效事件的速率、三条燃烧率告警，共 40 条。**还没有下发到集群**，只用 `kubectl apply --dry-run=server` 确认过清单合法。单元测试还没有写。
- 拿最近两天的数据（含 3b 的过载轮次）试算过四条错误比例的表达式：都有值；可用性的 1 小时窗口最高到过 3.5%，下单延迟到过 45%，落库时延到过 75%，都出在有意打过拐点的轮次里。也就是说这几轮重跑的话快的一档会响，这是想要的。
- 只算命名空间 `oppenheimer`：`oppenheimer-e2e` 里的失败是测试故意制造的。

### 第 2 步的一半：`recon` 的 CronJob（2026-11-22）

- 清单 `deploy/cluster/recon/cronjob.yaml`，`make recon-cron`（`services.sh recon-cron`）装；`services.sh recon-cron-suspend`、`recon-cron-resume` 暂停和恢复。Sonnet 子 Agent 写的，主会话读过全部 diff。**已经装到集群上**：`recon-hourly`（每小时第 7 分钟，`-since 2h`）、`recon-daily`（UTC 21:30，装的时候是 `-since 26h`，2026-11-22 主人定了改成全量：不带 `-since`，加 `ARCHIVE_DSN` 读冷库），镜像是服务镜像 `recon:daac679`。
- 用的是 `recon` 自己的 `-settle` 默认值 30 分钟：只比 30 分钟之前创建的订单。每小时那次的窗口是“2 小时前到 30 分钟前”，相邻两次重叠 30 分钟，没有缝。`-recheck 30s`，不重试（`backoffLimit: 0`：重跑一次在途的状态可能就变了，失败被盖住）。
- **手工触发了一次**（`kubectl create job --from=cronjob/recon-hourly`）：20 秒跑完，退出码 0，差异 0。**窗口里一单都没有**（冒烟是在它的上界之后下的），所以这一次只验证了“连得上四个库、参数对、能跑完”，没有验证它在有订单时的耗时和内存。一个没查的现象：窗口里 0 行时 `index` 这一对的第一遍用了 16.7 秒，其余几对是几毫秒。
- **两种失败在指标上分不开**：有差异（退出码 1）和运行出错（退出码 2，例如连不上库）都是 Job 失败；kube-state-metrics 里没有能用的退出码指标（`restartPolicy: Never` 的容器没有“上一次终止”）。两种都按 `critical` 报，runbook 里教人看日志分。“上次成功距今太久”那条规则要排除 `kube_cronjob_spec_suspend == 1`，否则压测前暂停了它就响。
- **子 Agent 指出的、还没有处理的问题**：
  1. **增量对账看不到“很久以前的订单后来被改错”**：两个 CronJob 都只比创建时间在窗口里的订单。1d 的设计（7.17）说增量要配周期性的全量，这里没有。全量要读全部 1470 万单，耗时没有量过。要不要加一个每周的全量，等主人定。
  2. 消费者停住、从库落后超过 30 秒时，`recon` 会把“还没追上”报成差异。这时报出来不算错，但 runbook 要写“先看积压和主从延迟，再怀疑数据坏了”。
  3. CronJob 的镜像 tag 是装的时候定的：`make deploy` 之后要再 `make recon-cron`，否则它一直跑旧的。
  4. 暂停只挡新的一次，已经在跑的不停。空表对照（`loadtest.sh empty-up`）期间 order 指到 `e2e_*` 库而 CronJob 仍对着正式的库，那段时间也要先暂停。
  5. 它读两个订单主库（不只是从库），正式压测前要暂停。

### 第 2 步的另一半：常驻探针的程序（2026-11-22）

- **做成了压测工具的一个子命令 `load probe`**（`test/load/probe_cmd.go`、`test/load/internal/probe/`），没有另开模块：下单、轮询、解析响应的代码 `load run` 里本来就有，提到了 `test/load/internal/gwapi` 里两边共用，`load run` 发出的请求不变。Opus 子 Agent 写的，主会话读过周期和轮询的全部逻辑；`test/load` 的 lint 0 issues，`make test MODULES=$PWD/test/load` 通过。
- **一个周期**：查库存 → 下单 → 轮询到待支付 → 支付（每 10 个周期改成取消）→ 轮询到已支付；每 5 个周期查一次订单列表。开环的节拍（上一轮没完不推迟下一轮），在途的周期最多 20 个，超过就跳过并计数。不重试，轮询尊重 429 的 `Retry-After`。只走 HTTP 入口，不碰数据库。
- **它报的指标**（服务名 `probe`）：
  - `probe.steps{step, outcome}`：`step` 是 stock、create、land、pay、paid、cancel、list；`outcome` 是 `ok`、`rejected`（429）、`sold_out`（库存不足或超出限购）、`degraded`（带降级原因码的 503）、`error`、`timeout`。
  - `probe.cycles{outcome}`：`ok`、`incomplete`（被拒、售罄、降级）、`failed`（有一步是 error 或 timeout）、`skipped`。
  - `probe.step.duration{step}`（请求的耗时）和 `probe.step.delay{step=land|paid}`（等待的两步，用到 30 分钟的那组桶），都只记结果是 ok 的。
- **订单怎么认**：用户号在 [2^40 − 2^20, 2^40)，和 k6 默认的号段不重叠；幂等键前缀 `probe-`；请求带 `X-Load-Run: probe`。
- **SKU 的要求**：限购必须是 0（探针的用户号是轮流的，同一个用户约 12 天回来一次，有限购会被记成 `sold_out`）；每天约消耗 7.8 万件。
- **写告警时要按它的 `outcome` 分开的几种情况**（子 Agent 提的，都成立）：
  1. SKU 卖完、没建、没装进闸门时是 `sold_out` 或 `error`：这是探针自己的配置坏了，不是下单的路断了。“探针连续失败”只看 `failed` 里的 create、land、pay、paid；`sold_out` 另配一条。
  2. 探针的 Pod 不在了、卡住了，指标是断掉：用“周期数的速率是 0 或者没有序列”报；`skipped` 持续增长说明探针或入口卡住了。
  3. 探针和六个服务共用 Collector：Collector 挂了它的指标也没有，要靠抑制。
  4. 压测时如果发压机和探针出口是同一个地址，会共用按地址的限流；被 429 拒掉记成 `rejected`、周期 `incomplete`，不报警，但那段时间探针也就没有数据。（探针在集群里，发压机在外面，出口地址本来不同；入口看到的各是什么地址没有核对。）
- **它带来的偏差，定 SLO 时要记着**：平时探针就是全部的流量，SLI 反映的是“空系统上每秒 1 单”的表现，比真实负载下好看；它只下 1 件、单 SKU 的订单，不覆盖商家列表、查支付单、归档、超时关单、退款、多件商品；每秒 1 次，低于约 1% 的错误率要很久才看得出来；单个分片坏了只造成一半的周期失败。
- 没有做的：探针的请求不带链路（日志里没有 trace id）；计时类的测试用的是真实的短时长，没有用 `testing/synctest`。

### 探针上线（2026-11-22，12:24 UTC 起）

- 清单 `deploy/cluster/probe/deployment.yaml`，`make probe`（`services.sh probe`）装，`services.sh probe-stop`、`probe-start` 停和启。Sonnet 子 Agent 写的，主会话读过。镜像是带 `load` 的 chaos 镜像；放在 k3s-server 上（软性的偏好：阶段 4 关的是工作节点，探针不能跟着被关掉，也不和被测的服务抢 CPU）；不进网格，从入口的地址进来（`-addr http://192.168.50.51 -host api.oppenheimer.test`）。
- **主会话改的一处**：子 Agent 让镜像的 tag 默认沿用 order 的，而镜像是从工作区构建的，会把没提交的代码装进一个指向别的提交的名字里（它自己在报告里指出了这个问题）。改成和 `make deploy` 一样按工作区算。现在跑的是 `chaos:daac679-dirty-20261122122350`，**提交之后要再 `make probe` 一次换成干净的 tag**。
- 探针专用的 SKU：9001 到 9010（`loadtest.sh skus 9001 10`，各 10 亿库存，限购 0）。按每天 7.8 万件算够用几十年，不需要补库存的机制。装之前脚本会检查这些 SKU 存在且限购是 0。
- **上线后的头几分钟**：每分钟 60 个周期全部 `ok`，没有 `failed`、`skipped`。Prometheus 里探针的指标带着 `service_name="probe"`、命名空间、节点。四条 SLI 的表达式都有值了：下单的有效事件速率 0.99 次/秒，可用性和延迟的错误比例是 0。探针的 Pod 用 3 毫核、6 Mi。
- **头几分钟的读数（不是基线，基线等跑满一两个小时）**：下单 p99 约 17 毫秒，落库延迟 p99 约 0.09 秒，支付处理时延 p50 约 0.08 秒。探针自己量的“等落库”p99 约 0.5 秒、“等已支付”约 1 秒，比服务端量的大得多：它是轮询出来的，第一次问在 200 毫秒之后、之后翻倍，量到的是“下一次轮询时已经好了”。所以**落库和支付的 SLO 用服务端的直方图，探针的这两个数只用来发现“等不到”**。
- 没有验证的：`probe-stop`、`probe-start`；探针在入口看来是什么来源地址（和发压机会不会共用按地址的限流）。

### 全量对账的耗时（2026-11-22，12:57 UTC）

主人问全量对账随数据量增长是否可行，先量了一次：`RECON_FULL=1 RECON_SETTLE=30m scripts/cluster/loadtest.sh recon`（四对，不带 `-since`），探针在跑着。

- **从起 Pod 到结束 1 分 55 秒，差异 0。** 订单 1473.7 万行，订单明细 2473.5 万行，已支付的 180.8 万单。
- 各对的第一遍：订单对商家索引 40.7 秒，主从的订单表 22.2 秒，订单对预占 19.5 秒，主从的明细表 17.9 秒，订单对支付单 9.1 秒，主从的支付结果 1.7 秒。合计约 111 秒，折下来每 100 万单约 7.5 秒。
- 摘要是在 MySQL 里算的（报告里 `rows_read=0`：没有差异时一行明细都不用读回来），所以快；代价是这两分钟里四个实例各有两条并发的全表聚合。**这段时间数据库的 CPU、探针的延迟有没有受影响没有看。**
- 线性外推（没有实测）：1 亿单约 13 分钟，10 亿单两个小时以上。**所以在这个项目的量级上全量对账是便宜的，每天跑一次都可以；“随总量线性增长、迟早跑不动”这个判断仍然成立，只是拐点比主会话事先估的远得多。**
- 主会话之前对主人说“全量要读全部 1470 万单，代价大，耗时没有量过”，量出来不到两分钟，那个担心在现在的数据量上不成立。
- 对账方式按场景拆（钱对笔数和金额、库存对守恒式、派生数据按 CDC 的变更核对）和什么时候做，和主人讨论过，建议放在阶段 4 的故障实验之前；**主人还没有定**，定了再改计划、写 ADR。

### `recon-hourly` 第一次自己跑（2026-11-22，13:07 UTC）

- 到点自己起来了，18 秒跑完，退出码 0，差异 0。窗口是 11:07 到 12:37，里面有 983 单（冒烟的和探针头十几分钟的），其中已支付 804 单，主从、支付单、预占、商家索引都对上了。这是它第一次在有订单的窗口上跑通。
- **订单对商家索引这一对在增量时慢得不成比例**：983 行用了 15.0 秒，别的几对都是十毫秒上下；空窗口那次也是 16.7 秒；而全量 1454 万行才 40.7 秒。也就是说它的耗时几乎不随窗口大小变，像是商家索引那一侧按订单号的范围取数时走不了索引（那张表是按商家号组织的）、每次都扫大半张表。**这是从三个耗时推的，没有看执行计划。** 现在每小时多花 15 秒不碍事；对账方式重做时这一对正好是“按 CDC 的变更核对”的对象。

### 钱的笔数和金额（2026-11-22，主人定的：从“对账方式按场景拆”里提前到 3c）

主人提出对账该按场景用合适的方式：钱用笔数和金额，数据一致性用 CDC 或事件。整体放在 3d 之后（计划文件的 3e），钱这一块提前做成了指标。代码在 `services/recon/internal/totals`（规则写在包注释里）和 `recon watch-totals` 子命令，Fable 写的；清单 `deploy/cluster/recon/totals.yaml`，`make recon-totals` 装。

- **比的是什么**：支付一侧“成功且订单已答复”的支付单（`payments` 里 `status = SUCCEEDED AND notified = 1`），对订单一侧记下的“已接受”的支付结果（`payment_results` 里 `succeeded = 1 AND disposition = ACCEPTED`）。按支付单号分成一分钟一段（单号的高位是创建时间），两边各算笔数和金额合计，逐段比。每一轮是支付库一条、每个订单库一条按主键范围的聚合，默认每 30 秒一轮、看最近 30 分钟。
- **为什么不用“等几分钟再比”**：跨库的数据总有在途的。常见的做法是只比若干分钟之前的，容差短了误报、长了发现得晚；高负载下在途的量一直有几十笔，按时间切一刀两边永远差一点。这里用支付单上已有的状态把在途的精确分出来：
  - 订单服务是先在自己的事务里写下“接受”，提交之后答复才发得出去；答复是“要求退款”时，支付单在同一条 UPDATE 里变成已退款并打上已答复的标记。所以**任何时刻，支付一侧“成功且已答复”的集合都包含在订单一侧“已接受”的集合里**，多出来的就是答复还在路上的。
  - 一段里没有待出结果的、也没有未答复的支付单时，这一段不会再变（单号按时间递增，旧的一段不会再进新的支付单），叫“封口”。
  - 两条规则，都不带时间容差：**封口的段两边必须相等；没封口的段支付一侧不能比订单一侧多。** 先读支付库、后读订单库，包含关系对更晚的快照只会更宽松，不会因为两次查询之间又处理了几笔而误报。
- **指标**（服务名 `recon`）：`recon.totals.mismatch{kind=count|amount}`（差的笔数、金额，平时是 0）、`recon.totals.count{side}` 和 `recon.totals.amount{side}`（封口的段上两边的合计）、`recon.totals.in_flight`、`recon.totals.open_buckets`、`recon.totals.oldest_open_age`（最早的没封口的段有多老）、`recon.totals.runs{outcome}`。有差异时每一轮打一条 Error 日志，带那一分钟的起止时刻和两边的数，拿这个范围去跑逐行对账就能找到是哪一笔。还没有成功核对过时不报瞬时值（没有序列），不报 0。
- **它发现不了的**：同一段里一笔多、一笔少正好抵消（合计类对账固有的，逐行对账兜底）；订单库内部“已接受”和“订单是已支付”的一致（同一个事务保证，不在这里核对）；滑出 30 分钟窗口时还没封口的段（由 `oldest_open_age` 的告警和每小时的逐行对账盖住）。核对自己失败时瞬时值停在上一次成功的结果上，告警必须同时看 `runs{outcome="ok"}` 有没有在涨。
- **测试**：规则的九种情况（纯函数）；对着两边真实的迁移建出来的表，造一致的、缺订单记录的、订单接受了而支付已退款的、订单接受了支付库里没有的、金额不等的、没封口而支付一侧多的；指标在第一次成功之前不报、失败时计数。recon 模块 lint 0 issues，测试通过。
- **上线（14:02 UTC）**：Pod 在 k3s-server 上，1 毫核、3 Mi。第一批读数：封口的段两边都是 1620 笔、3223800 分（每单 1990 分），差异 0，在途 0。镜像是工作区的 tag，**提交之后要再 `make recon-totals` 换成干净的**。告警规则还没有写（业务告警那一步）：`mismatch > 0`（最高一级）、`oldest_open_age` 过大、`runs{outcome="ok"}` 不涨。
- **没有验证的**：有压测负载时在途的量和每一轮的耗时；真实的差异下它报得对不对只在测试里验证过，没有在集群上人为制造过（留到告警质量检查，在 `oppenheimer-e2e` 里做）。

### 第 3 步的后一半：SLO 规则的单元测试和下发（2026-11-22）

- `make rules-test`：`promtool check rules` 加 `promtool test rules`，四个测试文件 23 个用例，Sonnet 子 Agent 按主会话给的用例表写的。`promtool` 3.13.5（跟集群的 Prometheus）和 `yq` v4.54.3 装在 `./bin`，下载时校验 sha256，登记在 `versions.md`。CI 里加了 `rules` job。`monitoring.sh rules` 下发规则。
- **测试查出主会话写的规则的一个真问题，已修**：告警的表达式是“长窗口 and 短窗口 and 流量”，通知里的 `$value` 取的是最左边那一项，也就是长窗口的比例，而描述写的是“短窗口里现在是 X”。全部失败 5 分钟时，通知里显示的是 8.3%（1 小时窗口），实际是 100%。把短窗口挪到最左边，那条用例的断言现在钉着这个顺序。
- 用例钉下来的几个时刻，和 3.3 节的推算一致：5% 的失败约 19 分钟后快的一档响（1 小时窗口要 17.3 分钟才超过 1.44%），故障停了约 5 分钟后恢复；全部失败约 3 到 4 分钟响；1% 的失败约 3 小时 41 分钟后中的一档响；0.4% 的失败约 18 小时 15 分钟后慢的一档响。
- **测试钉下来、写 runbook 和其余规则时要用的行为**：
  1. **指标断更时告警自己消失**：故障还在而指标不上来了，记录规则没有值，快的一档约 5 分钟内自己停掉。靠探针和入口的规则兜底，“某个服务没有指标了”那条必须有。
  2. **直方图里没有阈值那一档的桶时，错误比例是 100%**（规则里“好的”那一项补了 0）：会大声误报而不是静默。改 SLO 的阈值只能取桶的边界，runbook 里写“先查 `le`”。
  3. 每秒不到 0.2 个有效事件时三档都不响，哪怕全部失败：这是有意的，靠探针保证平时有流量。
  4. 刚装上或者清过数据的头一天，长窗口里的数据不满，比例不会被冲淡，中、慢两档容易提前响。
- **已下发到集群**（14:15 UTC 前后）：`PrometheusRule` `oppenheimer-slo`，40 条全部求值正常，12 条告警都没有触发，四条 SLI 的 5 分钟错误比例都是 0。Alertmanager 仍然只有 `null` 接收器，响了也发不出去。
- 没有验证的：CI 的 `rules` job 没有实际跑过（Linux 上的下载和校验分支只读过代码）；`monitoring.sh rules` 删除多余规则对象的那一段没有触发过。

### 探针下的基线（2026-11-22，12:24 到 14:20 UTC 的 90 分钟）

每秒 1 单、没有别的流量时，服务端量到的：

| | p50 | p99 | p99.9 |
|---|---|---|---|
| 下单（受理，202） | 7 毫秒 | 10 毫秒 | 19 毫秒 |
| 受理到落库 | 36 毫秒 | 85 毫秒 | 99 毫秒 |
| 支付出结果到订单已支付 | 78 毫秒 | 0.2 秒 | 0.2 秒 |

- 探针 5399 个周期全部 `ok`，没有 `failed`、`incomplete`、`skipped`。支付处理时延 4861 次全部在 1 秒内，落库 5397 次全部在 1 秒内。
- 分位数是从直方图的桶里插值出来的，p99 以上的数受桶边界限制（支付那两个 0.2 秒就是桶的边界），只当量级看。
- **支付处理时延的 SLO 阈值保持 5 秒**。空载的基线是 0.2 秒，但这条路在负载下慢得多（3b 第 1 轮：支付出结果每秒 76 笔就到顶；今天 10 单/秒的冒烟 p99 是 0.36 秒），有负载时的数还没有量。下一次在 125 单/秒上跑的时候量一次，那时再看要不要收到 2 秒（桶的边界有 1、2、5）。


### `recon-daily` 改成全量（2026-11-22，主人定的）

- 清单里去掉 `-since 26h`，加 `ARCHIVE_DSN`：全量要读冷库，否则已归档订单的支付单、预占、索引在订单一侧找不到对应的行，全部报成差异。每小时的那个不用（窗口 2 小时，里面的订单不会被归档）。
- 16:05 UTC 手工触发过一次（`kubectl create job --from=cronjob/recon-daily`，看完已删）：1 分 57 秒，1474.9 万单，差异 0；订单一侧比不带冷库时多一条语句，是读冷库的。到点自己跑的第一次是 21:30 UTC。

### 第 4 步的一半：业务一侧的规则（2026-11-22）

`deploy/cluster/monitoring/rules/business.yaml`，8 个组，1 条记录规则加 31 条告警，主会话写的；测试 `tests/business_test.yaml` 20 个用例。**已下发**（`monitoring.sh rules`），32 条求值正常，没有一条在响。Alertmanager 还没配接收端。

- **每条表达式都在 Prometheus 上求过值**：不报错，现在没有结果。指标名和标签是对着代码和 Prometheus 查的，设计第 4 节里凭印象写的名字有几处不对：熔断器的标签是 `breaker` 不是 `name`；`payment_results_total` 的标签是 `result`；`order_transitions_total` 是 `from`、`to` 两个标签。
- **“有增长”的写法**（文件头注释第 1 条）：超卖、死信这类计数器平时没有序列，第一次 `Add` 才有。`increase()` 在序列从无到有的那一下是 0，第一次发生恰恰报不出来。所以写成两半：已有的序列又涨了，或者窗口之前没有、现在有了。第二半要逐条 `unless` 再 `sum`，否则 Pod 重启后新序列会被别的 Pod 的旧值盖住。三种情形都有测试（用例 a、b、c）。
- **对账失败不直接写 `kube_job_status_failed > 0`**：失败的 Job 为了留日志保留 5 个，那样告警在人删掉 Job 之前一直响，下一次对上了也不恢复。写成“最近一次失败的 Job 的开始时间晚于上次成功的时间”。退出码 1 和 2 在 kube-state-metrics 里分不开，都按 `critical`，runbook 里教人看日志分。
- **比设计多出来的几条**：`ReconDailySlow`（全量对账超过 20 分钟，时限 55 分钟；改成全量之后要盯耗时）；`PaidOrdersNotLanding`（支付在出成功的结果而 10 分钟没有订单变成已支付，`critical`；这种时候支付处理时延的 SLO 没有观测值、算不出来）；`CDCPipelineMissing`（按管道的清单查序列在不在）；钱的核对三条（有差异 `critical`、核对自己停了、有支付单 20 分钟没封口）。
- **人为停掉的不报**：探针的副本数是 0 时 `ProbeNoData` 不响，对账的 CronJob 暂停着时 `ReconNotRunning` 不响。`recon-totals` 没有“停”的子命令，缩到 0 的话 `PaymentTotalsCheckStalled` 会响，要加静默。
- **做过反向验证**：把“从无到有”那一半删掉、把探针的分子改成只算 `failed`，测试会失败；改回来通过。
- **已知的弱点，没有处理**：
  - `PaymentReplyOverdue` 在支付单滑出 30 分钟的核对窗口之后会自己恢复，问题可能还在（注释和描述里写了）。
  - `ServiceMetricsMissing`、`CDCPipelineMissing` 的清单是写死的，加服务、加分片时要跟着改。
  - `ConsumerNotConsuming` 的指标来自 Kafka Exporter，没有命名空间的标签，按消费组的名字限定；`oppenheimer-e2e` 如果用同名的消费组、同一个 Kafka，会混在一起。**没有核对 e2e 用的组名。**
  - 设计 4.1 的“入口到 gateway 的 5xx 比例高”没有写：按 `envoy_cluster_upstream_rq_xx`、`istio_requests_total` 都没有查到入口到 gateway 的序列，等基础设施那一半（Sonnet 子 Agent 在查网格和入口的指标名）回来再定。
  - 探针“SKU 快售罄”的告警没有写：探针没有报库存余量的指标。10 个 SKU 各 10 亿库存、每秒 1 单，够用几十年，不做。
  - 这些规则都**没有在集群上真的触发过**，那是第 5 节第 2 层的事。

### 第 4 步的另一半：网格、中间件、节点的规则（2026-11-22）

`mesh.yaml`（6 条）、`middleware.yaml`（18 条）、`platform.yaml`（9 条）和各自的测试。Sonnet 子 Agent 写的（指标名逐个在 Prometheus 上查过），主会话读过全部规则，改了两处、补了一条；只取了它那一个提交，worktree 和分支已删。**已下发**：现在集群上自己写的规则是 5 个 PrometheusRule、105 条（76 条告警加 29 条记录规则），求值都正常，**没有一条在响或在等**。`make rules-test` 8 个测试文件全过。

- **主会话改的**：
  - `ContainerMemoryNearLimit` 原来按工作集（working set）算，下发就会对 Kafka 一直响：三个 broker 的工作集是 limit（1.5 GiB）的 87% 到 96%，而常驻内存（RSS）只有 67% 到 71%。差的那部分是活跃的页缓存，内存紧的时候内核会回收，不会导致 OOM。改成按 RSS 算。MySQL 的 buffer pool、Redis 的数据、Go 的堆都是匿名页，RSS 量得到。
  - 补了 `IngressGateway5xxHigh`（设计 4.1）：入口的指标在 `job="monitoring/ingress-envoy"` 下，路由的标签是 `envoy_cluster_name="httproute/oppenheimer/gateway/rule/0"`（之前按 `cluster_name` 找所以没找到）。
- **与设计不一致的**：
  - KEDA 那条拆成两条（错误计数有增长、HPA 的 `ScalingActive` 不是 `True`），两个值的含义不同。
  - 入口没有“在途上限溢出”的指标（只有边车有），入口一侧看的是排队溢出和连接数上限溢出。
  - Kafka“没有 leader”用的是控制器的离线分区数，不是 Kafka Exporter 的 leader 指标（没有 leader 时那个指标的值没有确切的约定）。
  - 磁盘拆成三条（server 两档、agent 一档）。
- **没有写的**：“`Kafka` 资源不是 `Ready`”。Prometheus 里没有 Strimzi 的指标，kube-state-metrics 也没有配自定义资源的状态。要写就得二选一先把指标接进来，**这一条还欠着**（计划里点了名的，round-8 那次就是它）。
- **子 Agent 提的、值得记的**：
  - “有增长”那种写法里，“第一次出现”的那一半只管出现后的一个窗口；`for` 不短于窗口时，计数器出现之后不再动的情形撑不满 `for`，会漏。所以窗口都取得比 `for` 长。业务规则里这一类都没有写 `for`，不受影响。
  - 用正则匹配多个指标名再 `increase` 会因为去掉指标名后标签重复而报错，Collector 的三种信号是逐个算再合并的。
  - Collector 的 `send_failed_spans` 累计 120 多万（导出到 Tempo），`refused_spans` 累计 14 万，最近 10 分钟没有增长。是压测那两天留下的，**没有回头查是哪一天、丢了多少**。
  - Sentinel 那条只在少于法定数（2）时报，3 个掉到 2 个不报。要不要“掉一个就报”没有定。
  - 从库的清单（`mysql-order-0-1`、`mysql-order-1-1`）是写死的。
- **没有实测、只有单元测试的**：Exporter 连不上 Kafka 时 `kafka_brokers` 是不是真的消失；`OOMKilled` 的序列长什么样（集群上现在没有样例，标签是推断的）；Sentinel 掉线时两个指标的行为。都留到第 5 节的真实触发。
- **压线的**：`mysql-order-0-backup` 上次成功距今约 21 小时（阈值 26 小时，每天一次，正常）；server 的根分区剩约 23.6 GiB（阈值 15 GiB）。

### 第 5 步：筛 chart 自带的规则（2026-11-22）

- 自带的是 16 个组、149 条告警。现在在响的只有三条：`Watchdog`（本来就该一直响）、`KubeCPUOvercommit`、`KubeMemoryOvercommit`。其余 146 条求值正常、没有在响，**保留，不逐条改阈值**：它们盖住的是节点不在、kubelet、Pod 反复重启、副本不足、Job 失败、磁盘按趋势预测、etcd、Prometheus 和 Alertmanager 自己，这些没有理由自己重写。放一周看误报时（第 5 节第 3 层）再按实际响的情况调。
- 两条 Overcommit 在 values 里关掉了（`defaultRules.disabled`，理由写在 `deploy/cluster/monitoring/kube-prometheus-stack.yaml` 的注释里）。**values 改了，还没有 `helm upgrade`**：和下一步 Alertmanager 的配置改的是同一份 values，一起下发，免得监控栈滚两次。所以现在这两条还在响。
- 自带的规则没有 `runbook_url` 指到本仓库（指向的是上游的 runbooks.prometheus-operator.dev），按设计第 2 节定的不给它们写 runbook。

### 欠着的那条 Kafka 规则（2026-11-23）

计划里写的是“`Kafka` 资源的 `Ready` 条件”。**没有照这个做**，改成看 Strimzi operator 自己的调和计数：

- 抓取：`kafka-podmonitor.yaml` 里加了第二个 PodMonitor（`strimzi-operator`，端口 `http` 8080，只留 `strimzi_` 开头的指标，共 30 条上下）。
- 规则（`middleware.yaml`）：`KafkaOperatorNotReconciling`（成功调和的计数 20 分钟没有涨，或者 operator 看得到 Kafka 资源而成功的计数根本不存在；`for` 10 分钟，`warning`）、`KafkaOperatorMetricsMissing`（指标没有了）。
- 为什么不用 `Ready`：要给 kube-state-metrics 配自定义资源的状态（一段配置加一份 RBAC，还要重启它），而 round-8 出的事是“operator 每次调和都失败”，调和计数量的就是这件事。operator 每 2 分钟定时调和一次，正常时这个计数一直在涨。
- **第二半是专门对付 round-8 那种情形的**：operator 重启之后一次都没成功过，成功的计数器从未出现，`increase()` 没有结果；只写前一半的话恰恰是出过事的那种情况报不出来。有单元测试（`kafka_operator_never_reconciled_fires`）。
- 没有查到的：Strimzi 1.3.1 的 operator 没有导出 `strimzi_resource_state`（按资源报 Ready 的那个指标，在 operator 的 `/metrics` 上查过，没有）；失败的计数器 `strimzi_reconciliations_failed_total` 在没有失败过的时候也不存在，所以规则不依赖它。
- 局限：operator 在调和但 Kafka 资源因为别的原因不是 `Ready`（例如 broker 起不来）时，这条不一定响；那种情况由 `KafkaBrokerMissing`、`KafkaPartitionsOffline` 报。

### 第 6 步：Alertmanager 的路由和抑制、webhook 接收端（2026-11-23）

**配置放在哪**：`deploy/cluster/monitoring/alertmanager/alertmanager.yaml`（Alertmanager 自己的配置格式）加 `oppenheimer.tmpl`（Telegram 的消息模板），`monitoring.sh alertmanager` 把它们做成 Secret `alertmanager-oppenheimer`，values 里 `useExistingSecret` 指过去。没有用 `AlertmanagerConfig`（operator 会给它的路由和抑制都加上命名空间的匹配，表达不了全局的抑制），也没有写进 chart 的 values（会话号要入库，`amtool` 也检查不了）。Secret 变了之后 Alertmanager 自己重新加载，不重启（实测一分钟内生效）。

**路由**：默认去 `webhook`（alertsink）；`critical` 去 `critical` 接收端（一个接收端里放 webhook 和 Telegram 两种通知，没有用两条路由加 `continue`）；`info` 和 `InfoInhibitor` 不通知；`Watchdog` 每 5 分钟发给 webhook 一次当心跳。分组按 `alertname`、`namespace`、`service`、`component`、`slo`；`group_wait` 30 秒、`group_interval` 5 分钟、`repeat_interval` 12 小时（`critical` 1 小时）。

**抑制**（14 条，含 chart 自带的 4 条）：SLO 快档压中、慢档，中档压慢档；`ConsumerBlockedTooLong`、`CDCLaggingTooLong`、`RedisGateMemoryCritical`、`ServerDiskCritical` 各压自己轻的一档；`MySQLDown` 压同一实例的复制和连接数、同一套的 IO 线程；`KafkaBrokerMissing` 压副本不足；`OtelGatewayDown` 压各条“没有数据”。

**与设计 3.5 不一致的**：

- **“节点不在 → 抑制这个节点上的 Pod、中间件实例的告警”没有做。** 我们的告警上没有 `node` 标签（实例是 Pod 名），`equal` 对不上；要做得在每条规则里把 `kube_pod_info` 的 `node` 并进来。节点掉了的时候会有一批告警一起到，靠 runbook 总则里的“先看最上游的”。放一周看误报之后再定值不值得做。
- “Prometheus 取不到服务的指标（Collector 挂了）”原来没有对应的源告警，**新加了 `OtelGatewayDown`**（`critical`，`for` 2 分钟，比各条“没有数据”的 5 到 10 分钟短，所以它先响）。定成 `critical` 是因为它在响的时候业务告警和 SLO 都没有数据、不会响。
- 新加了 `AlertDeliveryStalled`（`critical`）：alertsink 把最后一次收到 `Watchdog` 的时刻报成指标 `alertsink_watchdog_last_received`，15 分钟没更新或者指标不在就响。它走 Telegram（不经过 alertsink）。断的是 Prometheus 或 Alertmanager 自己时它同样发不出来，那要集群外的东西（第 7 节，不做）。

**Telegram**：从 Alertmanager 的 Pod 里访问 `api.telegram.org` 是通的（返回 302，经 R2S 的透明代理）。**`.env` 现在不在，所以集群上的配置是“只发 webhook”的那一种**；主人写好 `.env`（`TELEGRAM_BOT_TOKEN`、`TELEGRAM_CHAT_ID`）之后重跑 `scripts/cluster/monitoring.sh alertmanager` 就带上。token 作为 Secret 里另一个键挂进容器，配置里用 `bot_token_file` 指过去；会话号在渲染时填进配置（只在 Secret 里，不入库）。模板用 `amtool template render` 在本机渲染过（HTML 模式下标签和注解里的 `<`、`&` 会被转义）。**后来接上了**（同一天稍晚，主人写了 `.env`）：重跑 `monitoring.sh alertmanager` 后一分钟内生效，一条 `severity=critical` 的合成告警发了出去，`alertmanager_notifications_total{integration="telegram"}` 是 1、各种失败原因的计数都是 0。主人实际看到的消息样子没有确认。

**webhook 接收端 alertsink**（Sonnet 子 Agent 写，主会话读过全部代码）：`load alertsink` 子命令（`test/load/internal/alertsink/`），装在 chaos 镜像里，`monitoring.sh alertsink` 部署到 `monitoring` 命名空间，偏好 k3s-server。每条告警打一行 JSON 日志；指标 `alertsink_alerts_total`（标签只有 `status`、`severity`）和 `alertsink_watchdog_last_received`（名字在 Prometheus 上核对过）。放在 `test/load` 而不是单开一个模块：它是 3d 之前的临时替身，几十行，不值得为它加一个模块、改三个 Dockerfile。

**检查和测试**：`make alertmanager-test`（`amtool check-config`，带和不带 Telegram 的两种渲染结果都查；再按 `alertmanager/tests/routes.txt` 的 11 行核对每类告警去哪个接收端），进了 CI 的 `rules` job。`amtool` 0.35.0 跟集群上的 Alertmanager 同版本，`make amtool` 装进 `./bin`。分组和抑制 `amtool` 测不了。

**在集群上验证过的**（往 Alertmanager 里塞合成的告警，标签带 `synthetic=true`，4 分钟后自己过期；没有动任何组件）：

- `severity=critical` 的走到了 `critical` 接收端，其余走 `webhook`，alertsink 的日志里都有。
- 同一个 `slo` 的快档在时，慢档没有送出来（Alertmanager 里标成 inhibited）；另一个 `slo` 的慢档照常送出。
- 三条同名、不同 `instance` 的告警在同一个组里一起送出（`group_wait` 30 秒之后）。
- chart 的两条 Overcommit 在这次 `helm upgrade` 之后消失了，Prometheus 里在响的只剩 `Watchdog`。

**没有验证的**：其余几条抑制（MySQL、Kafka、Collector、两档阈值的那几对）只有配置检查，没有用合成告警逐条试；`repeat_interval` 到点重发；`monitoring.sh silence` 加了没有用过。

**顺带改的**：`monitoring.sh up` 现在包含 `rules` 和 `alertsink`；`kps` 会先生成 Alertmanager 的 Secret。新增 `monitoring.sh alerts`（列出现在的告警，含被抑制和被静默的）、`silence`。

### 第 7 步：runbook（2026-11-23）

`docs/runbooks/`：44 份（按规则的 `runbook_url` 一份一个文件，49 到 104 行）加 `README.md`（总则和索引，主会话写）。44 份由三个 Sonnet 子 Agent 按 3.8 的模板分三批写（SLO 和业务 21 份、中间件 13 份、网格和平台 10 份）。

**验收到哪**：用脚本核对过 44 份的七个标题齐全、顺序一致，仓库内的相对链接都存在。主会话逐行读过的只有 `mesh-overflow.md`，改过的有 `probe.md`、`outbox.md`、`recon.md`、`ratelimit-fallback.md`、`mesh-overflow.md`（跟着下面的规则改动）。**其余 39 份没有逐行读；所有 runbook 里的 PromQL、kubectl 命令都没有在集群上执行过**（子 Agent 不许碰集群）。告警质量检查时每触发一条就照着它的 runbook 走一遍，走不通的当场改，这是这批 runbook 真正的验收。

子 Agent 自己标明没有核对的：Loki 里 `recon`、`cdc` 的 `service_name`；闸门 Redis 的 `redis-cli` 要不要密码；broker Pod 里 Kafka 脚本的路径；入口 Envoy 的容器名；`redis_instance_info` 的 `role` 标签；local-path 的目录。没有现成办法、只写了建议的：死信的重放；消费者被挡住后跳过一条消息；cdc 的 `-reset-checkpoint` 在集群里怎么执行。约一半告警的“怎么人为触发它”是“待定”加建议，和计划文件里登记的 16 项对一遍之后再定。

**写 runbook 时查出来的规则问题（“让它报告认为设计存在的问题”又一次有用），已改并下发的**：

- **`IngressRejectingRequests` 看不到入口最主要的拒绝路径。** round-6 的 7.3 量到过载时入口拒绝请求靠的是在途上限（`upstream_rq_active_overflow`，2384 档每秒 530 个 503，排队溢出是 0），而这个计数不在入口 PodMonitor 的保留列表里，规则的注释还写着“入口没有这条”。在入口 Envoy 的 `/stats/prometheus` 上核对过它有。补进了抓取和规则（`signal="active"`），加了测试。
- **`ProbeFailing` 把 `incomplete` 也算成失败**（分子是 `outcome!="ok"`）：过载的压测里探针被背压的 429 拒绝、或者探针的 SKU 卖完，都会报成 critical 的“下单的路断了”，和第 2 节第 2 项（429 不算坏）、探针小节第 1 点都不一致。分子改成 `failed` 加 `skipped`，另加 `ProbeIncomplete`（warning，10 分钟）。
- **`PaidOrdersNotLanding` 响的时候已经来不及**：10 分钟的窗口加 `for` 5 分钟，正好是关单的 15 分钟时限。收紧成 5 分钟加 2 分钟。
- **`ReconDailySlow` 慢过一次之后不恢复**：取的是所有保留的 Job 里最长的。改成只看最近开始的那一个。
- 两处注释和记录对不上：`MySQLConnectionsHigh`（round-6 4.3 没有撞上连接数，报过 1040 的是本机的 compose）、`RateLimitFallingBack`（过拐点的几档每秒 8.6 到 12 次，不是“偶发”；那时响是对的）。

**提了、没有改的**（放一周看误报时一起定）：

- `KafkaBrokerMissing` 的 `absent(kafka_brokers)` 在 Kafka Exporter 自己挂了而 broker 都好的时候也会响，并且是 critical、还会压住副本不足。没有拆：三个 broker 全不在时 Exporter 同样连不上，那时这一半是唯一的 critical。
- `NodeRebooted`：节点停机超过 30 分钟再回来时不响（`offset 30m` 那一刻没有数据）。停机本身由自带的 `KubeNodeNotReady` 报。
- `ContainerOOMKilled` 要重启次数增长，Job 的 Pod 被 OOM 杀掉（3b 的备份 Job 遇到过）不响，由自带的 `KubeJobFailed` 接；`ContainerMemoryNearLimit` 按 RSS 算，看不见页缓存撑爆的那种（同一个备份 Job）。
- `MySQLReplicaMetricsMissing` 的从库清单按 Pod 序号写死，主从切换之后要跟着改（已知）。
- `MySQLConnectionsHigh` 的阈值（80%，160）正好等于 order 扩到上限且池用满时的连接数，那时会响；注释里写明了这是对的。
- `ConsumerNotConsuming` 实际约 20 分钟才响（10 分钟的 `delta` 加 `for` 10 分钟）。
- `ReconFoundDifferences` 的恢复靠 CronJob 的 `lastSuccessfulTime`；手工 `create job --from=cronjob` 跑成功的 Job 会不会更新它没有核对（`MySQLBackupStale` 同理）。
- `MeshEndpointEjected`、`MeshConcurrencyLimitOverflow` 只看 `oppenheimer` 命名空间，`make cluster-fault`（在 e2e 的命名空间里）验证不了它们。
- server 的根分区剩 22 到 24 GiB，长稳时每小时掉约 1.5 GiB：一次长稳约 5 小时碰 `ServerDiskLow`，压测前看一眼。
