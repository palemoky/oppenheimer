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

### 第 8 步：告警质量检查（2026-11-23 起，按计划文件登记表的编号）

每一项记：做了什么、响了什么、和预期不一样的、runbook 走不走得通。“真实触发”和“只验证了路是通的”分开写。

**第 1 项：合成告警逐条试抑制（2026-11-22 17:23 到 17:30 UTC，没有动任何组件）**

- **做成了可以重跑的检查**：`scripts/cluster/monitoring.sh inhibit-test`，核对表是 `deploy/cluster/monitoring/alertmanager/tests/inhibit.txt`（每行“期望 | 标签”，47 条）。它把表里的告警一次塞进 Alertmanager，等 10 秒，从 API 读每一条的 `inhibitedBy`，把指纹换成源告警的名字打出来。改了 `inhibit_rules` 或者这些规则的标签之后跑一遍。它不进 CI（要集群）。
- **结果：14 条抑制规则都按预期生效，47 条里 0 条不符**。每条规则至少有一条被压住的和一条对照（`equal` 里的标签对不上的那一个不被压）。第一遍有 1 条不符，是核对表写错了，不是配置的问题：`InfoInhibitor` 自己是被“任何告警”压住的（chart 的第四条规则没有源的条件），而它被压住的同时仍然压住了同命名空间的 `info`。这正是“抑制不连锁”的另一面：被抑制的告警仍然是有效的源。
- **标签是照真实的写的，不是照规则里匹配的那几个凑的**。动手前从 Prometheus 里取了每对告警背后的指标实际带的标签：`mysql_up` 和复制的指标都带 `cluster` 和 `instance`（实例是 Pod 名），所以“主库连不上压同一套从库的 IO 线程”的 `equal: [cluster]` 两边都有值；`MySQLReplicaMetricsMissing` 是 `absent()` 出来的，只带 `instance`，和 `MySQLDown` 的 `instance` 对得上。`equal` 的坑是两边都没有某个标签也算相同，这一步核对的就是 14 条规则里没有哪一条是靠“都没有”才成立的。
- **静默第一次用上了，并且踩了一个坑**：critical 的合成告警会发到 Telegram，所以脚本先加一条 `synthetic=true severity=critical` 的静默。被静默的告警照样能当抑制的源（抑制看的是源告警在不在，不看它通知出去没有），所以不影响结果。第一版在脚本退出时把静默撤掉，而告警还要活 4 分钟，撤掉之后下一次分组刷新就会发出去；当场补了一条静默，Telegram 的发送计数前后都是 2，没有漏出去。现在静默比告警多活 3 分钟，自己到期。
- **alertsink 一侧对上了**：不被抑制的 9 条 warning 都在 30 秒（`group_wait`）后送到，被抑制的一条都没有；critical 的被静默，没有送。
- **一个事先没想到的行为：一直被抑制的告警，恢复时会收到一条“恢复”通知，而它从来没有发过“触发”。** 第一遍 alertsink 收到 9 条触发、14 条恢复，多出来的 5 条正是和某条已通知的告警同组、自己一直被压着的那几条（例如 `redis-gate-0` 的 `RedisGateMemoryHigh`，和没被压的 `redis-gate-1` 同组）。原因（按 Alertmanager 的流程推断，没有读源码核对）：源和目标同时恢复，分组刷新时源已经不在，抑制不再拦它；这一组之前发过通知，组里的内容变了就要再发一次，恢复的告警全都带上。第二遍的 14 条同理：它们加进了第一遍已经存在的组，要等 `group_interval`（5 分钟）才轮到，那时已经过期，于是只发了恢复。**对 3d 的诊断 Agent 的要求：webhook 的消费方不能假设“恢复”之前一定见过“触发”。** Telegram 一侧同样会出现（critical 的两档阈值、MySQL 的那几条），没有处理，放一周看的时候留意。
- 顺带看到的：`amtool alert add --end=T` 不带 `--start` 时，alertsink 记下的 `starts_at` 等于 `T`（未来的时刻）。只影响合成告警的日志，没有查是 amtool 还是 Alertmanager 填的。
- **仍然没有验证的**：`repeat_interval` 到点重发（要等 1 小时和 12 小时，放一周看的时候看）；“节点不在”的抑制没有做。

**第 2 项：探针缩到 0（真实操作；2026-11-22 17:30:26 到 17:47:37 UTC，17 分钟）**

- 预期是什么都不响，结果是什么都没响：每分钟查一次 `ALERTS`（`pending` 的也算），17 次全是空的（`tmp/3c-inject-02-probe-stop.log`）。周期速率 17:35 掉到 0，之后停了 12 分钟，超过 `ProbeNoData` 的 `for`（10 分钟）；它连 `pending` 都没有进，说明副本数那个条件在起作用。SLO 的四组、`PaymentTotalsCheckStalled`、CDC 的几条也都没有动。
- 这一项只验证了“人为停的不报”。`ProbeNoData` 该响的时候会不会响（探针活着但没有数据）还没有验证，在第 11 项（停 Collector）里它应当被抑制，所以真实触发要另想办法；没有办法就在第 16 项里单列。

**第 5 项：orderindex 缩到 0（真实触发；2026-11-22 17:47:50 到 18:15:08 UTC，27 分钟）**

| 告警 | 进 `pending` | `firing` | alertsink 收到 | 从停掉算起 |
|---|---|---|---|---|
| `ServiceMetricsMissing{orderindex}` | 约 17:53 | 约 17:58 | 17:58:46 | 约 11 分钟 |
| `ConsumerNotConsuming{orderindex.cdc-orders}` | 约 17:58 | 约 18:08 | 18:08:35 | 约 21 分钟 |

- 该响的两条都响了，没有别的告警跟着响（`tmp/3c-inject-05-orderindex.log`）。恢复：缩回 2 之后 3 分钟内两条都不在了，积压从 4854 回到 0。
- **两条都比规则里写的 `for` 慢一截，原因不同。** `ServiceMetricsMissing` 的 `for` 是 5 分钟，但服务停了之后它的序列还要 5 分钟才从查询结果里消失（Prometheus 对不再更新的序列默认回看 5 分钟），`absent()` 那时才成立；通知里的“已经 10 分钟没有报任何指标”是对的。`ConsumerNotConsuming` 是 10 分钟的 `delta` 等于 0（位点停下 10 分钟后才成立）再加 `for` 10 分钟；通知里写的“位点 10 分钟没有动”实际是 20 分钟，没有改。计划的登记表里写的“约 15 分钟”不够，这次按 27 分钟做的。
- 分组各自一组（`alertname` 不同），各发一条，都走 webhook，没有 critical。
- **runbook 走通了**：`service-metrics-missing.md` 和 `consumer-blocked.md` 的“先看什么”在故障现场逐条执行过，命令都能跑，并且走得到原因（六个服务里只少 orderindex、Deployment 是 0/0；积压 3324 而位点的 `delta` 是 0；`kafka-consumer-groups.sh --describe` 显示 `no active members`）。子 Agent 没核对的“broker Pod 里 Kafka 脚本的路径”是对的（`/opt/kafka/bin/`，Pod 选出来是 `kafka-dual-0`）。
- **对账的一个要记住的地方**：恢复后手工跑的 `recon-hourly` 差异 0，但它的上界是“现在往前 30 分钟”（这次是 17:48），故障那 27 分钟里的订单不在这次的窗口里。故障注入之后的对账要等故障结束 30 分钟以后跑的那一次才算数；这一项以 19:07 UTC 那次为准。
- 顺带看到的：第 2 项恢复探针之后 `ProbeNoData` 进过一次 `pending`（17:49，不到 1 分钟）：副本数先变成 1，周期速率要再等两次采集才不是 0。`for` 10 分钟挡得住，不用改。

**第 6 项：cdc 缩到 0（真实触发；2026-11-22 18:18:44 到 18:37:47 UTC，19 分钟）**

| 告警 | 进 `pending` | `firing` | alertsink 收到 | 从停掉算起 |
|---|---|---|---|---|
| `ServiceMetricsMissing{cdc}` | 约 18:24 | 约 18:28 | 18:29:16 | 约 10.5 分钟 |
| `CDCPipelineMissing`（order-0、order-1 各一条） | 约 18:24 | 约 18:34 | 18:34:35（一条通知里两条） | 约 16 分钟 |

- 该响的都响了，没有别的跟着响（`tmp/3c-inject-06-cdc.log`）。两条 `CDCPipelineMissing` 同组，合成一条通知。恢复的通知在 18:39 到了。通知里写的“10 分钟没有报延迟”实际是 15 分钟（同第 5 项的原因：序列消失要 5 分钟），没有改。
- **预期里“恢复后短暂的 `CDCLagging`”没有出现**：cdc 回来之后不到一分钟就追平了（19 分钟、每秒约 1 单的量），`cdc_lag_ms` 回到平时的几十到几百毫秒，`CDCLagging`（超过 60 秒并持续 5 分钟）连 `pending` 都没进。所以 **`CDCLagging`、`CDCLaggingTooLong` 还没有被真实触发过**，放到第 7 项（从库的 SQL 线程停了，cdc 读的就是从库）里看，不行就归到第 16 项。
- cdc 停着的时候 `orderindex.cdc-orders` 的积压是 0（上游没东西来），所以 `ConsumerNotConsuming` 不响，这是对的。
- **runbook（`cdc.md`）走通了**：五步的命令都能跑。第 1 步在 `CDCPipelineMissing` 响的时候返回空（指标没了），第 2 步看到 Deployment 是 0/0，就是原因。

**第 7 项：订单从库 `mysql-order-0-1` 停 SQL 线程（真实触发；2026-11-22 18:46:28 到 18:57:43 UTC，11 分钟）**

| 告警 | `firing` | alertsink 收到 | 从停掉算起 |
|---|---|---|---|
| `MySQLReplicaSQLThreadStopped`（critical） | 约 18:49 | 18:49:43，Telegram 也发了（发送计数加 1） | 约 3 分钟 |
| `CDCLagging{order-0}`（warning） | 约 18:52 | 18:53:05 | 约 6.5 分钟 |

- 两条都响了；`START REPLICA` 之后一分钟内两条都不在了，`cdc_lag_ms` 从 665 秒回到 0。**`CDCLagging` 到这里算真实触发过了**（cdc 读的是从库的 binlog，从库不回放它就没有新东西，延迟每分钟涨 60 秒）。`CDCLaggingTooLong`（1 小时）没有等，归到第 16 项。
- **预期里“之后 `MySQLReplicaLagHigh`”没有出现，也不会出现**：SQL 线程停着的时候 `Seconds_Behind_Source` 是 NULL，序列直接消失；恢复之后不到一分钟就追平，够不到“超过 30 秒并持续 5 分钟”。`MySQLReplicaMetricsMissing` 没有响，这是对的（线程状态的序列还在，两条是互补的）。`MySQLReplicaLagHigh` 归到第 16 项。
- **事先没想到的：从库的 Pod 变成了 NotReady**（就绪探针查复制状态），于是 chart 自带的 `KubePodNotReady`、`KubeStatefulSetReplicasMismatch` 进了 `pending`（它们的 `for` 是 15 分钟，这次 11 分钟没有响）。停得再久一点会是三条告警说同一件事，没有配抑制；放一周看的时候再定。
- 从库被摘掉的好处也看到了：故障的 8 分钟里探针 480 个周期全部 ok，读回到了主库。
- 通知里 `CDCLagging` 的文字是“落后 365.4k 毫秒”，不好读，该换成秒或者用 `humanizeDuration`；没有改。
- runbook（`mysql-replication.md`）第 1、2 步在现场执行过，能跑；人为停的线程错误号是 0、信息是空的（没有错误），这一点 runbook 里没有写，补了。

**第 8 项：同一个从库停 IO 线程（真实触发；19:06:01 到 19:12:02 UTC，6 分钟）**

- `MySQLReplicaIOThreadStopped`（critical）约 19:09 响，19:09:13 送到（Telegram 也发了），恢复的通知 19:14:13。`CDCLagging` 进了 `pending` 没到 5 分钟。
- **这一项出了一个事故，是排时间的失误：19:06 动手，正好压在 19:07 的 `recon-hourly` 上。** 从库 NotReady 之后被从 `mysql-order-0-replica` 这个 Service 里摘掉，对账连它被拒绝，退出码 2，Job 失败，`ReconFoundDifferences`（critical）19:08:52 发到了 Telegram。19:15 手工复跑：窗口 17:15 到 18:45，六对差异 0，告警一分多钟后恢复。失败的 Job `recon-hourly-29922907` 还留着（删除被权限拦下了，由主人定），所以自带的 `KubeJobFailed` 约 19:23 会响（warning，只到 alertsink）。
- 从这个事故里得到的：
  1. **`ReconFoundDifferences` 分不清“有差异”（退出码 1）和“没跑成”（退出码 2）**，名字和级别都是按前一种定的。没跑成也该有人知道（对账没在跑），但“一个从库有一分钟不可用”就按钱对不上的级别叫人，是误报。两个改法：规则按退出码拆成两条（要先确认 kube-state-metrics 有容器退出码的指标）；或者 `recon` 连不上从库时等一会儿重试、Job 的 `backoffLimit` 给 1。**没有改，等主人定**（涉及资金的对账，怎么报由主人定）。
  2. 手工 `create job --from=cronjob` 跑成功**会**更新 CronJob 的 `lastSuccessfulTime`，告警因此恢复。runbook 里原来写的是“没有核对”，改了。
  3. 手工复跑的上界是 30 分钟前；这次的窗口盖住了第 5、6 项的故障时段（差异 0），第 7、8 项的要看 20:07 那一次。
  4. 以后的故障注入避开每小时的第 5 到 9 分钟，或者先 `recon-cron-suspend`。已写进 `recon.md` 的“历史”。

**第 9 项：闸门 Redis 的 `maxmemory` 临时调低（真实触发；2026-11-22 19:17:56 到 19:31:39 UTC，只改主库 `redis-gate-0`）**

- 做法按 runbook 的，不按登记表原来写的：表里的“1.15 倍”是 87%，越过了 order 停止受理的 85%（ADR-0034），探针一直在下单，会真的拒单。改成用量除以 0.75（约 106.5 MB）和除以 0.82（约 97.4 MB）。动手前看了用量的波动：3 小时里 79.66 到 79.90 MB，不到 0.3%，离 85% 的 3 个百分点够用。脚本每 20 秒读一次比例，到 83.8% 就自动恢复，退出时也恢复。
- `RedisGateMemoryHigh`：19:17:56 调低，约 19:23 响，19:23:57 送到（约 6 分钟）。`RedisGateMemoryCritical`：19:25:31 调到 82%，19:28:27 送到，Telegram 也发了。期间 `gate_memory_full` 的受理拒绝是 0，探针没有受影响。
- 抑制在真的告警上也对：Critical 响着的时候 High 没有再发通知；恢复时两条各发了一条恢复。
- 恢复：`CONFIG SET maxmemory 805306368`，`CONFIG GET` 核对过，等于清单里的 768mb。两条告警 2 分半之内都不在了。
- 中间出过一次脚本的毛病：第一版的越线保护在 zsh 里取数写错了，调到 82% 之后 45 秒就退出并恢复了（退出时恢复起了作用），Critical 只到 `pending`；换成 bash 脚本重做了这一段。
- runbook（`redis-gate-memory.md`）的“先看什么”在现场执行过，都能跑：`redis-cli` 不用密码，`redis_instance_info` 有 `role` 标签（子 Agent 没核对的两处都是对的）。

**第 10 项：闸门的 Sentinel 缩到 1（真实触发；2026-11-22 19:34:34 到 19:40:26 UTC，6 分钟）**

- `RedisSentinelBelowQuorum`（warning）19:37:27 送到（约 3 分钟），缩回 3 之后一分钟内恢复，`sentinel ckquorum gate` 回到 `OK 3 usable Sentinels`。
- **事先没想到的：同时响了两条 critical 的 `RedisDown`**（`redis-gate-sentinel-1`、`-2`），发到了 Telegram，文字是“Redis redis-gate-sentinel-1（redis-gate）连不上”。exporter 的目标里 Sentinel 和数据节点是一样的，`redis_up == 0` 不分。少两个 Sentinel 的后果是“主库坏了切不了”，数据面没有受影响，不该和“闸门 Redis 连不上”一个级别、一个名字。**建议改**：`RedisDown` 排除 Sentinel 的实例，Sentinel 不在由 `RedisSentinelBelowQuorum` 管（三个全不在时这条不响，要另加一条 `absent`）。没有改，和本节其余几条规则的改动一起等主人定。
- runbook（`redis-sentinel.md`）的第 1 到 3 步在现场执行过，能跑；`ckquorum` 直接说出了原因（`NOQUORUM 1 usable Sentinels`）。

**第 13 项：Strimzi operator 缩到 0（真实触发；2026-11-22 19:43:59 到 20:02:04 UTC，18 分钟，没有动 broker）**

- `KafkaOperatorMetricsMissing`（warning）19:45 进 `pending`，19:55:44 送到（约 12 分钟），operator 回来之后 20:05:44 恢复。三个 broker 全程都在，`kafka_brokers` 一直是 3。
- **拉和推的差别在这里看得见**：这条是 Prometheus 自己去抓的指标，目标从服务发现里消失，序列立刻失效，不到 2 分钟就进了 `pending`；第 5、6 项那几条是服务经 Collector 推上来的，序列要过 5 分钟才从查询结果里消失。所以同样是“`absent()` 加 `for` 10 分钟”，一个 12 分钟响，一个 16 分钟响。
- **印证了当初不按 `Kafka` 资源的 `Ready` 写规则的取舍**：operator 不在的 18 分钟里 `kubectl -n kafka get kafka kafka` 一直显示 `READY True`（状态是 operator 写的，它不在就没人改）。
- operator 重启之后 `KafkaOperatorNotReconciling` 进过 2 分钟的 `pending`（成功的计数器还没出现，就是为 round-8 那种情形写的那一半），第一次调和成功后自己消了；`for` 10 分钟挡得住。调和计数每 2 分钟涨 1，恢复正常。自带的 `CPUThrottlingHigh`（info）也进过 `pending`，是 operator 启动时的 CPU。
- runbook（`kafka-operator.md`）的五步在现场执行过，能跑，第 3 步看到 Deployment 是 0/0。
- **20:07 的 `recon-hourly`**：窗口 18:07 到 19:37，差异 0。到这里第 5 到 9 项的故障时段都被一次差异为 0 的对账盖住了。

**第 12 项：alertsink 缩到 0（真实触发；2026-11-22 20:08:59 到 20:37:17 UTC，28 分钟）**

| 告警 | `firing` | 从停掉算起 | 去向 |
|---|---|---|---|
| 自带的 `AlertmanagerFailedToSendAlerts`（warning）、`AlertmanagerClusterFailedToSendAlerts`（critical） | 约 20:18 | 约 9 分钟 | critical 的一条 Telegram 发出去了 |
| `AlertmanagerNotificationsFailing{webhook}`（warning） | 约 20:22 | 约 13 分钟 | webhook 不通，发不出去 |
| `AlertDeliveryStalled`（critical） | 约 20:24 | 约 15 分钟 | Telegram 发出去了 |

- **要验证的那一点成立：webhook 不通的时候，critical 照样经 Telegram 到了。** `critical` 接收端里 webhook 和 Telegram 是两种独立的通知方式，一个失败不拖累另一个（Telegram 的发送计数在 20:18 和 20:25 各加 1，失败是 0）。
- `AlertDeliveryStalled` 是从 `absent()` 那一半响的（alertsink 的指标没了），不是“15 分钟没更新”那一半；所以是 5 分钟（序列消失）加 `for` 10 分钟。alertsink 回来 2 分钟后它就恢复了。
- alertsink 回来之后 16 秒，积着的几条告警一起送到（Alertmanager 一直在重试）。发送失败的计数停在 37。三条“发送失败”的告警要等 15 分钟的窗口滑过去才恢复（约 20:52），这是规则写法决定的。
- **重复**：同一件事四条告警，我们的 `AlertmanagerNotificationsFailing` 和自带的两条说的是一回事，而且自带的更快、有一条是 critical。放一周看的时候定留哪边（倾向于删我们这条，或者把自带的那两条关掉只留我们的：自带的 critical 那条在 webhook 这种“只是日志”的接收端失败时级别偏高）。
- runbook（`alertmanager-notifications.md`）的前四步在现场执行过，能跑；Alertmanager 的日志里直接有 `Notify for alerts failed ... dial tcp`。补了一句：第 2 步的第三条查询在 alertsink 不在时返回空。

**第 11 项：Collector 的 gateway（otel-gateway）缩到 0（真实触发；2026-11-22 20:53:20 到 21:10:24 UTC，17 分钟；这段时间服务的指标、日志、链路丢了）**

- `OtelGatewayDown`（critical）20:56:47 送到（约 3.5 分钟），Telegram 也发了。它确实比各条“没有数据”先响：`PaymentTotalsCheckStalled` 21:02、六条 `ServiceMetricsMissing` 21:03 到 21:04、`ProbeNoData` 21:07、两条 `CDCPipelineMissing` 和 `AlertDeliveryStalled` 21:09 才到 `firing`。
- **抑制在真的告警上成立**：故障期间 Alertmanager 里这 11 条全部是 `suppressed`，各有一个抑制源；alertsink 在这 17 分钟里除了 `OtelGatewayDown` 一条都没有收到。`ProbeNoData` 到这里算真实触发过了（响了，被压住）。
- **查出一个漏洞：恢复的时候抑制先撤了，被压着的告警漏出来两条。** gateway 回来的那一刻 `OtelGatewayDown` 就恢复了，而“没有数据”的那几条要等指标重新攒够一个窗口才恢复，慢 1 到 4 分钟。这中间谁的分组正好轮到刷新，谁就发出去：`ProbeNoData`（21:12:12）和 `PaymentTotalsCheckStalled`（21:12:50）各发了一条触发，5 分钟后又各发一条恢复。critical 的 `AlertDeliveryStalled` 那时也还在响（到 21:13），这次没轮到，是运气。**改了**：给 `OtelGatewayDown` 加 `keep_firing_for: 5m`（条件不成立之后再多响 5 分钟，Prometheus 的规则自带这个字段），让源比目标晚走。代价是 Collector 恢复后这条 critical 的“恢复”通知晚 5 分钟到。
- 同样的毛病别的“上游压下游”也可能有（`MySQLDown` 压复制的几条、`KafkaBrokerMissing` 压副本不足），没有验证，没有改；等真的见到再说。
- Collector 不在的时候 KEDA 从 Prometheus 查到的请求速率是 0（`keda-hpa-gateway` 的目标显示 `0/450`），`KedaHPAScalingInactive` 不响。现在上下限都是 2 所以没有后果；以后放开伸缩，Collector 不在会让它按“没有流量”缩到下限。记给放开伸缩的时候。
- 恢复：gateway 回来 4 分钟后所有告警都不在了。21:07 的 `recon-hourly`（窗口 19:07 到 20:37）差异 0。
- runbook（`otel-collector.md`）第 1 到 3 步在现场执行过，能跑；agent 的日志里是 `Exporting failed. Will retry`。

**第 3 项：inventory 缩到 0（真实触发；2026-11-22 21:33:22 到 21:40:11 UTC，7 分钟）**

按 7 分钟做，不是登记的 10 分钟：每秒 1 单全失败，每分钟烧掉的预算不小，够让要看的告警响就停。

| 告警（都是 critical，都发了 Telegram） | alertsink 收到 | 从停掉算起 | 恢复的通知 |
|---|---|---|---|
| `OrderLandingBreakerOpen`（`landing-db-0`、`-1` 两条） | 21:36:09 | 2.8 分钟 | 21:41:09 |
| `SLOOrderCreateAvailabilityBurnFast` | 21:37:10 | 3.8 分钟 | 21:47:10 |
| `ProbeFailing` | 21:39:12 | 5.8 分钟 | 21:44:12 |
| `IngressGateway5xxHigh` | 21:39:35 | 6.2 分钟 | 21:49:35 |
| `SLOOrderCreateAvailabilityBurnMedium` | **21:46:10** | 故障结束之后 6 分钟才发 | 约 22:10（30 分钟的窗口滑过去） |

- 快档的时间和单元测试钉的一致（3 到 4 分钟）。`ServiceMetricsMissing{inventory}`、`OrderAdmitRateDropped` 只到 `pending`（7 分钟不够）。`KedaHPAScalingInactive` 没有响，是对的。
- **没有响的：`MeshEndpointEjected` 和网格的溢出。** inventory 一个端点都没有的时候边车没有东西可摘，请求直接失败（“没有健康的上游”），不走异常点摘除。这两条用“缩到 0”触发不了，要用“一个副本坏、一个副本好”的办法（`make cluster-fault` 是这么做的，但它在 e2e 的命名空间里，规则不看那里）。归到第 16 项，或者另登记一项“杀掉一个副本的进程但不让它退出”。
- **`OrderLandingBreakerOpen` 最先响，而且文字指向数据库**（“落库健康检查的熔断器 landing-db-0 打开了”），实际坏的是 inventory：落库的健康检查走的是整条落库的路，里面有 `Reserve`。runbook（`breaker.md`）里要写明“这个熔断器开了先看 inventory 在不在”；补了。它比 SLO 快一分钟，作为“原因一级”的告警是有用的。
- **中档在故障结束之后才发出来，这是多窗口燃烧率加抑制的固有行为，不是毛病，但要知道**：中档 21:40:40 就成立了，那时快档在响，它被压着；21:46 快档恢复（5 分钟的短窗口干净了），抑制撤掉，中档的 30 分钟短窗口里还装着那 7 分钟的失败，于是手机上是“快档恢复”之后紧跟着一条新的 critical，再响 25 分钟。看到它要先看 `error_ratio_rate5m`：是 0 就是尾巴，不是新的故障。写进了 runbook。要不要处理（例如中档再加一个 5 分钟窗口的条件）放一周看的时候定。
- **慢档没有响，原因是 1 天的窗口被早上的长稳稀释了**：1 天的错误比例是 0.016%（分母里有 03:55 到 07:55 那 180 万单），到不了 0.3%。平时只有探针的流量时，同样 7 分钟的故障会让慢档响约 2 小时。
- **预算**：这 7 分钟里 381 个 5xx。7 天窗口的预算现在是 -93%（超支了将近一倍）。分母只有约 20 万个请求（探针 9 个多小时加上还在窗口里的压测），所以预算只有约 200 个失败。**这说明一件设计上的事：按请求数算的 SLO，在流量很小的系统上一次几分钟的故障就烧完一周的预算**，“预算烧完就停实验”这条规矩在练习环境里没法照字面执行；这一周里后面几项照做，预算的数只当读数看。
- 503 里带降级原因码的（`ORDER_LANDING_UNAVAILABLE`）也算进了 5xx，烧预算，和设计第 2 节第 2 项一致（只有 429 不算坏）。
- 恢复：inventory 回来 2 分钟后探针全部 ok。
- runbook（`slo-order-create-availability.md`、`probe.md`）在现场走过：状态码的查询显示 503 占 93%，受理结局是 `order_landing_unavailable`，探针的步骤里 `stock` 是 `error`，三步就指到 inventory。7 天预算的那条查询能跑（约 1 秒）。

**第 15 项：对账差异，在 `oppenheimer-e2e` 里手工改一行（2026-11-22 23:31 到 23:37 UTC；没有碰长期运行的那一套，没有告警该响，也没有响的）**

做法：`CHAOS_DELETE=0 CHAOS_DURATION=30s E2E_KEEP=1 scripts/cluster/chaos.sh` 拉起 e2e 那一套并留下现场（367 单，其中 180 单已支付；它自己的 `recon` 和 `verify` 通过），然后在留下的 `chaos` Pod 里重复跑 `recon -pairs payment,stock,replica,index -settle 0 -recheck 5s`，每次之前用 root 在 `mysql-main-0` 上改 `e2e_*` 库里的一行，看完改回去。同一张订单（`912486003548160055`）。每次的输出在 `tmp/3c-inject-15-recon-*.log`。

| 改了什么 | 退出码 | `recon` 报的 |
|---|---|---|
| 不改（基线） | 0 | 四对差异 0 |
| 商家索引那一行的 `total_amount` 1990 改成 1991 | 1 | `index` 一对 `mismatch=1`，给出订单号 |
| 改回去 | 0 | 差异 0 |
| 支付单的 `amount` 1990 改成 1991 | 1 | `payment` 一对 `mismatch=1`，并排印出两边：订单一侧 `amount=1990`，支付单一侧 `amount=1991` |
| 改回去 | 0 | 差异 0 |
| 删掉商家索引的那一行 | 1 | `index` 一对 `only_left=1`，给出订单号 |
| 插回去 | 0 | 差异 0 |
| 不改数据，把从库的 DSN 换成连不上的地址 | 2 | 没有对账结果，只有一行 `recon failed ... connect order_replica_0 ... connection refused` |

- **`recon` 报得出来，定位到行，而且只报改的那一对**：其余三对每次都是 0。差 1 分钱也报。复查（5 秒后重读）确认了才算差异，所以每次有差异的那一遍多花 5 秒。
- **退出码 1 和 2 从输出上很好分**：1 的最后一行是 `total outstanding differences: N`，前面有每一对的明细；2 没有这一行，只有一条 ERROR 的日志。但 **Kubernetes 那一侧两者都只是“Job 失败”**，`ReconFoundDifferences` 看的是 CronJob 最近一次失败的时间，所以分不开（第 8 项那次误报就是这样来的）。Job 的 `restartPolicy` 是 `Never`，kube-state-metrics 里没有这个 Pod 的退出码可用（主会话的判断，没有去翻它的全部指标）。要分开报，得让 `recon` 自己把结果报出来（比如跑完往 Collector 推一个“结果”和“差异数”的指标），批量的那个命令现在没有接 `otelx`。**怎么报仍然等主人定**，这里只是把材料备齐。
- 这一项**只验证了 `recon` 这个程序**；长期运行那一套的 `ReconFoundDifferences` 因为“有差异”而响（退出码 1）仍然没有真实触发过，归到第 16 项。
- 清理：`scripts/cluster/services.sh e2e-down`，命名空间、`e2e_*` 库、Kafka 和 Redis 里 `e2e-` 前缀的东西都删了。期间长期运行那一套没有不正常的 Pod。

**第 14 项：压测，普通下单阶梯打过写入的拐点（真实触发；2026-11-23 02:26 到 03:04 UTC 发压，03:43 静止）**

做法：本机 `make down`；`LOADTEST=1 IMAGE_TAG=daac679 scripts/cluster/services.sh deploy gateway`（只给 gateway 叠压测的那一层，关掉按地址的限流；六个服务的代码从 `daac679` 起没有变，所以没有重新构建镜像，没有用 `make deploy`）；`recon-cron-suspend`、`probe-stop`、`loadtest.sh pin gateway=2 order=2`；SKU 7599 到 7608。预热 100 单/秒 2 分钟（`3c14-warm`），阶梯 125、156、195 单/秒各 3 分钟、280 单/秒 25 分钟（`3c14-order-ladder`，02:29:46 起，顶档 02:38:46 起）。没有加静默。每分钟一行的记录在 `tmp/3c-inject-14-load.log`，在现场走 runbook 的输出在 `tmp/3c-inject-14-runbook-*.log`，各档的服务端指标在 `tmp/k6/3c14-order-ladder/stages/`。

各档（Prometheus 的数，每档取后段）：

| 到达（单/秒） | 125 | 156 | 195 | 280 |
|---|---|---|---|---|
| 受理 / 被背压拒绝（每秒） | 125 / 0 | 156 / 0 | 191 / 0 | 187 / 93 |
| 落库（单/秒） | 125 | 156 | 187 | 187 |
| 落库延迟 p99（秒） | 1.6 | 4.6 | 11.8 | 55 到 57 |
| 受理 p99（毫秒，gateway） | 45 | 73 | 192 | 195 到 406 |
| gateway 的请求（每秒） | 484 | 756 | 1259 | 约 1600 |
| 限流退回进程内（每秒，占请求的比例） | 0 | 48（6%） | 444（35%） | 约 800（50%） |
| 两个 agent 节点的 CPU | 52%、62% | 71%、82% | 82%、91% | 85% 上下、93% 到 96% |

响了什么（`firing` 的时刻取 Alertmanager 里的 `startsAt`）：

| 告警 | 级别 | 进 `pending` | `firing` | 通知 | 恢复 |
|---|---|---|---|---|---|
| `RateLimitFallingBack` | warning | 02:34（156 档） | 02:39:09 | 02:39:40 送到 alertsink | 03:09:40 |
| `SLOOrderPersistDelayBurnFast` | critical | 02:41（进顶档约 3 分钟） | 02:43:40 | 02:44:10，Telegram 发出 1 条 | 03:09:10，Telegram 1 条 |
| `SLOOrderPersistDelayBurnMedium` | critical | 02:40 | 02:45:10 | 被快档抑制，没有发 | 和快档同时 |
| `SLOOrderPersistDelayBurnSlow` | warning | 02:45 | 03:02 前后 | 没有发（没有送到 alertsink） | 和快档同时 |
| `NodeCPUHigh`（agent-2、agent-1） | warning | 02:37、02:40 | 02:47:06、02:49:36 | 送到；agent-1 02:57 恢复又进 `pending`，多一轮 | 03:07 |
| `OrderReadBulkheadRejecting` | warning | 02:38（195 档） | 02:47:39 | 02:48:09 | 03:09 前后 |
| `OrderBackpressureRejecting` | warning | 02:40 | 02:50:17 | 02:50:47 | 03:09 前后 |
| `ConsumerBlocked{order.close.delay}` | warning | 03:06（停压之后） | 03:11:35 | 03:12:05 | 03:44 前后（积压清完） |
| `ServerDiskLow` | warning | 03:03 | 03:12:36 | 03:13:06 | 没有恢复（见下面第 4 条） |
| chart 自带的 `NodeSystemSaturation`（两个节点） | warning | 02:32 起 | 02:46、02:49 | 送到 | 03:07 |

- **登记时预期的六条里响了四条**：`OrderBackpressureRejecting`、落库时延的 SLO（三档都响了）、`NodeCPUHigh`、`RateLimitFallingBack`。**没有响的两条**：`OrderQueueNearWaitLimit`（队列顶在上限 2497，落库 187 单/秒，折成的等待是 13 到 14 秒，够不着 24 秒；这条只有落库速率掉到约 104 单/秒以下或者落库停了才会响，打过拐点的压测触发不了它，归到第 16 项）；`OutboxRelaySlow`（outbox 的 p99 一直在 1 秒以内，归到第 16 项）。
- **登记时没有预期的四条**：`OrderReadBulkheadRejecting`、`ConsumerBlocked`、`ServerDiskLow`、自带的 `NodeSystemSaturation`。前三条都是真事，不是误报。
- **通知的数量**：这一次过载，Telegram 两条（快档的触发和恢复，发送失败 0），alertsink 上 warning 的触发 9 条（含 `NodeCPUHigh`、`NodeSystemSaturation` 因为第二个节点加进同一组而重发的）。落库时延的 SLO 响的时候，原因一级的几条 warning（背压、节点 CPU、读拒绝、限流退回）没有被它抑制，各发各的：是设计 3.5 里定的（SLO 只压同一条 SLO 的低档），这里记下实际的样子。
- **中档没有拖尾巴**：停压之后没有新的落库观测值，三档的比例同时没有了分母，一起消失（设计第 8 节的行为 1）。可用性的中档在第 3 项里拖了 25 分钟，是因为那时请求一直在。
- **runbook 都在现场走过**（`order-backlog.md`、`slo-order-persist-delay.md`、`node-resources.md`、`order-read-busy.md`、`ratelimit-fallback.md`、`consumer-blocked.md`、`node-disk.md`），命令都能跑，前四份走得到原因。后三份补了内容：见下面三条。七份的“历史”都写了。

这一轮查出来的问题，**都没有改**：

1. **限流的退回在过载时不是零星的，是一半的请求。** 整轮 123 万次，占 gateway 289 万请求的 42%；156 单/秒那一档节点 CPU 只有 71% 和 82% 就已经有 6%。机制（读过 `pkg/ratelimit/fallback.go`）：问 Redis 的时限 20 毫秒，任何一个请求超时就打开 1 秒的冷却，这 1 秒里这个 Pod 的请求全部走进程内；每个 Pod 每秒几百个请求时，节点一忙总有一个超过 20 毫秒。日志里是“恢复”之后几十到几百毫秒又“失败”。缓存 Redis 自己只用了 0.35 核。后果是按用户、按接口的限额在系统最忙的时候大部分时间是按副本各算各的（2 倍）。**和 3b 记的数（1907 请求/秒时每秒 12 次，0.6%）差两个量级：查清了，是 3b 把超时的次数当成了退回的次数，行为没有变，见下面“追查”第 1 条。**候选的改法：连续几次失败或者按失败比例才打开（和 `pkg/breaker` 一样的判据）。保护代码，等主人定。
2. **延迟关单的消费者在过载时落后，停压之后 40 分钟才追平。** 过载的 25 分钟里 `order.close.delay` 每秒处理 40 到 110 条，订单每秒进来约 190 张；order 的日志里有 25 条 `close expired order failed: context deadline exceeded`。停压后每秒处理 107 到 160 条，那时 order 只用 0.3 核、集群是空的，所以不是资源不够，是这个消费者的并发度（查清了，见下面“追查”第 2 条）。到期的消息最多晚了 1481 秒（约 25 分钟）才处理，也就是没付款的订单最晚在下单 40 分钟后才关单、放回预占（时限是 15 分钟）。账没有错（见下面的对账）。长稳的 125 单/秒 4 小时没有出现过，因为到达速率低于它的处理速率。`ConsumerBlocked` 因此在停压 7 分钟后响，它报的是对的；runbook 原来没有这一条原因，第 2 步的查询里也没有这一组，补了。
3. **落库延迟的 p99 到过 57 秒，离回收的宽限期（60 秒）差 3 秒。** 队列的上限是全局的 2500，按总的落库速率算等待是 13 秒，但 p50 约 11 秒、p99 是它的四倍多（3b 的 244 档是 27.8 秒）。这一轮没有订单被回收（受理 380792、落库 380790，落库的结局里没有 `hold_missing`），但已经没有余量：到达再高一点或者盘再慢一点就会出现“受理了的订单消失”。原因不只是 round-3 2.2 记的“慢的分片排得长”，见下面“追查”第 3 条。**背压承诺的“最多等 30 秒”在 280 单/秒时已经不成立。** 候选的改法：按分片各设上限，或者把全局的上限按最慢的分片的速率重算（更小）。涉及订单受理，等主人定。
4. **尾部采样在过载时留下的链路多得多，35 分钟把 k3s-server 的盘写掉约 8 GB（“等于没有采样”“带落库的链路都超过 500 毫秒”是当时的推断，不对，实际的数见下面“追查”第 4 条）。** Tempo 收了 7.5 GB 链路（卷 13 G）、Loki 2.6 GB，k3s-server 从约 22 GiB 掉到 14.1 GiB，`ServerDiskLow`（15 GiB）03:13 响。原因（按 Collector 的配置推的，采样计数的指标没有查到，没有用数字核对）：尾部采样的规则是“出错的全留、超过 500 毫秒的全留、其余 10%”，落库一排队，带落库的链路都超过 500 毫秒。Tempo 的保留期 3 天，**这条告警要挂到 11 月 26 日这批数据过期**，除非提前收（缩短保留期是可以先做的，删数据要主人确认）。离 critical 的 8 GiB 还有 6 GiB，停压之后没有东西在大量写。候选的改法：“慢”的那一条规则不看带异步落库的整条链路的时长，或者给“慢”和“出错”各加一个每秒的上限。**在这之前，打过拐点的压测先看 server 的剩余，至少留 10 GB**；`loadtest.sh preflight` 的 15 GB 下限这次是过了的（当时约 23 GB），不够用。
5. `NodeCPUHigh` 在阈值上下的节点会抖（agent-1：02:49 响、02:57 恢复、又进 `pending`），多发一轮通知。没有处理；改法是给它加 `keep_firing_for`，和别的规则问题一起等主人定。
6. gateway 的 5xx 整轮约 23 个（下单 5 个是 `stock_gate_unavailable`，查订单 11 个，支付 7 个），k6 一侧的失败比例 0.002%，可用性的错误比例一直是 0。受理时延的错误比例最高 3.7%，没有到任何一档；k6 的受理 p99 是 223 毫秒（阈值 200，没有过，过载时是预期的）。

收尾：

- **对账和核对都通过**。`wait-quiet` 第一次 20 分钟超时（延迟关单还有 7.1 万张没关），第二次 03:43 静止；`loadtest.sh recon -since 100m` 四对差异 0（窗口里 379641 单）；`loadtest.sh verify`（两次运行的 `orders.jsonl` 合在一起）`RESULT: PASS`，十个 SKU 的预占都是 0，闸门和库里的可用数相等。k6 一侧：下单请求 503601，受理 366310，轮询 10 秒内看到落库的 219981，轮询超时 146329（落库排队超过 10 秒的那些，订单本身都落库了）。
- **集群恢复成平时的配置**（03:45 UTC 核对）：gateway 重新部署成不带压测那一层的（`RATE_LIMIT_IP` 不在 ConfigMap 里，镜像仍是 `daac679`），两个 ScaledObject 的暂停去掉，`recon` 的两个 CronJob 恢复，探针起来了。`recon-hourly` 03:07 那一次因为暂停没有跑，04:07 那一次的窗口（2 小时）盖得住这一轮。本机 `make up`。
- **留下的**：`ServerDiskLow` 在响（第 4 条）；库里多了约 37.8 万单；落库时延的 7 天预算被这一轮烧掉一大截（1 小时窗口的错误比例到过 39%），和可用性的预算一样，这一周只当读数看。
- **恢复之后还有一条尾巴**：探针恢复之后落库时延的慢档（`SLOOrderPersistDelayBurnSlow`，warning，看 1 天和 2 小时两个窗口）又进了 `pending`（03:46 UTC）：探针一有落库，比例重新有了分母，而 2 小时的窗口里还装着这一轮的慢订单。它多半会在 04:01 前后响，到 05:04 前后（顶档滑出 2 小时窗口）恢复；这是多窗口的尾巴，不是新的问题，**没有等着看**。`ProbeNoData` 同时在 `pending`（探针刚起来），探针已经在以每秒 1 单下单，应当自己消失，同样没有等着看。所以上面“中档没有拖尾巴”只在没有流量的那 40 分钟里成立。

**第 14 项的追查（2026-11-23，主人要求先查清再定怎么改；都用 Prometheus 里留着的数和代码，没有再发压）**

1. **限流的退回：行为没有变，是 3b 记错了量。** 两天前同一个场景的那一轮（`l5a-order-overload`，2026-11-21 07:19 起，125、156、195、244 单/秒）在 Prometheus 里的退回是每秒 0、6、150、792 次，这一轮是 0、48、444、约 800，同一个样子。round-8 记的“1907 档每秒 12 次”取的是 Redis 调用超时的次数（`db_client_operation_duration_seconds_count{error_type="timeout"}`），不是 `gateway_ratelimit_fallbacks_total`；一次超时打开 1 秒，这 1 秒里这个 Pod 的请求都算退回。这一轮顶档是每秒 3.4 次超时对应约 800 次退回，156 档是 0.2 次对应 48 次，都是两百多倍。所以“0.6% 的请求”那句话是错的，round-8 第 3 节和 `ratelimit-fallback.md` 已更正。**问题本身（过载时一半的请求不走分布式的限额）从有这段代码起就在，不是新出现的。**
2. **延迟关单：每个分区串行，4 个分区在过载期间和之后一直是满的。** `kafkax.NewDelayConsumer` 不允许分区内并发（`pkg/kafkax/consumer.go`：延迟 topic 靠“分区内按到期时间有序、队首没到期整个分区等着”，并发会让后面的越过队首），`order.close.delay` 是 4 个分区，所以同时只有 4 条消息在处理。量到的：处理耗时之和从 02:44 到 03:25 一直是每秒 4.0 秒（4 个 worker 全忙）；每条平均 26 到 30 毫秒（空闲时）、36 到 94 毫秒（过载时），折成上限是每秒约 140 条和 40 到 110 条；订单每秒进来约 190 张。**所以它的上限低于落库的上限（约 190 单/秒），只要到达速率长时间高于约 140 单/秒就会落后，和过不过载无关**；长稳的 125 单/秒在它下面。日志里那 25 条 `close expired order failed` 是 sweeper（兜底的那一路，30 秒一批 100 张）的，不是这个消费者的。候选的改法：把这个 topic 的分区数加大（不改代码；延迟消息的 key 换分区没有顺序上的要求，但要确认加分区时老分区里没到期的消息照常处理）；或者让延迟消费者“队首到期之后”再按 key 分 lane 并发（改 `pkg/kafkax` 的并发代码）。
3. **落库延迟 p99 是按总速率算的四倍：全局的上限只管得住总数，管不住单个分区。** `order.submit` 是 4 个分区，各自约 44 单/秒，进出的速率四个分区几乎一样（生产 44.5 到 45.3，消费 42.7 到 45.8），但 02:58 时四个分区的消费延迟是 792、1844、1252、1015 条，两个 order Pod 上的落库延迟 p50 是 19 秒和 5.8 秒、p99 是 58 秒和 20 秒。背压把“受理的总速率”压到等于“落库的总速率”，总数顶在 2500；但每个分区自己的进和出是相等的，没有东西把它的队列拉回平均值，于是各个分区的长度自己漂，最坏的情况是 2500 个几乎都排在一个分区里：2500 ÷ 44 ≈ 57 秒，正是量到的最大值。**所以这个上限能保证的等待时间是“上限 ÷ 单个分区的落库速率”，不是“上限 ÷ 总速率”**；round-3 把 27.8 秒归到“两个分片的盘不一样快”，只是其中一部分。每个分区上到底排了多少个占用没有直接的指标，上面是用消费延迟和两个 Pod 的分位数推的。还有一点没有查：背压的第 2 条（排队超过 30 秒的占用多于 100 个就拒绝）为什么没有把等待拦在 30 秒左右，那时每秒约有 17 张订单是等了 30 秒以上才落库的。
4. **尾部采样：留下了 26% 的链路，不是全部；多出来的两类各有原因。** Collector 的计数（38 分钟）：链路 324 万条，留下 84.5 万条；按策略是“出错”21.6 万、“慢”35.8 万、“其余的一成”32.3 万。Tempo 收到 2053 万个 span，最多的是 inventory 和 order 的 MySQL 语句。(a) “出错”的 21.6 万和被拒绝的请求数对得上（背压的 429 约 13.8 万加查订单的 `READ_BUSY`）：这些请求不该算出错（工程规范：限流、降级是正常结果），多半是 gRPC 客户端一侧的 span 被自动标成了 Error，没有打开一条链路核对。(b) “慢”的 35.8 万是整条链路超过 500 毫秒的，过载时落库的 handler 平均就要 500 毫秒（`order.submit` 的处理耗时两个 Pod 是 516、498 毫秒），所以落库的链路大半都留下了，而它们正是 span 最多的那一类。异步的链路是用 link 关联的另一条链路（Collector 配置里的注释），所以不是“受理到落库整条算一条”。

**背压第 2 条为什么没有把等待拦在 30 秒（2026-11-23，主人要求查；读代码加 Prometheus 里留着的数）**

- **规则本身和配置都没有问题。** `Hold` 的脚本（`pkg/stockgate/stockgate.go`）里第 2 条是 `ZCOUNT gate:holds -inf (now-MaxWait)` 大于 `MaxStale` 就拒绝；集群上 `ADMIT_MAX_WAIT`、`ADMIT_MAX_STALE` 没有设，用的是默认的 30 秒（宽限期的一半）和 100 个。占用在落库的 handler 一开头认领时才从 `gate:holds` 里拿掉，所以排队的时间都算在里面。
- **它限制的是“排队超过 30 秒的占用有多少个”，不是“最老的排了多久”。** 这是有意的（注释里写了：按个数是为了容忍零星的孤儿占用，它们要留到宽限期才被回收）。100 个超时的占用如果分散在很多条队列的队首，每条只超一点点；如果集中在一两条慢的队列里，最老的可以超很多。
- **落库实际上是 96 条各自串行的队列，不是 4 条。** 集群上 `SUBMIT_CONCURRENCY` 是 24：每个分区里按 key 分成 24 条 lane，一条 lane 里串行。过载时 handler 的 p50 是 0.47 秒（p99 约 1.5 秒），一条 lane 每秒落约 1.9 单，96 条合起来约 180 单/秒，和量到的落库速率对得上（两个 Pod 上处理耗时之和是每秒 46 和 44.5 秒，也就是平均有 90 条 lane 在忙）。一条 lane 上排 100 个超时的占用要再等 100 ÷ 1.9 ≈ 53 秒，两条 lane 各 50 个是各等 26 秒：30 + 26 = 56 秒，和量到的 p99（55 到 57 秒）是一个数。**所以第 2 条在这种结构下给出的上界大约是 30 秒加“100 ÷ 一条队列的速率”，队列分得越细（并发度越高、每条越慢）这个上界越松。**
- **它那次到底拒绝了多少，没有数据。** 脚本对三条规则返回的都是 `BACKLOG`，order 不逐条记日志，指标里只有一个 `backlogged`。队列长度的采样一直贴着 2500（2497、2492），说明第 1 条在起作用；上面的算法说明第 2 条多半也在起作用，但这是推的。
- **对第 3 个问题的影响**：上限改成 1250 之后平均的等待减半，分区一级的最坏情况是 28 秒，但 lane 一级的不均和第 2 条“限个数不限时长”这两点没有变，落库延迟的尾巴会短，不能保证在 30 秒以内。要把它做实有两条路：把 `ADMIT_MAX_STALE` 调小（孤儿是零星的，20 个也容得下；上界变成 30 秒加 20 ÷ 1.9 ≈ 10 秒），**主人 2026-11-23 定了做，已改成 20**（`deploy/cluster/services/order.yaml`）；让脚本返回是哪一条拒绝的、把它做成指标（不增加 Redis 的查询），下一次过载时就有数据，**主人同一天定了做，做了**：`pkg/stockgate` 的 `HoldResult.Rule`，order 的 `order.admission.backlog_rejections{rule="queue|stale|stalled"}`（Prometheus 里是 `order_admission_backlog_rejections_total`），它是 `order_admissions_total{outcome="backlogged"}` 的细分；`loadtest-round.sh` 的存档和 `order-backlog.md` 的第 1 步加了这条查询。

**第 14 项查出的四个问题的修正（2026-11-23，主人确认都按建议改；决定和道理在 [ADR-0035](../adr/0035-overload-corrections.md)）**

| 问题 | 改了什么 | 在哪 | 生效了吗 |
|---|---|---|---|
| 限流一次超时就退回 1 秒 | 连续失败 5 次才打开；半开的试探失败马上重新打开 | `pkg/ratelimit/fallback.go`，加了两条测试（零星失败不打开、并发） | 见 `PROGRESS.md` |
| 延迟关单每秒最多约 140 条 | topic 8 个分区：新建时按 `eventx.CloseDelayPartitions`，已有的用 `middleware.sh kafka-topics` 加 | `api/eventx/eventx.go`、`services/order/cmd/order/main.go`、`scripts/cluster/middleware.sh` | 同上 |
| 落库延迟 p99 到 57 秒 | `ADMIT_MAX_QUEUE` 2500 改成 1250（30 秒 × 单个分区的落库速率）；`ADMIT_MAX_STALE` 100 改成 20 | `deploy/cluster/services/order.yaml` | 同上 |
| 过载时链路留得太多 | gRPC 客户端的拒绝不算出错（Collector 里改状态）；“出错”“慢”各加每秒 800、600 个 span 的上限 | `deploy/cluster/monitoring/otel-gateway.yaml` | 同上 |

- 验证到哪：`pkg/ratelimit` 的测试带 `-race` 跑了 3 遍通过；Collector 的配置用 `otel/opentelemetry-collector-contrib:0.165.0 validate` 校验过语法（只校验了这两个 processor，行为没有验证）；`shellcheck` 过了；`make lint` 11 个模块 0 issues；完整的 `make test` 见 `PROGRESS.md`。**四处都没有在过载下重新验证。**
- 没有改的：延迟消费者的并发、按分区计数的队列上限、`NodeCPUHigh` 的抖动。

**主人 2026-11-23 定的规则改动（“等主人定的”第 2 条的七件事；规则都已下发，`make rules-test` 通过）**

| 事 | 主人定的 | 做了什么 | 验证 |
|---|---|---|---|
| `ReconFoundDifferences` 分不清有差异和没跑成 | 按退出码拆成两条 | CronJob 加 `podFailurePolicy`（退出码 1 → `FailJob`，不重试）、`backoffLimit: 2`（其余的重试 2 次）；规则按 Job 的失败原因分成 `ReconFoundDifferences`（critical）和新的 `ReconRunFailed`（warning） | 第 17 项真实触发了“没跑成”那一条，见下 |
| `RedisDown` 把 Sentinel 算进去 | 排除 Sentinel | `RedisDown` 只看数据节点；新增 `RedisSentinelDown`（warning，某个 Sentinel 连不上 5 分钟） | 单元测试；没有在集群上触发 |
| `AlertmanagerNotificationsFailing` 和自带的重复 | 留自带的，删自己的 | 删了规则和三个测试，runbook 改成讲自带的两条 | 无 |
| `CDCLagging` 的文字、两条通知里的分钟数 | `humanizeDuration`；时间尽可能准确 | `CDCLagging`、`CDCLaggingTooLong` 除以 1000 按秒比，通知是“落后 6m 5s”；`ConsumerNotConsuming` 写“至少 20 分钟”，`CDCPipelineMissing` 写“至少 15 分钟” | 单元测试钉了 `CDCLagging` 的文字 |
| `NodeCPUHigh` 抖 | 问好不好解决 | 加 `keep_firing_for: 10m` | 单元测试（掉下去 5 分钟不恢复，掉下去 10 分钟多恢复） |
| 两个抑制源恢复时会不会先撤 | 先验证 | Kafka 的做了实验（第 18 项），MySQL 的只按机制和配置核对，都没有改规则，见下 | 见下 |
| 从库 NotReady 时自带的两条说同一件事 | 没看懂，问怎么改 | 没有改，解释和建议在 `PROGRESS.md` | 无 |

- **按退出码拆的做法，和事先想的不一样的两处。** (1) 第 15 项里写的“kube-state-metrics 里没有退出码可用”是对的（`last_terminated_exitcode` 只在容器原地重启之后才有），但不需要退出码：Job 的 `podFailurePolicy` 能让“退出码 1”的失败带上不同的失败原因。在集群上用两个一次性的 Job 核对过 kube-state-metrics v2.20.0 给出的序列：退出码 1（`FailJob`）是 `kube_job_failed{condition="true"}` 1、`kube_job_status_failed` 三种认识的原因（`BackoffLimitExceeded`、`DeadlineExceeded`、`Evicted`）都是 0、另有一条不带 `reason` 的是 1；重试耗尽是 `reason="BackoffLimitExceeded"` 1。它只认识这三种原因，所以规则写成“失败了，并且不是这三种”算有差异：认不出的原因一律报成重的那一条。(2) 原来的规则用 `kube_job_status_failed > 0` 判断失败，有了重试之后不能再用：重试途中失败的 Pod 数已经大于 0 而 Job 还没有失败。改成看 `kube_job_failed{condition="true"}`。
- **`RedisDown` 排除 Sentinel 之后多做的一步。** 只排除的话，3 个 Sentinel 掉到 2 个就没有任何告警了（`RedisSentinelBelowQuorum` 只在剩 1 个时响，以前这一档靠 `RedisDown`），三个全不在也没有（没有活着的 Sentinel 来报）。所以加了 warning 的 `RedisSentinelDown`，少一个就报。先写的是只管“三个全不在”的一条，子 Agent 改 runbook 时指出掉一个没人报，换成了这一条。
- **通知里的时间为什么写“至少”。** 这两条的条件成立时故障已经持续了一段固定的时间（位点的 `delta` 要 10 分钟才是 0；推上来的序列要约 5 分钟才从查询结果里消失），再加 `for`。通知之后告警还会继续响，所以写不出一个一直准确的数，“至少 N 分钟”在任何时刻都是对的。

**第 17 项：手工建一个连不上从库的对账 Job（真实触发；2026-11-23 07:20 到 07:25 UTC，没有动任何组件）**

- `kubectl create job --from=cronjob/recon-hourly --dry-run=client -o json`，把 `MYSQL_REPLICA_DSNS` 里一个端口改成 3399 再建。三个 Pod 都是 `connect order_replica_0: ... i/o timeout`（退出码 2），间隔 10 秒、20 秒，07:21:23 Job 以 `BackoffLimitExceeded` 失败。
- `ReconRunFailed`（warning）07:22 进 `pending`，07:22:52 `firing`，07:23:22 送到 alertsink，没有发 Telegram。**`ReconFoundDifferences` 全程没有进 `pending`。** 重试途中（失败的 Pod 数是 1、2）两条都没有动。
- 07:24 手工跑一次成功的（差异 0），约 1 分钟后告警恢复，07:28:22 恢复的通知送到；失败的 Job 已删。
- **没有验证的**：“有差异”那一条在长期运行的那一套上因为退出码 1 而响，仍然没有真实触发过（要有真的差异）；它依据的序列形状是用一次性的 Job 核对的。

**第 18 项：停一个 Kafka broker，量恢复时抑制的源和目标谁先撤（真实触发；2026-11-23 07:28 到 07:43 UTC，主人同意后做的）**

做法：Strimzi operator 缩到 0（不然它马上把 Pod 建回来），删掉 `kafka-dual-2`（不是 KRaft 的 leader，卷留着），停 8.5 分钟，operator 缩回 1。每 8 到 15 秒一行的记录在 `tmp/3c-inject-18-kafka-broker.log`。

| 时刻（UTC） | 事 |
|---|---|
| 07:28:25 | broker 2 删掉；95 个分区副本不足 |
| 07:31:15 | `KafkaBrokerMissing`（critical）`firing`，07:31:44 送到，Telegram 也发了 |
| 07:33:49 | `KafkaUnderReplicatedPartitions`（12 个 topic）`firing`，**被抑制，一条都没有送出去** |
| 07:36:55 | operator 缩回 1 |
| 07:42:00 前后 | operator 把 `kafka-dual-2` 建回来（**回来 5 分钟之后**，见下） |
| 07:42:30 | 副本不足的分区回到 0 |
| 07:42:46 | `KafkaUnderReplicatedPartitions` 不在了（目标先走） |
| 07:42:55 | `kafka_brokers` 回到 3 |
| 07:43:20 | `KafkaBrokerMissing` 不在了（源后走，晚约 35 秒） |

- **这一次没有“恢复时先撤”：目标比源早走约 35 秒，没有漏出通知。** 原因和事先想的相反：副本追上得很快（平时每秒 1 单，8 分钟里欠的数据很少，Pod 起来 30 秒内就同步完），而 Kafka Exporter 看到的 broker 数更新得慢。
- **但这个顺序不是有保证的，是量出来的一次。** 副本追上要多久取决于停的这段时间里欠了多少数据：压测中停一个 broker，追上要的时间会长得多，而 broker 数的更新不会变慢，顺序会反过来。所以结论是“平时不会漏，过载或者停得久的时候可能漏”。给 `KafkaBrokerMissing` 加 `keep_firing_for` 是便宜的保险（代价是恢复的通知晚几分钟），**没有加，等主人定**。
- **顺带看到的：operator 回来之后 5 分钟才把 Pod 建回来。** 它启动后的第一次调和从 07:37:04 一直“in progress”到 07:42:08，这期间 `StrimziPodSet` 的状态仍然显示 3/3，缺的 Pod 没有人建；第一次调和结束、下一次调和开始时才建。像是第一次调和在等连不上的那个 broker 直到超时（默认的操作超时是 5 分钟），没有查。含义：operator 和一个 broker 同时不在（例如它们在同一个节点上、节点重启）时，broker 的恢复要多等 5 分钟。记给阶段 4。
- `KafkaOperatorMetricsMissing`、`KafkaOperatorNotReconciling` 各进过 `pending`，没有到 10 分钟，没有响。停着的 8 分钟里探针照常下单（三副本、`min.insync.replicas` 2），没有别的告警。
- 收尾（07:45 UTC 核对）：3 个 broker，副本不足的分区 0，调和成功的计数在涨，没有在响的告警，没有不正常的 Pod。
- 做的时候的一个不小心：07:41 前后（恢复阶段）重新下发过一次规则（改 `RedisSentinelDown`）。改的是 `middleware.redis` 组，Kafka 的规则在另一个组里，状态没有受影响，但实验中间不该动规则。

**`MySQLDown` 这个抑制源（没有做实验，按机制和配置核对的）**

- Prometheus 里留着的两次 MySQL 掉线（2026-11-20 16:21 整个集群重启、2026-11-22 03:11 的 30 秒）都是主库和从库一起掉、一起回来，不是“主库单独掉几分钟”，当不了证据。
- 压同一个实例的那一条（`equal: [instance]`，目标是这个实例自己的复制指标）：源和目标来自 exporter 对同一个实例的同一次抓取，又在同一个规则组里求值，同一轮一起恢复，没有先后。
- 压同一套里从库 IO 线程的那一条（`equal: [cluster]`）：主库回来之后，exporter 下一次抓取（30 秒一次）`mysql_up` 就是 1，源恢复；从库要等自己的下一次重连（`CONNECTION_RETRY_INTERVAL` 是 10 秒，在 `mysql-order-0-1` 上读的）再被下一次抓取看到，目标才恢复。两个实例的抓取时刻不同步，所以**目标可能比源晚走一轮求值，最多约 40 秒**；这 40 秒里正好轮到这一组刷新（5 分钟一次）才会漏，一次事故大约一成多的可能，漏出来的是一条 critical。
- 真的验证要让一个订单主库单独停几分钟，是阶段 4 的实验，没有在这里做。**没有改规则，等主人定**（同样是加 `keep_firing_for`）。

**主人同一天定的两处代码改动：延迟消费者并发、队列上限按分区数（决定和道理在 [ADR-0036](../adr/0036-per-partition-backlog-and-concurrent-delay.md)）**

| 改动 | 在哪 | 测试 |
|---|---|---|
| 延迟消费者可以设 `Concurrency`：等到期放在分发那一步，到期之后按 key 并行 | `pkg/kafkax/consumer.go`（`runLanes`、`process`）、`delay.go` | `delay_test.go` 加三个：到期的确实并行并提交到末尾；队首没到期时后面到期的不处理、位点不越过它；等到期时停止 |
| order 的延迟关单用它，`CLOSE_CONCURRENCY` 默认 4 | `services/order/cmd/order/main.go`、`deploy/cluster/services/order.yaml` | 无新测试（配置） |
| 算一条有 key 的消息会进哪个分区、读 topic 的分区数 | `pkg/kafkax/partition.go` | Kafka 自己的 murmur2 测试向量；把 200 条消息真的发到 8 个分区核对 |
| 闸门的背压前两条按队列数，另记一份索引 | `pkg/stockgate/stockgate.go`（三个脚本） | `stockgate_test.go` 加七个：各队列各数各的、四条离开队列的路都清索引、第二条按队列、第三条不分队列、残留按时间清掉、不分队列时行为不变、并发下上限严格成立 |
| order 受理时传分区号，启动时读 `order.submit` 的分区数 | `services/order/internal/service/{service,submit}.go`、`cmd/order/main.go` | `TestSubmitBackpressurePerPartition`：4 个分区、上限 1 时正好受理 4 单，每单的队列等于它的消息会去的分区 |

- 验证到哪：`pkg`、`services/order`、`api` 三个模块 `make lint` 0 issues、`make test` 通过（28 个包 ok，`tmp/item5-test.log`）。**没有跑别的模块，没有部署，没有在过载下验证。**
- 写的时候的一处取舍：索引里的残留靠 `Hold` 自己按时间清（比“两倍宽限期加一个回收间隔”更老的条目），不靠分两次发布。原因和代价在 ADR-0036 的“道理”第三段。
- `OrderQueueNearWaitLimit` 的含义跟着变了：它拿全部占用的个数除以总的落库速率，四个分区都满时算出来约 27 秒，超过它的 24 秒线，会响。那时确实贴着等待的上限，没有改规则；下一次过载压测时留意它是不是在该响的时候响。

**主人 2026-11-23 第二轮定的（规则和 Alertmanager 的配置都已下发）**

- **两个抑制源加 `keep_firing_for: 5m`**：`MySQLDown`、`KafkaBrokerMissing`。单元测试各加一个。
- **从库 NotReady 时自带的两条重复，加了两条抑制**：源是 `MySQLDown`、`MySQLReplicaSQLThreadStopped`、`MySQLReplicaIOThreadStopped`，目标是 `KubePodNotReady`（`equal: [namespace, pod]`）和 `KubeStatefulSetReplicasMismatch`（`equal: [namespace, statefulset]`）。两边原来没有同名的标签（我们的是 `instance`、`cluster`），所以在三条规则上补了 `namespace: mysql`、`pod`（取 `instance`）、`statefulset`（取 `cluster`）。两条复制的规则也加了 `keep_firing_for: 5m`：复制恢复之后 Pod 的就绪要等探针和下一次抓取，目标会比源晚走。比原来建议的多一个源（`MySQLDown`）：实例连不上时 Pod 同样是 NotReady。`monitoring.sh inhibit-test` 加了 7 条用例，54 条 0 条不符（合成的告警；真实的告警上没有验证，要停复制超过 15 分钟）。
- **`NodeCPUHigh` 的 `keep_firing_for` 从 10 分钟改成 3 分钟**：主人嫌恢复的通知晚 10 分钟太长。表达式是 5 分钟的平均，秒级的抖动已经抹掉了，3 分钟盖得住平均值在线上下来回蹭；掉下去超过 3 分钟再回来算新的一次。上次压测里 agent-1 是恢复之后马上又进 `pending`，3 分钟够不够，这一轮压测看。
- **第 4 项拆成 4a、4b**，4b 归第 16 项（计划文件的登记表）。

**重跑打过拐点的压测（2026-11-23 08:29 到 09:06 UTC 发压；镜像 `bc84433`，和第 14 项同一个阶梯）**

做法和第 14 项一样：本机 `make down`，gateway 叠压测那一层，对账的 CronJob 暂停，探针停掉，gateway、order 各钉 2 个副本；SKU 7609 到 7618（**下一批从 7619 起**）。预热 `3c14r-warm`，阶梯 `3c14r-order-ladder`（08:32:14 起，顶档 280 单/秒 08:41:19 到 09:06:24）。每 45 秒一行的记录在 `tmp/3c-rerun-load.log`，两轮顶档的对比在 `tmp/3c-rerun-compare.txt`，各档的服务端指标在 `tmp/k6/3c14r-order-ladder/stages/`。

顶档 280 单/秒的 23 分钟，两轮对比（Prometheus 的数）：

| | 第 14 项（`daac679`） | 这一轮（`bc84433`） |
|---|---|---|
| 受理 / 被背压拒绝（每秒） | 186 / 94 | 185 / 94 |
| 背压按规则（每秒） | 没有这个指标 | `queue` 20.5，`stale` 73.5 |
| 落库（单/秒） | 186 | 186 |
| 落库延迟 p50（秒） | 10.7 | 17.3 |
| 等了 10 秒、20 秒、30 秒、60 秒以上的订单 | 52%、28%、7.7%、0 | 77%、40%、10.1%、0 |
| 未认领的占用，最大 / 平均 | 2500 / 2359 | 4706 / 3216 |
| 延迟关单：处理（条/秒）/ 最老的到期消息晚了多久 | 80 / 510 秒（停压后到 1481 秒） | 152 / 15 秒 |
| 限流退回进程内（每秒，占 gateway 请求） | 738（47%） | 360（21%） |
| gateway 问 Redis 超时（每秒） | 3.4 | 21.9 |
| Tempo 收到的（整轮） | 7.3 GB，2014 万个 span | 2.3 GB，593 万个 span |
| 留下的链路：出错 / 慢 / 其余一成 | 21.1 万 / 35.1 万 / 31.6 万 | 3.5 万 / 2.8 万 / 35.2 万 |
| k3s-server 的盘，压完时 | 14.2 GiB | 23.6 GiB |
| 查订单被读名额拒绝（`read_gate`，每秒） | 52 | 126 |
| gateway 的 5xx（每秒） | 0.017 | 0.46 |
| order 等连接池（次/秒） | 4.7 | 20.7 |
| 关单的 handler 平均耗时 | 45 毫秒 | 91 毫秒 |

k6 一侧：下单请求 503168，受理 366946，失败比例 0.033%（上一轮 0.002%）；原因码 `BACKLOGGED` 135379、`READ_BUSY` 191885（上一轮 72833）、`ORDER_LANDING_UNAVAILABLE` 735、`STOCK_GATE_UNAVAILABLE` 208、5xx 45。

逐处的结论：

1. **延迟关单：修好了。** 到期的消息最多晚 15 秒（上一轮 25 分钟）。这里面有两个改动（8 个分区、每个分区并发 4），这一轮分不出各自的贡献。
2. **链路采样：修好了。** 写盘的量是上一轮的三分之一，“出错”“慢”两类都被上限压住，盘只掉了约 3.7 GiB。
3. **限流的退回：只好了一半。** 从 47% 降到 21%，`RateLimitFallingBack` 照样响。同时 gateway 问 Redis 的超时从每秒 3.4 次涨到 21.9 次：以前打开之后一秒不问 Redis，现在问得多了，超时也就多了。“连续失败 5 次才打开”在这里不够，推断的原因（没有用数据核对）：一个 gateway 实例每秒有八百多个请求同时在问 Redis，节点一卡，同一刻在途的几十个请求一起超时，“连续 5 次”一瞬间就数满了。这和 3b 里熔断器的那一条是同一个道理（被动数失败的判据要和并发度比，要加持续时间的条件）。**没有改，等主人定。**
4. **队列上限按分区数：机制是对的，这个数取得不对，结果比上一轮差。** 受理和落库的吞吐一点没变（都是 186 单/秒），变的只有“排着的有多少”：平均从 2359 涨到 3216，等待的中位数从 10.7 秒涨到 17.3 秒，等了 30 秒以上的从 7.7% 涨到 10.1%。原因有两层：
   - **持续过载时缓冲越深，等得越久，吞吐不会多一单。** ADR-0036 把“能吸收的突发从 7 秒的量回到 27 秒的量”当成好处，那只在突发时成立；过载持续的时候，多出来的缓冲全部变成等待。
   - **每个分区 1250 是按“正好等 28 秒”算的，没有给 lane 之间的不均留余量**，所以真正在拦的是第二条：`stale` 每秒拒绝 73.5 次，`queue` 只有 20.5 次。第二条的拦法是“等到已经有 20 张订单排了 30 秒以上才拒绝”，它是兜底，不是用来定等待时间的；让它当主力，结果就是总有一成订单等过 30 秒。
   - 要让等待明显短于 30 秒，每个分区的上限要取到第一条先拦住为止，比如 600（约 14 秒）。**没有改，等主人定。**
5. **“落库延迟 p99 约 57 秒”是分桶造成的，上一轮的也是。** 这个直方图在 30 秒和 60 秒之间没有桶；只要有百分之几的订单落在这一格，按线性插值算出来的 p99 就是五十多秒（10% 落在这一格时是 30 + 30 × 9/10 = 57）。它能说明的只有“有多少订单等了 30 秒以上”，说不出最长等了多久。两轮都没有一张订单等过 60 秒，也没有订单被回收。**上一轮据此写的“离宽限期差 3 秒”“2500 ÷ 44 ≈ 57 秒正是量到的最大值”，后一句的吻合是巧合**；全局上限管不住单个分区这个机制仍然成立（那一轮四个分区的消费延迟是 792 到 1844 条），但它造成的最坏等待有多长，当时和现在都没有量到。要量得给直方图在 30 到 60 秒之间加桶（40、50）。**没有改。**
6. **这一轮新出现的，都没有查清：**
   - `MeshEndpointEjected` 响了两条（order 到 inventory、到 payment 的端点被摘除，08:46 到 08:56），k6 一侧有 735 个 `ORDER_LANDING_UNAVAILABLE`、208 个 `STOCK_GATE_UNAVAILABLE`，gateway 的 5xx 是上一轮的 27 倍（仍然只有每秒 0.46 个）。上一轮没有摘除。
   - order 等连接池的次数是上一轮的 4 倍，关单的 handler 慢了一倍。延迟关单现在同时关 32 张（上一轮 4 张），而连接池在过载时本来就贴着上限，这是最可疑的原因，没有核对。
   - `InventoryGateDrift` 响了（08:51）。
   - 查订单被读名额拒绝的多了一倍多：等落库的订单多了、等得久了，轮询跟着多（3b 记过的正反馈）。
7. **告警**：critical 只有 `SLOOrderPersistDelayBurnFast` 一条发了 Telegram。`NodeCPUHigh` 这一轮没有抖（agent-2 08:50 响、agent-1 09:00 加进同一组，没有出现“恢复又触发”），`keep_firing_for` 3 分钟够不够只有这一个样本。`OrderQueueNearWaitLimit` 的情况见 `PROGRESS.md`。

**第 4a 项：payment 缩到 0（真实触发；2026-11-23 09:25:24 到 09:51:16 UTC，26 分钟，探针不停）**

| 告警 | 级别 | `firing` 送到 | 从停掉算起 | 恢复的通知 |
|---|---|---|---|---|
| `IngressGateway5xxHigh` | critical | 09:31:35，Telegram 也发了 | 约 6 分钟 | 09:56:35 |
| `ProbeFailing` | critical | 09:31:42，Telegram 也发了 | 约 6 分钟 | 09:56:42 |
| `ServiceMetricsMissing{payment}` | warning | 09:36:16 | 约 11 分钟 | 09:56:16 |
| `ConsumerNotConsuming{payment.order-events}` | warning | 09:46:05 | 约 21 分钟 | 09:56:05 |

- **登记时预期的五条里只响了 `ConsumerNotConsuming` 一条，另外响的三条是没写进预期的。** 原因是登记时把支付想成了异步的：
  - 探针的“支付”一步是同步调 gateway 的 `/pay`，payment 不在时直接 503（26 分钟里 1365 个），所以最先响的是 `IngressGateway5xxHigh` 和 `ProbeFailing`。这两条响是对的：用户付不了款。
  - 支付单根本建不出来，所以没有“建了 20 分钟还没封口的支付单”，**`PaymentReplyOverdue` 连 `pending` 都没有进**；支付处理时延的 SLO 没有观测值，也不响。这两条要“支付单建出来了而结果到不了订单”才触发，和 `PaidOrdersNotLanding` 是同一类，一起归到第 16 项（4b）。
  - `PaymentTotalsCheckStalled` 没有响，是对的（核对读的是库，不经过 payment 服务）。
- **担心的“退款的路”没有发生，也是同一个原因。** 没有支付单就没有可退的。这段时间下的约 1300 张探针订单到 15 分钟的时限被正常关单（`pending_payment` 到 `closed`），库存放回。payment 回来时 `payment.order-events` 上积着 719 条订单事件，一分钟内消费完，没有产生支付单。
- 可用性的 SLO 没有响：它只看下单的接口，`/pay` 的 503 不算在里面。**付款整个不可用 26 分钟而四个 SLO 没有一个响**，发现它靠的是探针和入口的 5xx。支付的可用性要不要进 SLO，记给写 SLO 的 ADR 时定。
- 恢复：payment 回来 5 分钟内四条都恢复，没有漏出别的通知。`TargetDown` 进过一次 `pending`（payment 的 Pod 刚起来）。
- runbook：这四条都是真实触发过的（第 3、5 项），这次没有再逐份走。

