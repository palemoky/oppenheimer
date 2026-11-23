# 3d 设计：最小诊断 Agent

状态：**主人已确认**（2026-11-24，第 2 节的十项都按建议）。决定记在 [ADR-0038](../adr/0038-diagnosis-agent-own-loop.md)。计划见 [phase-3-observability.md](../plan/phase-3-observability.md) 的 3d 一节，完整版的方向见 [phase-6-agent.md](../plan/phase-6-agent.md)。

这一版的目标是骨架和留痕，不追求准确率：告警来了能自己查一轮、写出带证据的报告，每一步都落盘，阶段 4 的每次故障实验都能留下一份可以回放的运行记录。

## 1. 动工前查到的事实（2026-11-24）

### 1.1 Claude API 和 SDK

来源是 Claude Code 自带的 `claude-api` 参考（缓存日期 2026-11-03）和 Go 模块代理；没有另外去读官方网页。

| 事实 | 对设计的影响 |
|---|---|
| Go 的官方 SDK 是 `github.com/anthropics/anthropic-sdk-go/v2`，模块代理上最新的是 v2.13.0（v1 还在发，最新 v1.84.0） | 用 v2 |
| Claude Agent SDK 没有 Go 版本（它是把 Claude Code 打包成库，自带读写文件和 bash 的工具） | 不用它。计划里写的“Go + Claude API 工具调用”就是 Messages API |
| 做 Agent 有三条路：自己写循环；SDK 的 Tool Runner（beta，循环由 SDK 跑）；Managed Agents（beta，循环和工具的沙箱都在 Anthropic 那边） | 见第 2 节第 1 项 |
| 当前的模型和价格（每百万 token，输入 / 输出）：Fable 5.2 8 / 40 美元，Opus 5.5 4 / 20，Sonnet 5.5 2 / 10，Haiku 5.5 0.1 / 0.5；上下文都是 100 万 token | 见第 2 节第 2 项 |
| Opus 5.5 的思考关不掉，深度只由 `output_config.effort` 控制，默认是 `medium` | 显式写 effort，不靠默认值 |
| Opus 5.5 上强制调用某个工具（`tool_choice` 的 `any`、`tool`）返回 400 | “最后必须交报告”不能靠强制，靠提示加循环里的检查（3.5） |
| 思考块绑定在产生它的那段对话上：历史只能在末尾追加，回头改写前面的消息会让后面的思考块失效 | 工具输出的裁剪必须在结果进入历史**之前**做，进去之后不再动（3.4） |
| 一次回复里可以有多个工具调用；结果要放在**同一条** user 消息里一起返回，失败的工具返回 `is_error`，不能丢 | 循环里并发执行，一起返回（3.2） |
| 缓存按前缀匹配，顺序是工具、系统提示、消息；前缀里任何一个字节变了，后面全部失效。缓存读的价格是 Opus 5.5 每百万 0.20 美元（输入价的二十分之一） | 系统提示和工具定义里不放时间、运行号；告警内容放在第一条 user 消息里（3.2） |
| 安全分类器可能拒绝请求：HTTP 200，`stop_reason` 是 `refusal` | 循环要认这个结束原因；要不要开服务端的回退见第 2 节第 3 项 |
| SDK 自己重试 2 次（429、5xx、连接错误） | 这就是这条调用链上唯一的一层重试，循环不再重试 |

### 1.2 仓库和集群里现在有什么

- Alertmanager 的两个接收端（`webhook`、`critical`）都发到 `http://alertsink.monitoring:8080/alerts`。`alertsink` 只打日志、记数，并记下最近一次收到 `Watchdog` 的时刻，规则 `AlertDeliveryStalled` 靠它判断“告警这条路断了”。清单里写着“3d 的 Agent 做出来之后换成它”。
- Prometheus、Loki 在 `monitoring` 命名空间里（`loki.monitoring:3100`；Prometheus 是 kube-prometheus-stack 的 Service）。自己写的告警 86 条，runbook 46 份。
- Alertmanager 能把 Telegram 发出去，说明集群到外网是通的。**Pod 到 `api.anthropic.com` 通不通没有验证过。**
- 这台机器上没有 Claude API 的凭据（没有 `ANTHROPIC_API_KEY`，没有装 `ant`）。
- 3c 量到的、和 webhook 有关的行为：会收到没有“触发”在前的“恢复”通知；一组告警有变化时每 5 分钟重发一次；`Watchdog` 每 5 分钟一条；被抑制的告警不发通知。

## 2. 主人确认的十项（都按建议）

每项先写建议，再写放弃了什么。

1. **自己写循环，用 Messages API，不用 Tool Runner，不用 Managed Agents。**
   这个项目里循环本身就是要学的东西（终止条件、步数和费用的上限、每一步的留痕都长在循环里），面试问的也是它。Tool Runner 是 beta，能做同样的事但循环在 SDK 里。Managed Agents 省掉自己跑循环，但它的工具在 Anthropic 的沙箱里执行，够不着家里集群的 Prometheus 和 Loki（要绕就得把三个工具都做成由我们回传结果的自定义工具，循环省了、留痕却不在自己手里），另外按会话时长收费。
2. **模型用 `claude-opus-5-5`，effort 用 `high`，都做成配置。**
   诊断是要推理的活，放在中间一档；阶段 6 的“模型分层”再拿 Sonnet 5.5（半价）和 Fable 5.2（两倍）用同一批故障比。粗估（没有实测）：一次诊断十几步，上下文涨到几万 token，有缓存时每次 0.3 到 1 美元。
3. **开服务端的拒绝回退（`fallbacks: "default"`，beta `server-side-fallback-2026-07-01`）。**
   日志里什么内容都可能有，被分类器拒掉时由回退的模型在同一次调用里接着答，否则这次诊断就白跑。代价是整个循环要用 SDK 的 beta 命名空间（`client.Beta.Messages`）。不开的话循环把 `refusal` 当成一种结束原因记下来，报告里写明“模型拒绝了”。你不想依赖 beta 就选不开。
4. **跑在集群里**：`monitoring` 命名空间的一个 Deployment，1 个副本，不进网格，偏好控制面节点（和 `alertsink` 一样的理由：故障实验关的是工作节点）。命令行提问是它的客户端（`POST /ask`，经 `kubectl port-forward`）；同一个二进制也能在本机对着 compose 里的 otel-lgtm 跑，那时没有 Kubernetes 工具。
   放弃的是“Agent 跑在 Mac mini 上”（阶段 6 计划里写的设备）：webhook 要从集群里打过来，三个工具要的地址也都在集群里。
5. **`alertsink` 留着，Agent 是加在旁边的第二个 webhook 地址，不替换它。**
   `Watchdog` 的心跳是“告警送不送得到”的判据，不该依赖一个要调外部 API、会排队、会花钱的程序。清单里“换成它”那句注释跟着改。
6. **什么告警触发诊断**：只处理 `firing`；跳过 `Watchdog`、`info`、带 `synthetic="true"` 的合成告警；同一组告警（`groupKey`）一小时内只诊断一次，除非组里多了新的告警；同一时刻只跑一次诊断，排队最多 5 个，排不进去的记一条日志和指标后丢掉。webhook 收到就回 200，诊断在后台跑。
   不区分 warning 和 critical：这一版要的是积累运行记录。
7. **上限**：每次诊断最多 25 轮模型调用、2 美元、10 分钟；每天合计 20 美元，到了就只记录不诊断。撞到每次的上限时不是直接断掉，而是再给模型一轮、不带工具，让它用已有的证据把报告写完，并在报告里标明是被上限截断的。
8. **报告的结构由程序把关**：模型通过一个 `submit_report` 工具交报告（结构见 3.5），每条证据必须引用它在这次运行里做过的某一步；报告里的“查询和结果”由程序从运行记录里取出来渲染，不采信模型自己抄的数。引用了不存在的步骤就打回去重交一次。
   这样“每条结论附查询和结果”是程序保证的，阶段 6 的指标“报告中无法溯源的结论比例”也现成。
9. **运行记录放在一个 2 GiB 的 PVC 上**（`local-path`，在控制面节点），按运行号一个目录；Agent 自己提供只读的 HTTP 接口列出和取回，脚本把它们拉到本机的 `tmp/agent-runs/`。超过 30 天或总量超过 1.5 GiB 时删最老的。
   k3s-server 的盘紧张过（3c 的 `ServerDiskLow`）；一次运行估计几百 KB，量小。
10. **API 密钥要你提供。** 放在仓库根目录的 `.env`（`ANTHROPIC_API_KEY`，不入库，和 Telegram 的 token 同一个办法），脚本把它做成 Secret。建议在 Console 里单建一个工作区、给它设月度的花费上限，密钥只属于这个工作区。
    顺带要你知道的：诊断时查到的日志、指标、Pod 的状态会发给 Anthropic 的 API。这个项目里没有真实用户的数据。

## 3. 设计

### 3.1 总体的样子

```
Alertmanager ──webhook──▶ alertsink（不变：日志、计数、Watchdog 心跳）
            └─webhook──▶ agent  POST /alerts ─▶ 筛选、去重、排队
命令行 ──port-forward──▶ agent  POST /ask   ─┘        │
                                                       ▼
                                             一次运行（run）
                                        ┌──────────────────────────┐
                                        │ 循环：模型 ⇄ 工具         │──▶ Claude API
                                        │   prom_*  loki_*  kube_*  │──▶ Prometheus / Loki / kube-apiserver
                                        └──────────────────────────┘
                                                       │
                                  runs/<运行号>/ meta.json  events.jsonl  raw/  report.json  report.md
```

新的 Go 模块 `services/agent`（进 `go.work`）。单独一个模块是因为 client-go 的依赖很重，不该进 `pkg`。

```
services/agent/
  cmd/agent/            serve | ask | replay
  internal/loop/        主循环、上限、结束原因
  internal/llm/         对 SDK 的一层薄接口（测试里换成假的）、价格表、费用计算
  internal/tool/        Tool 接口、注册、裁剪的公共部分
  internal/tool/prom/   internal/tool/loki/   internal/tool/kube/
  internal/trigger/     webhook 的解析、筛选、去重、队列
  internal/runlog/      运行记录的写和读
  internal/report/      submit_report 的结构、校验、渲染成 Markdown
```

### 3.2 主循环

一次运行的输入是一个“问题”：告警触发时是这一组告警的标签、注解、开始时间；手动提问时是一句话。

```
messages = [user: 问题 + 现在的时刻（UTC）]
每一轮：
  检查上限（轮数、已花的钱、已过的时间）→ 撞到就走“收尾”
  调模型（工具定义 + 系统提示 + messages）
  把回复原样追加进 messages（含思考块）
  按 stop_reason：
    tool_use   → 并发执行这一轮所有的工具调用（各自 15 秒时限），
                 结果裁剪后放进同一条 user 消息追加进去；
                 其中有合法的 submit_report → 结束
    end_turn   → 没交报告：追加一条 user 消息要求调用 submit_report，只催一次
    max_tokens → 记下，走“收尾”
    refusal    → 结束，原因记为 refused
收尾：追加一条 user 消息说明撞到了哪个上限，再调一轮；这一轮查询类工具的调用一律返回 is_error（“上限已到”），
      只接受 submit_report。工具定义不变（改了会让缓存前缀失效）
```

- **结束原因是枚举**：`reported`、`reported_truncated`（撞上限后收尾交的）、`no_report`、`refused`、`api_error`、`canceled`。它是指标的标签，也写进运行记录。
- **时限逐跳收紧**：一次运行 10 分钟；一次模型调用 3 分钟；一次工具调用 15 秒。工具超时返回 `is_error`，模型可以换个查法。
- **重试只有一层**：SDK 对 429、5xx 的 2 次重试。循环自己不重试；模型调用最终失败，这次运行以 `api_error` 结束，已经做过的步骤都在记录里。工具全是只读的，重复执行没有后果。
- **费用**：每次回复的 `usage`（输入、输出、缓存写、缓存读四个数）乘价格表累加。价格表按模型写在代码里，注明查证日期；配置里的模型不在表里时启动失败，不猜。
- **缓存**：系统提示的末尾放一个缓存断点（盖住工具定义和系统提示），再开请求顶层的自动缓存盖住对话的尾部。系统提示和工具定义是编译进去的常量。记录里每一轮都有 `cache_read_input_tokens`，一直是 0 就是有东西在悄悄改前缀。
- **思考**：自适应，`display` 设成 `summarized`，摘要进运行记录（回放时能看到它当时怎么想的）。思考块原样带回下一轮。
- **现在几点**：模型不知道时间。第一条 user 消息里给出当前时刻和告警的开始时刻；工具的时间参数接受 RFC 3339 和相对写法（`-30m`）。
- **提示注入**：日志里有外部输入（请求头、参数）。系统提示写明工具返回的是数据不是指令。工具全部只读，最坏的结果是一份错的报告。

系统提示的内容（这一版写短，阶段 4 边跑边改）：它是谁、在什么系统上（六个服务、哪些中间件、命名空间，指标和日志的命名约定各几行）；怎么查（先看告警的规则表达式和当前还有哪些告警在响，再找最早出现异常的地方）；证据的要求；什么时候停。不放 runbook 的内容（检索是阶段 6b 的事）。

### 3.3 三组只读工具

每个工具是一个有类型参数的专用工具，不给通用的 shell：参数能校验、能逐个限流、能在记录里按类型统计。

| 工具 | 参数 | 做什么 |
|---|---|---|
| `prom_query` | `query`、`time?` | 即时查询 |
| `prom_query_range` | `query`、`start`、`end`、`step?` | 范围查询，`step` 不给时按“每条序列最多 60 个点”算 |
| `prom_alerts` | `state?` | 现在在响和 `pending` 的告警 |
| `prom_rule` | `alertname` | 这条告警规则的表达式、`for`、标签、注解（含 runbook 的地址） |
| `loki_query` | `logql`、`start`、`end`、`limit?`、`direction?` | 日志查询 |
| `loki_labels` | `label?` | 有哪些标签、某个标签有哪些值 |
| `kube_get` | `kind`、`namespace?`、`name?`、`selector?` | 一个或一组对象的摘要 |
| `kube_describe` | `kind`、`namespace`、`name` | 一个对象的摘要加它的事件（Pod 带上每个容器的状态、上次退出的原因和退出码） |
| `kube_events` | `namespace?`、`kind?`、`name?`、`since?` | 事件 |
| `kube_logs` | `namespace`、`pod`、`container?`、`previous?`、`since?`、`tail?` | 容器日志（`previous` 看崩溃前那一次的） |

- Prometheus 和 Loki 直接用 `net/http` 调它们的 HTTP API（各两三个接口），不引入客户端库。
- Kubernetes 用 client-go（v0.37.2，对应集群的 1.37）。`kind` 是白名单：Pod、Node、Deployment、StatefulSet、DaemonSet、ReplicaSet、Job、CronJob、Service、EndpointSlice、HorizontalPodAutoscaler、PersistentVolumeClaim、Event。**Secret 不在名单里，RBAC 里也不给**；ConfigMap 这一版也不给。
- 权限：单独的 ServiceAccount `agent`，一个 ClusterRole 只有上面这些资源的 `get`、`list`、`watch`，加 `pods/log` 的 `get`。没有 `pods/exec`，没有任何写。清单入库。
- `prom_alerts`、`prom_rule` 是计划里没有单列的：告警触发的诊断第一步就是“这条规则到底在看什么”，没有它模型只能从告警名猜表达式。

### 3.4 工具输出的裁剪

原则三条：

1. **在工具这一层裁，裁完才进历史**，进去之后不再改（1.1 里思考块的约束；也是缓存前缀稳定的条件）。
2. **裁掉了什么要告诉模型**：总数是多少、给了多少、怎么缩小范围。不说的话模型会把看到的那一部分当成全部。
3. **完整的原始结果落盘**（`raw/`），模型看到的是裁过的那份，两份都留。

| 工具 | 规则 |
|---|---|
| 即时查询 | 最多 50 条序列，超过时给总数和前 50 条，并提示用 `topk`、`sum by` 聚合；所有序列共有的标签提出来只写一次；数值保留 4 位有效数字 |
| 范围查询 | 最多 20 条序列、每条最多 60 个点（`step` 不够大时自动放大并说明）；每条序列附最小、最大、最后一个值 |
| 日志 | 默认 100 行、最多 300 行；每行截到 500 个字符；连续相同的行合并成一行加次数；JSON 日志只留 `time`、`level`、`msg`、`error`、`trace_id` 和少数几个字段 |
| `kube_get`、`kube_describe` | 不给整个对象，按类型投影成摘要（不含 `managedFields`、注解的大块内容、**环境变量的值**） |
| `kube_logs` | 默认最后 200 行，同日志的截行和合并 |
| 所有工具 | 单次结果最多 20 KB，超过就截断并写明 |

这些数是先拍的，阶段 4 按运行记录里“被裁掉之后模型又去要了什么”来调。

### 3.5 报告

`submit_report` 的参数（`strict: true`，保证结构合法）：

```
summary            一两句话
root_cause         判断的根因；判断不出来就写不知道，并说明卡在哪
confidence         high | medium | low
evidence[]         { claim: 结论, steps: [这次运行里的步骤号] }
ruled_out[]        { hypothesis: 排除掉的猜测, steps: [...] }
next_checks[]      人接着该查什么
suggested_actions[] 建议的处置（只是建议，Agent 不执行任何变更）
```

每次工具调用由程序编一个步骤号（`s1`、`s2`……），写在返回给模型的结果开头，模型引用的就是它。程序校验：`evidence` 不为空，每个引用的步骤号存在并且那一步没有失败。校验不过把原因作为 `is_error` 返回，让它改一次；第二次还不过就按原样收下并在报告上标 `unverified`。

`report.md` 由程序渲染：结论、置信度；每条证据下面是它引用的每一步的工具名、参数（PromQL、LogQL 原文）、裁剪后结果的前若干行；最后是这次运行的轮数、token、费用、耗时、结束原因。这一版报告只落盘、可以经 HTTP 取，另外打一行日志（运行号、告警名、结束原因、费用）。不发 Telegram。

### 3.6 触发

- `POST /alerts`：解析 Alertmanager 的 webhook（version 4），按第 2 节第 6 项筛选、去重，放进队列，回 200。请求体上限 1 MiB。不合法的请求体回 400（Alertmanager 对 4xx 不重试）。
- 去重的键是 `groupKey`，值是上次诊断时组里告警指纹的集合和时刻。同一个键在冷却期内再来：指纹集合没有新增就跳过。状态只在内存里，重启后丢了的后果是同一组告警多诊断一次。
- 不假设“恢复”之前一定见过“触发”（3c 量到的），`resolved` 的通知一律只记日志。
- `POST /ask`：`{"question": "..."}`，同步等结果，返回运行号和报告。它会花钱，所以 Service 是 ClusterIP，不开对外的路由。
- `GET /runs`、`GET /runs/{id}/{file}`：只读。
- 队列和并发：一个工作协程。停止时正在跑的那次运行收到取消，以 `canceled` 结束，记录完整。

### 3.7 留痕和回放

```
runs/<运行号>/            运行号：UTC 时间加 4 位随机，例 20261124T031502Z-k3f9
  meta.json              触发方式、问题原文（告警的完整内容）、模型、effort、各项上限、镜像版本、系统提示和工具定义的摘要值
  events.jsonl           只追加，一行一个事件
  raw/s<N>.json          第 N 步工具调用的原始结果
  report.json  report.md
  summary.json           结束原因、轮数、工具调用次数（按工具）、四种 token 的合计、费用、总耗时
```

`events.jsonl` 的事件：`model_request`（第几轮、这一轮新加的消息）、`model_response`（完整的内容块含思考摘要、`stop_reason`、`usage`、请求号、耗时）、`tool_call`（步骤号、工具名、参数、耗时、是否出错、原始结果的字节数、裁剪后的结果）、`limit_hit`、`report_rejected`、`end`。每个事件写完就落盘，进程中途死了记录也停在最后一个完整的事件上。

回放（`agent replay <运行号>`）在这一版是**按原样重放给人看**：从磁盘逐步打印模型说了什么、调了什么、看到了什么，不调 API、不碰集群。“换一个模型或提示，用录下来的工具结果重跑”不做：模型换了问法，录下来的结果就对不上，那是阶段 6 评测流水线要解决的事。记录的内容按“以后做得了”来留。

### 3.8 Agent 自己的可观测

`otelx.Setup`，日志 `slog` JSON。指标（标签都是有限的枚举）：

- `agent.runs`：按触发方式（`alert`、`ask`）和结束原因
- `agent.triggers.skipped`：按原因（`resolved`、`filtered`、`cooldown`、`queue_full`、`daily_budget`）
- `agent.tool.calls`：按工具名和结果（`ok`、`error`、`timeout`）
- `agent.tokens`：按种类（输入、输出、缓存写、缓存读）
- `agent.cost.usd`、`agent.run.duration`、`agent.run.turns`

告警名不进标签（放在日志和运行记录里）。这一版不给 Agent 自己配告警规则，阶段 4 用起来之后再看要哪几条。

## 4. 测试

- **循环**：`internal/llm` 是接口，测试里用按脚本回放的假实现。用例：一轮多个工具并发并且结果在同一条消息里；工具出错；撞轮数、费用、时间三种上限后的收尾；`refusal`；`max_tokens`；没交报告被催一次；报告引用了不存在的步骤被打回。时间相关的用 `testing/synctest`。
- **SDK 的那一层**：`httptest` 起一个假的 Messages 接口，核对请求的形状（缓存断点的位置、effort、工具结果的排列）和 `usage` 到费用的换算。
- **工具**：Prometheus、Loki 用 `httptest`；Kubernetes 用 client-go 的 fake clientset。裁剪规则是表驱动的测试。
- **触发**：筛选、冷却、队列满、并发的 webhook（`-race`）。
- **RBAC**：一个测试读清单，断言里面没有写动词、没有 `secrets`、没有 `pods/exec`。
- 不在 CI 里调真的 API。

## 5. 验证（用 3c 里人为触发告警的办法试跑）

每一项动集群之前先在计划里登记、拿到你对具体做法的同意。候选（都是 3c 做过、影响小的）：

1. 手动提问一次（不动任何东西）：“现在集群里有没有不正常的地方”。
2. 对账没跑成：手工建一个从库地址连不上的对账 Job（3c 第 17 项），应触发 `ReconRunFailed`。
3. 一次性的 OOM Pod（3c 第 16 项里做过）：`ContainerOOMKilled`，看 Kubernetes 工具有没有用上。
4. payment 缩到 0 约 10 分钟（3c 第 4a 项）：几条告警同时响，看它能不能说出共同的原因。

每次看的是：链路通不通、报告能不能溯源、轮数和费用落在哪、缓存有没有命中、裁剪有没有让它反复要同一份数据。准不准只记下来，不当验收条件。

## 6. 实现顺序和分工

| 步 | 内容 | 谁做 |
|---|---|---|
| 0 | 核对：Pod 到 `api.anthropic.com` 通不通（一次性的 Pod，要你同意）；密钥到位；SDK 在 Go 1.27 下能编译；版本登记到 `versions.md` | 主会话 |
| 1 | 模块骨架、`Tool` 接口、`runlog` 的格式（共享的部分，在主线上串行做） | 主会话 |
| 2 | 主循环、费用和上限、报告的校验和渲染、触发和去重 | 主会话（Fable） |
| 3 | 三组工具和各自的裁剪，三个 worktree 并行 | Opus 子 Agent |
| 4 | Dockerfile、Deployment、RBAC、PVC、Secret 的脚本、Alertmanager 加第二个地址（含 `amtool` 的用例）、`make` 目标 | Sonnet 子 Agent |
| 5 | 第 5 节的试跑 | 主会话 |
| 6 | ADR-0038（为什么自己写循环、报告为什么由程序渲染证据）、阶段笔记、面试考点 | 主会话定内容，Sonnet 写 |

## 7. 不在这一步做的

- Tempo、pprof、MySQL、Redis、Kafka 的工具，runbook 和复盘的检索（阶段 4 按需加，阶段 6 补全）。
- 评测、判分、模型对比（阶段 6）。
- 任何写操作和自动处置。
- 把报告发到 Telegram。
- 多轮对话（追问上一次诊断）。
- 上下文压缩：一次运行被轮数和费用的上限卡住，到不了需要压缩的长度。

## 8. 实现中的发现和量到的数

### 第 1、2 步：骨架、主循环、报告、触发、留痕（2026-11-24）

写完了，`services/agent` 六个包的测试（`-race`）和 lint 通过。三组工具还没有接（`buildTools` 是空的），所以现在的 Agent 只有 `submit_report` 一个工具。**没有对真的 API 跑过。**

和上面的设计不一样的地方：

- **循环拿不到对话的历史。** 3.2 的伪代码里 `messages` 是循环自己的变量；实现里历史收在 `llm.Conversation` 的实现里，循环只调 `Send`。原因：SDK 的回复要原样放回历史（思考块带签名），用自己的类型转一道会丢；收起来之后“只追加”由结构保证。
- **收尾给两轮，不是一轮。** 一轮交报告，一轮改被打回的报告。收尾期间查询类的调用返回“上限已到”的错误，工具定义不变（改了缓存的前缀就失效）。两轮用完时，最后一份能解析的报告按 `unverified` 收下，不丢。
- **`max_tokens` 截断的那一轮里的工具调用一个都不执行**（参数可能不完整），但每个都回一条错误的结果：历史里只要有工具调用，下一轮就必须给它结果。
- **步骤号先编后执行。** 按调用在回复里的顺序编号，再并发执行，号和谁先跑完无关。未知的工具不占号。
- **报告在同一轮的查询做完之后才校验**，所以它可以引用同一轮里的步骤。
- **一轮里有两个 `submit_report`**：只认第一个，第二个回错误。
- **每天的预算重启后从运行记录里加出来**（`summary.json` 的费用），不会因为重启清零。
- **建不了运行目录时诊断照做**，只是没有留痕（打一条错误日志）。盘满的时候一份没有留痕的报告仍然比没有报告好。
- **费用的指标是 `agent.cost.micro_usd`**（整数，百万分之一美元），不是 3.8 里写的 `agent.cost.usd`：`otelx` 的计数器是整数的。
- **记录里的上限按秒写。** `encoding/json/v2` 对 `time.Duration` 没有默认的表示，直接编码会失败。第一版因此 `meta.json` 写不出来、整次运行没有留痕，而留痕失败只打日志，单元测试全过；是端到端测试（假的模型，真的磁盘）发现的。

设计里没有想到、现在知道的限制：

- 发生拒绝回退时，费用只算了最后作答的那一次（`usage` 顶层），被拒绝的那一次在 `usage.iterations` 里，没有算。
- 程序只核对“引用的步骤存在并且没有出错”，核对不了这一步的结果是否真的支持那条结论。
- 手动提问的人断开之后，那次诊断照常做完并落盘（钱照花）。

### 第 3 步：三组工具（2026-11-24，UTC 是 11 月 23 日 17:00 前后）

三个 Opus 子 Agent 各在一个 worktree 里写，主会话读了 diff（`prom` 全部非测试代码、`loki` 的查询和裁剪、`kube` 的白名单和脱敏；`kube` 的 `project.go` 900 多行的各类型投影**没有逐行读**）、跑了测试和 lint 之后 cherry-pick 进来。十个工具，名字和参数同 3.3 的表。加了一个子命令 `agent tool <工具名> '<JSON 参数>'`：手工调一次工具，打印模型会看到的那份结果，不调模型。

**对着真的后端核对过**（Prometheus、Loki 经 `kubectl port-forward`，Kubernetes 用本机的 kubeconfig；都是只读）：十个工具里试了 `prom_alerts`、`prom_rule`、`prom_query`、`prom_query_range`、`loki_labels`、`loki_query`（日志和指标两种）、`kube_get`、`kube_describe`、`kube_events`、`kube_logs`，还有写错的 PromQL、LogQL 和不允许的类型（Secret）。**没有经过模型**：模型会不会用、裁剪的数合不合适，要等有了密钥试跑才知道。

核对查出来、单元测试没有发现的：

- **Loki 里每一行日志自成一条流。** Collector 写进来的每一行都带 `observed_timestamp`（每行不同）、`k8s_pod_uid` 这些结构化元数据，Loki 把它们和标签一起返回。结果是行首的标记比日志本身还长，连续相同的行也永远合并不了。改成先去掉一组对诊断没用的标签（`loki/query.go` 的 `noiseLabels`）。子 Agent 的测试数据是按文档里的响应形状造的，造不出这个。
- **Loki 的指标查询返回 61 个点。** step 按“区间 ÷ 60”算，点数是区间 ÷ step 再加一。改成除以 59 并向上取到整秒。Prometheus 那一组是对的（它一开始就按 `maxPoints-1` 算）。

和 3.3、3.4 不一样的地方：

- **工具的 Schema 都不是 strict 的**：strict 要求字段全部 required，而这些工具有可选参数。`additionalProperties` 仍是 false，`tool.Decode` 拒绝多出来的字段。只有 `submit_report` 是 strict。
- **3.4 里日志的两个数合不上**：300 行 × 500 字符约 150 KB，远超单次结果 20 KB 的上限。`loki_query` 自己按约 18 KB 装行，并写明装了多少、没装多少（否则是 `tool.Cap` 从末尾悄悄截，行数的说明会丢）。
- **时间的相对写法多认一种“天”**（`-1d`）：模型常这么写，`time.ParseDuration` 不认。
- **裁序列时留哪些**：即时查询超过 50 条时按值从大到小留（没超过时保持 Prometheus 给的顺序，查询里可能用了 `sort()`）；范围查询超过 20 条时留峰值最高的。自动选的 step 取整齐的值（1 小时的区间是 2 分钟、31 个点）。
- **`prom_rule` 找不到告警名时返回错误**，并列出名字相近的最多 10 条。
- **`kube_describe` 不给命名空间时在所有命名空间里按名字找**，唯一就用，多个就报错并列出来。
- **`kube_events` 的 `kind` 只是事件的过滤条件，不在白名单里的也接受**（比如看 Ingress 的事件）；它不读那个对象本身。
- **落盘的原始结果里多抹了两样**：`kubectl.kubernetes.io/last-applied-configuration` 注解（里面是一份完整的旧对象，环境变量的值也在）；命令行参数里名字像密钥的值和带用户名密码的地址（尽力而为，`--password x` 这种分成两个参数的认不出来）。

留下的限制：

- `kube_logs` 的原始结果就是日志原文，应用打了什么就落盘什么。
- `tool.TrimLines` 按整行比较，带时间戳的行合并不了；`loki_query` 自己按“不含时间的内容”合并，`kube_logs` 没有做（容器日志每行开头就是时间，重复行不会合并）。
- 20 KB 的总上限截断 50 条的列表时，只说截了多少字节，不说留下了几条。
- client-go 的 fake 不认字段选择器，“服务端按对象过滤事件”只在真集群上走到过。

### 第 4 步：部署用的清单和脚本（2026-11-24；只写了文件，没有下发）

Sonnet 子 Agent 写的，主会话读了 RBAC 的规则和 `cmd_agent`，重跑了 `make alertmanager-test`、shellcheck、`services/agent` 的测试和 lint，本机构建了一个现有服务的镜像（order，不推送）。**清单没有在集群上 apply 过，`cmd_agent` 没有运行过。**

- **修了主会话引入的一个问题**：`services/agent` 进了 `go.work` 之后，`deploy/docker/` 下三个 Dockerfile（服务、chaos、e2e）都要多复制它的 `go.mod`，否则所有镜像都构建不出来（`go.work` 列出的目录在构建上下文里不存在）。CI 不构建镜像，所以两轮 CI 都是绿的。`bench1a.Dockerfile` 用的是 1a 的旧工作区，不受影响。
- `deploy/cluster/monitoring/agent.yaml`：ServiceAccount、ClusterRole（只有白名单里那些资源的 get、list、watch 和 `pods/log` 的 get）、2Gi 的 PVC、Deployment、Service。`services/agent/deploy_test.go` 读这份清单，断言没有写动词、没有 `secrets`、`configmaps`、`pods/exec`、通配符。
- `scripts/cluster/monitoring.sh agent`（`make agent`）：镜像没有就构建，`.env` 里有 `ANTHROPIC_API_KEY` 就做成 Secret `agent-anthropic`（没有也能起来，只是每次诊断都以 `api_error` 结束），下发，等就绪。不在 `monitoring.sh up` 里。
- **Alertmanager 的第二个地址是带开关的**：配置里两个接收端各加了一段 `# agent-begin` 到 `# agent-end`，渲染时默认删掉，`AGENT_WEBHOOK=1 scripts/cluster/monitoring.sh alertmanager` 才带上。原因：Agent 没有部署时带着这个地址，Alertmanager 每次通知都会失败并触发它自己的失败告警。`make alertmanager-test` 现在检查四种渲染（有没有 Telegram × 有没有 Agent）。给 Agent 的那一条 `send_resolved: false`（它不处理恢复通知）。
- 留下的风险：`local-path` 的卷把 Pod 钉在一个节点上，那个节点没了 Pod 会一直 Pending，要删 PVC 才能换节点（运行记录也就没了）；`/ask` 没有鉴权，集群里任何 Pod 都能让它花钱（有每天的预算兜底）；`/healthz` 不检查 Claude、Prometheus、Loki，依赖坏了 Pod 不会被重启，要靠指标发现。

