# chaos/：故障场景

设计：[docs/design/4a-chaos-prep.md](../docs/design/4a-chaos-prep.md) 的 3.5、3.6。注入脚本是 `scripts/cluster/chaos-run.sh`，稳态的压测和检查是 `scripts/cluster/chaos-steady.sh`。

**盲测时主人不读 `chaos/scenarios/` 和 `chaos/runs/`，不 `kubectl get` 任何 `*.chaos-mesh.org` 的资源（见最后一节）。** 这份 README 本身可以读：里面没有任何一个场景的内容。

## 目录

```
chaos/
  README.md
  scenarios/<编号>/
    scenario.yaml     元数据（字段见下）
    inject/           注入用的东西，随方式不同（见下）
    truth.md          答案：标准根因、定位必需的关键证据、容易误判成的原因
  runs/<运行号>/      不入库（.gitignore）。每次注入一个目录，脚本建，人不用碰
```

`scenario.yaml` 加 `truth.md` 就是阶段 6 评测集的一条。

**编号不透明**：`s` 加三位数字，按建立的先后给，不按类别排，目录名和文件名里不带任何含义（不叫 `kill-pod`）。
盲测抽签时看不出一个编号是什么；这条规则也适用于 `inject/` 里的文件名和对象名（对象名一律用 `c-<运行号>`）。

## 怎么加一个场景

1. 取下一个没用过的编号，建 `scenarios/<编号>/`。
2. 写 `scenario.yaml` 和 `inject/`。
3. 写 `truth.md`，三栏标题固定（脚本按标题抽取，填进复盘草稿的“评测用标注”）：
   `## 标准根因`、`## 定位必需的关键证据`、`## 容易误判成的原因`；可以再有 `## 备注`。
4. `scripts/cluster/chaos-run.sh lint` 检查格式（不连集群）。
5. 第二、三档的场景在阶段计划里登记，并先问主人（`AGENTS.md`：故障注入属于计划内操作，要先登记）。

## `scenario.yaml` 的字段

| 字段 | 必填 | 含义 |
|---|---|---|
| `id` | 是 | 编号，必须和目录名一致 |
| `method` | 是 | 注入方式：`chaos-mesh`、`fault-switch`、`istio`、`script` |
| `risk` | 是 | 风险档 1、2、3（见下） |
| `category` | 是 | 类别，给 `--pool` 抽签时筛选用：`pod`、`network`、`app`、`stress`、`time`、`dns`、`io`、`data`、`mesh`…（自由的词，保持小写） |
| `target.workload` | 是 | 目标工作负载的名字（对应标签 `app.kubernetes.io/name`），**不写命名空间**：命名空间由脚本的环境变量 `CHAOS_NS` 定 |
| `duration` | 是 | 预定时长，如 `5m`、`30m`、`1h`。Chaos Mesh 清单里的 `duration` 不能比它长；故障开关的 ttl 不能比它长 |
| `requires` | 是 | 前置条件，给人看的清单（可以是 `[]`）。脚本不检查它们，`inject` 时打印出来 |
| `recover.restart` | 是 | 恢复时脚本要滚动重启的工作负载（可以是 `[]`）。泄漏类故障关了开关之后后果还在，要靠它 |
| `recover.notes` | 是 | 恢复步骤的说明，给人看（可以是 `[]`） |
| `blind` | 否 | `yes` 或 `no`，不写是 `no`。在集群上试过（注入生效、到时恢复、`truth.md` 的证据和实际对得上）才写 `yes`；`chaos-blind` 只从 `yes` 的里面抽 |
| `also` | 否 | 在主方式之外再叠一种注入方式，唯一合法的值是 `chaos-mesh`，并且只有 `method: fault-switch` 时才能写。清单放在 `inject/mesh/`（见下）。不写就是只有主方式；没写 `also` 却有 `inject/mesh/` 目录，`lint` 报错（防止写了清单忘了字段） |
| `report` | 否 | 一句“有人报了什么现象”，注入之后打印在终端上（盲测也打印）。只给完全安静的场景用：没有告警、用户无感，不给这一句诊断的人没有入口。只写现象，不写原因、不写该看哪里；不能有行尾注释 |
| `soak` | 否 | 注入之后先放多久再开始排查，如 `60m`。给涨得慢的场景用：症状在 30 分钟的排查时限里还看不出来。注入之后终端打印“某个时刻之前不要开始排查”（盲测也打印，不说原因），30 分钟的时限从那个时刻算起；时刻记在运行的 `state.json`（`investigate_at`），`make chaos-status` 也会再打印一次。`soak` 加 30 分钟不能超过 `duration`（故障开关的还不能超过 `ttl`），`lint` 会查 |
| `expected_alerts` | 是 | 预期会响的告警名（可以是 `[]`）。盲测时不看，复盘时和实际对照 |

**YAML 的子集**：脚本里没有 YAML 库，用自带的小解析器读。只支持：顶层 `键: 值`，一层嵌套（缩进 2 个空格），
列表项写成 `- 值`（缩进 2 个空格）或者行内的 `[a, b]` / `[]`，整行的 `#` 注释。不支持锚点、多行字符串、行尾注释、行内的 `{}`。
`lint` 会报解析不了的写法。

### `inject/` 里放什么（随 `method`）

- **`chaos-mesh`**：一个或几个 `*.yaml`，每个是一个 Chaos Mesh 对象。要求：
  - `metadata.namespace` 是 `chaos-mesh`（对象建在 Chaos Mesh 自己的命名空间里，设计 3.2）；
  - 选择器的命名空间写 `namespaces: [__TARGET_NS__]`，脚本填成 `CHAOS_NS`；`lint` 拒绝写死别的命名空间；
  - 名字用 `c-__RUN_ID__`（多个对象加后缀），`metadata.labels` 里带 `oppenheimer.chaos/run: __RUN_ID__`：恢复时按这个标签删；
  - `duration` 写在清单里（到时 Chaos Mesh 自己撤掉）。
- **`fault-switch`**：`switch.yaml`，字段 `switch`（`f01` 到 `f99`）、`ttl`（必须有，不超过 `duration`）、`admin_port`（目标服务管理端口，gateway 是 8089，order 8081，inventory 8082，payment 8083，orderindex 8084）。
  脚本对目标工作负载的**每个**副本经 `kubectl port-forward` 到管理端口，`PUT /fault/<开关>?mode=on&ttl=<ttl>`。
  可选字段 `persist`（`yes` 或 `no`，不写是 `no`）：`yes` 时开关要在进程重启之后还开着。注入时脚本先把开关名写进目标命名空间里一个 Secret，
  等 `CHAOS_FILE_SYNC_WAIT` 秒（默认 120，kubelet 把它同步进已经在跑的 Pod 要一个周期），再经管理端口打开；恢复时先从 Secret 里去掉，再关开关，
  再做 `recover.restart` 的滚动重启（所以 `persist: yes` 要求 `recover.restart` 含目标工作负载，`lint` 检查）。`recover --all` 无条件删掉这个 Secret。
  目标的 Deployment 要挂着它（chart 在部署带故障代码的镜像时自动挂；没挂的话 `persist: yes` 的注入会被拒绝）。
  **叠加 Chaos Mesh（`scenario.yaml` 写 `also: chaos-mesh`）**：`inject/mesh/` 里放一个或几个 `*.yaml`，每个文件的要求和 `method: chaos-mesh` 的清单完全一样（同一段检查，见上面的 `chaos-mesh`）。
  注入时先开故障开关（含 `persist` 那一路），再 `kubectl apply` 这些清单；任何一步失败都走撤销。恢复时顺序相反：先按运行标签删 Chaos Mesh 对象，再关开关。
  至少要有一个 `*.yaml`。盲测时终端上看不出有没有 `also`；老的运行目录（`sealed.json` 没有 `also`）照常恢复。
  清单里可以写运行时才知道的 Pod 地址：`__POD_IPS:<命名空间>:<标签选择器>__`，例如 `externalTargets: [__POD_IPS:redis:app.kubernetes.io/name=redis-cache__]`。
  注入时脚本取这个命名空间里符合选择器、状态是 `Running` 的所有 Pod 的 IP，换成 `"10.42.1.5", "10.42.2.7"` 这样的列表（外面的方括号自己写）；一个地址都取不到就注入失败并撤销。
  地址只写进运行目录的 `private.log`。选择器里不能有下划线（结尾的 `__` 靠它分界），`lint` 查写法；dry-run 不连集群，所以不渲染。
- **`istio`**：一个或几个 `*.yaml`（VirtualService 等），`metadata.namespace: __TARGET_NS__`，名字和标签的规则同 `chaos-mesh`。脚本 `kubectl apply`，恢复时按标签删。
- **`script`**：`run.sh`，接收一个参数 `inject` 或 `recover`，环境变量 `TARGET_NS`、`RUN_ID`、`RUN_DIR`、`KUBECONFIG`。恢复要能重复执行。

## 三档风险

| 档 | 含义 | 例子 | 同意 | 盲测 |
|---|---|---|---|---|
| 一 | 到时自己恢复，不留状态 | 网络延迟、CPU 压力、杀无状态的 Pod | 按池 | 可以 |
| 二 | 要一步明确的恢复动作，数据不受影响 | 泄漏类（重启 Pod）、从库停复制 | 按池 | 可以 |
| 三 | 可能丢数据，或恢复要人工判断 | 主库宕机、磁盘写满、关节点、两个 broker 同时停 | 每次单独同意 | **拒绝**，要加 `--confirm-risk3 <编号>` 显式确认 |

“按池同意”指主人对一个池（比如“第一档、网络类”）同意一次，之后在池里抽哪个都算同意。第三档每次单独问，不进池。

## 怎么用

```sh
make chaos-steady ARGS=start     # 先起稳态的压测（见 chaos-steady.sh 的头部）；启动时刻记在 tmp/chaos-steady/started_at
make chaos-blind POOL=1          # 盲测：从第一档里抽一个，密封，注入；终端只打印运行号和密封的 SHA-256
make chaos-inject SCENARIO=s002  # 不盲：指定场景
make chaos-recover               # 恢复当前的运行，等回到稳态；前台查三个常驻核对的指标，逐行对账放到后台
make chaos-status                # 有没有进行中的运行；逐行对账是不是还在后台跑、有没有不通过又没确认的
make chaos-recon-ack RUN=<运行号> # 看过不通过的对账结果之后确认，下一次注入才放行
make chaos-recover ALL=1         # 总闸：所有 Chaos Mesh 对象、所有故障开关、脚本建的 VirtualService 全部撤掉
make chaos-reveal                # 打开答案，生成 docs/postmortems/ 里的复盘草稿（后台的逐行对账还在跑就等它）
```

**注入前的两道闸**（`chaos-blind`、`chaos-inject`，dry-run 不查）：

- **压测要先跑够 30 分钟**（`CHAOS_STEADY_MIN_AGE` 秒，默认 1800）。不够，或者 `started_at` 不存在（压测没起，或者是旧版脚本起的）就拒绝，并报出已经跑了多久、还差多久：
  诊断的一方会把“流量刚上来”当成故障的原因（上一次走通时压测起了 13 分钟就注入）。`SKIP_WARMUP=1`（`--skip-warmup`）显式跳过，
  跳过时终端打一行说明，运行的 `state.json` 记 `warmup_skipped: yes`；不跳过时记 `steady_age_s`（注入时压测跑了多少秒）。
- **前面的运行有没看过的对账不通过就拒绝**：逐行对账是 `failed`（有差异）或 `error`（出错、或者后台进程没留下结果就没了），或者前台指标那一段的退出码不是 0，
  而这个运行目录里没有 `recon-rows.acked`。看过 `chaos/runs/<运行号>/recon.txt`、`recon-metrics.txt` 之后用 `make chaos-recon-ack RUN=<运行号>` 确认。
  逐行对账还在跑（`running`）不拦，只提示一行。老的运行目录（只有 `recon_rc`）不拦。

**恢复后的对账分两段**：

1. 前台：`recon-check --metrics-only`。三个常驻核对（钱、库存、变更）越过结束时刻并且没有不一致，就算这次实验的数据检查过了，可以开始下一个。
   输出在 `recon-metrics.txt`，退出码在 `state.json` 的 `recon_metrics_rc`。
2. 后台：`recon-check --rows-only`（逐行对账，有负载时要几分钟）。`nohup` 起，脱离终端和 make。输出在 `recon.txt`，退出码写进 `recon-rows.rc`，pid 在 `recon-rows.pid`
   （后台进程不写 `state.json`，避免和前台互相盖掉）。指标那一段不是 0、场景是第三档、或者 `WAIT_RECON=1` 时，逐行对账不放后台，前台等它跑完。
   `NO_RECON=1` 两段都不做。

`chaos-blind`、`chaos-inject`、`chaos-recover` 加 `DRY_RUN=1` 只打印将要做什么，不连集群（`chaos-steady` 用 `ARGS="check --dry-run"`）。命名空间默认 `oppenheimer`，`CHAOS_NS=oppenheimer-e2e` 改成 e2e 那一套；
别的命名空间一律拒绝（尤其 `monitoring`、`kube-system`、`istio-system`、`chaos-mesh`）。

**盲测时脚本的约定**：终端、日志里不出现编号、目标、注入方式；这些只在 `chaos/runs/<运行号>/` 里（`sealed.json`、`private.log`），到 `reveal` 才读。
例外是场景自己写的 `report` 和 `soak`（见字段表）：`report` 那一句是故意给诊断的人看的；`soak` 让终端多出“某个时刻之前不要开始排查”一行，
这一行本身就说明抽到的是涨得慢的那一类，是知道的、接受的泄漏。等的这段时间里不看集群、不看面板（看了就等于提前开始）。

**池里有带 `soak` 的场景时压测要起得够长**（现在是第二档）：抽到哪个事先不知道，所以按最长的算：热身 30 分钟、`soak` 最长的那个（现在 60 分钟）、排查 30 分钟、恢复和对账约 10 分钟，
一个实验就要 2 小时 10 分，默认的 `CHAOS_STEADY_DURATION=2h` 不够。做第二档的盲测时用 `CHAOS_STEADY_DURATION=4h`，SKU 的库存跟着加（每秒约 46 单，4 小时约 66 万单，20 个 SKU 每个备 4 万）。
脚本不检查压测还能跑多久：压测在 `soak` 期间停了的话，到时稳态的面板上会看到流量没了，那一次作废重做。
`sealed.json` 里有随机数，所以终端上那个 SHA-256 猜不出场景编号（编号空间很小，没有随机数的话可以穷举）。

## 盲测时的痕迹（4a 第 1 步冒烟的发现）

Chaos Mesh 不是不留痕迹的：

- **NetworkChaos、IOChaos** 会在**目标命名空间**里给每个目标 Pod 建一个同名的 `PodNetworkChaos` / `PodIOChaos` 对象，实验结束后也留着；
  每次更新还会在目标命名空间产生一条事件（`PodNetworkChaos <Pod 名> Updated`）。`kubectl -n <目标> get podnetworkchaos -o yaml` 直接读得出注入的是什么。
- 给命名空间加的注解 `chaos-mesh.org/inject=enabled` 也看得见。所以**脚本对所有注入方式都加这个注解**（不只是 Chaos Mesh 的），不让“有没有注解”泄漏方式。
- PodChaos、StressChaos、TimeChaos、DNSChaos 在目标命名空间里不留对象和事件。

**约定**：盲测期间（从 `chaos-blind` 到 `chaos-reveal`）主人

- 不看 `*.chaos-mesh.org` 的任何资源（`kubectl get podnetworkchaos` 之类），也不看 `chaos-mesh` 命名空间；
- 看事件时，`involvedObject.kind` 是 `Pod*Chaos` 的那几行不算数，当作没看到；
- 不看 `kubectl get ns -o yaml` 上的注解。

`chaos-recover --all` 之后，脚本会列出目标命名空间里还留着的 `Pod*Chaos` 对象和相关事件；**还有盲测的运行没有 reveal 时，这份清单只写进文件**
（`chaos/runs/leftovers-<时间>.txt`），不打印到终端，等 reveal 之后再看。Agent 一侧的屏蔽（工具里滤掉这类事件、拒绝 `chaos-mesh` 命名空间）是第 5 步的事。
