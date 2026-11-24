# 4a 设计：故障实验的准备

状态：**主人已确认**（2026-11-24：“4a 的设计待定项都按建议”，第 2 节的第 1 到 9 项都按建议；第 10 项早先已定、已做）。2026-11-24 写。

计划：[phase-4-chaos.md](../plan/phase-4-chaos.md) 的“4a 准备”。这一步只做准备：把“能注入、能盲测、能验数据、Agent 能看 pprof”这四件事搭好，并用一个不盲的场景从头到尾走通一遍。4b 的每个实验另外登记、另外做。

## 1. 动工前查到的事实（2026-11-24）

### 1.1 仓库里已经有的

| 东西 | 在哪 | 和 4a 的关系 |
|---|---|---|
| 删 Pod 加对账 | `make cluster-chaos`（`scripts/cluster/chaos.sh`），在 `oppenheimer-e2e` 里 | 最小版本的“随机杀 Pod”，判定标准是数据一致。4a 的注入脚本沿用它“注入、等静止、对账”的顺序 |
| 边车的故障场景 A 到 G | `make cluster-fault`（`scripts/cluster/fault.sh`） | 验证的是熔断、并发上限、超时，是断言式的测试，不是盲测 |
| 调试端口（pprof） | `pkg/app/debug.go`，`DEBUG_ADDR`，集群上是 `127.0.0.1:6060`（`deploy/charts/service/values.yaml`） | 只绑在回环上，Pod 外面连不到；`block`、`mutex` 默认不采样，取到的是空的 |
| 降级开关 | `pkg/degrade`，管理端口上 `GET /degrade`、`PUT /degrade/{name}?mode=`，不持久 | 故障开关的接口照它的样子做 |
| 诊断 Agent 的工具 | `services/agent/internal/tool`，接口是 `Def()` 加 `Call(ctx, input)`；现有 prom 4 个、loki 2 个、kube 4 个 | pprof 工具是第四组 |
| Agent 的权限 | `deploy/cluster/monitoring/agent.yaml`，ClusterRole 只读，不含 secrets、configmaps、`pods/exec` | pprof 要多给一样权限（第 3.3 节） |
| 发压 | `make k6 SCENARIO=order\|flash\|mixed\|ratelimit`（k6，没有账本）；`make loadtest`（Go 的发压工具，有账本，结束后 `load verify`） | 稳态用 k6；要证明“不丢”的实验用带账本的 |
| 按时间段对账 | `make recon-check FROM= TO=`（3e 做的），退出码 0 通过、1 有差异、2 出错、3 还没核对到 TO | 每次实验结束后的数据检查就是它 |
| 三个常驻核对 | `recon-totals`、`recon-stock`、`recon-changes` 和九条告警 | 实验期间一直在跑，数据坏了会响 |
| 复盘模板 | `docs/postmortems/TEMPLATE.md`，末尾有“评测用标注”三栏（标准根因、关键证据、容易误判成的原因） | 场景的答案文件用同样的三栏 |

仓库里没有 `pkg/fault`，没有带构建标签的文件，没有 `chaos/` 目录，没有 NetworkPolicy。阶段 6 的计划只说评测集是“注入清单加标准根因加关键证据”，没有规定格式，格式由这里定。

### 1.2 集群（只读核对的）

- k3s v1.37.2，containerd 2.3.7，三个节点 Ready。内存已用：`k3s-server` 60%、`k3s-agent-1` 52%、`k3s-agent-2` 68%。Chaos Mesh 的预算是 0.5 GB（`hardware.md`）。
- Istio 1.32.0，边车模式。
- **有一条告警在响**：`ContainerMemoryNearLimit`，`mysql-main-0` 的常驻内存是 limit 的 90.4%，从 11 月 23 日 22:44 UTC 起。只读查过，不是泄漏：malloc 的堆里约 700 MiB 是 MySQL 已经释放、glibc 没有还给系统的，主要来自 3c 过载压测时 225 个连接的那一次高峰（数和依据在 `PROGRESS.md` 的“4a 设计写完”）。同样大小的高峰不会再涨多少，但连接数更高的高峰会：现在离 limit 只剩约 240 MiB，连满 `max_connections=400` 估计正好要这么多。阶段 4 的故障会造出这种高峰，所以实验开始前要处理；不处理的话这条告警也会一直响着。

### 1.3 Chaos Mesh（查的官方发布页和文档）

- 最新的正式版是 **v2.8.8**（发布页标着 Latest；页面上的日期没有年份）。Helm 仓库 `https://charts.chaos-mesh.org`，chart 用 `--version 2.8.8`。
- k3s 上要给两个值：`chaosDaemon.runtime=containerd`、`chaosDaemon.socketPath=/run/k3s/containerd/containerd.sock`。
- **它支持哪些 Kubernetes 版本没有查到**（文档里“支持的版本”那一页打不开）。能找到的只有 v2.8.2 的发布说明里把端到端测试升到了 1.33 到 1.35。我们是 1.37，**装上之后要逐类冒烟**，不能假设都能用。
- `chaos-daemon` 是特权的 DaemonSet（每个节点一个，`hostPID`）。
- `controllerManager.enableFilterNamespace=true` 之后，只有带注解 `chaos-mesh.org/inject=enabled` 的命名空间能被注入。
- Helm 升级不更新 CRD，升级时要另外 `kubectl apply --server-side` 一次。
- DNSChaos 要 `chaos-dns-server`，默认会装。
- 文档里没有提和 Istio 边车一起用的注意事项。

## 2. 主人确认的十项（都按建议）

每项的理由在第 3 节对应的小节。

| # | 要定的事 | 建议 |
|---|---|---|
| 1 | 实验在哪个命名空间做 | 长期运行的那一套（`oppenheimer`）。告警规则、SLO、探针、常驻核对都对着它，`oppenheimer-e2e` 里什么都没有。破坏数据的场景除外（第 9 项） |
| 2 | 阶段 4 期间长期运行的那一套换成带故障代码的构建 | 换。镜像 tag 带 `-faults` 后缀；阶段 4 结束换回来 |
| 3 | 主人在实验之前读不读故障代码和场景文件 | 不读。`*_faults.go` 和 `chaos/scenarios/` 在复盘时再看（每个场景自带答案文件）。合并 PR 时这些文件由主会话评审，主人只看框架部分 |
| 4 | 盲测怎么拿“同意” | 按池同意：场景分三档风险，主人同意的是“今天从第一、二档里抽”，不是具体哪一个；注入的命令由主人自己运行（`! make chaos-blind ...`）。第三档（可能丢数据或要手工恢复的）每次单独同意，不盲测 |
| 5 | Agent 怎么连到 pprof | 经 Kubernetes 的端口转发（给 `pods/portforward`，只在 `oppenheimer` 命名空间）。调试端口继续只绑回环 |
| 6 | `mutex`、`block` 采样 | 阶段 4 期间用环境变量常开一个低的采样率，装上之后量开销；不做成“运行时打开”的接口 |
| 7 | 服务之间的延迟和错误用什么注入 | 用 Istio 的故障注入（VirtualService），不用 Chaos Mesh 的 HTTPChaos；Chaos Mesh 的 dashboard 不装 |
| 8 | Agent 能不能看到 Chaos Mesh | 不能。Agent 的工具拒绝 `chaos-mesh` 命名空间，日志采集不收它 |
| 9 | 会写坏数据的代码缺陷 | 只允许写坏派生数据（商家索引、缓存），不碰钱、库存、订单状态；并且只在 `oppenheimer-e2e` 里做 |
| 10 | `mysql-main-0` 内存那条告警（第 1.2 节） | **已定、已做（2026-11-24）**：加了 `MALLOC_ARENA_MAX=4` 并重启了主库；管不管用等第一次发压时看（计划文件“4a 准备”里登记了） |

## 3. 设计

### 3.1 两条注入的路

故障分两类，注入的办法不同：

- **从外面能造的**（杀 Pod、网络延迟和分区、CPU 和内存压力、磁盘、时钟、DNS）：Chaos Mesh。一个场景就是一份清单，`duration` 到了自己恢复。
- **只能从代码里造的**（goroutine 泄漏、内存泄漏、死锁、锁竞争、连接泄漏）：服务里预埋的故障开关（`pkg/fault`）。

另有两类不走这两条路：服务之间的延迟和错误用 Istio 的故障注入；关节点用宿主机上的 `vm.sh`（Chaos Mesh 管不到虚拟机）。MySQL、Redis 的一部分场景（慢查询、大 key、锁等待）是往里面放数据或跑语句，用脚本。

所以场景的“注入方式”有四种：`chaos-mesh`、`fault-switch`、`istio`、`script`。注入脚本对四种一视同仁（第 3.5 节）。

### 3.2 Chaos Mesh 的安装

- `scripts/cluster/chaos-mesh.sh install|uninstall|status`，`make chaos-mesh`；values 在 `deploy/cluster/chaos-mesh/values.yaml`。版本登记到 `versions.md`。
- 命名空间 `chaos-mesh`。打开 `enableFilterNamespace`。**命名空间的注解由注入脚本在实验开始时加、结束时去掉**，平时没有任何命名空间能被注入：误 apply 一份清单不会生效。`monitoring`、`kube-system`、`istio-system` 永远不加。
- 不装 dashboard：省内存；盲测时它上面直接写着答案；所有实验都从入库的清单来，不需要界面。
- 清单（Chaos Mesh 的对象）建在 `chaos-mesh` 命名空间里，用选择器指向目标命名空间。这样目标命名空间里不出现“这里有个 NetworkChaos”的痕迹。**这一点要在冒烟时核对**：Chaos Mesh 会不会往目标 Pod 上打事件或注解。
- 不用 HTTPChaos：它靠劫持目标 Pod 的端口工作，边车也在同一个 Pod 里改 iptables，两个一起用的结果文档没有说。服务之间的延迟和错误本来就该在边车这一层造（边车是真实的调用路径），Istio 自带。**这是推断，没有实测**；如果以后某个场景非它不可，再单独试。
- NetworkChaos（延迟、丢包、分区）作用在 Pod 的网卡上，在边车的外面，推断不冲突，冒烟时核对。
- **冒烟**（在 `oppenheimer-e2e` 里，每类一个最小的对象，各 30 秒）：PodChaos（杀 Pod）、NetworkChaos（延迟、分区）、StressChaos（CPU、内存）、IOChaos、TimeChaos、DNSChaos。每类记：生效没有、到时恢复没有、目标命名空间里留下什么痕迹。1.37 加 containerd 2.3 上哪一类不能用，在这里发现。用不了的那一类，对应的 4b 场景改用脚本。
- 特权的 `chaos-daemon` 在练习集群上接受。生产里这是要单独评估的权限（它能进任何 Pod 的命名空间），记进面试考点。

### 3.3 故障开关 `pkg/fault`

计划里的三条要求：运行时触发、默认关；写得像真的 bug；用构建标签隔离。再加一条这里要守的：**从外面看不出来是故意的**，否则 pprof 的调用栈上写着 `fault.leakGoroutine`，盲测就没有意义。

**构建标签。** 标签名 `faults`。`pkg/fault` 有两套文件：

- 不带标签的构建：`fault.Compiled` 是常量 `false`，`New` 返回的开关永远是关的，不注册任何 HTTP 接口。
- 带 `-tags faults` 的构建：开关可以开，管理端口上多出 `GET /fault`、`PUT /fault/{name}?mode=on|off&ttl=30m`。

**有缺陷的实现放在业务包里，不放在 `pkg/fault` 里。** 每个故障点是业务包里的一个“接缝”函数，有两个文件：

```
services/order/internal/quote/fetch.go            // 调用 fetchPrice(ctx, sku)
services/order/internal/quote/fetch_default.go    //go:build !faults   fetchPrice 就是正确的实现
services/order/internal/quote/fetch_variant.go    //go:build faults    开关关时走正确的实现，开时走有缺陷的那个
```

正常构建里第三个文件根本不参加编译，这比“靠编译器把走不到的分支删掉”可靠，也能检查：CI 里对正常构建的二进制跑一次 `go tool nm`，确认没有故障文件里的符号。

**不露痕迹的规则**（写进 `pkg/fault` 的包注释，评审时逐条查）：

- 有缺陷的函数、文件、变量的名字里不出现 fault、chaos、bug、leak 这类词，起一个业务上说得通的名字。
- 有缺陷的路径不打多余的日志、不加指标。开关被打开这件事**服务自己不打日志**（Agent 会读 Loki），由注入脚本记在答案文件里。
- 开关的名字是不透明的编号（`f07`），和场景的对应关系只在答案文件里。
- 缺陷要有触发条件，像真的那样：例如“下游超时的时候泄漏一个 goroutine”，而不是开关一开就每秒泄漏一百个。

**自动关。** `ttl` 到了开关自己关（进程里的定时器）。开发机断网、注入脚本死掉，故障也不会留在集群上。泄漏类的故障关了开关之后已经泄漏的不会回来，恢复要重启 Pod，场景里写明。

**重启后还要在的故障**（panic 导致反复重启）。开关在内存里，进程一重启就复位。办法：带 `faults` 的构建启动时读一个文件（`FAULT_FILE`），文件来自一个一直挂着的、可以不存在的 Secret。注入就是改 Secret 的内容。Deployment 的定义不变，不触发滚动；Agent 读不到 Secret。这一条到做那个场景时再实现，4a 只留出读文件的入口。

**CI。** 加一组带 `-tags faults` 的 build、vet、lint 和测试，否则故障代码没人编译，过几周就坏了。故意写的数据竞争不能进 `-race` 的测试，那个场景的测试单独处理。

**不碰的地方。** 故障代码不进资金、库存扣减、订单状态机的路径（第 2 节第 9 项）。要演示“数据竞争导致错误数据”，选派生数据。

**分工。** `pkg/fault`、接缝的写法、第一个故障点（goroutine 泄漏，用来证明这套办法走得通）主会话写。之后每个故障点是独立的小任务，派给 Opus，主会话评审。

### 3.4 Agent 的 pprof 工具

**怎么连。** 调试端口只绑在回环上，三个办法：

| 办法 | 好处 | 坏处 |
|---|---|---|
| A. 端口转发（`pods/portforward`） | 调试端口不用改，仍然不在网络上；每次连接经过 API server 的认证和审计；绕过边车 | 这个权限不能限定端口：给了之后 Agent 的身份能连 `oppenheimer` 里任何 Pod 的任何端口，包括没有认证的降级开关。工具的代码只发固定路径的 GET，但权限本身比用到的大 |
| B. 调试端口改绑 Pod 的地址，Agent 直接连 | 权限最小（不动 RBAC） | 集群里任何 Pod 都能来抓 CPU 剖析；要过边车（Agent 不在网格里），得把这个端口从边车里排除，再配 NetworkPolicy 限制来源 |
| C. `pods/exec` 进去 `curl` | | 镜像是 distroless，没有 `curl`；`exec` 的权限更大 |

建议 A，Role 只建在 `oppenheimer` 命名空间（不是 ClusterRole：`mysql`、`redis` 里的端口不能让它连）。“权限的粒度到不了你想要的那一层时怎么办”是个面试考点。

**工具。** 一个工具 `pprof_capture`，参数：Pod、类型（`cpu`、`goroutine`、`heap`、`mutex`、`block`）、秒数（只对 `cpu`，默认 10，上限 30）、可选的 `base`（同一次运行里前一次抓取的编号，返回两次的差）。抓不抓、抓哪种由模型决定，系统提示里只写“按症状选哪种”的对照。

**给模型的是摘要：**

- `cpu`、`heap`、`mutex`、`block`：用 `github.com/google/pprof/profile` 解析，按函数聚合，给前 20 个的 flat、cum 和占比，再给占比最大的三条完整调用栈。`heap` 默认看在用的字节数。
- `goroutine`：按调用栈聚合，给数量最多的前 15 组，每组的数量、等待的状态和最里面几帧。泄漏的特征是“几千个 goroutine 停在同一个地方”，聚合之后一眼能看到。
- 带 `base` 时给增长最多的那几项。内存泄漏和 goroutine 泄漏看的是“涨了什么”，单次的快照里大户往往是正常的缓存。

原始文件落在这次运行的 `raw/` 里（`runlog` 已经有），人可以拿去 `go tool pprof`。

**上限**（CPU 剖析有开销，不算纯只读）：同一个 Pod 同时只有一个 CPU 剖析；同一个 Pod 两次 CPU 剖析至少隔 60 秒；一次运行最多抓 8 次。超了返回一句说明，不报错。

**`mutex`、`block`。** 现在不采样。让工具去打开采样等于让只读的 Agent 改被诊断的进程，不做。阶段 4 期间用环境变量常开（`DEBUG_MUTEX_FRACTION`、`DEBUG_BLOCK_RATE`，起步值 100 和 1 毫秒），装上后用 3b 的压测对比一次吞吐和 p99，开销量得出来就调低或关掉。

**没有 API 密钥也能做。** 工具用 `agent tool pprof_capture '{...}'` 手工调，对着集群上真的 Pod 验证摘要好不好读。模型用得好不好等密钥。

### 3.5 场景的格式和注入脚本

```
chaos/
  README.md                 怎么写场景、三档风险的定义
  scenarios/<编号>/
    scenario.yaml           元数据：注入方式、目标、时长、风险档、前置条件、恢复步骤、预期会响的告警
    inject/                 Chaos Mesh 清单，或者脚本，或者开关名和参数
    truth.md                答案：标准根因（一句话）、定位必需的关键证据、容易误判成的原因
  runs/                     不入库。每次注入一个目录
```

`scenario.yaml` 加 `truth.md` 就是阶段 6 评测集的一条。编号不透明（`s017`），目录名不带含义。

**风险三档：**

| 档 | 含义 | 例子 | 同意 |
|---|---|---|---|
| 一 | 到时自己恢复，不留状态 | 网络延迟、CPU 压力、杀无状态的 Pod | 按池 |
| 二 | 要一步明确的恢复动作，数据不受影响 | 泄漏类（重启 Pod）、从库停复制 | 按池 |
| 三 | 可能丢数据，或恢复要人工判断 | 主库宕机、磁盘写满、关节点、两个 broker 同时停 | 每次单独同意，不盲测 |

**注入脚本** `scripts/cluster/chaos-run.sh`（`make chaos-blind`、`make chaos-inject`、`make chaos-recover`、`make chaos-reveal`）：

1. **检查稳态**：没有 warning 以上的告警在响，探针通过，稳态的压测在跑。不满足就拒绝注入。
2. **选场景**：`--pool` 给风险档和类别，在里面随机抽；最近做过的降权。不盲的用 `--scenario`。
3. **密封**：把选中的编号、参数、时间写进 `chaos/runs/<运行号>/sealed.json`，终端上只打印运行号和这个文件的 SHA-256。答案在开发机上，不在集群里；打印出来的摘要用来证明答案事后没有改过。
4. **注入**：给目标命名空间加注解，按注入方式执行。记下开始的时刻。
5. **到时或 `chaos-recover`**：执行恢复步骤，去掉注解，记下结束的时刻。`chaos-recover --all` 是总闸：删掉所有 Chaos Mesh 的对象、关掉所有故障开关、删掉脚本建的 VirtualService。
6. **等静止，验数据**：`make recon-check FROM=<开始> TO=<结束>`，加上三个常驻核对的告警没有响、超卖是 0。结果写进运行目录。
7. **`chaos-reveal`**：复盘时打开答案，按模板生成一份 `docs/postmortems/` 的草稿，把 `truth.md` 的三栏填进“评测用标注”。

**Agent 看不到答案**：答案在开发机上；Agent 的 kube、loki 工具拒绝 `chaos-mesh` 命名空间（权限也改成不含它），日志采集不收这个命名空间；故障开关的名字不出现在日志里。主人这一边靠自觉：不看 `chaos/scenarios/`，不 `kubectl get` Chaos Mesh 的对象。

### 3.6 稳态的压测和数据检查

- **稳态**：`make k6 SCENARIO=mixed`，固定速率，取 3b 容量结论的三成左右（具体的数动工时从 `docs/notes/3b-capacity.md` 里取），从 Mac mini 发。脚本 `scripts/cluster/chaos-steady.sh start|stop|check`。`check` 查的是五个 SLO 的指标在基线范围里、没有告警。
- **发压机的约束**：Mac mini 同时是开发机，k6 在跑的时候不能构建镜像、跑测试。所以带故障的镜像要在实验之前构建并部署好，实验期间不改代码。
- **数据检查两层**：
  - 系统内部对不对得上：`recon-check`（上面第 6 步）。每次实验都做。
  - 客户端被告知成功的有没有丢：k6 没有账本，查不了。**要证明“不丢、不重”的实验（主库切换、broker 宕机）改用带账本的 `make loadtest`**，结束后 `load verify`。这类都在第三档，本来就单独做。
- 3c、3e 留下的告警验证（计划里那四条）在对应的实验里顺带做，不单独排。

## 4. 实现顺序和分工

| 步 | 做什么 | 谁 | 动集群 |
|---|---|---|---|
| 0 | 主人确认第 2 节；处理 `mysql-main-0` 的内存 | 主会话 | 是（重启主库），先同意 |
| 1 | Chaos Mesh 的 values、脚本、`versions.md`；安装；六类冒烟 | Sonnet 写脚本，主会话装和验 | 是，先登记、先同意 |
| 2 | `pkg/fault`、Dockerfile 加 `GO_TAGS`、CI 加带标签的一组、`nm` 检查、第一个故障点 | 主会话 | 否 |
| 3 | `pprof_capture` 和摘要；Role；`mutex`、`block` 的环境变量 | Opus 写工具，Sonnet 写清单，主会话评审 | 部署 Agent 时 |
| 4 | `chaos/` 的格式、`chaos-run.sh`、`chaos-steady.sh` | Sonnet，主会话评审密封和总闸 | 否 |
| 5 | Agent 的工具和日志采集屏蔽 `chaos-mesh` | Sonnet | 是 |
| 6 | 走通一遍：一个不盲的第一档场景（杀 Pod）和第一个故障点，从检查稳态到生成复盘草稿 | 主会话 | 是 |
| 7 | ADR（两条注入的路、故障代码怎么隔离）、阶段笔记、面试考点 | 主会话 | 否 |

第 2、3、4 步互不依赖，第 2 步的框架合进主线之后可以并行。

## 5. 不在这一步做的

- 4b 的各个场景本身（除了走通用的两个）。
- 阶段 6 的评测流水线（自动注入、等告警、打分）。这里只保证每次实验留下的东西它用得上。
- `go tool trace`、core dump 加 delve：计划里列在“要练到的工具”，是主人手工练的，不做成 Agent 的工具。
- 物理机层面的故障（宿主机断电、网线）。

## 6. 实现中的发现和量到的数

### 第 1 步：装 Chaos Mesh、六类冒烟（2026-11-24 08:30 到 08:45 UTC；主人同意后做的）

- **装了什么**：chart `chaos-mesh` 2.8.8 到命名空间 `chaos-mesh`（`make chaos-mesh`，values 在 `deploy/cluster/chaos-mesh/values.yaml`）。五个 Pod：controller-manager 1 个（chart 默认 3 个加选主，改成了 1 个）、`chaos-daemon` 每个节点 1 个（特权）、`chaos-dns-server` 1 个。没有装界面。空闲时实际占用的内存合计约 120 MiB（67 + 13 + 16 + 13 + 12，`kubectl top`），预算是 0.5 GB。
- **冒烟**（`scripts/cluster/chaos-smoke.sh`，`make chaos-smoke`；只动 `oppenheimer-e2e`，做完那一套删了，Chaos Mesh 留着）。集群是 k3s v1.37.2、containerd 2.3.7、内核 7.0.0：

  | 项 | 怎么验的 | 生效 | 到时恢复 | 量到的 |
  |---|---|---|---|---|
  | 命名空间过滤 | 没有注解时建一个延迟对象 | 不生效（对） | | `Selected=False`，延迟没有变 |
  | PodChaos 杀 Pod | gateway 两个副本里杀一个 | 是 | 是（Deployment 补上） | |
  | NetworkChaos 延迟 | gateway 的 Pod 上加 300 毫秒，从探测 Pod 访问 `/healthz` | 是 | 是 | 10 → 600 → 10 毫秒（一来一回各 300） |
  | NetworkChaos 分区 | gateway 和探测 Pod 之间，双向 | 是 | 是 | 期间超时，之后正常 |
  | StressChaos CPU | 探测 Pod（limit 500m）里 1 个 worker | 是 | 是 | 5 秒里用掉的 CPU：0.01 → 2.5 → 0.01 秒 |
  | StressChaos 内存 | 探测 Pod 里 96 MB | 是 | 是 | cgroup 内存 2 → 95 → 2 MiB |
  | IOChaos | 探测 Pod 的 `emptyDir` 上加 500 毫秒 | 是 | 是 | 读一个小文件 0 → 3010 → 0 毫秒（一次 `cat` 是好几次文件操作，每次都被延迟） |
  | TimeChaos | gateway 的 Go 进程往回拨 1 小时，看响应的 `Date` 头 | 是 | 是 | 慢 0 → 3600 → 0 秒 |
  | DNSChaos | 探测 Pod 里让 `gateway.*` 解析失败 | 是 | 是 | 模式之外的名字（`order`）期间照常能解析 |

  六类在这个版本组合上都能用，4b 不需要因为“某一类用不了”改用脚本。

- **设计里要在冒烟时核对的两点**：
  1. **NetworkChaos 和 Istio 边车不冲突**：gateway 的 Pod 带边车，延迟、分区都按预期生效，恢复之后 Pod 仍然是 2/2、`/healthz` 正常。**只验了不经过边车的 HTTP 端口**（8080 不过边车）；经边车的 gRPC 那条路上延迟是什么样，没有量。
  2. **目标命名空间里有痕迹**（设计 3.2 以为把对象建在 `chaos-mesh` 里就没有了，不对）：
     - NetworkChaos 和 IOChaos 会在**目标 Pod 所在的命名空间**里给每个目标 Pod 建一个同名的 `PodNetworkChaos` / `PodIOChaos` 对象，实验结束之后也留着（至少留到 Pod 没了；冒烟结束 7 分钟后还在）。`kubectl -n <目标> get podnetworkchaos -o yaml` 能直接读出注入的是什么。
     - 这些对象每次更新都会在目标命名空间里产生一条事件（`PodNetworkChaos <Pod 名> Updated`），`kubectl get events` 看得到。
     - Pod 上没有多出注解。PodChaos、StressChaos、TimeChaos、DNSChaos 在目标命名空间里没有留下对象和事件（杀 Pod 当然有 Pod 自己的 `Killing` 事件，和正常删除一样）。
     - **这对盲测和 Agent 的评测都是泄漏**：主人排查时看事件是正常动作；Agent 的 `kubectl` 类工具读事件时也会看到。**怎么处理没有定**，在第 4、5 步里定：主人这一侧只能靠约定（不看 `*.chaos-mesh.org` 的资源、事件里的这几行不算数）；Agent 这一侧可以在工具里把 `involvedObject.kind` 是 `Pod*Chaos` 的事件滤掉（第 5 步本来就要屏蔽 `chaos-mesh` 命名空间，一起做）。
- **冒烟里碰到的三件事**（都是验法的问题，不是 Chaos Mesh 的）：
  1. 第一遍把 TimeChaos 打在探测 Pod（busybox）上，它的主进程看到的时间没有变；换成 gateway 的 Go 进程就生效。**busybox 那边为什么不生效没有查**（猜测是这个静态链接的进程不经过 vDSO 取时间，而 TimeChaos 改的是 vDSO；没有验证）。4b 的时钟偏移场景打的是 Go 服务，不受影响；要对别的进程用时先单独验。
  2. TimeChaos 期间 gateway 每 15 秒一条的“指标上报失败”日志停了，恢复之后又有。说明时钟回拨对进程里和时间有关的逻辑确实有影响（那条日志本身是 e2e 那一套连不上 Collector，和这次无关）。没有深究停的原因。
  3. 这个集群的上游 DNS 在代理后面，**不存在的域名也会解析出一个假地址**（198.18.0.0/15）。所以 DNSChaos 用“让能解析的名字失败”来验，不能用“让不存在的名字解析出来”。以后设计 DNS 类的场景、写“解析失败”的告警时也要记得这一点：外部域名在这里永远不会 NXDOMAIN。
- **没有验的**：HTTPChaos（设计里就不用）；节点级的故障（关虚拟机不经过 Chaos Mesh）；同时有两个实验对象；controller-manager 挂掉时已注入的故障怎么收回；长期运行的那一套（`oppenheimer`）上一次都没有注入过。
- **顺带**：e2e 这一套（`E2E_KEEP=1 make cluster-e2e`）这次也跑了，通过。删命名空间时那些 `PodNetworkChaos` 对象没有挡住删除。

### 第 2 步：`pkg/fault`、构建标签、检查、第一个故障点（2026-11-24；没有动集群）

决定记在 [ADR-0041](../adr/0041-two-fault-injection-paths.md)。

- **`pkg/fault`**：两套文件。平时的构建只有常量 `Compiled = false` 和一个空的 `Register`；带 `faults` 标签的有 `Switch`（包级登记，名字必须是 `f01` 到 `f99`）、`Set`、`Snapshot`、`GET /fault`、`PUT /fault/{name}?mode=on|off&ttl=`。ttl 默认 30 分钟、最长 6 小时，到了进程自己关；重新打开会换掉旧的定时器。`FAULT_FILE` 的入口留了（启动时读，列出的开关一登记就是开着的）。打开、关掉都不打日志、不记指标。
- **和设计不一样的两处**：
  1. 开关是包级的登记表，不是一个要一路传下去的 `Set` 对象。理由：往业务的构造函数里传一个“故障开关集合”本身就是痕迹，而且每个故障点都要改装配代码。
  2. 平时的构建里**没有** `Switch` 这个函数（设计写的是“返回的开关永远是关的”）。这样不带标签的文件里想用开关会编译不过，比“能用但永远关着”更早暴露写错的地方。
- **五个服务的管理端口都挂了 `fault.Register`**（gateway、order、inventory、payment、orderindex），不只是有故障点的那个：只挂一个的话，从“哪个服务有 `/fault`”就能看出故障在哪。gateway 的管理端口只在回环上（`127.0.0.1:8089`），注入要经端口转发。
- **镜像**：`deploy/docker/Dockerfile` 加了 `ARG GO_TAGS`，`scripts/cluster/images.sh` 读环境变量 `GO_TAGS`，含 `faults` 时 tag 带 `-faults` 后缀。**没有构建过带标签的镜像**（只在本机 `go build -tags faults` 过）。
- **三道检查**：CI 的 lint 多一步 `--build-tags faults`；`make test-faults`（CI 里新的一组 `faults`）；`make check-faults`（`scripts/check-faults.sh`）。最后这个第一次跑就报了“带标签的二进制里一个符号都找不到”，是脚本自己的毛病（`grep -q` 找到就退出，前面的 `go tool nm` 收到 SIGPIPE，在 `pipefail` 下整条管道算失败），改成先落文件再查。这也说明“反过来确认一次”那一步是有用的：没有它，同样的毛病出在正向的那一半时检查会一直是绿的。
- **第一个故障点 `f01`**：在一个服务里做了，三个文件（接缝的调用、`_default.go`、`_variant.go`），带标签的测试钉住了“开关关着和平时一样、开着时出现预期的症状”。是什么、在哪，写在 `chaos/scenarios/s001/`（场景、注入参数、答案），**主人在实验之前不读**。主会话按 `pkg/fault` 包注释里的规则自己查过一遍；没有第二个人评审过。
- **本机的状态（和这件事无关，顺带看到）**：`make test-faults` 在 `pkg` 上失败，原因是本机 compose 的 Redis 连不上（`pkg/cache` 的用例），不带依赖的环境变量重跑是过的。`make test`（整个仓库）这时在本机也会因为同样的原因失败，**没有查 compose 为什么是半起的状态**。
- **没有做的**：CI 没有跑过（没有推送）；带标签的镜像没有构建、没有部署；`FAULT_FILE` 只有读的入口，没有对应的 Secret 和清单；注入脚本（第 4 步）还没有，现在要打开开关只能手工 `kubectl port-forward` 加 `curl -X PUT`。
