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
6. **等静止，验数据**：`make recon-check FROM=<开始> TO=<结束>`，加上三个常驻核对的告警没有响、超卖是 0。结果写进运行目录。（4b 改成了两段：指标在前台、逐行对账在后台，注入前还多了两道闸，见[设计 4b](4b-experiments.md) 第 1.4 节。）
7. **`chaos-reveal`**：复盘时打开答案，按模板生成一份 `docs/postmortems/` 的草稿，把 `truth.md` 的三栏填进“评测用标注”。

**Agent 看不到答案**：答案在开发机上；Agent 的 kube、loki 工具拒绝 `chaos-mesh` 命名空间（权限也改成不含它），日志采集不收这个命名空间；故障开关的名字不出现在日志里。（实现时改了两处：不是“拒绝”而是“当它不存在”，权限没有收，Prometheus 的工具也筛。理由见第 6 节“第 5 步”。）主人这一边靠自觉：不看 `chaos/scenarios/`，不 `kubectl get` Chaos Mesh 的对象。

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

### 第 5 步：Agent 看不到注入工具（2026-11-24；主会话写；代码和配置写了，**没有部署**）

- **做了的**：
  - `services/agent/internal/tool/hidden.go`：`tool.Hidden`，一份“对模型不存在”的名单（命名空间 `chaos-mesh`、API 组 `chaos-mesh.org`；`agent serve` 和 `agent tool` 的 `-hidden-namespaces`、`-hidden-api-groups`，默认就是这两个，给空串关掉）。
  - 十个工具里和它有关的都接上了：
    - `kube_get`、`kube_describe`、`kube_events`、`kube_logs`：指名读隐藏的命名空间，回答和一个不存在的命名空间**一字不差**（有测试钉住）；跨命名空间的列表里没有它。事件多筛一层：事件说的对象（`involvedObject`、`related`）属于隐藏的 API 组的也丢掉，这是第 1 步发现的“目标命名空间里的 `PodNetworkChaos`、`PodIOChaos` 事件”。按 API 组认，不按 kind 的名字。
    - `prom_query`、`prom_query_range`：结果里标签（`namespace`、`k8s_namespace_name`、`exported_namespace`）指着隐藏命名空间的序列丢掉；`prom_alerts`、`prom_rule` 里这样的告警丢掉。
    - `loki_query`：同样按流的标签丢；`loki_labels` 问命名空间标签的值时不给它。
  - 日志采集：`otel-agent.yaml` 加了 `filter/hidden_namespaces` 处理器，`chaos-mesh` 的日志不进 Loki。`services/agent/deploy_test.go` 核对它和 Agent 的默认名单一致。
- **为什么是“不存在”而不是“拒绝”**：一句“不允许读这个命名空间”本身就告诉模型这里有东西，而且告诉它这是一次演练。所以指名去读得到的是找不到，列表里没有。
- **验证了的**：`make lint`、`make test`（`services/agent`）通过。本机的 `agent tool` 对着真的集群（管理员的 kubeconfig，只读）：`kube_get pod` 全命名空间的列表里没有 `chaos-mesh` 的 5 个 Pod；指名列 `chaos-mesh` 和列一个不存在的命名空间输出一样；关掉名单（`-hidden-namespaces=`）时看得到。Loki 里装命名空间的标签确实是 `k8s_namespace_name`，Prometheus 里现在有 144 个指标名带 `namespace="chaos-mesh"`（kubelet、cAdvisor、kube-state-metrics），都在名单认的三个标签里。
- **和 3.5 不一样的地方**：
  1. **权限没有收**（3.5 写的是“权限也改成不含它”）。RBAC 只有允许、没有拒绝：要让一个 ClusterRole 不含某一个命名空间，只能改成在每个允许的命名空间里各建一个 RoleBinding。那样跨命名空间的列表（`kubectl get pods -A` 这一类，Agent 的工具默认就是这样查的）会整个 403，工具要改成逐个命名空间查，以后每加一个命名空间还要记得加一份绑定。这里防的是“模型无意中读到答案”，不是一个存心绕过的调用方，放在工具里就够了。真要在权限上隔开，路子是另一头：给注入工具单独的集群，或者用准入策略，都不值得在这里做。
  2. Prometheus 也要筛（3.5 只写了 kube 和 loki）：kube-state-metrics 和 cAdvisor 的指标带着 `chaos-mesh` 的 Pod 名和资源用量。
- **挡不住的（知道，接受）**：
  1. **聚合掉命名空间标签的查询**：`sum(container_memory_working_set_bytes)` 把它算在里面；`count by (pod) (...)` 会露出 `chaos-daemon-xxxx` 这样的 Pod 名；LogQL 里 `| drop k8s_namespace_name` 之后流就认不出来了。露出的是“装着 Chaos Mesh”，不是“注入了什么”；后者在日志和 `Pod*Chaos` 对象里，日志不再收，对象读不到（kind 白名单里没有）。
  2. **已经在 Loki 里的日志**：冒烟那一次（08:30 UTC）`chaos-controller-manager` 的日志还在，到保留期才没。工具按流的标签筛掉了，上一条说的绕法读得到。
  3. **StressChaos 的痕迹在目标自己的指标里**：`stress-ng` 是放进目标容器的 cgroup 里跑的，目标 Pod 的 CPU、内存会涨，但进程列表 Agent 看不到。这是症状，不是答案，本来就该让它看到。
  4. **关于 `chaos-mesh` 自己的告警**会经 Alertmanager 发给 Agent（比如 `chaos-daemon` 的内存到了 limit），问题里就带着这个命名空间。触发那一层没有筛。现在这样的告警没有响过。
  5. `loki_labels` 问别的标签（`k8s_pod_name`、`service_name`）的值时，一小时内有过日志的 `chaos-mesh` 的 Pod 名还在；日志停收一小时后自己没有。
  6. 主人这一侧仍然只靠约定：Grafana 里不再搜得到它的日志了（顺带的好处），但 `kubectl` 什么都看得到。
- **部署和核对（2026-11-24 10:49 UTC，主人同意后）**：`make agent`（镜像 `9dda461`，工具从 10 个变成 11 个，Role `oppenheimer-agent-pprof` 第一次 apply）、`monitoring.sh collector`（三个 `otel-agent` 几乎同时重启，不是逐个；都起来了，日志里没有配置错误）。用 Agent **自己的身份**（`kubectl -n monitoring exec deploy/agent -- /app/agent tool -in-cluster ...`）：全命名空间的 Pod 列表 54 行里没有 `chaos`；指名列 `chaos-mesh` 是 0 个；`count by (namespace) (kube_pod_info)` 11 个命名空间里没有它；`loki_labels` 的 12 个命名空间里没有它；对 `order` 的一个 Pod 抓 goroutine 剖析成功（Role 管用）。Loki：`s002` 注入前后的 10 分钟里 `chaos-controller-manager` 自己写了 99 行日志，Loki 里这个命名空间是 0 行。

### 第 6 步：走通一遍（2026-11-24 10:52 到 11:26 UTC；主人同意后做的；长期运行的 `oppenheimer` 第一次被注入）

- **做了什么**：
  | 时刻（UTC） | 事 |
  |---|---|
  | 10:52 | `GO_TAGS=faults make deploy IMAGE_SERVICES="gateway order inventory payment orderindex"`，五个服务滚动成 `9dda461-faults`（cdc 没有动，仍是 `07c0dcc`） |
  | 10:53 | `make down`；`loadtest.sh skus 12001 20 25000`；`chaos-steady.sh start`（k6 mixed，570 请求/秒） |
  | 10:56:13 | `make chaos-inject SCENARIO=s002`：杀了 `inventory` 的一个 Pod |
  | 10:59:18 到 11:05:38 | `make chaos-recover`：对账差异 0；`make chaos-reveal` |
  | 11:05:55 | `make chaos-inject SCENARIO=s001`：`gateway` 两个副本的开关 `f01` 打开 |
  | 11:10:15 到 11:13:20 | 手动问了集群里的 Agent 一次 |
  | 11:13:33 到 11:25:08 | `make chaos-recover`（关开关、滚动重启 gateway）：对账差异 0；`make chaos-reveal` |
  | 11:25 | `chaos-steady.sh stop`；`make chaos-recover ALL=1`（总闸，试了一次，10 秒，没有东西可撤） |
- **脚本**：注入、恢复、对账、揭晓、总闸、压测的启停，会改集群的路径这次都真的走了一遍，**没有改一行就通过了**。没有走到的：`blind`（随机抽、终端不打印内容）；`recover --all --restart`；`istio`、`script` 两种注入方式；`s003`；注入中途失败的撤销（trap）；到 ttl 自己关。
- **`s002`（杀 Pod）量到的**：新 Pod 3 秒后调度、约 30 秒内 2/2。gateway 的 5xx 是 0，告警没有响也没有进 pending，探针没有失败。`truth.md` 里“会有一小簇失败”按设计推的那句**不成立**（至少在这个负载下）：被杀的 Pod 是正常收到 TERM 退出的，边车和优雅停止把在途的请求做完了。要造出失败得用 `pod-failure` 或者 `kill -9`（`gracePeriod: 0`），4b 写场景时定。
- **`s001`（故障开关）量到的**：gateway 每个副本的 goroutine 从 88 涨到 239（8 分钟，约每分钟 19 个）；查库存的 504 约每秒 0.3 到 0.8 次（这个接口每秒约 200 次成功，即约 0.3%）；gateway 的内存没有看得出的变化（8 分钟太短）；**没有任何告警**（没有针对 goroutine 数或者单个读接口错误率的规则，`expected_alerts` 是空的，符合）。k6 自己的阈值（失败率小于 0.1%）也没有破。关掉开关后 504 立刻没有了，泄漏的 goroutine 留到 Pod 重启。重启 gateway 期间没有 503。
- **Agent（`deepseek-flash`，手动提问，只告诉它“查库存的接口从 11:06 起有少量 504”）**：3 分 05 秒，26 轮、57 次工具调用，0.0245 美元，结束状态 `reported_truncated`。运行号 `20261124T111015Z-eebw`（在 Agent 的卷里）。
  - **对的**：自己决定抓了 gateway 的 goroutine 剖析，找到了停在 `stockWithin`（`stock_variant.go:39`）的 `chan send`；排除了 inventory 变慢、CPU、重启、网格；指出 504 只在这一个接口上。建议里写了“select 上 `ctx.Done()`、不要阻塞地发”。
  - **不对的**：把原因说成“10:53 起流量涨了 100 倍，请求在进程里排队”（那是稳态的压测；阻塞的发送被理解成了排队的信号量，不是泄漏）；说“Prometheus 里只有 istio 的指标”（不对，有应用自己的 HTTP 指标）；没有抓第二次带 `base` 的剖析来证明“只涨不降”（它自己列在了“下一步”里）。
  - Agent 的内存峰值 27 MiB（上限 256 Mi），第 3 步担心的“剖析放大后吃紧”对 goroutine 剖析不成立；heap 剖析还没有量。
- **之后改了的（2026-11-24 11:45 UTC，主人说“继续”之后按建议改的；不动集群，带新 tag 的镜像还没有部署）**：问题 1：tag 的后缀换成 `-p4`（不去掉：同一个提交号的两种构建不能共用一个 tag）。问题 2：`stock_variant.go` 改名 `stock_deadline.go`，`pkg/fault` 的包注释和 ADR-0041 里“统一用 `_variant`”的规则改成“按代码在做什么起名”。问题 5：`chaos-run.sh recover` 在恢复动作之后先等 `CHAOS_RECOVER_SETTLE`（默认 180 秒）再查稳态。`s002` 的 `truth.md` 补了实测的一句。验证：`make lint`（gateway、pkg）、`make test`（gateway）、`make lint-faults`、`make test-faults`、`make check-faults`、`chaos-run.sh lint` 都过；**新加的等待没有真的跑过**（只看了 `--dry-run` 的输出）。
- **发现的问题（当时都没有改）**：
  1. **镜像 tag 的 `-faults` 后缀是泄漏**：它出现在 Pod 的事件和 describe 里，Agent 读到了，并且在报告里写了“要查 `-faults` 这个构建是不是改了 `stock_variant` 的行为”。人看 `kubectl get pods -o wide` 也看得到。建议：阶段 4 期间带标签的镜像不加后缀（改成从 tag 上看不出来的写法，比如只在镜像的 label 里记），或者平时的镜像也一直用带标签的构建。
  2. **文件名 `stock_variant.go` 出现在调用栈里**，`_variant` 这个后缀对每个故障点都一样，做过一次的人和读过一次的模型下次一眼认得出。建议故障文件不用统一的后缀。
  3. **`recon-check` 的 Job、`recon-hourly` 在事件里很显眼**，Agent 花了几步去看它们；不是泄漏，是噪声。
  4. **稳态的 mixed 压测有约三成是 429**（363 个 200、43 个 202、170 个 429，每秒）：Zipf 分布造出来的热点用户和热点 SKU 被限流，是这个场景设计好的，但 `chaos-steady.sh` 的说明里没有写，“570 请求/秒”里真正进到后面的约 400。Agent 也被它带偏了（把压测当成了异常的流量）。以后给 Agent 的问题里要不要说明“有一条固定的压测在跑”，是评测设计要定的事。
  5. **恢复后的“等回到稳态”1 秒就过了**：`s001` 恢复时 gateway 正在滚动重启，检查读的是 5 分钟窗口的比例，重启造成的抖动还没有进来。之后探针的失败比例到过 3.75%（阈值 10%）。应当在恢复动作之后先等一个固定的时间再查。
  6. **逐行对账在有负载时慢**：商家索引那一对 8 分钟的窗口（1.6 万行）用了 160 秒（没有负载时整个 24 分钟的窗口约 15 秒）。恢复一次总共 6 到 12 分钟，大头是等常驻核对越过结束时刻和这一步。
  7. `chaos-steady.sh start` 用 `seq -s,` 拼的 SKU 列表末尾多一个逗号（macOS 的 `seq`），k6 的配置会滤掉空项，没有影响；是主会话敲命令的问题，不是脚本的。
- **`mysql-main` 的 `MALLOC_ARENA_MAX=4`**：这一轮连接数从 16 到最多 97，常驻内存 1652 → 1711 MiB（+59，其中有缓冲池还在回填的份）。没有到 200 个连接，**验证不了**“连接高峰时不再涨 300 MiB”，只能说这个负载下没有异常。计划里那一项不勾。
- **集群现在的状态**：五个服务是 `9dda461-faults`（开关全关，阶段 4 期间留着）；压测停了；没有 Chaos Mesh 的对象，`oppenheimer` 没有注入的注解；SKU 12001 到 12020 留着（卖掉了一部分）；本机的 compose 是停的（`make up` 再起）。

### 第 3 步：`pprof_capture`（2026-11-24；Opus 写，主会话评审；Role 没有 apply，集群里的 Agent 还是旧镜像）

- **做了的**：`services/agent/internal/tool/pprof`（工具、SPDY 端口转发、摘要）；`tool.Scope`（一次运行里各工具共享的状态，“最多 8 次”和 `base` 要它）和 `tool.Timeouter`（工具自己声明时限：原来每次工具调用固定 15 秒，装不下 30 秒的 CPU 剖析）；原始文件按 `raw/<步骤>.pb.gz` 落盘；`agent serve` 的 `-pprof-namespace`、`-pprof-port`；`pkg/app/debug.go` 读 `DEBUG_MUTEX_FRACTION`、`DEBUG_BLOCK_RATE`（只在调试端口开着时生效）；`oppenheimer` 命名空间里的 Role `oppenheimer-agent-pprof`（`pods/portforward` 的 create、`pods` 的 get）。
- **对着真的 Pod 跑过一次**（主会话，用本机的 `agent tool` 和管理员的 kubeconfig，不是 Agent 自己的身份）：`order` 的一个 Pod 的 goroutine 剖析，186 个 goroutine、46 种调用栈，摘要可读（第一组是 72 个停在 `kafkax.(*worker).runLane` 的 select）。**没有验的**：Agent 自己的身份加这个 Role 能不能转发（Role 没有 apply）；`cpu`、`heap`、`mutex`、`block` 对着真的 Pod；`base` 的差值；模型会不会用、用得对不对。
- **和 3.4 不一样的地方**：
  1. goroutine 的“等待的状态”是从最里面的运行时帧推断的，没有“等了多久”。原因：带状态和时长的文本格式（`debug=2`）在输出期间会停住整个进程，goroutine 多的时候对被诊断的进程影响明显；用的是二进制格式。代价是模型看不到“已经等了 20 分钟”这个泄漏的强信号，只能靠隔一会儿再抓一次带 `base` 看数量涨没涨。
  2. 调试端口是配置给的（默认 6060），不是从 Pod 上发现的：`DEBUG_ADDR` 在 ConfigMap 里，Agent 没有读 ConfigMap 的权限。同样的原因，工具没法事先知道某个服务开没开 mutex、block 采样，只能抓回来是空的再说明“为空不等于没有争用”。
  3. 多了参数 `heap_sample`（默认在用的字节数）；`base` 允许是另一个 Pod 的抓取，结果里会写明“这是两个进程的差别，不是随时间的增长”。
- **设计的问题（子 Agent 提的，主会话认为成立）**：
  1. CPU 剖析的间隔是按 Pod 算的：一个服务有 N 个副本就可以同时剖 N 个。名额和间隔只在 Agent 进程的内存里，重启清零。
  2. Agent 的内存上限 256 Mi：单份剖析最多 16 MiB，解析会放大几倍，一次运行里成功的几份还要留着给 `base` 用。大的 heap 剖析可能吃紧，**没有量过**。
  3. mutex、block 和 heap 的 `alloc_*` 是从进程启动累计的，不是“最近”；结果里写了，模型仍然可能读错。这是阶段 6 评测要看的。
  4. 只给了 `pods/portforward` 的 create（SPDY）。以后有人把转发换成 WebSocket 那一路会 403，要另给 get。

### 第 4 步：场景格式、`chaos-run.sh`、`chaos-steady.sh`（2026-11-24；Sonnet 写，主会话评审；没有注入过）

- **和 3.5、3.6 不一样的地方**（都接受了）：
  1. 注解对所有注入方式都加，不只 Chaos Mesh 的：否则看命名空间有没有注解就能猜出方式。
  2. 命名空间是白名单（只有 `oppenheimer`、`oppenheimer-e2e`），不是“拒绝那四个”。
  3. `sealed.json` 里放了 128 位随机数：场景只有几十个，没有它的话终端上的 SHA-256 可以穷举出是哪一个。
  4. 有盲测的运行还没 reveal 时，总闸（`recover --all`）之后留下的 `Pod*Chaos` 清单只写文件、不打到终端。
  5. 稳态的判定：`ALERTS` 里没有 warning 以上在响的（被静默的也算）、gateway 的速率不低于目标的七成、探针没有失败、五个 SLO 的 5 分钟错误比例不超过预算本身。速率 570 请求/秒是 3b 混合读写拐点 1907 的三成。
  6. `inject` 注入完就返回，不等到时；`recover` 会阻塞（等稳态最多 15 分钟，等对账核对到结束时刻最多 30 分钟）。
- **设计的问题（子 Agent 提的，主会话认为成立）**：
  1. **密封挡不住主人自己**：`sealed.json` 就在开发机上；摘要也只打印在主人自己的终端上。它能证明的只是“事后没有改过”，前提是摘要记在了别处。盲测靠的是自觉，这一点设计 3.5 本来就写了。
  2. 盲测的泄漏渠道不止答案文件：`Pod*Chaos` 对象和事件（第 1 步的发现）、注入时 `ps` 里 `kubectl port-forward pod/<名字>` 的参数、被杀的 Pod 的新名字。
  3. **故障开关只对注入那一刻的副本生效**：之后扩容或重启出来的副本没有开关，故障会悄悄变弱。泄漏类的场景要么先把副本数钉住，要么改成启动时读 `FAULT_FILE`。4b 写这类场景时要定。
  4. 脚本被 `kill -9` 或开发机断网时不会走撤销：Chaos Mesh 的对象靠 `duration`、开关靠 ttl 自己结束，但命名空间的注解会留着（要 `recover --all` 清）。
  5. 稳态用的阈值（SLO 的预算本身、速率的七成）没有基线数据支撑，可能偏严或偏松；发压刚开始的 3 分钟里速率那一项会误判。
  6. 长时间的 mixed 压测会把 SKU 的库存耗光（2 小时约 33 万单），要事先备够。
- **没有验证的**：所有会改集群的路径（apply 清单、经端口转发开关故障开关、总闸、滚动重启、k6 的启停、`recon-check` 的重试）；`istio`、`script` 两种注入方式没有样例场景；`reveal` 生成的复盘草稿只在假数据上看过一次；`s002`、`s003` 的 `truth.md` 里的证据是按设计推的，没有量过。第 6 步走通一遍时逐条验。

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
