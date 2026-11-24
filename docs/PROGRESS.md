# 进度与断点

每次会话结束时更新本文件。新会话先读这里。

## 当前状态

- **现在（2026-11-24 03:00 UTC，这一条最新）**：**3e 的第 6 步做完了，计划里 3e 的四项都勾了，只剩第 7 步（阶段笔记、面试考点）。** 造差异验证在 `oppenheimer-e2e` 里做完并清理了（e2e 整套、临时规则都删了；Alertmanager 里还有一条匹配 `env=e2e` 的静默，到 06:20 UTC 前后自己过期，匹配不到任何正式的告警）。九条告警里八条真实触发过，`DerivedDataCheckStalled` 没有（登记在 `docs/plan/phase-4-chaos.md` 的“4a 准备”）。结果在设计 3e 第 9 节“第 6 步：造差异验证”和三篇 runbook 的最后一节。集群 e2e 这次也跑了，通过。已提交并推送到 `6b115e4`（PR #19 的 CI 全过）；**之后写的文档（runbook、设计、两个计划文件、本文件）没有提交**，主人还没有说。长期运行的那一套：inventory 是 `4cf911d-dirty-20261124014241`（代码就是 `4cf911d`），另外五个服务 `07c0dcc`，`recon-totals` 和两个 CronJob 是 `recon:4cf911d`，`recon-stock`、`recon-changes` 是 `recon:6b115e4`。**下一步**：主人说提交之后提交、推送；第 7 步；PR #19 从 draft 转正、合并由主人定；3d 等 API 密钥。
- **现在（2026-11-24 02:00 UTC）**：**PR #18（3d）已合并进 `main`（`1ec99b9`，主人要求）；PR #19（3e，draft）的 CI 全过；3e 第 6 步的部署和只读核对做完了，剩“造差异验证告警”，它的具体做法还要给主人看过再动。** 见下面“3e 第 6 步：部署”。工作区有没提交的改动（`watch_changes.go` 和 `changes.yaml` 加调试端口、`recon-check.sh` 修窗口、三个文档），主人还没有说提交。
- **现在（2026-11-24，在上一条之前）**：**主人确认了 3e 等着定的几件事，第 3、4、5 步已提交并推送**：`2827002`（钱的分组和下钻）、`d23bf6d`（`watch-changes`）、`5016318`（清单、规则、脚本、runbook），加一个文档提交（`git log -1` 看）。下一条里“没有提交”“没有推送”“主人没有确认过”的说法到这里作废。主人的答复：“都按建议做”（下钻不用树、第 4 步的七件事都照现在的做法）；单个模块怎么跑测试写进了 `AGENTS.md`。3e 开了 draft PR（号码用 `gh pr list --head phase-3e` 看；它带着 3d 的提交，PR #18 合并后会变干净）。**主人问 3d 能不能先合并、密钥到了再试跑**：主会话答可以（PR #18 的 CI 全过、可合并，Agent 没有部署、第二个 webhook 地址默认不带，合并不改变集群上的任何东西；没做的两项“触发”“试跑”留在计划里不勾），**等主人说一句才合并**。**没有动集群**，第 6 步仍然要先登记、先拿同意。
- **现在（2026-11-24，UTC 是 11 月 23 日 24:00 前后）**：**3e 的代码部分（第 0 到 5 步）都写完了，剩部署验证（第 6 步）和笔记（第 7 步）**。第 0、1、2 步已提交（`1502d8e`、`336cdb9`、`14fccc1`）；**第 3、4、5 步没有提交**（`git status` 里的全部改动）。整个仓库 `make lint test`（12 个模块 0 issues，73 个包）、`make rules-test`、`make alertmanager-test` 通过。分支 `phase-3e`（从 `phase-3d` 开的，**没有推送**），**没有动集群**。见下面“3e 代码写完”。3d 停在“等 API 密钥”，状态见下一条。
- **3d（2026-11-24，UTC 是 11 月 23 日 17:40）**：**3d 的程序部分写完了**：核心（主循环、SDK 适配、报告、触发、留痕、命令行）加三组只读工具，都已提交，PR #18（draft）。**没有部署，没有对真的 Claude API 跑过**（主人现在没有 API 密钥）。见下面“3d 三组工具写完”。

- **2026-11-24 早些时候**：**3c（SLO 与告警）做完了，PR #17 由主人同意后合并进 `main`**（合并提交用 `git log --merges -1 origin/main` 看；合并前最后一次推送只改了本文件）。**下一步是 3d 最小诊断 Agent，在新会话里做**：从最新的 `main` 开分支 `phase-3d`，先读 `docs/plan/phase-3-observability.md` 的 3d 一节，动工前先查最新的 Claude API 和 Agent SDK 文档，先写设计给主人确认。3c 留下没有做的：“上线后放几天看误报”；没有真实触发过的告警登记在 `docs/plan/phase-4-chaos.md` 的“4a 准备”；SLO 的大盘没有在 Grafana 里打开看过；长期运行的那一套里有两条测试用的死信（见下面“3c 第 16 项做完”）。集群没有在响的告警（2026-11-23 15:50 UTC 核对的）。下面各条是更早写的。
- **当前阶段（2026-11-22）**：3b（压测）做完并合并了（PR #16，合并提交 `0142f02`），下一步是 3c（SLO 与告警），还没有开工，分支还没有开。当前分支仍是 `phase-3b`。以下是更早写的：2d 必做的部分（服务之间的流量治理迁到 Istio）做完并合并了（PR #12，2026-11-15，合并提交 `4005fa6`）。2c 已完成并合并（PR #11，合并提交 `d36449f`）。集群在 2026-11-16 又从空虚拟机重建过一次（带 Istio），六个服务在上面跑着，除 cdc 外各 2 个副本。2b 已完成并合并（PR #10，合并提交 `65adb85`，最后一次推送 `e8eafe5` 的 CI 27 项全部通过）。2a、阶段 1 早已合并。
- **当前分支**：`phase-2d`（2026-11-15 从合并了 2c 的 `main` 开出），已推送。PR #12 已合并；合并之后在同一个分支上追加了 2026-11-17 的两个提交（宿主机硬件、`vm.sh resize`、`AGENTS.md`），另开了 **PR #13** 等主人看，`40063bd` 的 CI 27 项全部通过（`test (./services/order)` 4 分 16 秒、e2e 3 分 57 秒，和上次相比没有明显变化）；那之后只有记录这个结果的一次文档提交，它的 CI 没有再看。PR #12 的 CI：第一次推送失败 1 项（和 2d 无关的偶发失败，见“2d 做到哪”的下一步）；`cb0d456` 的 27 项全部通过（最长的是 `test (./services/order)` 4 分 09 秒、e2e 3 分 50 秒）。那之后只有记录这个结果的一次文档提交，它的 CI 没有再看。**PR #13 已由主人合并**（2026-11-17，合并提交 `9012921`）。合并之后同一个分支上又追加了宿主机的地址和兜底通道、`hardware.md` 精简、修 `redis-gate-1` 的记录，已推送，另开了 **PR #14**（CI 只在 PR 和推到 `main` 时跑，所以要开 PR 才有结果）：`84f73d5` 的 CI 27 项全部通过（`test (./services/order)` 2 分 58 秒、e2e 4 分 32 秒，没有明显变化）。之后追加的文档提交 `eb671d4` 的 CI 也是 27 项全部通过，**PR #14 已合并**（合并提交 `0af2c2d`）。合并之后 `phase-2d` 上还有一次只改本文件的提交（记录删备份和这句话），不在 `main` 里：**阶段 3 的分支从 `phase-2d` 开，或者从 `main` 开之后把这个提交带过去。** 工作区是干净的。
- **最后更新**：2026-11-23（第 16 项做完，第五个 SLO、SLO 的大盘、ADR-0037 做了，3c 的计划项全部勾了，都没有提交，见下面“3c 第 16 项做完”；以下括号里是更早写的）（主人对待定项给了决定，规则改了并下发，第 17、18 项做了，延迟消费者并发和按分区计数的队列上限写完了，都没有提交，见下面“3c 待定项的处理”；以下括号里是更早写的）（3c 的告警质量检查开始做了，见下面“3c 告警质量检查”；以下括号里是更早写的）（3c 的第 6、7 步：Alertmanager 的路由和抑制、webhook 接收端、runbook 做了，都没有提交，见下面“3c 的 Alertmanager 和 runbook”；以下括号里是更早写的）（3c 的设计写完，等主人确认十项，见下面“3c 设计写完”；当前分支 `phase-3c`；以下括号里是更早写的）（3b 收尾：集群已恢复成平时的配置，下一步是 3c，见下面“3b 收尾”；以下括号里是更早写的）（容量结论写完，`GOALS.md` 已修正，没有提交，有一件事等主人定；见下面“3b 容量结论”；以下括号里是更早写的）（长稳跑完并对账通过，binlog 和 Kafka 的保留期已改并生效，已提交未推送，下一步是容量结论；见下面“3b 定 requests 和弹性测试”开头“主人定了 B 之后”的最后一条；以下括号里是更早写的）（中间件的 requests 也调了并验证，已提交；以下括号里是更早写的）（CPU requests、HPA 阈值、弹性测试做完并对账通过，有一件事等主人定，见下面“3b 定 requests 和弹性测试”；以下括号里是更早写的）（主人定的三件事做完并验证，读从库的模式对比做完，已提交并推送，见下面“3b 三件事和读模式对比”；以下括号里是更早写的）（扩容系数做完并对账通过，有三件事等主人定，见下面“3b 扩容系数”；以下括号里是更早写的）（空表对照做完并对账通过，PR #15 已合并，下一步是扩容系数，见下面“3b 空表对照”；以下括号里是更早写的）（混合读写和秒杀的阶梯做完并对账通过，下一步是空表对照，在新会话里做，见下面“3b 混合读写和秒杀的阶梯”；以下括号里是更早写的）（`Reserve` 的 3 秒路由做完并对账通过，下一步是秒杀和混合读写的阶梯，见下面“3b 第 3 轮”的“下一步”第 3 项；以下括号里是更早写的）（3b 第 3 轮进行中，见下面“3b 第 3 轮”；以下括号里是更早写的）（宿主机上验证了盘变慢的原因、幂等键只留 1 小时，见下面“3b 第 2 轮之后”；以下括号里是更早写的）（3b 第 1 轮优化做完，见下面“3b 第 1 轮”；以下括号里是 2026-11-18 写的）（3b 第 1 步的改动已提交到新分支 `phase-3b`（从 `phase-3a` 开出）并推送，见下面“3b 做到哪”；以下是 2026-11-17 深夜写的）（3b 的设计已确认并开工；3a 的状态没有变：只剩“空转一天后再量一次”就收尾。按请求速率的 HPA、验证 5、6、7、从空集群重建、笔记都做完了，全部已提交并推送，工作区干净。**PR #15（draft）**：`18d6c41` 的 CI 27 项全部通过（`test (./services/order)` 4 分 30 秒、e2e 4 分 13 秒，都在以往的范围内）；那之后只有记录这个结果的一次文档提交，它的 CI 没有再看。当前分支 `phase-3a`）
- **开始阶段 3 之前**：`redis-gate-1` 已修好（见会话日志 2026-11-17 最后），Redis 主从现在的主库是 `redis-gate-0`；Mac mini 已开机（`ssh mini` 能登录，macOS 27.2，还没有装 k6）；集群里没有不正常的 Pod。

### 3e 第 6 步：部署（2026-11-24 02:00 UTC；这一段最新，新会话先读这里）

主人说“同意第6步的操作”。登记在计划文件 3e 一节末尾。细节和数：设计第 9 节“第 6 步”。

- **做了的**（都在长期运行的那一套上）：
  - `make deploy IMAGE_SERVICES=inventory`：只滚了 inventory（另外五个服务从 `07c0dcc` 到现在代码没有变，仍是 `07c0dcc`）。迁移 00006 执行了，599 个 SKU 的 `stocked` 都等于三个数之和。inventory 的镜像 tag 是 `4cf911d-dirty-20261124014241`（部署时计划文件有没提交的改动，代码就是 `4cf911d`）。
  - `recon-totals`、`recon-stock` 用 `recon:4cf911d`；`recon-changes` 用带调试端口的 dirty tag（见下）；`recon-hourly`、`recon-daily` 两个 CronJob 用 `RECON_IMAGE_TAG=4cf911d make recon-cron` 换成了 `4cf911d`（`recon-check` 的 Job 是照 `recon-hourly` 的模板建的，旧镜像没有 `-from`）。规则下发了。
  - 规则、脚本、runbook 里用到的指标名在 Prometheus 上都有，除了 `recon_totals_drilldowns_total`（没有下钻过就没有序列，正常）。
- **量到的**：
  - `recon-changes` 第一次启动从 topic 开头读（kafkax 的新消费组都是这样），`cdc.order.orders` 是压缩保留的，等于把每个订单核对一遍：约 5 分钟核对了商家索引 1648 万次、从库 1677 万次，**全部通过，没有不一致、没有没追上**。期间它自己占约 2.8 核（没有 CPU 上限，调度在 `k3s-server` 上），两个从库各约 0.3 核，没有新的告警。追上之后占 4m CPU、14 Mi，`verified_through` 落后现在约 45 秒，`pending` 是 0。冷库那一个目标一次都没有核对（没有归档产生的变更）。
  - `recon-stock`：599 个 SKU，完整的一轮在启动后 60 秒内做完，两类不一致都是 0，卡住的预占 0。
  - `recon-totals`：两组（`succeeded`、`refunded`）差异都是 0。
  - 每天的全量对账（11 月 23 日 21:30 UTC，2 分 09 秒）：五个 MySQL 实例各有一个 2 分钟的点到 0.65 到 1.1 核（平时 0.01 到 0.08），gateway 的 p99 没有变（约 20 毫秒）。**只有探针的流量（每秒 2 单）时量的，有负载时没有量。**
  - `make recon-check FROM= TO=` 真跑了：逐行对账那一步 24 分钟的窗口约 15 秒，差异 0；整体退出码 3（“已售”那一轮每小时一次，还没越过 TO），符合预期。
- **主会话的一次误判**：看到 `recon-changes` 占 2.8 核、以为指标停了，先把它缩到 0，加了调试端口重新部署去抓剖析。其实指标没有停（查询的时间点看错了），它只是在重放。被停的那一次差 22 万条没放完，第二次接着放完了。调试端口留下了（别的服务都有）。
- **查出并改了的**：`recon-check.sh` 在 TO 之后不到 4 分半钟时，`increase` 的窗口里不够两个点，被读成“没有成功的一轮”；加了 `MIN_RATE_WINDOW=150`，窗口不够时明说。
- **主人 2026-11-24 的答复（下面四件都定了）**：1 提交并推送（做了，`git log` 看）；2 保留重放（加了 CPU 上限 1 核；**没有把它挪出 `k3s-server`**：清单里“软性放在控制面节点”是有意的，阶段 4 要关的是工作节点，主会话原先的建议和它冲突，按清单的）；3 “已售”改成 5 分钟一轮（`stock.yaml` 的 `-full-every=5m`，规则的注释、runbook、`recon-check.sh` 的说法跟着改了）；4 造差异的做法同意了。
- **当时等主人定的**：
  1. 没提交的改动要不要提交、推送（调试端口、`recon-check.sh`、文档）。提交后 `make recon-changes` 一次换成干净的 tag。
  2. `recon-changes` 第一次启动（或者消费组的位点丢了）会全量重放，没有 CPU 上限。建议：留着重放（等于白得一次逐行核对），给它加 CPU 上限 1 核，并且不让它调度到 `k3s-server`。
  3. “已售”的完整一轮每小时一次，`recon-check` 收尾最长等 1 小时；完整一轮实测不到 1 分钟，建议 `-full-every` 改成 5 分钟。
  4. 造差异验证六条告警和下钻的具体做法（主会话给了方案，见会话里的最后一条回复；**没有同意之前不做**）。
- **下一步**：主人定上面四件 → 造差异验证 → 勾计划里 3e 的四项 → 第 7 步（阶段笔记、面试考点）。3d 等 API 密钥（见下面 3d 的“下一步”）。

### 3e 代码写完（2026-11-24；在上一段之前）

细节：设计 `docs/design/3e-reconciliation-by-scenario.md` 第 9 节“第 3 步”“第 4 步”。

- **第 4 步 `recon watch-changes` 写完了**（没有提交）：
  - `services/recon/internal/changes`：`changes.go`、`checker.go`（解码、判定、延迟核对、复查、指标，主会话写的）；`targets.go`（商家索引、从库、冷库三个派生端，Opus 子 Agent 写的，主会话逐行读了实现，**测试 `targets_test.go` 没有逐行读**）。
  - `cmd/recon/watch_changes.go`：消费组 `recon.cdc-orders`（加在 `api/eventx`）、开关 `changes_check`、内部端口 `:8086`。
  - `services/recon/go.mod` 跑过 `go mod tidy`（新依赖 `api` 模块、protobuf），`GOWORK=off go build` 通过。
  - 实现时定的七件事和子 Agent 指出的六个设计问题都在设计第 9 节，**主人都没有确认过**。最要紧的：等着复查的变更只在内存里（重启会丢）；冷库里没有那一行直接算不一致；派生端整体落后时复查没有限流。
  - **没有对着真的 Kafka 和真的 CDC 输出跑过**：消费、等待、位点提交只有假的派生端加 `synctest` 的测试；派生端只有对着真实表结构、手工造映像的测试。
- **第 5 步写完了**（Sonnet 子 Agent，没有提交；主会话读了 `services.sh`、`Makefile`、`changes.yaml` 的 diff 和规则的表达式，重跑了检查，**两篇 runbook、`stock.yaml`、`recon-check.sh`、规则的测试用例没有逐行读**）：
  - 清单 `deploy/cluster/recon/stock.yaml`、`changes.yaml`；`make recon-stock`、`make recon-changes`（`services.sh` 里三个常驻核对的安装抽成了一个函数 `install_recon_watcher`，`recon-totals` 的行为应当不变，没有运行过）。
  - 六条告警（`business.yaml` 的 `business.recon_stock`、`business.recon_changes`）：`StockLedgerMismatch`、`StockReservationsStuck`、`StockCheckStalled`、`DerivedDataMismatch`、`DerivedDataLagging`、`DerivedDataCheckStalled`，17 个单元测试用例。Alertmanager 没有改。
  - runbook `recon-stock.md`、`recon-changes.md`。
  - `make recon-check FROM= TO=`（`scripts/cluster/recon-check.sh`，退出码 0 通过、1 有差异、2 出错、3 没有差异但还没核对到 TO）。**只跑过 dry-run。**
  - **规则、脚本、runbook 里的 Prometheus 指标名都是照 `recon_totals_*` 类推的，没有在 Prometheus 上核对。**
  - 子 Agent 指出的问题（没有处理）：`DerivedDataCheckStalled` 的“源库有写入”用的是订单状态流转的计数器，订单创建算不算一次流转没有核对；`DerivedDataMismatch` 响过 5 分钟后自己恢复而那一行可能还没修；“已售”和“卡住”只在每小时的完整一轮更新，`recon-check` 收尾最长要多等 1 小时；`verified_through` 只在有新变更时往前走。
- **验证时犯的一个错**：主会话在模块目录里直接跑 `go test`，没有 `OPPENHEIMER_TEST_MYSQL_DSN`，对着库的用例全部被跳过而输出是 `ok`。子 Agent 带着环境变量跑、报出 `TestDrill` 失败才发现。失败的是一条写错的期望值（代码没错），已改，原因见设计第 9 节。**单个模块用 `make test MODULES=$PWD/services/<模块>` 跑。** 已写进 `AGENTS.md`（主人同意的）。
- **等主人定的**（**2026-11-24 主人答复：第 1、2 项都按建议做，已提交并推送；第 3 项还没有谈**）：
  1. 第 3、4、5 步要不要提交、怎么分（建议四个：钱的分组和下钻；`watch-changes`；清单、规则、脚本、runbook；文档）。提交后要不要推送、开 PR（3d 的 PR #18 还没合并，3e 的 PR 会带着 3d 的提交）。
  2. 下钻不用 `payment` 那一对的树（下一段）；第 4 步的七件事。
  3. 第 6 步动集群的具体做法（下面“下一步”），每一项先拿同意。
- **下一步**（第 6 步，都要动集群，先登记到计划文件、先拿同意）：
  1. `make deploy`（会执行库存库的迁移 00006，`sku_stock` 加列重建表）→ `make recon-totals`（换成带 `group` 和下钻的版本）、`make recon-stock`、`make recon-changes` → 规则下发。
  2. 在 Prometheus 上核对指标名，量“已售对流水”的完整一轮要多久、`watch-changes` 平时的 `pending` 和派生端的查询量。
  3. 在 `oppenheimer-e2e` 里造差异验证六条告警和下钻（要另起指向 e2e 库的实例，清单现在对着正式库）。
  4. 补量每天全量对账期间数据库和探针受不受影响（21:30 UTC 那次，只看指标）。
  5. 之后第 7 步：阶段笔记、面试考点、勾计划里 3e 的四项。

### 3e 做到钱（2026-11-24；在上一段之前）

细节：设计 `docs/design/3e-reconciliation-by-scenario.md` 第 9 节“第 3 步”。

- **主人说的**：“提交代码，然后根据进度继续”。第 0、1、2 步分三个提交进了 `phase-3e`（库存的迁移和夹具、`recon` 的两样、文档），提交前跑了整个仓库的 `make lint test`：12 个模块 0 issues，72 个包通过（上一段写的“没有跑整个仓库的”到这里补上了；e2e 仍然没有跑）。没有推送。
- **第 3 步写完了**（`services/recon/internal/totals`，主会话写的，**没有提交**）：
  - 退款单独一组：支付一侧 `REFUNDED` 且已答复，对订单一侧“支付成功但要求退款”，套用和“成功”一组同样的两条规则。指标 `recon.totals.mismatch` 多了标签 `group`（`succeeded`、`refunded`），别的指标含义没变，现有的告警规则不用改。
  - 下钻：一段连续两轮对不上时逐笔比一次那一分钟，每笔打一条日志，每段只做一次，一轮最多 3 段。指标 `recon.totals.drilldowns{outcome}`。`recon watch-totals` 多了 `-drill-timeout`。
  - **重读了支付和订单两边处理答复的代码，退款的包含关系成立**。发现一条第 0 步没看到的例外：订单已归档时要求退款不写记录。它够不着这里的窗口（归档至少 30 天，核对只看 30 分钟），理由写在包注释和设计里。
  - **和设计不一样**：下钻没有用 `payment` 那一对的树，直接比那一分钟两边的每一笔支付（树比的不是同一件事、看不到退款、在那个时刻会把在途的报成差异）。设计 4.1 加了一行指向第 9 节。**这是主会话定的，主人没有确认过**。
- **验证**：写这一段时说的“`go test -race ./...` 通过”对其中对着库的用例不成立（被跳过了，见上一段“验证时犯的一个错”）；之后带着环境变量重跑并跑了整个仓库的 `make lint test`，通过。没有部署，集群上的 `recon-totals` 还是旧版本。
- **文档**：runbook `payment-totals.md` 补了分组和下钻的日志（里面 `recon_totals_drilldowns_total` 这个名字是推的，没有在 Prometheus 上核对）；计划文件“钱”那一项加了说明，没有勾（没有部署）。
- **顺带**：在模块目录里跑 `golangci-lint fmt` 或 `go build` 之后 `go.work.sum` 被改了（少了 60 多行），和这次的改动无关，已经用 `git checkout` 还原；还原之后构建和测试照常通过。哪条命令改的没有查。
- **下一步**：第 4 步 `recon watch-changes`（设计 4.3；判定和消费主会话写）。之后的顺序见下一段“还没做的”第 2 到 5 条。

### 3e 做到库存（2026-11-24；在上一段之前）

设计 [`docs/design/3e-reconciliation-by-scenario.md`](design/3e-reconciliation-by-scenario.md)（第 9 节是做到哪、量到什么、和设计不一样的地方），决定 [ADR-0039](adr/0039-reconciliation-by-scenario.md)。

- **思路**：对账按数据的性质拆三类，各找一个不靠“等一会儿再比”的判据。钱用支付单的状态（3c 已做）；库存在库存库的一个一致性快照里核对“计数对得上流水”；派生数据跟着 CDC 的变更逐条核对，用行的版本号区分“没追上”和“不一致”。逐行的树留作下钻和不经过 CDC 的独立一路。
- **做完的**（设计第 7 节的第 0、1、2 步）：
  - 第 0 步：三个前提读代码核对都成立（预占的状态和库存增量同一个事务；预占的流水从不删除；退款两边的状态，这一条没有重读支付服务处理答复的代码）。
  - 第 1 步：`recon -from`、`-to`（RFC 3339 的绝对时刻），给下钻和阶段 4“只比实验那一段”用。
  - 第 2 步：迁移 `services/inventory/migrations/00006_sku_stocked.sql`（`stocked` 列，默认值是表达式，加 CHECK `available + reserved + sold = stocked`；先在本机 MySQL 9.7.4 上验证了行为）；八处直接改库存数字的测试夹具跟着改了；`services/recon/internal/stock` 和 `recon watch-stock`。
- **验证**：`services/inventory`、`services/recon`、`test/load` 三个模块对着本机的 MySQL、Redis、Kafka 跑测试（`-race`）和 lint 通过。**没有跑整个仓库的 `make lint test`，e2e 没有跑**（e2e 建 SKU 的语句不写 `stocked`，按默认值应当不受影响，没有实际验证）。
- **下一次 `make deploy` 会执行库存库的迁移**（`inventory` 启动时跑）：`sku_stock` 加一列并重建表，表小。迁移时每一行的 `stocked` 取当时三个数之和。**这个迁移没有在集群上跑过。** 之后手工改 `sku_stock` 的一个数会被拒绝，要连 `stocked` 一起改。
- **还没做的**（设计第 7 节）：
  1. 第 3 步，钱：退款单独一组、对不上时自动下钻（主会话写；先重读支付服务处理答复的代码核对退款的包含关系）。
  2. 第 4 步，派生数据：`recon watch-changes`（判定和消费主会话写，三个目标的读取可以分给 Opus）。
  3. 第 5 步：三个常驻程序的清单、告警规则和单元测试、runbook、`make recon-check`（Sonnet）。
  4. 第 6 步：部署、在 `oppenheimer-e2e` 里造差异验证、量“已售对流水”那条查询和全量期间对数据库的影响。动集群之前先拿同意。
  5. 第 7 步：阶段笔记、面试考点。
- **下一步**：主人说要不要提交这一段 → 第 3 步。（主人说了提交，已提交；第 3 步也做了，见上一段。）

### 3d 三组工具写完（2026-11-24，UTC 是 11 月 23 日 16:50 到 17:40；在上一段之前）

细节：设计 `docs/design/3d-diagnosis-agent.md` 第 8 节“第 3 步”。

- **主人定的**：现在没有 API 密钥；同意提交、推送、开 PR。
- **做了的**：
  - 三个 Opus 子 Agent 各在一个 worktree 里写了 `internal/tool/prom`（4 个工具）、`loki`（2 个）、`kube`（4 个）。主会话读 diff、跑测试和 lint 后 cherry-pick 进来，worktree 和分支已删。`kube/project.go`（各类型的投影，900 多行）**没有逐行读**。
  - 共用的小件在 `internal/tool/shared.go`（解析参数、时间、裁剪成行的文本）。
  - 接进了 `agent serve`：`-prometheus-url`（或 `PROMETHEUS_URL`）、`-loki-url`（或 `LOKI_URL`）、`-in-cluster`、`-kubeconfig`；没给的那一组不登记。
  - 新子命令 `agent tool <工具名> '<JSON>'`：手工调一次工具，看模型会看到什么，不调模型。
  - **对着真的后端只读核对过**（Prometheus、Loki 经 port-forward，已关；Kubernetes 用本机 kubeconfig）。查出两个问题并修了、加了回归测试：Loki 里每行日志带每行不同的元数据标签，重复行合并不了；Loki 指标查询多一个点。
  - 落盘的 Kubernetes 对象多抹了命令行参数里像密钥的值。
- **验证**：`services/agent` 里 `go test -race ./...` 十个包通过，lint 0 issues，`GOWORK=off go build` 和 `make build` 通过。**没有跑整个仓库的 `make lint test`。**
- **CI**：PR #18 上 `8be9b74`（只有核心部分）30 个 job 加 codecov 两项全部通过（`ci-watcher` 报的）：新的 `lint (./services/agent)` 54 秒、`test (./services/agent)` 2 分 29 秒；`rules` 2 分 24 秒（以往 50 秒到 1 分半）、`test (./services/recon)` 2 分 24 秒（上次 1 分 53 秒）变慢，这次没有动规则和 recon，没有查；order 4 分 10 秒、pkg 3 分 29 秒、load 3 分 18 秒、e2e 3 分 58 秒。之后：`0b1d0ee` 那一轮被下一次推送取消了（29 个通过，e2e 被取消）。**PR #18 上 `91b0288`（3d 当前的头）30 个 job 全部通过**（`ci-watcher` 报的）：`lint (./services/agent)` 2 分 02 秒（只有核心时 54 秒）、`test (./services/agent)` 5 分 31 秒（只有核心时 2 分 29 秒），加了三组工具和 client-go 之后慢了一倍多，**现在是 CI 里最慢的 test job，没有拆开看时间花在哪**（依赖下载、带 `-race` 编译 client-go、还是测试本身）；`rules` 1 分 48 秒、order 4 分 05 秒、pkg 3 分 30 秒、load 2 分 40 秒、recon 2 分 09 秒、e2e 4 分 01 秒，都在以往的范围内。
- **顺带看到的（集群，没有动）**：`PrometheusErrorSendingAlertsToAnyAlertmanager`（critical）和 `...ToSomeAlertmanagers` 时不时进 `pending`。Prometheus 发给 Alertmanager 的请求 24 小时里失败 22 次（`EOF` 或 `connection reset by peer`，每次 1 条），平时只有 `Watchdog` 在发（约 90 秒一次），一次失败在 5 分钟窗口里就是三到五成，超过规则的阈值；`for` 是 15 分钟，7 天里只到过 `pending`、没有响过。两个 Pod 都是 Running。**原因没有查**（推断是空闲的长连接被对端关掉时正好在发）。这是 3c 留下的“放几天看误报”该看的一条：连续两三次失败挨得近就会真的响一条 critical。
- **第 4 步（部署用的清单和脚本）也写了**（Sonnet 子 Agent，主会话读了 RBAC 和 `cmd_agent`、重跑了检查后合并；设计第 8 节“第 4 步”）：`deploy/cluster/monitoring/agent.yaml`、`scripts/cluster/monitoring.sh agent`（`make agent`）、Alertmanager 的第二个 webhook 地址（默认不带，`AGENT_WEBHOOK=1` 才渲染进去）、RBAC 的测试。**只写了文件：清单没有 apply 过，`cmd_agent` 没有运行过。**
- **修了主会话引入的镜像构建问题**：新模块进 `go.work` 之后三个 Dockerfile 要多复制它的 `go.mod`，否则所有镜像构建失败。CI 不构建镜像，发现不了；是写第 4 步的任务说明时读 Dockerfile 看出来的。修后本机构建 order、recon、agent 三个镜像通过（没有推送）。**从 `4e6460e` 到修复之前的提交上 `make images`、`make deploy` 是坏的。**
- **没有做的**：
  1. 部署和试跑（设计第 6 节的第 0、5 步）：`make agent`、Pod 到 `api.anthropic.com` 通不通、`AGENT_WEBHOOK=1` 打开第二个地址、用 3c 的场景试跑。**等主人有了 API 密钥**；每一步动集群之前先登记、先拿同意。
  2. 阶段笔记 `docs/notes/3d-*.md`。
- **下一步**：主人有密钥之后：把 `ANTHROPIC_API_KEY` 放进仓库根目录的 `.env` → `make agent` → 手动提问一次（`kubectl -n monitoring port-forward svc/agent 8080` 加 `bin/agent ask "..."`）→ 打开第二个地址 → 试跑 → 勾计划里剩下的两项（触发、试跑）→ 阶段笔记。密钥没到之前可以先做 3e（对账方式按场景拆），它不依赖 3d。

### 3d 核心部分写完（2026-11-24；在上一段之前）

设计 [`docs/design/3d-diagnosis-agent.md`](design/3d-diagnosis-agent.md)（第 8 节是实现中的发现和与设计不一样的地方），决定 [ADR-0038](adr/0038-diagnosis-agent-own-loop.md)。

- **主人的答复**：十项都按建议。“暂时用 Claude Code 的模型”的意思是**现在没有 API 密钥**（主人后来说明的）。所以：模型的默认值不变（`claude-opus-5-5`）；写工具和单元测试不需要密钥，照常做；**试跑（第 5 步）和“Pod 到 `api.anthropic.com` 通不通”等有了密钥再做**。没有另写调 `claude -p` 的后端。
- **写了的**（新模块 `services/agent`，已加进 `go.work`）：
  - `internal/llm`：循环和模型之间的接口（`Conversation.Send`，历史收在实现里只能追加）、官方 SDK 的实现（beta 命名空间，两个缓存断点，自适应思考带摘要，effort，拒绝回退）、价格表和费用。
  - `internal/loop`：主循环。上限（25 轮、2 美元、10 分钟）是软的，撞到后最多再给两轮交报告；工具并发执行；六种结束原因。
  - `internal/report`：`submit_report` 的结构（strict）、溯源的校验、渲染成 Markdown。
  - `internal/trigger`：webhook 的筛选、按 `groupKey` 冷却一小时、队列 5 个、每天 20 美元的预算。
  - `internal/runlog`：一次运行一个目录（`meta.json`、`events.jsonl`、`raw/`、报告、`summary.json`），经 `os.Root` 读写，按时间和总量清理。
  - `internal/tool`：工具的接口和注册表。**三组工具（prom、loki、kube）还没有写**，`cmd/agent/serve.go` 的 `buildTools` 是空的。
  - `cmd/agent`：`serve`（`/alerts`、`/ask`、`/runs`）、`ask`、`replay`，系统提示 `system.md`。
- **验证**：`services/agent` 里 `go test -race ./...` 六个包通过，`golangci-lint run ./...` 0 issues，`make build` 通过（全部模块）。**没有跑整个仓库的 `make lint test`**（别的模块没有动；`go.work`、`go.work.sum` 变了）。**没有对真的 API 跑过一次。**
- **文档**：ADR-0038 和索引；`versions.md` 登记了 SDK（v2.13.0）；计划文件勾了主循环、报告、留痕三项（各自写了没验证的部分），触发那一项没有勾（没有部署）；面试考点加了一段。
- **没有做的**（设计第 6 节的第 0、3、4、5 步）：
  1. 第 0 步：Pod 到 `api.anthropic.com` 通不通（要起一个一次性的 Pod，等主人同意）；凭据。
  2. 第 3 步：三组工具和各自的裁剪。按分工是三个 Opus 子 Agent 各一个 worktree；**worktree 从提交建，所以要先把现在的改动提交**（主人还没有说提交）。任务说明里要给：`internal/tool/tool.go` 的接口和约定、设计 3.3 和 3.4 的两张表、能改的目录只有各自的 `internal/tool/<名字>/`、client-go 用 v0.37.2（kube 那一个要动 `go.mod`，合并时处理）。
  3. 第 4 步：Dockerfile、Deployment、RBAC、PVC、Secret 的脚本、Alertmanager 加第二个地址（`amtool` 的用例）、`make` 目标（Sonnet）。
  4. 第 5 步：试跑（每一项先登记、先拿同意）。
- **下一步**：主人说清“Claude Code 的模型”是哪个意思、同不同意提交 → 提交后推送，CI 派给 `ci-watcher`（新模块会多出一组 lint、test 的 job）→ 第 3 步并行写三组工具。

### 3c 第 16 项做完，3c 的计划项全部勾了（2026-11-23，UTC 12:15 到 14:10；在上一段之前）

细节：设计 `docs/design/3c-slo-alerting.md` 第 8 节最后一小节“第 16 项”（含“做了之后的结果”），[ADR-0037](adr/0037-slo-and-burn-rate-alerts.md)，计划文件 3c 一节。**这一段的改动全部没有提交**（`git status` 里的十几个文件，没有 Go 代码；`make rules-test`、`make alertmanager-test` 通过，没有跑 `make lint test`）。

- **清单**（从 Prometheus 的 `ALERTS` 查的）：82 条里 35 条响过、47 条没有；41 条改阈值能响，7 条的序列不存在（含“有差异”的 `ReconFoundDifferences`）。
- **改阈值的那一批做了**（第一次动手被权限检查拦下，主人说“同意跑”之后做的；12:53 到 13:56 UTC，四段）：41 条里 40 条响并送到 alertsink，`OrderAdmitRateDropped` 只到 `pending`（推断：一小时前探针停着，对比的那一边没有值；没有改）。Telegram 34 条，失败 0；模板在真实标签上都渲染对了。恢复后集群上五个 PrometheusRule 和仓库逐个比对相同。脚本和记录在 `tmp/3c-inject-16*`（不入库）。
- **序列不存在的 7 条**（主人同意按建议做）：死信两条（往 `order.submit`、`payment.events` 各写一条坏消息）、`ContainerOOMKilled`（`default` 里的一次性 Pod，已删）、“有差异”的 `ReconFoundDifferences`（退出码 1 的假 Job，已删；随后真的对账差异 0）都真实触发了。超卖、闸门内存写满、Collector 导出失败不做，留给阶段 4。**长期运行的那一套里留下两条死信**（两个死信 topic 各一条，内容 `not-a-proto-item-16-alert-check`），不是故障。
- **`RateLimitFallingBack` 改成按比例**（退回占 gateway 请求一成以上并且每秒 0.2 次以上，5 分钟），已下发，单元测试 u、v、w。没有真实触发过。
- **第五个 SLO“支付可用性”加了**（主人定的）：`slo.yaml` 的 `slo.order_pay_availability` 组、测试文件、runbook `slo-order-pay-availability.md`、Alertmanager 的路由和抑制用例、大盘的一行，已下发（14:04 核对有值）。自己写的告警现在 85 条。没有真实触发过。`inhibit-test`（集群上的）加了用例之后重跑过：57 条 0 条不符（合成的告警）。
- **SLO 的大盘**：`deploy/cluster/monitoring/dashboards/slo.json`（uid `opp-slo`，五个 SLO；生成脚本 `tmp/gen_slo.py` 不入库，改大盘要么改它要么直接改 JSON），已下发，表达式都在 Prometheus 上跑过有值。**没有在 Grafana 里打开看过**（虚线、对数轴的样子）。
- **ADR-0037 写了**，索引加了。
- **3c 的计划项全部勾了**（各自下面写了勾的依据和没做的部分）。85 条告警：真实触发 38，只验证路是通的 40，只到 `pending` 1，只有单元测试 6。
- **等主人定的**：
  1. `OtelCollectorExportFailing` 在 agent 上按配置就响不了（agent 一直重试、队列满了堵住上游，`send_failed` 永远不加；设计里写了）。要不要加一条“agent 的导出队列占满”的规则。
  2. `OrderAdmitRateDropped` 在“一小时前没有流量”时是瞎的，要不要改。
  3. 这一段的改动怎么提交（建议分开：`RateLimitFallingBack`；第五个 SLO 的规则、测试、runbook、Alertmanager 用例；大盘；ADR 和文档）。推送后 CI 派给 `ci-watcher`。
- **集群的状态（14:04 UTC 核对）**：没有在响的告警（除 `Watchdog`），没有 `pending`，没有不正常的 Pod；没有动过任何业务组件；`recon-hourly` 13:07 那次成功，13:13 手工的一次差异 0。本机 compose 起着。
- **阶段笔记写了**：`docs/notes/3c-slo-alerting.md`（255 行，Sonnet 子 Agent 写的；主会话读了开头、结论和统计，核对了它标出的几处文档不一致并改了其中两处（runbook 总则、ADR-0037 后果第 8 条），**其余部分没有逐行读**）。
- **提交并推送了**（主人说“都按建议做，提交并推送”）：`17e0fad`（`RateLimitFallingBack`）、`1b727c8`（第五个 SLO）、`71be1c3`（大盘）、`1244620`（ADR、笔记、文档）。上面“全部没有提交”是提交之前写的。**PR #17 上 `1244620` 的 CI 28 个 job 全部通过**（`ci-watcher` 报的）：`rules` 1 分 36 秒、`test (./services/order)` 3 分 24 秒（上次 4 分 33 秒）、`test (./pkg)` 3 分 32 秒、`test (./test/load)` 2 分 51 秒、`test (./services/recon)` 1 分 59 秒、e2e 3 分 34 秒（上次 4 分 28 秒），没有变慢的。**这一条写进本文件之后没有提交**，下次提交时带上。“等主人定的”第 1、2 条主会话给了建议（第 1 条加一条按队列占用比例的 warning；第 2 条先不改，记到阶段 4 核对原因），主人还没有答复，没有动。
- **第 1、2 条主人定了（2026-11-23，UTC 15:40 到 15:50 做的）**：
  1. **加了 `OtelCollectorExportQueueFull`**（`middleware.yaml`：导出队列占用超过八成持续 5 分钟，warning，被 `OtelGatewayDown` 抑制），三个单元测试，runbook `otel-collector.md` 补了，规则和 Alertmanager 的配置都已下发；集群上 `inhibit-test` 58 条 0 条不符（下发后马上跑的那一次有 1 条不符，是 Alertmanager 还没重新加载，过两分钟重跑通过）。表达式在 Prometheus 上有 6 条序列，现在都是 0，没有在响的告警。**只有单元测试，没有真实触发过，也没有改阈值验证过。** 自己写的告警现在 86 条，只有单元测试的 7 条。`make rules-test`、`make alertmanager-test` 通过。
  2. **`OrderAdmitRateDropped` 不改**，登记到 `docs/plan/phase-4-chaos.md` 的“4a 准备”（连同 3c 留下的其余没有真实触发的告警）。
  - **这两条的改动提交并推送了**：`cda125c`（规则、测试、抑制、runbook）、`40f4518`（文档）。**PR #17 上 `40f4518` 的 CI 28 个 job 全部通过**（`ci-watcher` 报的）：`rules` 1 分 10 秒、`test (./services/order)` 4 分 37 秒（上次 3 分 24 秒）、`test (./pkg)` 3 分 24 秒、`test (./test/load)` 3 分 18 秒（上次 2 分 51 秒）、`test (./services/recon)` 1 分 53 秒、e2e 4 分 14 秒（上次 3 分 34 秒）。order、load、e2e 比上一次慢，这两个提交没有改 Go 代码，三项都在以往出现过的范围内，没有查。这一条和记 `1244620` 的 CI 的那一条随合并前的最后一次文档提交进去了。
- **下一步**：PR #17 合并（主人 2026-11-24 同意）→ 3d 最小诊断 Agent（动工前先查最新的 Claude API 和 Agent SDK 文档）。放几天之后回头看误报（计划里“上线后放几天看误报”没有做）。

### 3c 第三轮过载压测（2026-11-23，UTC 10:35 到 12:05；在上一段之前）

主人对上一段“等主人定的”五条都按建议定了，都做了。细节：设计第 8 节最后一小节，ADR-0035、ADR-0036 各自的最后一节。

- **改了四处**：每个分区的队列上限 `ADMIT_MAX_QUEUE` 1250 改 600；等待类直方图加 40、50 秒两个桶（`pkg/otelx` 的 `WaitBuckets`）；限流要“连续失败够次数并且持续超过 200 毫秒”才打开（`pkg/ratelimit` 的 `FailureSpan`）；`CLOSE_CONCURRENCY` 4 改 2。提交前 `make lint` 11 个模块 0 issues、`make test` 60 个包 ok。
- **提交、CI、部署**：三个提交推送了（`6cd576a` 限流、`93026dc` 直方图的桶、`07c0dcc` 两个配置），之后是记录这一轮的文档提交。**PR #17 上 `07c0dcc` 的 CI 30 项全部通过**（`ci-watcher` 报的）：`test (./pkg)` 3 分 50 秒（上次 2 分 53 秒）变慢，这一项以前也这样波动过，没有查；`rules` 50 秒、`test (./services/order)` 4 分 19 秒、`test (./test/load)` 2 分 29 秒、`test (./services/recon)` 2 分 04 秒、e2e 4 分 22 秒。`make deploy`、`make recon-cron` 做了，六个服务的镜像是 `07c0dcc`，order 的启动日志确认每个分区 600。
- **第三轮压测**（11:10 到 11:47 UTC 发压，12:02 静止，12:05 恢复成平时的配置；运行号 `3c14s-warm`、`3c14s-order-ladder`，SKU 7619 到 7628，**下一批从 7629 起**）。`recon -since 100m` 差异 0，`verify` 通过。三轮的对比表在设计第 8 节。结论：
  - **等待压到了目标以内**：等了 30 秒以上的订单 0.9%（上一轮 10.1%），p99 29.9 秒，p99.9 39 秒，没有超过 50 秒的。拦住请求的是队列长度那一条，兜底的那条每秒不到 1 次。吞吐和被拒绝的比例三轮都一样。
  - **限流不再整段退回**：退回占请求的 3.4%（47%、21%），每一次都只是超时的那个请求自己。
  - **端点没有再被摘除**，`ORDER_LANDING_UNAVAILABLE` 是 0，order 等连接池回到每秒 7.6 次。代价：延迟关单并发 2 时到期的消息最多晚约 2 分钟（并发 4 时 15 秒）。这一轮同时改了队列上限，不是干净的对照。
  - **没有改、记给第 16 项的**：`RateLimitFallingBack` 的阈值是按次数写的（每秒超过 1 次），过载时零星的超时就让它响，该改成按比例。
- **集群的状态（12:05 UTC 核对）**：gateway 不带压测那一层，两个 ScaledObject 没有暂停，两个对账 CronJob 没有暂停（12:07、13:07 两次的窗口合起来盖住这一轮，**没有等着看**），探针起来了，没有在响的告警，没有不正常的 Pod，k3s-server 的盘约 21 GiB（Tempo 里这三轮的链路 11 月 26 日过期）。本机 compose 起着。落库时延的慢档多半会在探针恢复后再响一次（2 小时窗口里的尾巴），约 13:50 UTC 自己恢复。
- PR #17 上 `78370f2`（文档，当前的头）的 CI 全部通过（`ci-watcher` 报的是 28 个 job 全部成功，没有另数 codecov 的两项）：`rules` 1 分 32 秒（上次 50 秒）变慢，这一项以前也在 50 秒到 1 分半之间波动，只改了文档，没有查；`test (./services/order)` 4 分 33 秒、`test (./pkg)` 3 分 27 秒、`test (./test/load)` 2 分 54 秒、`test (./services/recon)` 1 分 47 秒、e2e 4 分 28 秒。**这一条写进本文件之后没有提交**，下次提交时带上。
- **过载这条线到这里收住了**，没有等主人定的。
- **下一步**：第 16 项（改阈值的那一批：先列出 82 条里还没真实触发的清单；含 4b 的 `PaidOrdersNotLanding`、`PaymentReplyOverdue`、支付时延的 SLO，`RateLimitFallingBack` 改成按比例，`RedisSentinelDown`，“有差异”的 `ReconFoundDifferences`）→ 勾 3c 的五项 → SLO 的大盘、ADR（SLO 怎么定的；支付的可用性要不要进 SLO）、阶段笔记。

### 3c 第二轮决定和重跑压测（2026-11-23，UTC 07:55 到 09:25；在上一段之前）

主人对上一段的四条给了决定，都做了。细节：设计第 8 节最后两小节，ADR-0035、ADR-0036 各自的最后一节。

- **规则和 Alertmanager（已下发、已提交）**：`MySQLDown`、`KafkaBrokerMissing`、两条复制线程的告警加 `keep_firing_for: 5m`；MySQL 的三条告警补了 `namespace`、`pod`、`statefulset` 标签，用来抑制自带的 `KubePodNotReady`、`KubeStatefulSetReplicasMismatch`（`inhibit-test` 54 条 0 条不符，合成的告警；真实的告警上没有验证）；`NodeCPUHigh` 的 `keep_firing_for` 从 10 分钟改成 3 分钟；第 4 项拆成 4a、4b，4b 归第 16 项。
- **提交和 CI**：五个提交推送了（`d230db5` 规则、`e0d02d2` kafkax、`a260836` stockgate、`808661c` order、`bc84433` 文档）。提交前 `make lint` 11 个模块 0 issues、`make test` 60 个包 ok。**PR #17 上 `bc84433` 的 CI 30 项全部通过**（`ci-watcher` 报的）：`test (./services/order)` 4 分 18 秒（上次 3 分 18 秒）、`test (./test/load)` 3 分 16 秒（上次 2 分 34 秒）变慢，都在以往出现过的范围内，没有查；`rules` 53 秒、`test (./pkg)` 2 分 42 秒、`test (./services/recon)` 2 分 13 秒、e2e 3 分 08 秒。
- **`make deploy`、`make recon-cron` 做了**：六个服务的镜像是 `bc84433`，order 的启动日志确认按 4 个分区各自计数（每个分区 1250、20）。
- **重跑了打过拐点的压测**（08:29 到 09:06 UTC 发压，09:21 静止，09:24 恢复成平时的配置；运行号 `3c14r-warm`、`3c14r-order-ladder`，SKU 7609 到 7618，**下一批从 7619 起**）。对账 `recon -since 100m` 差异 0，`verify` 通过。结论（表在设计第 8 节）：
  - 延迟关单修好了（到期的消息最多晚 15 秒，上一轮 25 分钟）；链路采样修好了（整轮 2.3 GB，上一轮 7.3 GB，盘只掉了约 3.7 GiB）。
  - 限流的退回只好了一半（47% 到 21%）。
  - **按分区计数的队列上限让等待变长了**：吞吐没变，等待的中位数从 10.7 秒到 17.3 秒，等了 30 秒以上的订单从 7.7% 到 10.1%。机制没问题，每个分区 1250 取得太贴，真正在拦的是兜底的第二条。
  - **“落库延迟 p99 约 57 秒”是直方图分桶造成的**（30 到 60 秒之间没有桶），上一轮的也是；两轮都没有订单等过 60 秒，没有订单被回收。
  - 新出现、没有查清的：order 到 inventory、到 payment 的端点各被摘除过一次，735 个 `ORDER_LANDING_UNAVAILABLE`，order 等连接池的次数是上一轮的 4 倍，`InventoryGateDrift` 响了（核对发现 1 次偏差、自动修了 1 次；`verify` 通过）。最可疑的是延迟关单的并发挤占连接池。
  - 新指标 `order_admission_backlog_rejections_total` 有数了：顶档 `queue` 每秒 20.5、`stale` 每秒 73.5，没有 `stalled`。
  - `NodeCPUHigh` 这一轮没有抖。`OrderQueueNearWaitLimit` 只到 `pending`，没有响（设计里估的“四个分区都满时约 27 秒”没有出现：第二条先拦住了，平均只排到 3216 单，折成约 17 秒）。
- **集群的状态（09:24 UTC 核对）**：gateway 不带压测那一层，两个 ScaledObject 没有暂停，两个对账 CronJob 没有暂停（09:07 那一次因为暂停没有跑，10:07 的窗口盖得住这一轮，**没有等着看**），探针起来了，没有在响的告警，没有不正常的 Pod，k3s-server 的盘约 23.6 GiB。本机 compose 起着。落库时延的 7 天预算又被烧掉一截，这一周只当读数看；停压后慢档的尾巴（探针恢复后 2 小时窗口里还装着慢订单）多半会再响一次，约 11:10 UTC 恢复。
- **等主人定的（主会话的建议）**：
  1. 每个分区的队列上限调小（建议 `ADMIT_MAX_QUEUE` 600，约 14 秒），让第一条先拦住。
  2. 给落库延迟的直方图在 30 到 60 秒之间加桶（40、50），否则量不出尾巴有多长。改桶会让这条指标的历史在新旧桶之间对不上，SLO 的规则用的是 `le` 的某一个桶，改之前要核对。
  3. 限流的判据再改一次（建议：连续失败并且持续超过一小段时间，比如 200 毫秒，才打开）。
  4. `CLOSE_CONCURRENCY` 从 4 调到 2 再跑一轮，分清连接池的等待和端点被摘除是不是它引起的。
  5. 上面几条改完要再跑一轮过载压测才算验证。
- **第 4a 项做了**（09:25 到 09:51 UTC，payment 缩到 0，探针不停；设计第 8 节“第 4a 项”）：响了 `IngressGateway5xxHigh`、`ProbeFailing`（两条 critical，Telegram 发了）、`ServiceMetricsMissing{payment}`、`ConsumerNotConsuming{payment.order-events}`，payment 回来 5 分钟内全部恢复。`PaymentReplyOverdue` 和支付时延的 SLO 没有响：支付是同步建单，payment 不在时 `/pay` 直接 503、支付单建不出来，这两条和 4b 一起归第 16 项。没有退款发生，约 1300 张探针订单到时限被正常关单。付款不可用 26 分钟而四个 SLO 都没响（SLO 只看下单），记给写 SLO 的 ADR。对账的结果见本段最后一条。
- PR #17 上 `7f6b0ff`（文档）的 CI 30 项全部通过（`ci-watcher` 报的）：`rules` 1 分 30 秒（上次 53 秒）、`test (./pkg)` 3 分 24 秒（上次 2 分 42 秒）、e2e 3 分 52 秒（上次 3 分 08 秒）变慢，`test (./services/order)` 3 分 24 秒、`test (./services/recon)` 1 分 54 秒变快，只改了文档，像运行机的波动，没有查。
- **收尾的对账（10:24 UTC）**：`recon-hourly` 10:07 那一次成功，差异 0（窗口 08:07 到 09:37，盖住压测）；10:23 手工跑的一次差异 0（窗口 08:23 到 09:53，盖住 payment 停着的 26 分钟）。10:24 时在响的只有 `SLOOrderPersistDelayBurnSlow`（warning，压测留在 2 小时窗口里的尾巴，约 11:10 UTC 自己恢复，**没有等着看**），没有不正常的 Pod。手工的 Job `recon-manual-1795429387`、`recon-manual-1795418641` 留着（成功的，会被历史上限挤掉）。
- PR #17 上 `adc63c8`（文档，当前的头）的 CI 30 项全部通过（`ci-watcher` 报的）：`test (./services/order)` 4 分 24 秒（上次 3 分 24 秒）、e2e 4 分 20 秒（上次 3 分 52 秒）变慢，`rules` 1 分 05 秒、`test (./pkg)` 2 分 53 秒、`test (./test/load)` 2 分 43 秒变快，`test (./services/recon)` 1 分 54 秒；只改了文档，都在以往的范围内，没有查。**这一条写进本文件之后没有提交**，下次提交时带上。
- **下一步**：主人看上面五条 → 第 16 项（含 4b；先列出 82 条里还没真实触发的清单）→ 勾 3c 的五项 → SLO 的大盘、ADR、阶段笔记。

### 3c 待定项的处理（2026-11-23 下午，UTC 07:10 到 07:50；在上一段之前）

主人对待定项逐条给了决定。做了什么、细节在哪：设计 `docs/design/3c-slo-alerting.md` 第 8 节最后六小节，[ADR-0036](adr/0036-per-partition-backlog-and-concurrent-delay.md)，计划文件登记表的第 17、18 项。**全部没有提交。**

- **规则（都已下发到集群，`make rules-test` 通过）**：
  - `ReconFoundDifferences` 按退出码拆成两条：CronJob 加了 `podFailurePolicy`（退出码 1 立刻判失败）和 `backoffLimit: 2`（其余的重试 2 次），`make recon-cron` 已下发；规则分成 `ReconFoundDifferences`（critical，有差异）和新的 `ReconRunFailed`（warning，没跑成）。第 17 项真实触发过“没跑成”那条，critical 那条没有动。
  - `RedisDown` 不含 Sentinel；新增 `RedisSentinelDown`（warning，少一个 Sentinel 就报：只排除的话掉一个、三个全掉都没人报）。没有在集群上触发过。
  - 删了自己的 `AlertmanagerNotificationsFailing`，留 chart 自带的两条。
  - `CDCLagging`、`CDCLaggingTooLong` 的通知用 `humanizeDuration`；`ConsumerNotConsuming`、`CDCPipelineMissing` 的通知写“至少 20 分钟”“至少 15 分钟”。
  - `NodeCPUHigh` 加 `keep_firing_for: 10m`。
  - 自己写的告警现在 82 条（删 1 加 2）。八份 runbook 和 `README.md` 跟着改了（Sonnet 子 Agent 改，主会话读过 `recon.md`、`redis-sentinel.md` 的 diff，其余六份没有逐行读）。
- **第 17 项（对账没跑成）做了**，07:20 到 07:25 UTC，结果在设计第 8 节。失败的 Job 已删。
- **第 18 项（停一个 Kafka broker）做了**，07:28 到 07:43 UTC，主人同意后做的。结论：这一次恢复时目标比源早走约 35 秒，没有漏；但顺序取决于欠了多少数据，过载或者停得久时会反过来。`MySQLDown` 那个源没有做实验（要让订单主库单独停几分钟，是阶段 4 的事），按机制和配置核对：目标可能比源晚走最多约 40 秒。**两个源都没有加 `keep_firing_for`，等主人定。**顺带发现 operator 回来 5 分钟后才把 Pod 建回来，没有查，记给阶段 4。
- **两处代码改动（ADR-0036），没有部署**：
  - 延迟消费者可以并发（等到期放在分发那一步），order 的延迟关单用 `CLOSE_CONCURRENCY`（默认 4，`order.yaml` 里写了）。
  - 受理的队列上限按 `order.submit` 的分区各数各的：`kafkax.PartitionOf`、`stockgate` 的三个脚本、order 受理时传分区号。`ADMIT_MAX_QUEUE`（1250）、`ADMIT_MAX_STALE`（20）的数没变，含义变成每个分区的上限。
  - 验证：`pkg`、`services/order`、`api` 三个模块 `make lint` 0 issues、`make test` 通过（28 个包 ok，`tmp/item5-test.log`）。其余模块没有跑（没有动）。**没有提交、没有部署、没有在过载下验证。** 部署时不要在过载中发布（新旧版本共存的两分钟里队列的索引会留下残留，自己会清，见 ADR-0036）。
- **集群的状态（07:45 UTC 核对）**：3 个 broker，没有副本不足的分区，Strimzi 的调和在成功；没有在响的告警，没有不正常的 Pod；探针每秒约 1 单；六个服务的镜像仍是 `6ec35a7`；两个对账 CronJob 是新的清单（`backoffLimit: 2`），镜像 `recon:6ec35a7`。本机 compose 起着。
- **主人定了、记下的**：新指标 `order_admission_backlog_rejections_total` 放到下一次正式压测看，不单独做一分钟的过载。
- **主会话的建议，等主人定**：
  1. 重跑打过拐点的压测的时机：这次的改动提交并 `make deploy` 之后就做，放在第 16 项和写 ADR 之前。一轮要看的东西都齐了（ADR-0035 的四个数、ADR-0036 的两个、新指标、`NodeCPUHigh` 不再抖、`OrderQueueNearWaitLimit` 在不在该响的时候响）；参数用和 `3c14` 完全一样的阶梯，数才能比。动手前 k3s-server 的盘至少 18 GiB（现在约 19.9），压的时候盯着它，采样的上限没验证过，掉到 13 GiB 就停。SKU 从 7609 起。
  2. 两个抑制源要不要加 `keep_firing_for: 5m`（建议加：代价只是恢复的通知晚 5 分钟）。
  3. 从库 NotReady 时自带的两条重复：建议加两条抑制（给我们的两条复制告警加 `namespace`、`pod` 标签，用它们压 `KubePodNotReady`；`KubeStatefulSetReplicasMismatch` 没有 Pod 一级的标签，按 `namespace` 加 `statefulset` 压）。没有做。
  4. 第 4 项（payment 缩到 0）的设计：拆成两项。4a 是 payment 整个缩到 0 约 25 分钟，探针不停，看 `PaymentReplyOverdue`、`ConsumerNotConsuming{payment.order-events}`、`ServiceMetricsMissing{payment}`、支付时延的 SLO；这段时间的约 1500 张探针订单会走关单的路，payment 回来之后补建的支付单对着已关的订单，要走退款，结束后等 30 分钟对账。4b 是“支付成功了而订单不变已支付”（`PaidOrdersNotLanding`），要停的是 order 消费 `payment.events` 的那一侧，现在没有不改代码的办法；建议在 `pkg/degrade` 登记一个暂停这个消费者的人工开关，停 10 分钟以内（告警约 7 分钟响，10 分钟内恢复的话订单在关单时限之前变成已支付，没有退款），或者归到第 16 项只验证路是通的。
- **下一步**：主人看上面四条；提交这次的改动（规则和 CronJob、runbook、kafkax、stockgate 和 order、ADR 和文档，建议分开几个提交），推送后 CI 派给 `ci-watcher`；`make deploy`、`make recon-cron`；然后是重跑压测、第 16 项、勾 3c 的五项、SLO 的大盘、ADR、阶段笔记。

### 3c 告警质量检查（2026-11-23，UTC 是 11 月 22 日 17:20 起；这一段最新，新会话先读这里，再读下面“搬到 Mac mini”里“新会话开头先做的一件事”）

逐项的结果在设计 `docs/design/3c-slo-alerting.md` 第 8 节“第 8 步”，登记表在计划文件 3c 一节（做过的标了“做了”）。每一项的逐分钟记录在 `tmp/3c-inject-*.log`（不入库）。

- **PR #17 上 `2efc147` 的 CI 30 项全部通过**（`ci-watcher` 报的，核对过提交）：`rules` 1 分 00 秒（加了 `make alertmanager-test` 之后的第一个数，以前 44 秒）；`test (./services/order)` 4 分 36 秒（以往的上沿）、`test (./pkg)` 2 分 46 秒、`test (./test/load)` 3 分 31 秒、e2e 4 分 15 秒。
- **第 15 项做了（2026-11-22 23:31 到 23:37 UTC，主人要求先做它）**：在 `oppenheimer-e2e` 里改一行。改商家索引的金额、改支付单的金额（差 1 分）、删商家索引的一行，`recon` 都是退出码 1 并定位到那张订单、只报改的那一对；改回去都是 0；把从库的 DSN 换成连不上的是退出码 2。e2e 的现场已经 `e2e-down` 清掉。结果在设计第 8 节“第 15 项”，那里也写了退出码 1 和 2 在 Kubernetes 一侧为什么分不开（给“等主人定的”第 2 条备的材料）。改了设计、计划、`recon.md` 和本文件，**已提交并推送**（2026-11-23，主人要求；只改文档，一个提交），PR #17 的 CI 派给了 `ci-watcher`，本段没有这次 CI 结果的话就是还没看。
- **PR #17 上 `562560a` 的 CI 30 项全部通过**（`ci-watcher` 报的，核对过提交）：`rules` 1 分 25 秒（上次 1 分 00 秒）、`test (./pkg)` 3 分 34 秒（上次 2 分 46 秒，这一项以前也这样波动过）；`test (./services/order)` 3 分 18 秒、`test (./test/load)` 2 分 27 秒、e2e 3 分 31 秒都比上次快。没有改 Go 代码，像运行机的波动，没有查。
- **PR #17 上 `34fe7c8` 的 CI 30 项全部通过**（`ci-watcher` 报的，主会话用 `gh pr view` 核对过提交和 30 项成功）：`rules` 1 分 26 秒、`test (./pkg)` 3 分 32 秒、e2e 3 分 17 秒没有变化；`test (./services/order)` 4 分 31 秒（上次 3 分 18 秒）、`test (./test/load)` 3 分 22 秒（上次 2 分 27 秒）慢了约一分钟，都在以往出现过的范围内，这次只改了文档，没有查；`test (./services/recon)` 1 分 44 秒。
- **第 14 项做了（压测；2026-11-23 02:26 到 03:04 UTC 发压，03:43 静止，03:45 恢复成平时的配置）**：普通下单阶梯 125、156、195 单/秒各 3 分钟再 280 单/秒 25 分钟（运行号 `3c14-warm`、`3c14-order-ladder`，SKU 7599 到 7608，**下一批从 7609 起**）。结果在设计第 8 节“第 14 项”。
  - 真的响了：`RateLimitFallingBack`、`SLOOrderPersistDelayBurnFast`（Telegram 触发和恢复各 1 条）、`BurnMedium`、`BurnSlow`（被快档抑制）、`NodeCPUHigh`、`OrderReadBulkheadRejecting`、`OrderBackpressureRejecting`，停压之后 `ConsumerBlocked`（延迟关单落后）和 `ServerDiskLow`。没有响的 `OrderQueueNearWaitLimit`、`OutboxRelaySlow` 归到第 16 项。七份 runbook 在现场走过并写了“历史”，其中三份补了内容。
  - 对账：`recon -since 100m` 四对差异 0（379641 单），`verify` 通过。
  - **查出四个问题，都没有改，等主人定**（设计里有机制和候选的改法）：(1) 限流的退回在过载时占 gateway 请求的 42%（一次 20 毫秒的超时就打开 1 秒）；(2) 延迟关单的消费者过载时落后，没付款的订单最晚在下单 40 分钟后才关（时限 15 分钟），停压后 40 分钟才追平，它空闲时也只有每秒一百多条，原因没有查；(3) 落库延迟 p99 到过 57 秒，离回收的宽限期 60 秒差 3 秒，这次没有订单被回收，但全局的队列上限 2500 在 280 单/秒时已经保证不了“最多等 30 秒”；(4) 尾部采样在过载时等于不采样，35 分钟写掉 k3s-server 约 8 GB。另有 `NodeCPUHigh` 在阈值上下会抖。
  - **`ServerDiskLow` 现在在响，会挂到 11 月 26 日**（Tempo 的保留期 3 天）：k3s-server 剩约 14.5 GiB，critical 的线是 8 GiB，现在没有东西在大量写。要不要提前收（缩短 Tempo 的保留期）等主人定；**在那之前不要再做打过拐点的压测**。
  - 做法上和登记的不一样的一处：没有 `LOADTEST=1 make deploy`，只对 gateway 做了 `LOADTEST=1 IMAGE_TAG=daac679 scripts/cluster/services.sh deploy gateway`（六个服务的代码从 `daac679` 起没变，不用重建镜像；工作区不干净时 `make deploy` 会打出带 `-dirty` 的 tag）。恢复是同一条命令不带 `LOADTEST=1`，再 `unpin`、`recon-cron-resume`、`probe-start`。
  - **集群的状态（03:46 UTC 核对）**：六个服务的副本数是平时的，镜像 `daac679`；`RATE_LIMIT_IP` 不在 gateway 的 ConfigMap 里；两个 ScaledObject 没有暂停；`recon` 的两个 CronJob 没有暂停（03:07 那一次没有跑，04:07 的窗口盖得住这一轮，**没有等着看**）；探针在跑；没有不正常的 Pod；没有静默。本机 compose 起着。落库时延和可用性的 7 天预算这一周都只当读数看。
  - **主人 2026-11-23 的答复和之后做的**（设计第 8 节“第 14 项的追查”）：四个问题先查清再定怎么改，`ServerDiskLow` 提前收。
    - 查清了：(1) 限流的退回没有变差，3b 记的“每秒 12 次”是 Redis 超时的次数，不是退回的次数（同一场景 2026-11-21 的退回是 0、6、150、792 次/秒），round-8 和 runbook 已更正；(2) 延迟关单是分区内串行、4 个分区，上限约每秒 140 条，低于落库的上限；(3) 落库延迟 p99 是 2500 ÷ 单个分区的落库速率（44 单/秒）≈ 57 秒，全局的上限管不住单个分区；(4) 尾部采样留下了 26% 的链路，多出来的是被算成“出错”的 429 和被算成“慢”的落库链路。
    - **主人 2026-11-23 定了：四条都按建议改**（决定和道理在 ADR-0035，改了什么在设计第 8 节最后一张表）。做到哪：
      - (1) 限流连续失败 5 次才打开：`pkg/ratelimit/fallback.go` 加测试，**代码改了，集群上还是旧镜像（`daac679`），没有生效**，要提交之后 `make deploy`。
      - (2) 延迟关单 8 个分区：集群上的 topic 已经加到 8（04:22 UTC，`scripts/cluster/middleware.sh kafka-topics`，核对过 `kafka_topic_partitions` 是 8）；代码里新建 topic 的分区数也改了（同样要新镜像才用得上，只影响新环境）。
      - (3) `ADMIT_MAX_QUEUE` 1250：已下发（`IMAGE_TAG=daac679 scripts/cluster/services.sh deploy order`，order 滚动过，ConfigMap 里是 1250）。
      - (4) Collector：已下发（`monitoring.sh collector`，04:21 UTC）。三条策略都在求值；发了 30 个查不存在订单的请求（客户端一侧是 `NOT_FOUND`），“出错”那条一条没留，改状态的那一步是起作用的。两类的上限没有在过载下验证。
      - 验证：`make lint` 11 个模块 0 issues；`make test` 通过（60 个包 ok，没有失败，`tmp/14fix-test.log`）；`shellcheck` 过了；Collector 的配置用 0.165.0 的 `validate` 校验过。**四处都没有在过载下重新验证**，下一次打过拐点的压测要看的四个数写在 ADR-0035 的“后果”里。
      - **主人随后要求提交、推送、部署，并把 `ADMIT_MAX_STALE` 改成 20**（`deploy/cluster/services/order.yaml`，道理在那里的注释和 ADR-0035）。分五个提交推送了；`make deploy` 在提交之后做（要干净的 tag），本段后面没有部署结果的那一条就是还没有做或者没有做完。
    - **`make deploy` 做了（2026-11-23 05:30 UTC 前后核对；日志 `tmp/deploy-3c-61f66d2.log`）**：六个服务的镜像是 `61f66d2`，五个服务各 2 个副本、cdc 1 个，没有不正常的 Pod；order 的 ConfigMap 里 `ADMIT_MAX_QUEUE` 是 1250、`ADMIT_MAX_STALE` 是 20；gateway 没有 `RATE_LIMIT_IP`；两个 ScaledObject 没有暂停。之后 `make recon-cron`（两个 CronJob 的镜像是 `recon:61f66d2`，没有暂停）。探针、`recon-totals` 的镜像没有动（`chaos:e3c3ca7`、`recon:131024f`）。四处修正现在都在集群上了，**仍然没有在过载下验证**。order 启动时照旧有那条连接池的提示（`SUBMIT_CONCURRENCY` × 分区数大于连接池），是已知的。
    - **Tempo 的保留期已改回 72 小时（05:33 UTC，`monitoring.sh tempo`，ConfigMap 里核对过）。** k3s-server 的盘回到约 19.8 GiB（压测前约 22），`ServerDiskLow` 恢复了。过去 3 天的链路没有了。
    - `recon-hourly` 04:07、05:07 两次都成功，差异 0（04:07 那次的窗口盖住了压测，26 秒）。05:31 UTC 时没有在响的告警（两条 `pending` 是刚滚动留下的，没有等着看）；落库时延的慢档已经恢复。
    - PR #17 的 CI：`61f66d2` 那一轮 28 个 job 里 27 个成功，`test (./services/order)` 被后一次推送（`95a5270`）取消，不是失败；**当前的头 `95a5270`（代码和 `61f66d2` 相同，多一个文档提交）30 项全部通过**（`ci-watcher` 报的，主会话用 `gh pr view` 核对过提交和 30 项成功）。`61f66d2` 那一轮的耗时：`rules` 1 分 07 秒、`test (./pkg)` 3 分 32 秒、`test (./test/load)` 3 分 24 秒（上次 2 分 38 秒）、`test (./services/recon)` 2 分 12 秒、e2e 3 分 10 秒。
    - **背压拒绝原因的指标做了（主人 2026-11-23 确认）**：`pkg/stockgate` 的 `Hold` 在拒绝时多返回是三条里的哪一条（`HoldResult.Rule`：`queue`、`stale`、`stalled`），order 记成 `order.admission.backlog_rejections{rule}`（Prometheus 里 `order_admission_backlog_rejections_total`）；不增加 Redis 的查询，受理的行为不变。`loadtest-round.sh` 的存档、`order-backlog.md` 第 1 步、ADR-0035、设计第 8 节都补了。验证：`pkg` 和 `services/order` 两个模块 `make lint` 0 issues、`make test` 通过（28 个包 ok，`tmp/backlog-rule-test-full.log`），没有跑别的模块（没有动）。主人随后要求提交、推送、部署验证：两个提交（`aa6b2f0` 代码、`6ec35a7` 文档）已推送，**PR #17 上 `6ec35a7` 的 CI 30 项全部通过**（`ci-watcher` 报的，主会话用 `gh pr view` 核对过提交和 30 项成功）：`rules` 59 秒、`test (./services/order)` 3 分 18 秒、`test (./pkg)` 2 分 43 秒、`test (./test/load)` 2 分 34 秒、`test (./services/recon)` 2 分 07 秒、e2e 3 分 25 秒，没有明显变慢的。
    - **`make deploy` 和 `make recon-cron` 做了（2026-11-23 05:48 UTC 核对；日志 `tmp/deploy-3c-6ec35a7.log`）**：六个服务的镜像是 `6ec35a7`，副本数是平时的，没有不正常的 Pod；两个 CronJob 的镜像是 `recon:6ec35a7`，没有暂停；探针每秒约 1 单在受理；gateway 没有 `RATE_LIMIT_IP`；k3s-server 的盘 19.9 GiB。
    - **新指标在集群上还没有出过数，没有验证**：`order_admission_backlog_rejections_total` 只有背压真的拒绝时才有序列，探针的流量触发不了。主会话打算做一次一分钟的过载（125 单/秒 30 秒、300 单/秒 60 秒）来看它，**动手时被权限检查拦下了（没有主人明确的同意），什么都没有做**：计划文件没有登记，本机 compose 起着，探针、对账、限流都是平时的状态。等主人说做不做。
    - 部署后看到的一件小事：Prometheus 往 Alertmanager 发告警在 05:44:38 UTC 出过一次 `EOF`（10 分钟里 1 次），自带的 `PrometheusErrorSendingAlertsToAnyAlertmanager`、`...ToSomeAlertmanagers` 进了 `pending`；Alertmanager 的 Pod 没有重启。像一次性的，没有查；新会话看一眼它们有没有消失。
    - **背压的第 2 条（排队超过 30 秒的多于 100 个就拒绝）查过了**（设计第 8 节）：规则和配置没有问题；它限制的是个数不是时长，而落库实际是 96 条各自串行的 lane（4 个分区 × `SUBMIT_CONCURRENCY` 24，过载时每条约 1.9 单/秒），100 个超时的占用集中在两条 lane 上就是 30 + 26 = 56 秒，和量到的 p99 一致。它那次拒绝了多少没有数据（脚本不记是哪一条）。`ADMIT_MAX_STALE` 已按主人的决定调成 20；让脚本返回是哪一条拒绝的并做成指标没有做（主人问了它是什么问题、怎么修，答复之后等主人定）。
    - PR #17 上 `7104ff1`（第 14 项的文档）的 CI 30 项全部通过（`ci-watcher` 报的，提交核对过）：e2e 4 分 23 秒（上次 3 分 17 秒）、`test (./services/recon)` 2 分 25 秒（上次 1 分 44 秒）慢了，`test (./services/order)` 4 分 01 秒、`test (./test/load)` 2 分 38 秒快了；只改了文档，没有查。
    - （已改回，见上面）**Tempo 的保留期临时改成了 1 小时（03:55 UTC 下发），为了把压测的那批链路提前删掉；仓库里的 `deploy/cluster/monitoring/tempo.yaml` 仍然是 `72h`（临时的 `1h` 是改了文件下发、随后把文件改回去的，没有提交过），所以集群上的和仓库里的现在不一样。** 盘放回来之后运行 `scripts/cluster/monitoring.sh tempo` 就改回 72 小时。本段后面没有“已改回”的话就是还没有改回，新会话先做这件事（`tmp/3c-inject-14-tempo-reclaim.log` 里每 5 分钟记一次 k3s-server 的剩余）。过去 3 天的链路会一起被删掉。
  - **新会话先看一眼的**：探针恢复之后落库时延的慢档（`SLOOrderPersistDelayBurnSlow`，warning，看 1 天和 2 小时两个窗口）又进了 `pending`（03:46 UTC）：探针一有落库，比例重新有了分母，而 2 小时的窗口里还装着这一轮的慢订单。它多半会在 04:01 前后响，到 05:04 前后（顶档滑出 2 小时窗口）恢复；这是多窗口的尾巴，不是新的问题，**没有等着看**。`ProbeNoData` 同时在 `pending`（探针刚起来），探针已经在以每秒 1 单下单，应当自己消失，同样没有等着看。
  - 这一段的改动（设计、计划、七份 runbook、本文件）**没有提交**。每分钟一行的记录和现场的输出在 `tmp/3c-inject-14-*.log`，各档的服务端指标在 `tmp/k6/3c14-order-ladder/stages/`（不入库）。
- **做完的（16 项里 13 项，含上面的第 15、14 项）**：第 1 项（抑制，做成了 `monitoring.sh inhibit-test`，14 条规则 47 条用例全部符合）、第 2 项（停探针，什么都没响）、第 3 项（inventory，7 分钟）、第 5 项（orderindex）、第 6 项（cdc）、第 7、8 项（从库的 SQL、IO 线程）、第 9 项（闸门内存）、第 10 项（Sentinel）、第 11 项（otel-gateway）、第 12 项（alertsink）、第 13 项（Strimzi operator）。每一项响的告警都在现场照 runbook 走过，命令都能跑，走得到原因；走的过程中改了 5 份 runbook。
- **真实触发过的我们自己写的告警（22 种）**：`ServiceMetricsMissing`、`ConsumerNotConsuming`、`CDCPipelineMissing`、`CDCLagging`、`MySQLReplicaSQLThreadStopped`、`MySQLReplicaIOThreadStopped`、`RedisGateMemoryHigh`、`RedisGateMemoryCritical`、`RedisSentinelBelowQuorum`、`RedisDown`、`KafkaOperatorMetricsMissing`、`AlertmanagerNotificationsFailing`、`AlertDeliveryStalled`、`OtelGatewayDown`、`ProbeNoData` 和 `PaymentTotalsCheckStalled`（响了，被抑制）、`OrderLandingBreakerOpen`、`SLOOrderCreateAvailabilityBurnFast` 和 `BurnMedium`、`ProbeFailing`、`IngressGateway5xxHigh`、`ReconFoundDifferences`（意外）。critical 的 Telegram 都发出去了（发送失败 0）。81 条里其余的还没有真实触发。
- **改了一条规则并下发了**：`OtelGatewayDown` 加 `keep_firing_for: 5m`（第 11 项里 Collector 一恢复抑制就撤了，被压着的“没有数据”漏出来两条）。加了单元测试，`make rules-test` 通过，`monitoring.sh rules` 下发，集群上核对过字段在。
- **出过的一个事故**：第 8 项 19:06 UTC 动手，压在了 19:07 的 `recon-hourly` 上，对账连不上被摘掉的从库（退出码 2），`ReconFoundDifferences`（critical）发到了 Telegram。不是差异。19:15 手工复跑差异 0，告警恢复。**以后的故障注入避开每小时的第 5 到 9 分钟。**
- **可用性的 7 天预算现在是负的（约 -93%）**：第 3 项的 7 分钟里有 381 个 5xx，而 7 天窗口里总共只有约 20 万个请求。到 11 月 29 日这 7 分钟滑出窗口之前它一直是负的；这一周里预算的数只当读数看（设计第 8 节第 3 项里有说明）。
- **等主人定的**：
  1. （已做，2026-11-23 主人要求）失败的 Job `recon-hourly-29922907` 删掉了（23:28 UTC），`oppenheimer` 命名空间里没有不正常的 Pod 了；`KubeJobFailed` 应该随之恢复，没有等着看。日志和退出码记在 `docs/runbooks/recon.md` 的“历史”。手工跑的 `recon-manual-1795374925` 已经不在了。
  2. 检查里查出来、**没有改**的规则问题（设计第 8 节“第 8 步”各项里有理由和改法）：
     - `ReconFoundDifferences` 分不清“有差异”（退出码 1）和“没跑成”（退出码 2）。涉及资金的对账怎么报，主人定。
     - `RedisDown` 把 Sentinel 也算进去，少两个 Sentinel 会发两条 critical。
     - `AlertmanagerNotificationsFailing` 和 chart 自带的两条重复（自带的更快，其中一条是 critical）。
     - 可用性的中档在快档恢复之后才发出来，再响 25 分钟（多窗口加抑制的固有行为，runbook 里写了怎么判断是尾巴）。
     - `CDCLagging` 的通知文字是“365.4k 毫秒”；`ConsumerNotConsuming`、`CDCPipelineMissing` 的通知里写的分钟数比实际短 5 到 10 分钟。
     - 从库 NotReady 超过 15 分钟时自带的两条会和我们的那条说同一件事。
     - `MySQLDown`、`KafkaBrokerMissing` 这两个抑制源可能有和 `OtelGatewayDown` 一样的“恢复时先撤”的毛病，没有验证。
  3. 第 4 项（payment 缩到 0）按登记的做法触发不了它要看的告警，要重新设计（计划文件的表里写了原因），并且它会让探针的订单走关单和退款的路。怎么做、做多久，主人定了再做。
- **还没有真实触发、要归到第 16 项（改阈值）或者另想办法的又多了几条**：`CDCLaggingTooLong`、`MySQLReplicaLagHigh`（停 SQL 线程时延迟的序列会消失）、`MeshEndpointEjected` 和网格的溢出（缩到 0 时没有端点可摘，要“一个副本坏一个好”）。
- **对账**：今晚每一次都是差异 0。`recon-daily` 到点自己跑的第一次（21:30 UTC）成功：116 秒，1476.7 万单，上界 21:00。`recon-hourly` 22:07 那次的窗口到 21:37，第 3 项的后 3 分钟（21:37 到 21:40）在 23:07 那一次的窗口里（到 22:37），**看过了：成功，18 秒，差异 0**。
- **集群的状态（22:12 UTC 核对）**：六个服务、探针、`recon-totals` 的副本数都是平时的；otel-gateway、alertsink、Strimzi operator、Sentinel（3 个）都缩回去了；闸门 Redis 的 `maxmemory` 是 805306368（等于清单）；`mysql-order-0-1` 的复制两个线程都是 ON；没有静默；除了那个失败的 Job 的 Pod 没有不正常的 Pod。
- **下一步**：
  1. 主人看“等主人定的”三条。
  2. 登记表里剩的：第 4 项（重新设计之后）、第 16 项（改阈值的那一批，先列出 81 条里还没真实触发的清单再做；第 14 项之后真实触发过的多了 9 种：`RateLimitFallingBack`、落库时延的三档、`NodeCPUHigh`、`OrderReadBulkheadRejecting`、`OrderBackpressureRejecting`、`ConsumerBlocked`、`ServerDiskLow`，上面“22 种”那一条没有重写）。主人看第 14 项查出的四个问题和 `ServerDiskLow` 怎么收。
  3. 告警质量检查做完之后勾计划里 3c 的五项，然后是 SLO 的大盘、ADR（SLO 怎么定的、为什么 7 天、429 算什么、为什么手写、Alertmanager 的配置为什么不用 `AlertmanagerConfig`；这次的“抑制的源要先来后走”“流量小的系统上按请求数算的预算”可以并进去）、阶段笔记。
- **已提交并推送**（2026-11-23，主人要求）：`inhibit-test` 和它的用例、`OtelGatewayDown` 的 `keep_firing_for` 和测试一个提交，文档（设计、计划、六份 runbook、本文件）一个提交。PR #17 的 CI 派给了 `ci-watcher`；本段没有这次 CI 结果的话就是还没看。`make rules-test`、`make alertmanager-test`、`shellcheck -x` 过了；没有改 Go 代码，没有跑 `make lint test`。
- **主人 2026-11-23 说“等主人定的”第 2、3 条晚点定。** 它们挡住什么、不挡什么：第 3 条只挡登记表的第 4 项；第 2 条里的规则改法挡第 16 项里涉及这几条规则的部分，也最好在写 ADR 和勾 3c 的“告警质量检查”之前定。第 15 项、第 14 项（压测）、第 16 项的清单、SLO 的大盘、阶段笔记都不依赖它们，可以先做。

### 3c 的 Alertmanager 和 runbook（2026-11-23，UTC 是 11 月 22 日 16:37 起；在上一段之前）

细节在设计 `docs/design/3c-slo-alerting.md` 第 8 节最后三小节。

- **已提交并推送**（2026-11-23，主人要求）：`b77c202`（alertsink）、`b3e128e`（Alertmanager 的配置和检查）、`b441099`（规则）、`3ee2cd2`（runbook），再加一个记录这些的文档提交。PR #17 的 CI 派给了 `ci-watcher`；本段最后没有 CI 结果的那一条就是还没看。
- **验证**：`make rules-test`、`make alertmanager-test` 通过；`make lint` 11 个模块 0 issues；`make test MODULES=$PWD/test/load` 通过；`shellcheck` 过了。**没有跑完整的 `make test`**（Go 代码只动了 `test/load`）。CI 里 `rules` job 新加的一步（`make alertmanager-test`）没有实际跑过，推送之后留意。
- **欠着的 Kafka 规则补上了**：改成看 Strimzi operator 的调和计数（`KafkaOperatorNotReconciling`、`KafkaOperatorMetricsMissing`），没有按计划里写的看 `Kafka` 资源的 `Ready`，理由在设计第 8 节。另加了 `OtelGatewayDown`（critical）、`AlertDeliveryStalled`（critical）。自己写的告警现在 81 条（加上 chart 自带的共 227 条），**都已下发**，求值都正常，在响的只有 `Watchdog`。
- **Alertmanager 的配置下发了**（`monitoring.sh kps`，同一次把两条 Overcommit 关掉了）：全部告警发到集群里的 alertsink（`monitoring` 命名空间，`kubectl -n monitoring logs deploy/alertsink`），用合成的告警验证过路由、分组和 SLO 的抑制。alertsink 的镜像在文档提交之后换成了干净的 tag（`scripts/cluster/monitoring.sh alertsink`）。
- **PR #17 上 `2efc147` 的 CI 全部通过**（`ci-watcher` 报的是 29 项；主会话用 `gh pr view` 核对提交和结论时数到 30 项成功，差的一项没有查是哪个）：`rules` 1 分 00 秒（上次 44 秒，多了 `make alertmanager-test` 一步，跑了并通过）；`test (./services/order)` 4 分 36 秒（以往的上沿）；`test (./test/load)` 3 分 31 秒（上次 2 分 54 秒，之前有过 3 分 24 秒；这次多了 alertsink 的测试）；`test (./pkg)` 2 分 46 秒；`test (./services/recon)` 2 分 20 秒；e2e 4 分 15 秒。
- **Telegram 接上了**（主人写了 `.env`，2026-11-22 17:17 UTC `monitoring.sh alertmanager`）：配置一分钟内生效，token 挂在 `/etc/alertmanager/config/telegram-token`。塞了一条 `severity=critical` 的合成告警（`SyntheticTelegramTest`），Alertmanager 的计数是发出 1 条、失败 0 条。**主人确认收到了**（2026-11-23）。 主会话不读 `.env` 的内容，脚本读。
- **runbook 写了**：`docs/runbooks/` 44 份加 `README.md`（三个 Sonnet 子 Agent 写，没有留下 worktree）。结构和链接用脚本核对过；**主会话逐行读过的只有 1 份，命令都没有在集群上执行过**，告警质量检查时逐条照着走当验收。计划文件里这一项已勾。
- **写 runbook 时子 Agent 查出四条规则的问题，已改并下发**（设计第 8 节“第 7 步”有清单）：入口的在途上限溢出原来没有抓（round-6 量到的主要拒绝路径）；`ProbeFailing` 把被 429 拒绝和售罄算成了失败（改了，另加 `ProbeIncomplete`）；`PaidOrdersNotLanding` 响的时候已经到关单时限（收紧到约 7 分钟）；`ReconDailySlow` 慢过一次不恢复。同一节里还有九条提了没有改的。
- **故障注入登记了**（计划文件 3c“告警质量检查”下面的表，16 项），**一项都还没有做**。
- **`recon-daily` 到点自己跑的第一次（21:30 UTC）仍然没看**：这次会话结束时还没到点。
- **下一步**：
  1. 主人说告警质量检查在新会话做。本文件在 `2efc147` 之后的修改（Telegram 的确认、CI 结果）没有提交，下次提交时带上。
  2. runbook 的验收并进第 3 项：每触发一条告警就照它的 runbook 走一遍，走不通的当场改。
  3. 告警质量检查：按登记表从第 1 项（合成告警试其余的抑制）做起；第 14 项要发压，按 3b 的规矩。
  4. SLO 的大盘、ADR（SLO 怎么定的、为什么 7 天、429 算什么、为什么手写；Alertmanager 的配置为什么不用 `AlertmanagerConfig` 可以并进去）、阶段笔记、面试考点。
- **没有做、记着的**：“节点不在”的抑制（告警上没有节点标签）；其余几条抑制只有配置检查；`monitoring.sh silence` 没有用过；alertsink 不记 `truncatedAlerts`。

### 3c 开工：补指标和 SLO 的规则（2026-11-22 夜；在上一段之前）

- **最新（2026-11-22 17:40 UTC）**：`131024f` 已推送，PR #17 的 CI 全部通过（新加的 `rules` 44 秒；`test (./pkg)` 2 分 59 秒，`test (./test/load)` 2 分 54 秒，`test (./services/recon)` 2 分 35 秒）。那之后的改动（四份规则和测试、关掉两条 Overcommit 的 values、文档）**主人要求提交了，分两个提交，没有要求推送，所以没有推送**；新会话先 `git status`、`git log origin/phase-3c..` 确认。`make rules-test` 通过（8 个测试文件）；没有改 Go 代码，没有重跑 `make lint test`。
- **主人 2026-11-22 问的两件事，答过了，没有形成决定**：(1) 重复告警会不会合并：会，靠 Alertmanager 的分组（设计 3.5），现在还没配；(2) 能不能引入大模型做决策：建议只放在“通知之后”（归并、给出初步诊断，就是 3d 的诊断 Agent 的入口），不放在“要不要通知”和自动处置的路径上。主人如果要把它纳入，改 3d 的设计时一起定。
  - 主人随后给了具体的产品：Gemini Decisions API（`gemini-decision-1`，Google 2026-11-11 的博客 https://blog.google/innovation-and-ai/technology/developers-tools/gemini-decisions-api/ ）。它不是生成式的，是对给定的问题（是/否、多选一、按量表打分）一次前向给出概率和置信度，官方说端到端 150 到 250 毫秒、每百万输入 token 0.10 美元。主会话的看法：适合做告警到达之后的归并和分诊（“这几条是不是同一个事故”“最可能是哪一类原因”“该翻哪份 runbook”），低置信度时退回原样通知；仍然不放在“发不发”和自动处置的路径上。博客没有说是预览还是正式版、有没有确定性、有没有解释，这些以及校准的好坏都没有独立验证过。**没有形成决定**，写 3d 的设计时作为一个候选方案和生成式模型对比。
- **设计第 6 节的第 4 步（其余的规则）做完了**：自己写的规则一共 5 个 PrometheusRule、76 条告警，**都已下发**（`scripts/cluster/monitoring.sh rules`），求值正常，没有一条在响。细节、与设计不一致的地方、没实测的几条在设计第 8 节“第 4 步的一半”“第 4 步的另一半”。没有在跑的子 Agent，没有留下的 worktree。
- **欠着的一条规则**：“`Kafka` 资源不是 `Ready`”，Prometheus 里没有可用的指标（要给 kube-state-metrics 配自定义资源的状态，或者接 Strimzi operator 的指标）。计划里“基础设施告警”一项因此没有勾。
- **`recon-daily` 到点自己跑的第一次是 21:30 UTC，还没看过**（`kubectl -n oppenheimer get jobs`；手工触发的那次 1 分 57 秒、差异 0）。
- **chart 自带的 149 条筛过了**（设计第 8 节“第 5 步”）：只有两条 Overcommit 在响，已在 `deploy/cluster/monitoring/kube-prometheus-stack.yaml` 里关掉（`defaultRules.disabled`），**values 改了还没有下发**（`scripts/cluster/monitoring.sh kps`，和 Alertmanager 的配置一起下发）；其余保留。
- **下一步**：补 `Kafka` 资源那一条 → Alertmanager 的路由和抑制、webhook 接收端、Telegram（`.env` 里的 `TELEGRAM_BOT_TOKEN`、`TELEGRAM_CHAT_ID`，只检查在不在，不读值；先验证集群能不能出网到 Telegram）→ runbook（文件名以各规则的 `runbook_url` 为准，`rg -o 'runbooks/[a-z0-9-]+' deploy/cluster/monitoring/rules/*.yaml | sort -u` 列得出来）→ 告警质量检查（故障注入先登记到计划）→ SLO 的大盘、ADR、阶段笔记、面试考点。

- **主人 2026-11-22 确认了 3c 的设计**：十项都按建议；通知渠道用 Telegram（机器人的 token 和会话号还没有给，做到设计第 6 节第 6 步时向主人要）；两个失败的手工备份 Job 已删。设计的状态、计划文件（3c 加了三项、CDC 心跳不做）、计划变更记录都改了。
- **按设计第 6 节的顺序做到哪**（细节在设计第 8 节）：
  - 第 1 步补指标：`order.payment.delay`（主会话写，`services/order/internal/service/{metrics,payment,payment_test}.go`）和 `outbox.relay.delay`（Opus 子 Agent 写，`pkg/outbox/`，已用 `git cherry-pick -n` 拿进工作区）做完了。`kafkax.consumer.oldest_pending_age` 也做完了（同一个子 Agent，`pkg/kafkax/`，主会话读过 diff，已拿进工作区）。三个指标都齐了。子 Agent 的 worktree（`.claude/worktrees/agent-a9f9db70927227d67`，分支 `worktree-agent-a9f9db70927227d67`，提交 `1840dd4`、`1303c60`）**还留着**：工作区的改动没有提交，提交之后再删 worktree 和分支。工作区里的 `pkg/kafkax/oldest_test.go` 比它的提交多一处主会话的修改（见下一条）。
  - 第 3 步的前一半：`deploy/cluster/monitoring/rules/slo.yaml` 写了（40 条），**没有下发到集群**。
- **验证到哪**：order 模块 `golangci-lint` 0 issues；`make test MODULES=$PWD/services/order` 跑过一次，只有 `TestLandingHealthProbe` 失败（连 Redis 超时，当时子 Agent 也在跑测试），单独重跑它和支付的用例都通过。**合并 kafkax 的改动之后 `make lint test` 通过**（11 个模块 0 issues，56 个包 ok，没有被跳过的，`tmp/3c-lint-test.log`、`tmp/3c-test.log`）。第一次跑时子 Agent 写的 `TestOldestPendingAgeWithLanesReportsTheBlockingKey` 失败：它等 `b`、`c` 两个 key 做完，而 key 到 lane 的哈希种子每个进程随机，它们有四成多的概率和被挡住的 key 同一条 lane、排在后面出不来。主会话改成只等位点在前面的 `a`，连跑 8 遍通过。 注意：直接 `go test` 不带 Makefile 的环境变量时集成测试全部被跳过、显示 `ok`，要用 `make test`（指定模块用 `MODULES=`，指定用例用 `GOFLAGS='-run=...'`）。
- **本机 compose 是起着的**（子 Agent `make up` 的）。下次正式压测前要 `make down`。
- **已提交并推送**（2026-11-22，主人要求）：`phase-3c` 上 `d20b999`（三个指标）、`7a49245`（SLO 规则）、`daac679`（文档）。开了 **PR #17（draft）**，CI 派给了 `ci-watcher`；本段最后没有 CI 结果的那一条就是还没看。子 Agent 的 worktree 和分支已删。
- **`make deploy` 做了**（主人要求；日志 `tmp/deploy-3c-daac679.log`）：六个服务的镜像是 `daac679`，五个服务各 2 个副本、两个节点各一个，cdc 1 个，没有不正常的 Pod。规则仍然没有下发。
- **三个新指标在集群上验证过**：`kafkax_consumer_oldest_pending_age_seconds` 六个消费组都在报 0。跑了一次冒烟（`smoke-3c-*`，普通下单 10 单/秒 20 秒，SKU 3001，201 次迭代，`http_req_failed` 通过）：`order_payment_delay_seconds` 记了 92 次（支付出结果 100 笔，差的 8 笔没有查是哪一类，可能是取消在先被要求退款的），p50 约 0.12 秒、p99 约 0.36 秒；`outbox_relay_delay_seconds` 按 topic 有数，`order.events` 的 p99 约 0.2 秒、`payment.events` 约 0.4 秒。**这只是 10 单/秒 20 秒的数，不是基线**；支付处理时延那条 SLO 的阈值（暂定 5 秒）等探针跑起来再定。
- **PR #17 上 `daac679` 的 CI 29 项全部通过**（`ci-watcher` 报的，核对过运行对应的提交）：`test (./services/order)` 4 分 32 秒（以往 3 到 4 分半，在上沿），`test (./services/recon)` 1 分 59 秒，`test (./pkg)` 2 分 42 秒，e2e 3 分 47 秒。
- **第 2 步派出去了两个子 Agent**（各自的 worktree，基于 `origin/phase-3c`）：Opus 写 `load probe`（常驻探针，只改 `test/load/`）；Sonnet 写 `recon` 的两个 CronJob（`deploy/cluster/recon/`、`services.sh` 的 `recon-cron` 子命令、Makefile 目标；不许碰集群）。回来之后主会话读 diff、`cherry-pick`、跑 `make lint test`；探针的清单（Deployment、SKU 和库存的准备、镜像）还没有人写，等探针的 flag 定了再做。
- **`recon` 的 CronJob 做完并装上了**（Sonnet 子 Agent 写，主会话读过 diff，`cherry-pick -n` 拿进工作区，worktree 和分支已删；`shellcheck` 通过）：集群上 `oppenheimer` 命名空间里有 `recon-hourly`、`recon-daily`。手工触发的一次 20 秒跑完、差异 0，但窗口里没有订单，有订单时的耗时没有量。细节和没处理的五个问题在设计第 8 节“第 2 步的一半”。**以后 `make deploy` 之后要再 `make recon-cron`；正式压测和空表对照之前要 `services.sh recon-cron-suspend`。** 计划文件里这一项还没有勾（告警规则和 runbook 没有做，有订单时没有验证过）。
- **全量对账量过了（12:57 UTC）**：四对、1473.7 万单，1 分 55 秒，差异 0（设计第 8 节“全量对账的耗时”）。比事先估的便宜得多。**主人 2026-11-22 定了**：对账方式按场景拆放在 3d 之后（计划文件加了 3e），笔数和金额那一类提前到 3c（计划文件 3c 加了一项；涉及资金，Fable 写，动手前在设计文档里补一节）。ADR 等 3e 写设计时一起写。**`recon-daily` 主人 2026-11-22 定了改成全量**：清单里去掉了 `-since 26h`，加了 `ARCHIVE_DSN`（全量要读冷库，否则已归档的订单全报成差异；和量耗时那次 `loadtest.sh recon` 用的是同一个冷库），`make recon-cron` 已下发。16:05 UTC 手工触发过一次（`kubectl create job --from=cronjob/recon-daily`，看完已删）：1 分 57 秒，1474.9 万单，差异 0，订单一侧多出的一条语句是读冷库的，说明 `ARCHIVE_DSN` 生效。到点自己跑的第一次是 21:30 UTC，还没有看过。提交 `131024f` 已推送；`recon-totals` 已换成 `recon:131024f`。
- **这之后做的三件事，都在工作区里没有提交**（2026-11-22，14:20 UTC 前后；细节在设计第 8 节最后三小节）：
  1. **钱的笔数和金额核对做完并上线**（主人定的提前到 3c；Fable 写）：`services/recon/internal/totals/`、`services/recon/cmd/recon/{main,watch}.go`、`services/recon/go.{mod,sum}`、`deploy/cluster/recon/totals.yaml`、`services.sh recon-totals`、Makefile 目标。recon 模块 lint 0 issues、`make test MODULES=$PWD/services/recon` 通过。集群上 Deployment `recon-totals` 在 k3s-server 上跑着，镜像是工作区的 tag（`daac679`…带 `-dirty-` 的那种，**提交后再 `make recon-totals` 换干净的**）；第一批读数两边 1620 笔、金额相等、差异 0。计划文件里这一项已勾。
  2. **SLO 规则的单元测试和下发**（Sonnet 子 Agent 写测试，主会话验收；worktree 和分支已删）：`deploy/cluster/monitoring/rules/tests/`、`scripts/{rules-test,install-bin}.sh`、Makefile 的 `promtool`、`yq`、`rules-test`、CI 的 `rules` job、`monitoring.sh rules`、`versions.md`。`make rules-test` 23 个用例通过。测试查出规则的一个问题（通知里的 `$value` 是长窗口的比例），主会话把 `slo.yaml` 里 12 条告警的短窗口挪到了最左边并改了对应的断言。**规则已下发到集群**（`oppenheimer-slo`，40 条求值正常，没有告警在响）。**CI 的 `rules` job 没有实际跑过，推送之后第一次跑要留意。**
  3. **探针 90 分钟的基线量了**：下单 p99 10 毫秒、落库 p99 85 毫秒、支付处理 p99 0.2 秒，5399 个周期全部 ok。支付那条 SLO 的阈值保持 5 秒，等有负载时再量。
- **没有跑完整的 `make lint test`**：这一批只动了 recon 模块的 Go 代码（单独跑过）和脚本、清单（`shellcheck` 过了）。提交前跑一次完整的。
- **3c 还剩的**（设计第 6 节的第 4 到 9 步）：其余的告警规则（业务、网格、中间件、节点、监控自己；第 4 节的清单，加上探针、`recon` 的 CronJob、`recon.totals` 的几条）→ 筛 chart 自带的 149 条 → Alertmanager 的路由和抑制、webhook 接收端、Telegram（主人说 token 和会话号准备好了，约定写在仓库根目录的 `.env` 里，变量名 `TELEGRAM_BOT_TOKEN`、`TELEGRAM_CHAT_ID`；**开工前先检查这个文件在不在、两个变量有没有，不要读出它们的值**）→ runbook → 告警质量检查（动手前逐条登记故障注入）→ SLO 的大盘、ADR、阶段笔记、面试考点。计划文件 3c 里没勾的五项对应这些。
- **已提交并推送**（主人要求）：`977ab7b`（探针）、`3648ad9`（CronJob 和探针的清单）、`e3c3ca7`（文档）。PR #17 的 CI 派给了 `ci-watcher`。探针换成了干净的镜像 `chaos:e3c3ca7`（第一次构建拉 Docker Hub 的令牌失败，重试成功）。这之后对文档的修改（全量对账的耗时）没有提交。
- **PR #17 上 `e3c3ca7` 的 CI 29 项全部通过**（`ci-watcher` 报的，核对过提交）：`test (./services/order)` 4 分 19 秒，`test (./services/recon)` 1 分 58 秒，e2e 3 分 41 秒，`test (./test/load)` 3 分 24 秒（加了探针的测试之后的第一个数，以后拿它比）。**`test (./pkg)` 3 分 44 秒，上一次（`daac679`）是 2 分 42 秒**：这两次之间 `pkg/` 没有改动，像是运行机的波动，没有查；下一次 CI 再看这一项，还慢就查。
- **探针的程序做完了**（`load probe`，Opus 子 Agent 写，主会话读过核心逻辑，已拿进工作区，worktree 和分支已删；`test/load` lint 0 issues、`make test MODULES=$PWD/test/load` 通过）。用法、指标、对告警的要求在设计第 8 节“第 2 步的另一半”。**探针的集群清单（`deploy/cluster/probe/`、`services.sh probe`、Makefile 目标）派给了 Sonnet 子 Agent，直接在工作区里改、不许碰集群，还没回来。** 回来之后：读 diff → `loadtest.sh skus 9001 10` 建探针专用的 SKU（先确认建出来的限购是 0）→ 构建 chaos 镜像 → 装探针 → 看它的指标和六个服务的 SLI 有没有数 → 一两小时后量支付处理时延的基线。
- **探针上线了（12:24 UTC）**：清单和 `make probe` 做完（Sonnet 子 Agent，主会话读过并改了镜像 tag 的默认值）；建了 SKU 9001 到 9010；集群上 `oppenheimer` 命名空间里 Deployment `probe` 1 个副本在 k3s-server 上，镜像 `chaos:daac679-dirty-20261122122350`（**提交之后再 `make probe` 换成干净的 tag**）。头几分钟每分钟 60 个周期全部 ok，四条 SLI 都有数了。计划文件里“补指标”“常驻探针”两项已勾。细节在设计第 8 节“探针上线”。**从现在起集群上一直有每秒约 1 单的流量，每天约 8.6 万单留在库里；正式压测前 `services.sh probe-stop` 和 `recon-cron-suspend`。**
- **`recon-hourly` 13:07 UTC 第一次自己跑成功**：983 单，18 秒，差异 0；计划文件里 CronJob 那一项已勾。订单对商家索引那一对增量时固定要 15 秒上下（推断是那一侧按订单号取范围走不了索引，没有看执行计划），见设计第 8 节。**还要看的**：探针跑满一两个小时后（14:30 UTC 之后）量支付处理时延和落库的基线，定支付那条 SLO 的阈值（现在暂定 5 秒，头几分钟的 p50 是 0.08 秒）。
- 注意：子 Agent 的 worktree 是从 `main` 的合并提交开的，快进不到 `phase-3c`（两边内容相同但不是祖先关系），它们各自做了一次普通合并；收回时只 `cherry-pick` 它自己的那个提交。以后派任务时直接写“`git merge origin/phase-3c`”，不要写 `--ff-only`。
- **Telegram**：和主人约定 token 和会话号写在仓库根目录的 `.env` 里（已被 `.gitignore` 排除），变量名 `TELEGRAM_BOT_TOKEN`、`TELEGRAM_CHAT_ID`，主人自己写。**还没有脚本读它**，做 Alertmanager 那一步时让 `monitoring.sh` 读出来生成 Secret；主会话不要去读这个文件的内容，只检查两个变量在不在。
- **下一步**：
  1. （已做）验收、合并子 Agent 的两个提交，`make lint test`。
  2. （已做）提交、推送、`make deploy`。这一段之后对本文件的修改（记录部署和冒烟）没有提交，下次提交时带上。
  3. （已做，未提交）探针和 `recon` 的 CronJob。工作区里没提交的：`Makefile`、`scripts/cluster/services.sh`、`deploy/cluster/{recon,probe}/`、`test/load/`（探针的程序）、`docs/`。等主人说提交；提交、推送之后派 `ci-watcher`，再 `make probe` 换干净的 tag。
  4. 第 3 步的后一半：`promtool` 的单元测试和 CI（Sonnet，主会话给用例表；`promtool` 的版本跟 Prometheus 3.13.5，登记到 `versions.md`）；`monitoring.sh` 加 `rules` 子命令，下发规则。
  5. 之后按设计第 6 节的第 4 到 9 步。

### 3c 设计写完，等主人确认（2026-11-22 晚；在上一段之前，主人已确认）

- **分支 `phase-3c` 开了**（从 `phase-3b` 开出，带着那个只改本文件的提交），**没有推送，没有提交**：新增 `docs/design/3c-slo-alerting.md`，改了本文件。
- **3c 的设计文档写完了：`docs/design/3c-slo-alerting.md`，状态是“等主人确认”**。没有碰集群（只读了 Prometheus 和 Alertmanager 的现状），没有改代码。计划文件 3c 的条目一个都没有勾。
- **要主人定的十项在设计第 2 节**，每项有建议。最要紧的四项：
  1. 平时没有流量，SLO 拿什么算（建议在集群里常驻一个每秒约 1 单的探针）。
  2. 可用性里 429 算什么（建议算好的，背压另配业务告警）。
  3. 目标和窗口（建议 7 天窗口；下单可用性 99.9%，99% 的受理在 200 毫秒内，99% 的落库在 10 秒内）。
  4. 通知发给主人用什么渠道（要主人给：Telegram、Slack、ntfy、Bark、邮件，或者先不要）。
  其余：补四个缺的指标（要改 order、`pkg/outbox`、`pkg/kafkax`）、`recon` 做成 CronJob、CDC 心跳不做、规则手写、自带的 149 条怎么筛和 runbook 的范围。
- **查到的现状**（设计第 1 节）：告警规则 149 条全是 chart 自带的；Alertmanager 只有 `null` 接收器；在响的有 `KubeCPUOvercommit`、`KubeMemoryOvercommit`（3b 把 requests 写满之后的预期结果）和两条 `KubeJobFailed`（11 月 18 日两个失败的手工备份 Job：`mysql-order-0-backup-manual-261118173325`、`mysql-order-1-backup-manual-261118173344`，在命名空间 `mysql`，**没有删，删之前要主人确认**）。
- **指标清单是 Haiku 子 Agent 检索的**，设计里用到的主会话核对过一部分（gateway 的 HTTP 直方图的标签和桶、`order_admissions_total` 的结局、两个 `.delay` 直方图的桶、`payment.result.delay` 量的是什么、`recon` 是一次性命令且集群里没有它的 CronJob）。没有核对的：边车裁剪后的溢出指标名、`inventory_oversell_detected_total` 在 Prometheus 里的名字（它还没有出现过序列）、kube-state-metrics 有没有容器退出码的指标、支付事件里有没有“出结果的时刻”。
- **下一步**：主人答复设计第 2 节的十项 → 把设计的状态改成已确认、需要时改计划文件并记计划变更（CDC 心跳不做的话要记）→ 按设计第 6 节的顺序动工，第 1 步是补指标（order 的那个由 Fable 写）。
- 集群没有变：状态同下一段。

### 3b 收尾：集群恢复成平时的配置（2026-11-22 傍晚；在上一段之前）

主人 2026-11-22 答复：1 亿数据量不做；提交并推送；写阶段笔记；把集群恢复成平时的配置；3c 在新会话开始。

- **容量结论和 `GOALS.md` 已提交并推送**（`746d9fe`，连同之前只在本地的几个提交）。CI 的结果见本段最后；没有那一条就是还没看。
- **集群恢复成平时的配置了**：
  - `make deploy`（不带 `LOADTEST=1`），六个服务的镜像是干净的 tag **`746d9fe`**（日志在 `tmp/deploy-restore-746d9fe.log`）。gateway 的 `RATE_LIMIT_IP=off` 已经不在（Deployment 的环境变量和 ConfigMap 里都查不到），**按地址的限流恢复成默认的 200 请求/秒**。只核对了配置，没有发流量验证它在拦。
  - `loadtest.sh unpin gateway order`：KEDA 的暂停注解去掉了，两个 ScaledObject `PAUSED=False`，HPA 重建出来是下限 2、上限 2、阈值 450 和 200、CPU 70%。
  - 核对过：五个服务各 2 个副本、两个节点各一个，cdc 1 个；没有不正常的 Pod；入口的两个 Envoy 在 agent-1、agent-2 各一个；Deployment 上没有 `OTEL_TRACES_SAMPLER*`、`ORDER_READ_MODE`；经入口访问 `/healthz` 返回 200。
  - **以后再压测**：从一台机器发压超过 200 请求/秒会被按地址的限流挡住，要先 `LOADTEST=1 make deploy` 再 `loadtest.sh pin gateway=2 order=2`。压测 SKU 下一批从 7599 起。
  - 没有恢复、也不打算恢复的：库里压测留下的约 470 万单（设计定的不删）；binlog 1 天、Kafka 24 小时的保留期（这是新的平时配置）。
- **3b 的阶段笔记写了：`docs/notes/3b-load-testing.md`**（541 行，按主题组织，结构同 3a 的笔记；Sonnet 子 Agent 写的，主会话验收）。验收读了第 0 到 3 节和第 10 到 12 节，**第 4 到 9 节和第 13 节没有逐行读**，主人读的时候发现和记录对不上的以记录为准。它补的“通用知识”段落面试前要对照官方文档（它自己最没把握的：`innodb_flush_log_at_trx_commit=0` 丢什么、`VirtualService` 第一条匹配生效、B+ 树层数、cgroup 里页缓存和匿名内存的回收）。有一条考点只写了一半（时限按路径定的同步那一半，材料里没有展开）。
- **写笔记时查出容量结论里的几处错，已改**（笔记第 12 节有清单）：`mysql-main` 没有从库（原来写成一主一从，集群上核对过只有一个 Pod）；“一单是入口上 5.4 个请求”是加退避之前的数，改成 4 到 5 个；受理 p99 的“94 到 191”分开写成同样配置的 94 到 158 和调完 requests 之后的 188、191；“换盘到约 240 单/秒”补了限定（那一次下游欠着活，240 是上界）；补了第 1 轮 608 个受理了没落库的订单。round-9 结论一节的“12 MiB”改成约 20。`GOALS.md` 不受影响。没有改的一处：设计第 6 节备份表的表头“从从库读”对 `mysql-main` 不适用。
- **PR #16 上 `746d9fe` 的 CI 29 项全部通过**（核对过这次运行的提交号）：`test (./services/order)` 3 分 19 秒、e2e 4 分 01 秒，都在以往的范围内；`test (./services/recon)` 1 分 50 秒，上次 9 分 20 秒的反常没有再出现，不用查了。
- **PR #16 上 `0db0499`（阶段笔记等，只改文档）的 CI：第一次失败 1 项，重跑那一个 job 后 29 项全部通过**（`test (./services/order)` 重跑 4 分 25 秒）。失败的是 `services/order/internal/service` 的 `TestLandConcurrentRedeliveredShuffled`：落库时认领闸门占用的 Redis 调用超过 `gateTimeout`（300 毫秒，`protect.go`），连续几次之后 `stock-gate` 的熔断器打开，后面的调用被直接拒绝；这条测试有意不重试，handler 返回任何错误都判失败（`land_concurrent_test.go` 里 `deliverConcurrently` 的注释）。日志里没有数据竞争，没有状态或库存的断言失败，全是超时和它引出的熔断；同样的代码在 `746d9fe` 上通过。**判断是 CI 机器上 Redis 那一下慢了，不是并发缺陷；这是从日志推断的，没有在本机复现，也没有查 Redis 为什么会慢到 300 毫秒。** **主人 2026-11-22 定：暂时不改这条测试。** 再出现时把这次的记录一起拿给主人看（改法的候选：给这组并发落库测试放宽闸门的时限，和 `TestRateLimitCreateOrder` 那次同一类处理）。
- **PR #16（`phase-3b`）已合并**（2026-11-22，主人要求；合并前从 draft 标成了 ready，合并提交 **`0142f02`**）。合并前 `9fb4343` 的 CI 29 项全部通过（`test (./services/order)` 4 分 14 秒、e2e 3 分 33 秒）；`main` 上 `0142f02` 的 CI 27 项全部通过（`test (./services/order)` 3 分 03 秒、e2e 4 分 06 秒、`test (./services/recon)` 2 分 11 秒，都在以往的范围内）。`phase-3b` 分支没有删。合并之后 `phase-3b` 上还有一次只改本文件的提交（记录合并和这句话），不在 `main` 里：**3c 的分支从 `phase-3b` 开，或者从 `main` 开之后把这个提交带过去。**
- **3b 到此做完**：计划文件 3b 一节全部勾掉，3a 也早已做完；阶段 3 还剩 3c、3d。
- **下一步（新会话）**：3c（SLO 与告警）。先读计划文件的 3c 一节（3b 各轮留给它的告警已经写在条目里）和 `docs/notes/3b-capacity.md`（SLO 的目标值来自这里的基线），写设计文档给主人确认，再动工。

### 3b 容量结论（2026-11-22 下午，UTC 08:50 之后；在上一段之前）

只写了文档，没有碰集群，没有改代码和配置，没有跑压测。

- **容量结论写完了：`docs/notes/3b-capacity.md`，计划里已勾**（“三轮优化”那个总项也一起勾了：它下面挂着的几项都做完了）。它不产生新的数，把 round-0 到 round-9 的结果收成一处，每个数标了出处。要点：三个限制（盘的刷盘管写入，持续 125 到 150 单/秒；两个工作节点的 16 个 vCPU 管其余的，混合读写 1907 请求/秒，售罄的秒杀在 4000 到 8000 请求/秒之间；磁盘的保留期管持续几小时以上的负载）；“单副本能力”换成每个请求的 CPU 开销（按场景、按服务各一张表）；扩容系数 1.0 和 0.8；requests 和 HPA 现在的数和依据；再往上各条路能换来多少的估算（换盘或者减提交次数都只到约 240 单/秒，然后撞上 CPU）。
- **这份结论里新写的三个推算（都是从记录里的数算的，没有实测，文档里标明了）**：日志的保留期撑得住的持续速率约 165 单/秒；日志走平之后表数据和 `cdc.order.orders` 每天约 12 GiB，持续 125 单/秒大约能跑两到三天；混合读写在拐点上整个系统每个请求约 7 到 8 毫核·秒。
- **`docs/GOALS.md` 的“目标量级”改了**：加了“量到的”和“修正后的目标”两列；“5000 QPS 的秒杀”拆成到达（售罄之后 4000 请求/秒）和受理（持续 125 单/秒，过载由背压拒绝）两行，另加了混合读写一行（1500 请求/秒下 P99 < 100ms）。**修正后的目标值是我按量到的数拟的，主人没有逐条定过**，看了不合适直接改。
- **1 亿数据量：主人 2026-11-22 定了不做**（三份文档里的措辞已改）。当时的建议是不做（容量结论第 8 节）：现有的负载只碰索引的右边缘，量不出数据量的影响；agent-2 的盘放不下（按比例要 100 GB 以上，剩 89 GiB，还没算 binlog 和 `cdc.order.orders`），要先扩盘；灌数 5 小时，恢复按小时算。要做的话放到阶段 4 的备份恢复实验，前提是先加一个随机读历史订单的 k6 场景。
- 其余改动：设计 3b 的 3.6 节加了指向结论的一段；计划文件的“面试考点”加了容量结论和长稳两段。
- **都没有提交**（主人没有说）：`docs/notes/3b-capacity.md`（新）、`docs/GOALS.md`、`docs/design/3b-load-testing.md`、`docs/plan/phase-3-observability.md`、本文件。`phase-3b` 上从 `b24576e` 起的提交仍然只在本地，没有推送。
- 集群没有动过，状态同下一段“主人定了 B 之后”的长稳那一条：gateway、order 钉在 2，压测 SKU 下一批从 7599 起，按地址的限流仍然关着（`LOADTEST=1` 部署的），镜像仍是 `1228d9c-dirty-20261121084655`。会话开头核对过：Mac mini 的桌面登录着，三个节点 `Ready`。
- **下一步**：
  1. 主人看容量结论和 `GOALS.md`，定 1 亿做不做；说了再提交、推送（推送之后等 CI 派给 `ci-watcher`；上次 `test (./services/recon)` 9 分 20 秒反常，这次留意）。
  2. 3b 的收尾，设计第 4 节第 9 步里还剩的：**3b 的阶段笔记**（`docs/notes/3b-load-testing.md`，前几个阶段各有一份，按主题整理面试要讲的点；3b 现在只有十份按轮次的记录和这份容量结论）。文档任务，按分工派 Sonnet，主会话给提纲和验收。做不做、做多细先问主人。
  3. 3b 结束时把长期运行的那一套恢复成平时的配置：不带 `LOADTEST=1` 重新部署（打开按地址的限流，同时把镜像换成干净的 tag）；gateway、order 的暂停注解要不要去掉（HPA 上下限都是 2，去不去结果一样）。PR #16 从 draft 标成 ready 由主人定。
  4. 之后是 3c（SLO 与告警）。3b 各轮留给它的告警已经写在计划文件 3c 的条目里；动工前先写设计文档给主人确认。
  5. 一直留着没有查的小事列在容量结论第 9 节（gateway 的堆、`read_db_1` 的舱壁、有货的秒杀在更高速率下等）。

### 3b 定 requests 和弹性测试（2026-11-22，UTC 01:45 到 02:36；在上一段之前）

记录在 `docs/notes/3b-rounds/round-8-cpu-requests.md`。两轮对账通过（`recon -since 90m` 差异 0，三次 `verify` 都是 `PASS`）。

**主人定了 B 之后（2026-11-22 02:50 到 03:48 UTC，这几条最新；再往下的条目是定之前写的，数以这里为准）**

- **主人 2026-11-22 答复：中间件的 requests 按 B 改，提交后跑长稳。**
- **B 按原数做不了，统一乘了 0.9**（记录 6.1 节）：按原数调完两个节点各剩 0.7 到 0.9 核，gateway、order 滚动时多出来的 Pod 放不下。现在生效的数：gateway 700m 加边车 400m，order 700m 加 250m，inventory 450m 加 250m，payment、orderindex、cdc 150m；入口 Envoy 700m，mysql-main 550m，订单库 400m，Kafka 400m，redis-cache 100m，redis-gate 没动（100m）。节点合计 5.75、6.10 核。
- **HPA 上限收到 2**（两个 values 的 `maxReplicas`，和下限相同，伸缩实际关着）；`perReplica` 重算成 gateway 450、order 200（不起作用）；order 连接池保持 40。
- **重跑阶梯（`q3`，对账通过）**：入口过载时的误拒（2384 档每秒 342 个到 2.7 个）和限流 Redis 的超时回到了改之前；服务端看到的 MySQL 查询 p99 没有回来，拐点以上落库延迟更长（2384 档开始欠账，由背压接住）；拐点仍是 1907。每组一次，后两项的原因没有验证（记录 6.2 节）。
- **Strimzi 的 operator 从 2026-11-20 16:26 UTC 起调和失败**（宿主机重启后启动时集群域名探测错了，记录 6.3 节），broker 一直正常所以没人发现。已修：`scripts/cluster/middleware.sh` 里把 `KUBERNETES_SERVICE_DNS_DOMAIN=cluster.local` 写死。3c 的基础设施告警里加了 `Kafka` 资源的 `Ready`。
- **入口的 Envoy 滚动时会落到 k3s-server 上**，这次手工删掉让它回到 agent-1。以后滚入口都要看一眼。
- 改动的文件：`deploy/cluster/services/*.yaml`、`deploy/cluster/services/e2e/*.yaml`、`deploy/cluster/{mysql,kafka,redis,envoy-gateway}/` 里的 requests、`scripts/cluster/middleware.sh`、文档。**已提交**（主人要求；没有推送，主人没有说）。
- 压测 SKU 用到 7587（长稳另算，见下一条）；集群上 gateway、order 仍然钉在 2。
- **长稳跑完了（2026-11-22 03:55 到 07:55 UTC 发压，08:45 UTC 对账完），记录在 `docs/notes/3b-rounds/round-9-soak.md`，计划里已勾**：普通下单 125 单/秒 4 小时（`soak1-order`，SKU 7588 预热、7589 到 7598），180 万单，`recon -since 330m` 差异 0，`verify` 两次 `PASS`。采样已恢复（核对过六个 Deployment 上没有 `OTEL_TRACES_SAMPLER*`），gateway、order 仍钉在 2，压测 SKU 下一批从 7599 起。
  - 平的：下单 p99（45 毫秒）、落库延迟 p99（2.4 到 3.2 秒）、goroutine 数、连接数、CPU；没有容器重启。闸门 Redis 的键数在 1 小时（幂等键的 TTL）处走平，内存 184 MiB，是上限的 24%，推算能撑持续约 500 单/秒。`backlogged` 是 0，2500 的队列上限没有碰到，“按分片各设上限”在这个负载下没有依据去做。
  - **磁盘没有稳态（记录第 3 节），主人同日定了按建议改，已生效（记录 3.4 节）**。原来 binlog 保留 30 天、Kafka 保留 7 天不限大小，都是没配过的默认值，125 单/秒下 agent-2 每小时少 4.4 GiB。现在：binlog 1 天（`deploy/charts/mysql/values.yaml` 的 `mysqld.binlogExpireLogsSeconds`，五个实例已滚动）；Kafka 24 小时、每分区 4 GiB 兜底、段 256 MiB（`deploy/cluster/kafka/kafka.yaml`，三个 broker 已滚动）；死信 topic 7 天（`middleware.sh kafka` 里的 `kafka_dlq_retention`，**新集群上要等服务建出 topic 之后再跑一次**）。代价：CDC 停超过 1 天要重做全量；一个分区被挡住超过 24 小时会丢消息，3c 的告警里已加。`cdc.order.orders` 是压缩保留的，不受保留期管，大小跟着订单数走。**改完之后没有再跑长稳验证磁盘走平。** 盘现在剩 agent-1 104、agent-2 89、k3s-server 22 GiB。
  - 没查原因的三件小事：gateway 的堆每个 Pod 每小时涨约 1.5 MiB、4 小时没有走平（记录 2.1 节有怎么分清）；闸门 Redis 的内存在键数走平后还涨了约 20 MiB；读的舱壁每分钟拒十来次，几乎全在 `read_db_1`（4 小时 2349 次对 `read_db_0` 的 26 次），两个分片的读量和查询 p99 一样。
  - `verify` 对 180 万单第一次 30 分钟没跑完、输出为空，推断是一次性 Pod 1 Gi 的内存上限不够；`scripts/cluster/loadtest.sh` 加了 `VERIFY_TIMEOUT`、`LOADTEST_MEMORY_LIMIT` 两个开关，6 Gi 时一分半跑完。Pod 里的进程被杀之后 `phase.sh` 不会报出来，没有改。
  - 和设计 3.5 不一致：跑的是普通下单，不是混合读写的五成；没有量文件描述符（没有指标）、MySQL 的 undo 和临时表。
  - **都已提交**（主人要求；没有推送，主人没有说，`phase-3b` 上从 `b24576e` 起的提交都在本地）。没有改 Go 代码，没有跑 `make lint test`。
  - **下一步（主人说在新会话里做）**：容量结论（设计 3.6；“各服务的单副本能力”改成“每个请求的 CPU 开销”加“水平扩容和读从库在这套硬件上都没有收益”，再加这一轮的“持续负载的上限由磁盘的保留期决定”）→ `GOALS.md` 的修正。

- **改了什么**（第一步，只调服务；已被上面的数取代）：
  1. 六个服务的 CPU requests 按“2 个副本、混合读写 1526 请求/秒”的实测用量定：gateway 750m 加边车 450m，order 750m 加 300m，inventory 500m 加 300m，payment、orderindex、cdc 各 150m（`deploy/cluster/services/*.yaml`）。两个节点的 requests 从 2.75、2.65 核到 5.10、4.95 核（可分配各 7.5），放得下 3 个副本加滚动多出的 1 个。
  2. `perReplica`：gateway 180 改 500，order 100 改 220（算法在两个 values 的注释里）。
  3. 集群 e2e 那一套保持改之前的 requests（`deploy/cluster/services/e2e/{common,gateway,order,inventory}.yaml`）：照抄的话和长期运行的一套一起放不下。只用 `services.sh e2e-render` 渲染核对过，**没有实际跑 `make cluster-e2e`**。
- **坑**：边车的资源注解要 `cpu`、`memory`、`memoryLimit` 三项一起写，只写一项时 Istio 不再用全局默认值。
- **弹性测试通过（`q2-elastic`）**：解除暂停，600 升到 1221 请求/秒，gateway、order 各从 2 扩到 3（CPU 触发的，请求速率的 1 分钟窗口还没爬到），690972 个请求 0 失败，新 Pod 10 秒就绪、之后 30 秒内接到三分之一的流量，落库不受影响，停压 5 分 14 秒后缩回 2。3 个副本比 2 个多用一成多 CPU，延迟不变。
- **重跑阶梯（`q1`，对照 `r1-primary`）**：拐点仍是 1907，1221、1526 两档没有差别。**1907 和 2384 两档变差了**：gateway 问限流 Redis 的超时每秒 2.2 到 11.8 次，服务端看到的 MySQL、Redis 调用的 p99 是原来的两到三倍，2384 档入口拒掉的从每秒 38 个到 342 个。原因（按数的方向推断，没有直接读 cgroup）：requests 是节点满了之后分 CPU 的权重，只调了服务，中间件的相对权重少了将近一半。每组只跑了一次。
- **要主人定的一件事**（记录第 5 节）：中间件的 requests 要不要也按实测调（入口 Envoy 750m、mysql-main 600m、订单库 450m、Kafka 450m、redis-cache 150m、redis-gate 150m）。建议调；代价是每个节点到 6.8 核上下，第三个副本放不下，HPA 上限要收到 2，还要滚动 MySQL、Kafka、Redis、入口，改完重跑一次阶梯。
- **集群现在的状态**（02:40 UTC 前后）：五个服务 2 个副本，gateway、order 钉回了 2（KEDA 暂停注解）；镜像仍是 `1228d9c-dirty-20261121084655`；压测 SKU 用到 7566，**下一批从 7567 起**；库里压测留下的订单约 285 万单（估的：这次三轮约 18 万单）；按地址的限流仍然关着；盘在慢的一档。
- **本机**：compose 仍然是停着的。没有改 Go 代码，**没有跑 `make lint test`**。原始数据在 `tmp/k6/{q1-warm,q1-mixed-ladder,q2-elastic}/`、`tmp/scale/q-rounds.log`、`tmp/scale/q2-elastic-watch.log`（不入库）。跑这两轮的脚本在会话的临时目录里（没有入库），流程：`loadtest.sh skus` → 预热 → 阶梯 → `loadtest-round.sh stages` → `loadtest.sh unpin gateway order` → 等 150 秒 → `STAGES=600:180s,1221:10s,1221:470s` 的 mixed → 等缩回 2 → `pin` → `wait-quiet`、`recon`、`verify`。
- **下一步**：
  1. 主人答复中间件的 requests。选 B 的话：改 `deploy/cluster/{mysql,kafka,redis,envoy-gateway}/` 里的 requests，逐个滚动（MySQL 先从库后主库，滚主库前确认复制正常），HPA 上限收到 2 并重算 order 的连接池注释，重跑一次阶梯对照 `r1-primary` 和 `q1`。
  2. 长稳（慢档上持续 125 单/秒几小时，同时看闸门内存是平的、2500 的队列上限够不够）。这个负载下节点只用到六成，不受第 1 项影响，可以先做；它要占住发压机几小时，开跑前跟主人说一声。
  3. 容量结论（“各服务的单副本能力”改成“每个请求的 CPU 开销”加“水平扩容和读从库在这套硬件上都没有收益”）。
  4. 下面“3b 空表对照”的“下一步”第 3、4 项仍然留着。

### 3b 三件事和读模式对比（2026-11-22 上午，UTC 是 11 月 21 日 23:10 起，在上一段之前）

主人 2026-11-22 答复：下面“3b 扩容系数”里的三件事都按建议做，先提交并推送。记录在 `docs/notes/3b-rounds/round-6-scaling-factor.md` 的第 7 节。

- **提交并推送了扩容系数的记录**（`c34163c`）。**PR #16 上 `c34163c` 的 CI 29 项全部通过**（`test (./services/order)` 3 分 39 秒、e2e 3 分 35 秒，都在以往的范围内）。
- **三件事改了什么**（都已部署到集群，已提交并推送）：
  1. 入口：`maxConnections` 1024 改 4096，加 `connectionIdleTimeout: 30s`，并把在途请求的上限 `maxParallelRequests: 1024` 写明（数没变）。chart 的 `templates/backendtrafficpolicy.yaml`、`values.yaml`，`deploy/cluster/services/gateway.yaml`。
  2. gateway、order 的 HPA 上限 4 改 3（两个 values 的 `autoscaling.maxReplicas`）。弹性测试改成“验证扩容期间不丢请求”（计划文件、计划变更记录已改），测试本身还没做。
  3. order 的连接池每库 50 改 40（`deploy/cluster/services/order.yaml`，算法写在注释里：(HPA 上限 + 1) × 连接池 ≤ `max_connections` − 40）。order 启动时会多一条落库并发超过连接池的警告，是预期的。
- **验证（`s2-r4`，4 个副本重跑同一个阶梯，对账通过）**：1907 档入口的 `503 UO` 从每秒 65 个到 0，连接不再被闲着的占满。**4 个副本的拐点没有变，还是 1526**：原来被拒掉的请求放进了已经满的节点，下单 p99 从 198 毫秒到 412 毫秒。
- **顺带纠正**：`maxParallelRequests` 对 HTTP/1.1 的上游也起作用（第 3 轮在模板里写的“只对 HTTP/2 起作用”是错的，已改）。2384 档入口的在途请求停在 1024，拒绝的原因是它，不是连接也不是排队。`loadtest-round.sh` 里“答 503 UO”那一项只数排队溢出，看入口拒了多少以按状态码的那一项为准。
- **没有重建镜像**：三件事只动配置，用现有的 tag `1228d9c-dirty-20261121084655` 对 gateway、order 重新 `helm upgrade`（`LOADTEST=1 IMAGE_TAG=<tag> scripts/cluster/services.sh deploy gateway order`）。
- **连接池 40 验证过了**：2 个副本上同一个阶梯（`r1-primary` 对昨天的 `s1-r2`），连接池的等待都是每秒零点几次，和 50 时没有差别，落库速率相同。
- **读从库的模式对比做完了**（`docs/notes/3b-rounds/round-7-read-modes.md`，三组对账通过：`recon` 差异 0，六轮 `verify` 都是 `PASS`）：`window`、`gtid` 都把七成多的订单读分到从库，主库 CPU 降三分之一，但降下来的原样出现在从库上（主从在同一批节点，到顶的是节点），**三组的拐点都是 1907，读从库在这套硬件上不加能力**；`gtid` 比 `window` 多用 14% 的数据库 CPU、order 多 0.14 核。**集群上的默认值没有改，仍是 `primary`**（我的判断，主人没有专门定过；想改的话在 `deploy/cluster/services/order.yaml` 加 `ORDER_READ_MODE`）。没有量的：读到旧数据的次数；每组只跑了一次。
- **切读模式的办法**（临时的，不入库）：`kubectl -n oppenheimer set env deploy/order ORDER_READ_MODE=window`，恢复用 `ORDER_READ_MODE-`。**已经恢复**（核对过 Deployment 上没有这个变量）。
- **这一段的改动已提交并推送到 `phase-3b`**（2026-11-22，主人要求；两个提交：配置、文档；CI 的结果见本段最后一条，没有那一条就是还没看）：`deploy/charts/service/{templates/backendtrafficpolicy.yaml,values.yaml}`、`deploy/cluster/services/{gateway,order}.yaml`、`docs/notes/3b-rounds/round-6-scaling-factor.md`（第 6 到 8 节）、`docs/notes/3b-rounds/round-7-read-modes.md`（新）、`docs/plan/phase-3-observability.md`、本文件。没有改 Go 代码，**没有跑 `make lint test`**（本机 compose 是停着的）；两个 chart 用 `helm template` 渲染核对过，并且已经在集群上生效。
- **PR #16 上 `7e24af7` 的 CI 29 项全部通过**（`test (./services/order)` 3 分 07 秒、e2e 4 分 10 秒，都在以往的范围内）。有一项耗时反常：`test (./services/recon)` 9 分 20 秒，上一次（`c34163c`）是 1 分 41 秒。这次没有改 Go 代码，它的每一步都慢（起依赖 2 分钟、构建 1 分半、测试 4 分 50 秒），像是那台运行机慢或者没有命中缓存，**没有查原因**；下一次 CI 再看这一项，还慢就要查。记录这个结果的这一条在工作区里，没有提交，下次提交时带上。
- **集群现在的状态**（2026-11-22 01:40 UTC 前后，预检通过）：五个服务 2 个副本（gateway、order 仍然用 KEDA 的暂停注解钉在 2，ScaledObject 的上限已经是 3），每个服务两个节点各一个；镜像仍是 `1228d9c-dirty-20261121084655`；压测 SKU 用到 7525，**下一批从 7526 起**；库里压测留下的订单约 267 万单；按地址的限流仍然关着；k3s-server 的系统盘剩 26 GB（下限 15）；盘在慢的一档。
- **本机**：compose 仍然是停着的。原始数据在 `tmp/k6/{s2-r4,r1-primary,r1-window,r1-gtid}-*`、`tmp/scale/`（不入库）。跑这几轮的两个脚本在会话的临时目录里（没有入库），流程和“3b 扩容系数”一段写的相同，只是没有跑普通下单 100 单/秒那一轮、没有抓 pprof。
- **下一步**：
  1. （已做）提交并推送。主人说在新会话继续，从第 2 项开始。
  2. 定 CPU requests（扩容系数的记录第 5 节有建议值：gateway 0.7 核、order 0.75 核、inventory 0.5 核上下，边车另算）。改之前先算两个节点放不放得下 3 个副本（HPA 的上限）加滚动时多出的 1 个。
  3. 重定 HPA 的阈值（`perReplica` 是按旧的 requests 算的），做弹性测试：解除暂停，加压让 gateway、order 从 2 扩到 3，看扩容期间有没有 5xx、新 Pod 多久接到流量；不追求扩完变快。
  4. 长稳（慢档上持续 125 单/秒几小时，同时看闸门内存是平的、2500 的队列上限够不够）→ 容量结论（“各服务的单副本能力”改成“每个请求的 CPU 开销”加“水平扩容和读从库在这套硬件上都没有收益”）。
  5. 下面“3b 空表对照”的“下一步”第 3、4 项仍然留着。

### 3b 扩容系数（2026-11-22 凌晨，UTC 是 11 月 21 日 16:33 到 18:35，在上一段之前）

记录在 `docs/notes/3b-rounds/round-6-scaling-factor.md`。三组都对账通过（`recon` 差异 0，九轮 `verify` 都是 `PASS`，384648 单全部落库）。

- **结论：节点不变时加副本不加能力，还要倒贴 CPU。** gateway、order、inventory、payment、orderindex 一起钉到 1、2、4 个副本（配置一律不动），各跑普通下单 100 单/秒 5 分钟和混合读写的阶梯（1221、1526、1907、2384，每档 3 分钟）。拐点：1 个副本 1907、2 个副本 1907（和 `m1` 一致）、4 个副本 1526。1907 这一档业务容器加边车的 CPU 是 5.1、7.6、8.4 核。写入和副本数无关（落库都是 99 单/秒上下）。pprof 里多出来的 CPU 主要是调度和系统调用；“流量分散后批处理被摊薄”是我的推断，没有单独验证。
- **量不出来的**：单个服务的上限（先到顶的始终是节点，1 个副本的 order 只用 1.2 核）；加节点时的扩容系数（只有两个 agent）。计划里“各服务的单副本能力”这一条按原样的问法答不了，容量结论里要改成“每个请求的 CPU 开销”加“水平扩容在这套硬件上没有收益”。
- **新发现的缺陷（没有改）**：入口到 gateway 的 `maxConnections: 1024` 数的是连接不是在途请求。Envoy 的连接池按线程和上游主机分开，4 个 gateway 副本、1526 请求/秒时在途只有 82 个请求，连接已经占满 1024 条；1907 档入口开始答 `503 UO`（每秒 65 个），2384 档拒掉 29%。HPA 把 gateway 扩到 4 个时入口的能力反而下降。记录第 4.1 节。
- **另外两处随副本数漂的配置**（记录第 4.2、4.3 节）：边车的并发上限、连接池、读名额按 Pod 给，1 个副本时提前触发；order 连接池 50 乘 HPA 上限 4 等于订单库的 `max_connections` 200（这一轮没撞上，需求只到一百四十上下，但配置上没有保证）。
- **要主人定的三件事**（记录第 6 节；**主人 2026-11-22 答复都按建议做，已做，见上一段**）：
  1. 入口的 `maxConnections`：建议调大（比如 4096）并给上游连接加空闲超时，改完用 4 个副本重跑 1907 这一档确认 `503 UO` 归零。
  2. HPA 和弹性测试：建议 gateway、order 的 HPA 上限收到 2 或 3，弹性测试改成“验证扩容时不丢请求”，不追求扩完变快。
  3. order 的连接池和 MySQL 连接上限：把连接池按最大副本数倒推（50 改 40 上下），或者调高 `max_connections`。
- **没有改任何入库的代码和配置**，新增的只有这份记录和文档。跑这一轮用的脚本在会话的临时目录里（没有入库）：每组的流程是 `loadtest.sh pin 五个服务=N` → `loadtest.sh skus <起始> 31` → `loadtest-round.sh order`（预热、100 单/秒）→ `STAGES=1221:180s,1526:5s,1526:175s,1907:5s,1907:175s,2384:5s,2384:175s MAX_VUS=6000 PRE_VUS=600 PPROF_AT=420 loadtest-round.sh mixed` → `loadtest-round.sh stages <RUN_ID>` → `wait-quiet`、`recon -since 60m`、`verify`。要重跑照这个顺序敲。
- **集群现在的状态**：五个服务恢复成 2 个副本（gateway、order 仍然用 KEDA 的暂停注解钉在 2），预检通过（18:40 UTC 前后）。**orderindex 的两个 Pod 现在都在 k3s-agent-2 上**（从 4 缩回 2 时留下的，分散约束是 `ScheduleAnyway`）；下一轮开跑前删掉其中一个让它重新调度，或者滚动一次。镜像仍是 `1228d9c-dirty-20261121084655`；压测 SKU 用到 7441，**下一批从 7442 起**；库里压测留下的订单约 226 万单；按地址的限流仍然关着；k3s-server 的系统盘剩 30 GB；闸门 Redis 的主库没有查，上次是 `redis-gate-0`。
- **本机**：compose 仍然是停着的（没有改 Go 代码，没有跑 `make lint test`）。原始数据在 `tmp/k6/s1-r{1,2,4}-*`、`tmp/loadtest/s1-*`、`tmp/scale/`（不入库）。
- **这一段的改动没有提交**（等主人说）：`docs/notes/3b-rounds/round-6-scaling-factor.md`、`docs/plan/phase-3-observability.md`、本文件（含上一段没提交的那一条）。
- **写日期时注意**：Mac mini 的本地时间是 UTC+8，记录里的时刻都是 UTC。这一轮本地是 11 月 22 日凌晨，UTC 还是 11 月 21 日；用 `date -j -u -f` 换算时间戳时写错过一次日期。
- **下一步**：
  1. 主人答复上面三件事。答复之前可以先做不依赖它们的第 2 项。
  2. 读从库的模式对比（`ORDER_READ_MODE` 的 `window`、`gtid` 对 `primary`），在 2 个副本上，用混合读写 1907 这一档看节点 CPU 和订单主库的 CPU（1907 档订单主库各用 0.55 到 0.6 核，从库 0.2 核）。要改 order 的配置并滚动，先读 `services/order/cmd/order/main.go` 里这个变量的说明和 2c 的设计。
  3. 定 CPU requests（记录第 5 节有建议值）→ HPA 和弹性测试（按主人的答复）→ 长稳（慢档上持续 125 单/秒几小时）→ 容量结论。
  4. 下面“3b 空表对照”的“下一步”第 3、4 项仍然留着。

### 3b 空表对照（2026-11-21 晚，在上一段之前）

记录在 `docs/notes/3b-rounds/round-5-empty-table-comparison.md`。四组都对账通过（`recon` 差异 0，各轮 `verify` 是 `PASS`）。

- **结论：这套负载下数据量不是变量。** 同一套服务指到空库，下单 100 单/秒、混合读写 500 和 1907 请求/秒三轮，延迟、CPU、落库延迟都落在前后两次大库（`e1a`、`e1d`，另有半组 `e1c`）之间的波动里。原因：负载只碰索引的右边缘和一小块热数据（记录第 3 节）。没有量的：随机读旧数据、深翻页、1 亿单。之后的扩容系数、弹性、长稳都直接在大库上做。
- **做法和原来设想的两条路都不一样**（已记进“计划变更记录”）：不另起 `oppenheimer-e2e`，不清库恢复，把长期运行的那一套临时指到同一批实例上的 `e2e_*` 空库。新东西：`deploy/cluster/services/loadtest-empty/`；`scripts/cluster/loadtest.sh empty-up`、`empty-down`；`loadtest.sh` 的其余子命令按 order 实际连的库选对象（`conditions`、预检里有一行 `dataset:`）；预检的消费延迟一项认带前缀的延迟关单组；`e2e-lib.sh` 在长期运行的那一套占着 `e2e_*` 库时拒绝建库删库。**切之前、切回之前都要先 `wait-quiet`。**
- **现在已经切回大库，空库和带前缀的 topic、消费者组、Redis 键都清掉了**；cdc 从原来的位点接上了。预检通过（13:30 UTC 前后）。
- **顺带查清的旧问题**（记录第 4 节）：订单缓存几乎不命中、订单读全走主库都不是缺陷。缓存只存终态订单，压测查的都是刚下的单；读主库是 `ORDER_READ_MODE` 的默认值，读从库的两种模式的定量对比（2c 挪到阶段 3 的那一项）还没有做，读路径的瓶颈是 CPU，这一项现在值得排进来。
- **PR #15（`phase-3a`）已合并**（2026-11-21，主人要求；合并前从 draft 标成了 ready，合并提交 `a481ec9`，`main` 上的 CI 27 项全部通过，`test (./services/order)` 3 分 12 秒、e2e 3 分 54 秒）。`phase-3a` 分支没有删。PR #16（`phase-3b`）仍是 draft。
- **已提交并推送到 `phase-3b`**（2026-11-21，主人要求；`cc1435a`、`d2730bc`）。**PR #16 上 `d2730bc` 的 CI 29 项全部通过**（`test (./services/order)` 4 分 19 秒、e2e 4 分 13 秒，都在以往的范围内）。记录这个结果的这一条还在工作区里，没有提交，下次提交时带上。下面这一条是提交之前写的：
- **这一段的改动当时都没有提交**：`scripts/cluster/{loadtest,services,e2e-lib}.sh`、`deploy/cluster/services/loadtest-empty/`、`docs/` 下的记录、计划、设计、`hardware.md`、本文件（含上一段没提交的那一条）。三个脚本过了 `shellcheck -x`；没有改 Go 代码，**没有跑 `make lint test`**（k6 在跑的时候不能跑，跑完之后本机的 compose 是停着的）。
- **操作端的一个坑**（`docs/infra/hardware.md` 的 Mac mini 一节）：会话放在 zellij 里、ssh 断开重连之后，这个会话里的 `kubectl`、`helm` 报 `no route to host`（macOS 的本地网络权限），集群是好的。`e1c` 的第四轮因此没跑成。办法：在新的 ssh 会话里重开 zellij 和 Claude Code；或者把命令包一层 `ssh mini "zsh -lc 'export PATH=/opt/homebrew/bin:\$PATH; cd ~/Workspace/oppenheimer && ...'"`。**新会话开头先 `kubectl get nodes` 试一下。**
- **集群现在的状态**：六个服务的镜像仍是 `1228d9c-dirty-20261121084655`（今天滚动过两次，内容没变）；压测 SKU 用到 7348（7211 到 7256 只在空库里用过，正式库里没有这一段），**下一批从 7349 起**；库里压测留下的订单约 188 万单；按地址的限流仍然关着，gateway、order 仍然钉在 2 个副本；k3s-server 的系统盘剩 34 GB；闸门 Redis 的主库是 `redis-gate-0`。
- **本机**：`make down` 过，compose 是停着的，跑测试之前先 `make up`。原始数据在 `tmp/k6/e1{a,b,c,d}-*`、`tmp/loadtest/e1*`（不入库）。
- **下一步**：
  1. **扩容系数**（1、2、4 个副本，设计第 2 节第 9 项）。预计接近 1（CPU 在节点上到顶），要用数验证。先定量哪几个服务、用哪个场景（建议混合读写，在 1907 的八成左右，另加普通下单 100 单/秒），以及 1 个副本时分散约束、PDB 会不会挡住；`loadtest.sh pin` 现成。之后定 CPU requests、弹性测试、重定 HPA → 长稳 → 容量结论。
  2. 读从库的模式对比（`ORDER_READ_MODE` 的 `window`、`gtid` 对 `primary`）：建议排在扩容系数之后、长稳之前，用混合读写 1907 这一档看节点 CPU 和订单主库的 CPU。
  3. 下面“3b 混合读写和秒杀的阶梯”的“下一步”第 3 项里剩下的（有货的秒杀在更高速率下、`f1` 第一档落库延迟 27 秒、限流的 Redis 超时的告警）和第 4 项（队列上限按分片）仍然留着。
  4. 这一轮看到、没有查的：带前缀的新 topic 上 Kafka 多用 0.1 到 0.3 核；要量“数据量大了哪里会慢”得加一个随机读历史订单的 k6 场景。

### 3b 混合读写和秒杀的阶梯（2026-11-21，在上一段之前）

记录在 `docs/notes/3b-rounds/round-4-mixed-and-flash-ladders.md`。两轮都对账通过。

- **已提交并推送到 `phase-3b`**（2026-11-21，主人要求；`23379d7` 到 `f437ee8`）。**PR #16 上 `f437ee8` 的 CI 29 项全部通过**（`test (./services/order)` 3 分 52 秒、e2e 4 分 20 秒，都在以往的范围内）。记录这个结果的这一条还在工作区里，没有提交，下次提交时带上。

- **`m1-mixed-ladder`**（500 到 2384 请求/秒共 8 档，SKU 7144 到 7163）：拐点 1907 请求/秒（其中下单 148 单/秒），这一档各接口 p99 在 100 毫秒内、没有 5xx。到顶的是两个 agent 节点的 CPU（87%、95%），不是某个服务。2384 档的 503 是 gateway 到 inventory 的边车并发上限（每个 Pod 128）溢出，inventory 自己没有错。`recon` 差异 0，`verify` 是 `PASS`，140002 单全部落库。
- **`f1-flash-ladder`**（一个 SKU 7164，库存 8 万，500、1000、2000、4000 次迭代/秒）：零超卖。**库存给少了**，第二档开头就卖完，有货的只有 500 这一档：它已经把入口打满（`503 UO` 每秒 43 个，查订单的轮询占了请求的六成，落库延迟 p99 27 秒）。售罄之后 4000 请求/秒很轻，8000 请求/秒时入口 Envoy（单个 Pod，2.08 核）和 gateway 到顶。只落库不支付时落库到了 287 单/秒。热点限流每个 SKU 实际放行 1000 次/秒（限额 500 乘两个副本，桶在进程里）。`recon` 差异 0，`verify` 是 `PASS`，80000 单全部落库并关单，库存回到 80000。
- **这一段改的东西**：`test/k6/flash.js` 认 `STAGES`（不给时行为不变）；`scripts/cluster/loadtest.sh` 预检的系统盘下限默认值从 30 改成 15（主人 2026-11-21 同意）；Tempo 和 Loki 的数据清空过一次（主人同意；做法是把两个 StatefulSet 缩到 0、删掉 k3s-server 上 `/var/lib/rancher/k3s/storage/` 里两个卷目录的内容、再扩回 1），所以 **2026-11-21 10:10 UTC 之前的日志和链路都没有了**，指标（Prometheus）还在。
- **集群现在的状态**：六个服务的镜像 `1228d9c-dirty-20261121084655`（内容等于提交 `23379d7`，下次 `LOADTEST=1 make deploy` 换成干净的 tag）；压测 SKU 用到 7164，**下一批从 7165 起**；库里压测留下的订单约 166 万单；按地址的限流仍然关着，gateway、order 仍然钉在 2 个副本；k3s-server 的系统盘剩 35 GB（清完是 41，两轮用了 6）；闸门 Redis 的主库是 `redis-gate-0`。
- **本机**：改了 `deploy/envoy/*.yaml` 之后要 `docker restart oppenheimer-envoy-<服务>-1`。`shellcheck -x scripts/fault.sh` 在 Mac mini 上报 SC1124 一类的语法错误，`1228d9c` 的版本同样报，没有处理（可能是 shellcheck 版本的差别，CI 是通过的）。
- **下一步（主人 2026-11-21 定：空表对照在新会话里做）**：
  1. （已做，2026-11-21，见最上面“3b 空表对照”；下面是当时写的）**空表对照**（设计第 2 节第 10 项、第 4 节）。先定空表从哪来，记录第 4 节写了两条路：（a）用集群 e2e 那套环境（`oppenheimer-e2e` 命名空间，`scripts/cluster/services.sh e2e-up`，同一批 MySQL 实例上的 `e2e_*` 空库，Kafka、Redis 单独的前缀；`make cluster-bench` 是现成的例子），不动现有数据，局限是和大库共用缓冲池、redo 和盘，而且 `loadtest.sh preflight` 在这个命名空间存在时拒绝运行、k6 的 `BASE_URL`/`HOST_HEADER` 要指到 e2e 的入口，要先读脚本确认怎么接；（b）清库后从 `keep-seed-10m` 恢复，是破坏性操作，**要主人同意**，而且会丢掉压测留下的 166 万单。我的建议是先按（a）做，在记录里写明局限。对照跑什么：普通下单 100 单/秒（对 `b1-order`）和混合读写 500 请求/秒（对 `b1-mixed`），再加混合读写的 1907 档，看大表对读路径的 CPU 有没有影响。注意 `b1` 是 2026-11-18 的代码和快档的盘，直接比不公平：同一天在大库上各重跑一次当对照。
  2. 扩容系数（1、2、4 个副本）→ 定 CPU requests、弹性测试、重定 HPA → 长稳 → 容量结论。这两轮的结论是 CPU 在节点上到顶，扩容系数预计接近 1，要用数验证。
  3. 记录第 2.3、第 4 节留下的：有货的秒杀在更高速率下的表现（库存要给到 50 万以上）；`f1` 第一档落库延迟 27 秒的原因；订单缓存几乎不命中、订单读全部走主库是不是缺陷；限流的 Redis 在高负载下超时（3c 的告警）。
  4. 下面“3b 第 3 轮”的“下一步”第 4 项（队列上限按分片）仍然留着。

### 开发和操作搬到 Mac mini（2026-11-21，这一段最新，新会话先读这里）

主人定的：之后的任务远程连到 Mac mini 上做，MacBook 不再需要开着。规矩和要在仓库之外准备的东西写在 `docs/infra/hardware.md` 的“开发机和操作端（Mac mini）”一节，**先读它**。最要紧的一条：k6 在跑的时候这台机器上不跑测试、lint、构建镜像。

- **做完的**（从 MacBook 经 `ssh mini` 做的）：仓库 clone 到 `~/code/personal/Oppenheimer`（`phase-3b`）；Homebrew 装了 golangci-lint 2.15.0、helm 4.3.2、kubectl 1.37.2、mysql-client、shellcheck，go 升到 1.27.4、gh 升到 2.105.0（`~/go/bin` 里旧的 golangci-lint 2.12.2 改名成了 `golangci-lint.old-2.12.2`，`~/.zshrc` 加了 mysql-client 的 `PATH`）；OrbStack 启动并配了集群仓库的 `insecure-registries`（仓库的 `/v2/` 返回 200）；`~/.ssh/config` 的 `ubt` 改成 192.168.50.7（原来指向它自己，改之前的备份是 `~/.ssh/config.bak-20261121`），加了指向 `127.0.0.1` 的 `mini`；它的公钥加进了三台虚拟机（宿主机上本来就有），宿主机和虚拟机的主机密钥是从 MacBook 的 `known_hosts` 抄过去的；`ssh ubt`（免密 sudo）、三台虚拟机、`ssh mini` 都通；kubeconfig 用 `k3s.sh kubeconfig` 取好了；`make up` 起来了（干净的 2 库环境）；Claude Code 的全局 `CLAUDE.md`、`ci-watcher` 和 `quick-ops` 两个子 Agent、本项目的记忆复制过去了；`tmp/` 里的压测和对账原始数据复制了约 290MB。
- **验证过了（2026-11-21，都在 Mac mini 上跑的）**：主人关掉了 Little Snitch 的过滤（整个关掉，不是逐个放行；以后又被拦时先看它是不是被重新打开了）。`make tools` 通过；`make lint test` 通过（11 个模块 0 issues，56 个包 ok，`tmp/lint-test-mini.log`）；`make loadtest ARGS="preflight"` 通过；`make images IMAGE_SERVICES=cdc` 构建并推送成功（`cdc:ca83c4d`）；`make k6` 的冒烟通过（`smoke-mini-022054-order`，SKU 3001，10 单/秒 10 秒，101 单全部落库，结果拉回、remote write 都正常，`ssh mini` 连自己这条路是通的）。`gh` 已登录（令牌存在 `~/.config/gh/hosts.yml`，经 ssh 也能用）。屏幕共享已打开（只开了屏幕共享，没有开远程管理）。
- **会话结束时的状态（2026-11-21 02:50 UTC 前后）**：有线接上了（`en0` 千兆，192.168.50.19，到入口 .51 的路由走 `en0`，经它访问入口 4 毫秒返回 200）；三台设备进了主人的 Tailscale（Mac mini 是 `100.115.188.105`，主人在外面经它连回来；从 MacBook 经这个地址 ssh 和屏幕共享都通）；`gh` 已登录；集群预检通过。
- **新会话开头先做的一件事**：**看 Mac mini 的桌面有没有登录**（`who` 里有没有 `console`）。它重启之后桌面没有登录时：OrbStack 没起来（0 个容器），Tailscale 是 stopped（经局域网 ssh 执行 `/Applications/Tailscale.app/Contents/MacOS/Tailscale up` 才连上；Tailscale 自己的“登录时启动”是关的，靠系统登录项里的那一条，只在登录桌面后生效）。桌面登录之后 `make up`。kubectl 要带 `KUBECONFIG=~/.kube/oppenheimer.yaml`（`scripts/cluster/` 下的脚本自己会设）。
- **`ca83c4d` 的 CI 失败的那 1 项 e2e 查清了（2026-11-21），不是限流多放**：`TestRateLimitCreateOrder` 受理 14 个（上限 12）。失败那一次的 gateway 日志里（CI 的 `e2e-logs` 产物）在测试的那 100 毫秒里有一条 `rate limiter primary failed, falling back to the local limiter ... i/o timeout`：30 个并发请求里有一次问 Redis 超过了 20 毫秒的时限，`ratelimit.Fallback` 按设计退回进程内的桶，那个桶是满的，又放行了一批。受理数的真实上限是“Redis 的桶加本地的桶”，测试只算了前者。这是 ADR-0019 里写明的行为（退回时限额变宽），不是缺陷。改法：`scripts/e2e.sh` 给 gateway 设 `RATE_LIMIT_REDIS_TIMEOUT=500ms`，让这条用例测的确实是 Redis 那条路径；断言和生产的默认值都没有动，退回的行为由 `pkg/ratelimit` 和 gateway 的单元测试覆盖。本机 `make e2e` 通过（`tmp/e2e-ratelimit-timeout.log`）。同一份日志里 11 秒后还有一次退回（`context deadline exceeded`），是并行用例把 CI 机器压满的时候；**生产上值得留意的是同一件事：Redis 慢一次就放宽 1 秒**，`gateway.ratelimit.fallbacks` 这个指标已经有，3c 给它配告警。
- **还没做的**：
  1. `bin/istio-<版本>/bin/istioctl` 没有取（`scripts/cluster/mesh.sh fetch`），`make cluster-fault`、`make mesh-status` 要用。
  2. Mac mini 开着 FileVault，断电后不会自动开机。主人不在家时它重启了，从外面就连不上（Tailscale 要有人执行 `up` 或登录桌面）。可选的办法：在宿主机或 R2S 上也装 Tailscale 当跳板。向主人提过，没有定。
  3. 冒烟在库里多留了 101 单（用户号在压测号段，SKU 3001），没付款的会超时关单。
  4. Little Snitch 的过滤是整个关着的（主人关的）。
- 这一段和下面“3b 第 3 轮”的改动是在 Mac mini 的工作区里做的（`/Users/xinyu/Workspace/oppenheimer`），2026-11-21 已提交并推送。
- 下面“本机开发环境”一节和更早的记录里说的“本机”是 MacBook。
- 3b 接着做的是第 3 轮，见下一段。

### 3b 第 3 轮：过载时的表现（2026-11-21，新会话读完上一段接着读这里）

记录在 `docs/notes/3b-rounds/round-3-overload-behavior.md`，决定在 ADR-0034 和 ADR-0011 的 2026-11-21 补充。这一轮不提高上限（写入的上限在盘上，主人定了不再推），改的是超过上限之后的表现。

- **改了什么（2026-11-21 已提交并推送）**：
  1. 闸门内存到 85% 时停止受理（ADR-0034）：`pkg/stockgate` 的 `Gate.Memory`，`services/order/internal/service/gatememory.go`（每个实例每秒采样，受理在 `Hold` 之前读它），环境变量 `GATE_MEMORY_WATERMARK_PERCENT`、`GATE_MEMORY_INTERVAL`。Fable 写的，有测试；压测里触发不了（写不满），只有单元测试。
  2. 429 一律带 `Retry-After`：下游说忙（`BACKLOGGED`、`READ_BUSY`）时 gateway 给 1 秒。规范先改的（`api/openapi/gateway.yaml`，同时写了轮询要退避的规则），`make openapi` 生成的文件没有变化。
  3. k6 的轮询按规范退避（200 毫秒起、翻倍、最长 2 秒，429 时听 `Retry-After`）：`POLL_BACKOFF`、`POLL_MAX_INTERVAL_MS`；**和 2026-11-21 以前的轮次比查询量时要用 `POLL_INTERVAL_MS=100 POLL_BACKOFF=1`**。
  4. 入口的 `BackendTrafficPolicy`（service chart 的新模板，gateway 的 values 里 1024/1024，和默认值相同，写出来是为了有出处）和入口 Envoy 的 PodMonitor（`deploy/cluster/monitoring/ingress-podmonitor.yaml`，`monitoring.sh exporters` 提交，已提交到集群）；`loadtest-round.sh` 的存档里多了入口的五项。
  5. 集群上 `ADMIT_MAX_QUEUE=2500`（原来用默认的 1 万，是第 1 轮 608 单被回收的原因；算法写在 `deploy/cluster/services/order.yaml`）。
  6. 落库健康的熔断器加“连续失败至少持续 1 秒”（`service.DefaultLandingFailureSpan`）：压测里发现它被一次停顿打开，每轮白白拒绝一千个上下的下单（`503 ORDER_LANDING_UNAVAILABLE`，前几轮一直有，没人看）。
  7. `scripts/e2e.sh` 给 gateway 设 `RATE_LIMIT_REDIS_TIMEOUT=500ms`（上一段 CI 的那一条）。
- **验证**：全部改完之后全量 `make lint test` 通过（2026-11-21 08:10 UTC 前后，11 个模块 0 issues、56 个包 ok，`tmp/lint-test-3b-round3-final.log`）；本机 `make e2e` 通过，但那是在第 7 项之后、其余各项之前跑的，**这一轮的改动没有跑过 e2e**，靠推送后的 CI。
- **压测**：`l5a-order-overload`（第 1 到 5 项，参数同 `l4a`，盘同样在慢档）对账通过（`recon` 差异 0，`verify` 是 `PASS`，124373 单，受理了没落库的 0 单）。和 `l4a` 比：请求总数 154 万到 73 万，受理 p99 1103 到 217 毫秒，入口的 `503 UO` 21919 到 1，落库没有变（这一轮没碰）。数在记录第 2 节。`l5b-order-landing-span`（加上第 6 项，SKU 7124 到 7133）同样对账通过（123659 单，0 单没落库），数和 `l5a` 在同一个范围，落库健康的熔断器瞬间打开的 14 次没有了。
- **要主人定的一件事（记录第 4 节）**：两轮各有一次“盘卡了一秒，被放大成 5 秒不受理”。几十个在途的 `Reserve` 同时超过 1 秒的路由时限，order 的边车按“连续 5 次 5xx”把 inventory 的两个副本一起摘除 5 秒，这 5 秒落库真的停了，受理被拒（每轮约 900 个下单，0.7%）。第 6 项只消掉了进程里的那一层。建议给 `Reserve` 单独一条 3 秒的路由（它是异步路径；两轮里 `Reserve` 超过 1 秒的有 53 次，超过 2 秒的 0 次），要改 `deploy/cluster/mesh/inventory.yaml`、`deploy/envoy/`、order 里的按跳时限和核对三处的测试，动的是 ADR-0020 定的数。**主人 2026-11-21 同意：order 调 `Reserve` 的时限从 1 秒放到 3 秒。已做（2026-11-21），见下一条。**
- **`Reserve` 单独一条 3 秒的路由（2026-11-21 做完，记录 5.1 节，ADR-0020 的“补充：同一个下游按方法分时限”）**：`pkg/grpcx` 新增 `WithHopTimeouts`（按方法分时限）；order 的 `reserveHopBudget` 3 秒、进程里 3.5 秒，`WarmUp` 仍是 1 秒；`deploy/cluster/mesh/inventory.yaml` 加了排在 `from-order` 前面的 `from-order-reserve`，`deploy/envoy/order.yaml` 加了对应的路由；`TestInventoryHopTimeoutMatchesEnvoy`、`TestInventoryHopTimeoutMatchesMesh` 核对三处的两个数、路由的先后，以及 3.5 秒的两倍不超过消费者的 10 秒。验证：全量 `make lint test` 通过（`tmp/lint-test-reserve-route.log`）、`make envoy-validate`、本机 `make e2e`、本机 `FAULT_ONLY="B F L" make fault` 都通过（**本机改了 `deploy/envoy/` 之后要 `docker restart oppenheimer-envoy-<服务>-1`，容器只在启动时读配置**，第一次没重启，量到的是旧配置）。代价：inventory 挂住时熔断打开前受理的订单从 6 单变成 14 单。压测 `l6a-order-reserve-3s`（参数同 `l5b`，SKU 7134 到 7143，盘在慢档）：这一轮 `Reserve` 超过 1 秒的有 128 次（`l5b` 是 53 次），inventory 被摘除 0 次（`l5b` 1 次），`ORDER_LANDING_UNAVAILABLE` 0 个（`l5b` 892 个），其余的数和 `l5b` 在同一个范围；对账通过（`recon` 差异 0，`verify` 是 `PASS`，124344 单全部落库）。
- **没有做的，和原因**（都记进了“计划变更记录”）：“202 的响应里给轮询建议间隔”不做；**“重定 HPA”挪到扩容系数之后**（记录第 6 节：每个请求的 CPU 随组合变两倍多，CPU requests 只有实际用量的四到六成，放开后会直接扩到上限，而多副本有没有用还不知道）。入口的并发上限没有收紧（两倍过载时在途请求只到 212）。
- **集群现在的状态**：六个服务的镜像都是 `1228d9c-dirty-20261121084655`（提交 `1228d9c` 加当时没提交的 `Reserve` 路由的改动，内容和之后的提交一致；下次 `LOADTEST=1 make deploy` 换成干净的 tag）；压测 SKU 用到 7143，**下一批从 7144 起**；库里压测留下的订单约 144 万单；按地址的限流仍然关着，gateway、order 仍然钉在 2 个副本。**k3s-server 的系统盘剩 28 GB，低于预检的下限 30，下一轮的预检会失败，要主人定**：盘一共 58 GB、用了 27 GB，其中 Prometheus 的卷约 7 GB、另一个卷约 4 GB、镜像约 6 GB，一轮压测约写 2 到 3 GB。两个办法：`PREFLIGHT_MIN_DISK_GB=15` 调低下限（建议：kubelet 的驱逐线在 10% 上下，约 6 GB，15 还有余量，而且各后端自己有保留期），或者清 Tempo 和 Loki 的数据（破坏性操作，要主人同意）。
- **下一步**：
  1. （已做，2026-11-21，见上面“`Reserve` 单独一条 3 秒的路由”；下面是当时写的做法）**给 `Reserve` 单独一条 3 秒的路由**（主人已同意，先读记录第 4 节）。要改的地方：`deploy/cluster/mesh/inventory.yaml`（`from-order` 之前加一条按 gRPC 路径 `/oppenheimer.inventory.v1.InventoryService/Reserve` 匹配、来源是 order 的路由，`timeout: 3s`、`retries: {attempts: 0}`；`from-order` 上同步的预热调用不动）、`deploy/envoy/` 里本机那一套的对应路由、order 里这次调用的按跳时限（路由超时加 `grpcx.SidecarMargin`）、核对三处一致的测试（`TestHopTimeoutMatches*` 一类）；落库的消费者处理一条消息的总时限要盖得住 3.5 秒，先查。在 ADR-0020 里补一段（按路径分时限：异步路径的时限按“区分慢和坏”定，不按用户等不等得起定）。做完部署、重跑同一个阶梯（SKU 7134 起），看 `no healthy upstream` 和 `ORDER_LANDING_UNAVAILABLE` 是不是归零，对账。
  2. （已做）这一轮的 5 个提交已推送到 `phase-3b`（2026-11-21，主人要求，`0b1d6f1` 到 `1228d9c`）。**PR #16 上 `1228d9c` 的 CI 29 项全部通过**（`test (./services/order)` 4 分 24 秒、e2e 4 分 36 秒，都在以往范围的上沿，没有明显变化；e2e 里包含这一轮没在本机跑过的改动）。`Reserve` 路由的改动连同这条记录在 2026-11-21 对账通过后提交并推送（主人要求），**PR #16 上 `906c9a3` 的 CI 29 项全部通过**（`test (./services/order)` 4 分 32 秒、e2e 3 分 43 秒，都在以往的范围内）；记录这个结果的这一句还在工作区里，没有提交。
  3. （秒杀和混合读写的阶梯已做，系统盘的事已解决，见最上面“3b 混合读写和秒杀的阶梯”；接下来是空表对照）设计第 4 节剩下的，按这个顺序：秒杀和混合读写的阶梯（入口的上限、gateway 的 CPU、限流在那里看）→ 空表对照 → 扩容系数（1、2、4 个副本）→ 定 CPU requests、弹性测试、重定 HPA → 长稳（慢档上持续 125 单/秒几小时，同时看闸门内存是平的、2500 的队列上限够不够）→ 容量结论。每一轮记下盘当时在哪一档。
  4. 记录第 2.2 节最后一条留下的：队列上限是全局一个数，而两个分片的落库速率不一样，244 档落库延迟的 p99 是 27.8 秒（按总速率算应该是 12 秒）。长稳之后看要不要按分片各设上限。

### 3b 第 2 轮之后：盘的验证和幂等键（2026-11-21，接着读这里）

**断点（2026-11-20 17:30 UTC）。** 先读 `docs/notes/3b-rounds/round-2-concurrency-and-consumers.md` 的第 10 节和 ADR-0033。下面“3b 第 2 轮”里“三件挡路的事”都有了答复。

- **主人 2026-11-21 的三个答复**：宿主机的管理通道修好了（`ssh ubt` 能登录；宿主机在 2026-11-20 16:23 UTC 重启过）；应用层的组提交先不做；幂等键按建议做（缩短 Redis 里的 TTL）。
- **宿主机重启之后的集群**：三台虚拟机是正常关机的（所以放松刷盘的从库不用按 ADR-0032 重建），自启后集群自己恢复，`preflight` 通过，Redis 和 MySQL 的复制正常。**闸门 Redis 的主库回到了 `redis-gate-0`。** k3s-server 关机用了 115 秒，`libvirt-guests` 的时限是 120 秒，差 5 秒就会被强制关掉：`SHUTDOWN_TIMEOUT` 已调到 300 秒（2026-11-21，主人同意）：宿主机上直接改了 `/etc/default/libvirt-guests`，同样的改法写进了 `host-setup.sh`（**没有重跑整个 `host-setup.sh`**，它会重新套用网络配置；也没有重启 `libvirt-guests`，它的 stop 会关掉全部虚拟机，文件在 stop 时才读）。
- **盘变慢的原因查清了一层（记录第 10 节）**：不是我们的写入量触发的。这块消费级的盘在持续的刷盘负载下，几分钟后单次刷盘的耗时自己翻倍（宿主机上 1.8 到 4 毫秒），空闲一两分钟恢复；`fio` 的恒定负载能单独复现。慢的一档是稳态。盘健康、没有热降速。**对照 A 的 239 单/秒只是突发能力，持续的能力是 125 到 150 单/秒；长稳和容量结论都要在慢的一档上量。** 快的一档能撑多久没有规律（2 到 12 分钟）。再往上只有两条路：每单的提交次数减半，或者换有掉电保护的盘。**主人 2026-11-21 定：两条都不走，接受现在的拐点**，容量结论按持续 125 到 150 单/秒写（记录第 10 节末尾、“计划变更记录”）。
- **幂等键只留 1 小时（ADR-0033）**：`DefaultIdempotencyTTL` 24 小时改成 1 小时；`IDEMPOTENCY_TTL` 比 `PAYMENT_TIMEOUT` 短时 order 拒绝启动（`main.go`，有配置测试）。order 模块 `make lint` 0 issues、`make test` 通过（`tmp/test-3b-idem-ttl.log`）；提交之前全量 `make lint test` 通过（2026-11-21，`tmp/lint-test-3b-close.log`）；**本机没有跑 e2e**，靠推送后的 CI。已部署（只滚动了 order），下了一单核对过键的 TTL 是 3599 秒。`redis-gate` 的 768mb 不降。原来 24 小时的键要到 2026-11-21 17:00 UTC 前后才过期完（2026-11-20 17:30 UTC 闸门里一共约 26 万个键）。
- **这次跑的一轮**：`l4a-order-hostio`（参数同对照 D），对账通过（125581 单，差异 0，`PASS`），数和 D 接近（125 档 124 单/秒、p99 1.8 秒；156 档 156、4.9 秒；195 档 190、10 秒；244 档 212、29 秒）。宿主机上的 `iostat` 等原始数据在 `tmp/k6/l4a-order-hostio/`。
- **已提交并推送到 `phase-3b`**（2026-11-21，主人要求；三个提交：幂等键、`host-setup.sh`、文档）。集群上 order 的镜像仍是当时带 `-dirty` 的 tag，内容和提交的一致，下次 `make deploy` 会换成干净的。`361d316` 的 CI：29 项里 28 项通过（`test (./services/order)` 4 分 27 秒、e2e 3 分 18 秒），失败 1 项 `test (./services/payment)`：`TestFullBatchRunsNextRoundWithoutWaiting` 报 `outbox: count pending: context canceled`。和这次的改动无关，是测试自己的竞态（测试里的 `kicker.Kick` 用 `t.Context()`，它在测试函数返回时就取消，早于 Cleanup 里的 `Stop`，还在收尾的 worker 调到它就失败；第 1 轮把支付出结果改成并发之后才可能出现）。已改成 `context.WithoutCancel`，只动了测试，payment 模块 `make lint`、`make test` 通过。**这个修复和搬到 Mac mini 的文档改动（`AGENTS.md`、`docs/infra/hardware.md`、`docs/design/3b-load-testing.md`、`scripts/cluster/cluster.env`、本文件）还在 MacBook 的工作区里，没有提交**，等主人说提交；Mac mini 上的 clone 因此停在 `361d316`，看不到这一段。
- **集群现在的状态**：order 的镜像 `3470edb-dirty-20261120171654`，其余五个服务仍是 `4c85475`；压测 SKU 用到 7113，**下一批从 7114 起**；库里压测留下的订单约 107 万单；宿主机上多装了 `fio`、`nvme-cli`（没有进 `host-setup.sh`）；按地址的限流仍然关着，gateway、order 仍然钉在 2 个副本；库里多了一单核对用的订单（用户 900777，SKU 3001，会超时关单）。
- **下一步**：
  1. （已答复，2026-11-21）提交并推送：做了；`SHUTDOWN_TIMEOUT`：调到 300；盘：接受现在的拐点。
  2. 第 3 轮（不依赖上面的决定）：读路径的过载表现（入口的 `BackendTrafficPolicy`、`READ_BUSY` 带 `Retry-After`、k6 轮询退避）、背压的水位和回收宽限期的关系（第 1 轮的 608 单）、**闸门内存水位进背压**（ADR-0033 的后果）、重定 HPA。
  3. 设计第 4 节剩下的：秒杀和混合读写的阶梯、空表对照、扩容系数、弹性测试、长稳、容量结论。每一轮记下盘当时在哪一档。

### 3b 第 2 轮（2026-11-20）

**断点（2026-11-19 17:45 UTC 前后）。** 先读 `docs/notes/3b-rounds/round-2-concurrency-and-consumers.md`，再读下面“3b 第 1 轮”。下面那一段“下一步”的第 1、2 项已经做了。

- **计划变了：没有做应用层的组提交。** 先做了不改代码的对照（`SUBMIT_CONCURRENCY` 8 改 24），落库从约 160 到了 239 单/秒（盘快的时候），说明 MySQL 自己的组提交在并发够的时候就够用。第 0 轮记录第 7 节第 4 条的判断是错的，已在原文加注。**建议不做应用层组提交，等主人确认**（已记进“计划变更记录”）。
- **做完的**：落库并发 24、连接池 50（只在 `deploy/cluster/services/order.yaml`）；inventory、order、payment 三个串行的消费者改成按订单号并发（默认 8，三个新环境变量），积压从上万条降到几百条；service chart 的分散约束加了 `matchLabelKeys`（**已入库，没有部署**）；`pkg/redisx` 一个偶发失败的测试改了断言；闸门 Redis 的 `maxmemory` 192mb 提到 768mb（**没有事先问主人**，原因见下）。四次缩短的阶梯（A、B、C、D）都对账通过；分散约束的修复已部署并生效。**这一轮之后的结论：下游全部跟上时拐点仍在 125 到 156 单/秒之间（和第 0 轮差不多），变了的是过载时的表现（能落 176 到 215，原来死停在 143；下游不欠活、支付不退款、从库不落后）。A 量到的 239 是下游欠着活时的数，不是整条链路的能力。**
- **三件挡路的事，都要主人定或帮忙**（记录第 8 节）：
  1. **节点的盘有快慢两档**（刷盘耗时翻倍、次数减半，三台虚拟机同时）。排了五次运行的时间线之后，读法从“盘随机变慢”改成了“**多半是我们自己的写入量把它打满了**”：每次变慢都在写入量上台阶的时刻（升到 195 档，或者那一档的订单 15 分钟后成批关单）。没有在宿主机上验证（见第 2 件）。记录第 5 节。查看：`scripts/cluster/monitoring.sh promql 'sum by (instance) (rate(node_disk_flush_requests_time_seconds_total{device="vda"}[2m])) / sum by (instance) (rate(node_disk_flush_requests_total{device="vda"}[2m])) * 1000'`（agent-1 在 4 毫秒上下是快，8 毫秒上下是慢）；`loadtest-round.sh` 存档的服务端指标里现在带这一项。
  2. **宿主机的 Wi-Fi 管理通道 2026-11-19 17:21 UTC 起 `ssh` 超时**（192.168.50.8），集群不受影响。之后没有再试。
  3. **闸门 Redis 被幂等键写满过一次**（17:19 UTC，82 万个键、TTL 24 小时）：写满后受理被拒、落库停住、不会自己恢复。提到 768mb 只是把上限抬到约 330 万单/24 小时，**按 150 单/秒以上持续压几小时还会满，长稳测试之前必须先定幂等键怎么办**（记录第 6 节的三个选项，建议缩短 Redis 里的 TTL，要写 ADR）。提上限时滚动了 `redis-gate`，**闸门的主库现在是 `redis-gate-1`**。
- **这一夜我做错的两处**：连着跑 A、B 两轮没有等 A 的订单关完，B 的数因此不能用；分散副本时写的检测条件有错，五个服务各被多删了几次 Pod（当时没有流量，最终状态是对的）。
- **下一步**：
  1. 主人答复上面三件。
  2. （已做）分散约束已部署，对照 D 是在它之后量的干净的一组数。
  3. 主人同意后在宿主机上验证“盘被打满”的读法；确认的话，下一个方向是减少每单的提交次数（单独写设计）。
  4. 第 3 轮：读路径的过载表现（入口的 `BackendTrafficPolicy`、`READ_BUSY` 带 `Retry-After`、k6 轮询退避）、背压的水位和回收宽限期的关系（第 1 轮的 608 单）、闸门内存水位进背压、重定 HPA。
  5. 设计第 4 节剩下的：秒杀和混合读写的阶梯、空表对照、扩容系数、弹性测试、长稳、容量结论。
- **集群现在的状态**：镜像 tag `4c85475`（干净的提交号）；各服务两个副本分在两个 agent 上；压测 SKU 用到 7103，**下一批从 7104 起**；库里压测留下的订单约 94 万单；Tempo 上限 3 Gi；`redis-gate` 768mb、主库是序号 1。按地址的限流仍然关着，gateway、order 仍然钉在 2 个副本。
- PR #16（draft）：`229584b` 的 CI 29 项里 28 项通过，失败的是 `test (./pkg)` 里 `pkg/redisx` 的那个偶发用例（已改，见上）。`4c85475` 的 CI 29 项全部通过（`test (./services/order)` 3 分 14 秒、e2e 4 分 05 秒，都在以往的范围内）。那之后只有文档和 `loadtest-round.sh` 多存两项指标的提交，它的 CI 没有再看。全量 `make lint test` 最后一次是在第 1 轮的提交上跑的；第 2 轮只跑了改到的四个模块（order、payment、inventory、pkg），都通过。

### 3b 第 1 轮（2026-11-19 到 20 日）

**断点（2026-11-19 16:20 UTC）。** 先读这一段，再读 `docs/notes/3b-rounds/round-1-replica-flush-and-payment.md`。下面 2026-11-18 那一段里“等主人定第 1 轮做哪几条”已经过时。

- **主人 2026-11-19 定的**：第 0 轮记录第 7 节的七条按建议的顺序做（第 1 轮：从库放松刷盘加支付的串行循环；第 2 轮：落库的应用层组提交；第 3 轮：读路径的过载表现加重定 HPA）；备份“轮转的和基准的都做”；夜间按阶段提交并推送、开 draft PR 让 CI 跑、Tempo 的内存上限可以提；旧分支 `worktree-agent-a97714952afd11fa3` 可以删（去删时本地和远端都已经没有这个分支）。
- **做完的**：
  - 备份：灌数后的三份基准备份钉成各自实例目录下的 `keep-seed-10m`（硬链接，轮转删不到）；手工跑了一轮轮转备份（`mysql-order-0/20261119T151935Z`、`mysql-order-1/20261119T152009Z`、`mysql-main/20261119T152044Z`）。新子命令 `scripts/cluster/middleware.sh mysql-backup-list`、`mysql-backup-pin <名字> [<实例>=<时间戳>...]`；恢复演练用 `BACKUP=keep-seed-10m` 指定某一份（**这个入口没有实际跑过恢复**，只过了 `shellcheck`）。
  - 从库不逐次刷盘（ADR-0032）：已部署，两个订单库从库 2/0，主库和 `mysql-main` 1/1。落盘方式跟着角色走（`deploy/charts/mysql/files/durability.sh`，`middleware.sh mysql` 每次都设一遍并核对）。切换手册改了提升那一步、加了第三节。
  - 支付出结果改成并发（`services/payment/internal/processor`，`PROCESSOR_CONCURRENCY` 默认 16），新指标 `payment.result.delay`；`.delay` 结尾的直方图用到 30 分钟的桶（`pkg/otelx`）。
  - 同一个阶梯重跑（`l2-order-ladder`）并对账通过：从库延迟全程 0（原来涨到 553 秒）；支付出结果跟上需求（76 笔/秒，原来上限约 17），退款 0（原来 22351 笔）；落库上限 154 到 171 单/秒（原来约 143）。
  - Tempo 的内存上限 2 Gi 提到 3 Gi（两轮阶梯都在第 16 分钟前后被 OOMKilled），已滚动。
  - 全量 `make lint test` 通过（`tmp/lint-test-3b-round1.log`）；之前只跑 payment 和 pkg 时 `pkg/cache` 的 `TestConcurrentColdKeyLoadsOnceInProcess` 失败过一次（`loaded 2 times, want 1`），重跑和全量都通过，没有改过这个包，原因没有查。
- **这一轮发现、没有处理的**（细节在记录里）：
  1. 阶梯最后两档（305、381）落库掉到约 100 单/秒：15:47 UTC 起三台虚拟机的刷盘耗时同时翻倍，停压 25 分钟后复测已恢复。原因没有定（Tempo 被杀只差一分钟，但第 0 轮同样被杀而没有掉）。**这两档不能和第 0 轮比。**
  2. inventory 消费 `order.events` 停在每秒约 105 条，积压到 2 万条。支付通了之后它成了下一个最窄的地方，原因没有查。
  3. 608 个受理了的订单没有落库（占用等不到落库被回收）。账是对的，用户看到的是订单消失。背压的水位和回收的宽限期是各自定的，要一起看（放在第 3 轮）。
  4. 两个 order Pod 这一轮都在 k3s-agent-1 上，该节点 CPU 97%，受理 p99 在过载时比第 0 轮差。下一轮开跑前先分开。
  5. 从库节点掉电后的自动重建没有做，留给阶段 4；3c 要有节点重启的告警（ADR-0032 的后果）。
- **下一步**：
  1. 不改代码的对照：把 `SUBMIT_CONCURRENCY` 调大（同时调 `MYSQL_MAX_OPEN_CONNS`），重跑阶梯的前几档。第 0 轮说“单独调大没有用”，但第 1 轮支付的结果说明并发的提交会被组提交合并，要重新量；量完再定第 2 轮（应用层组提交）怎么做、还要不要做。
  2. 定位 inventory 消费 `order.events` 的上限。
  3. 第 2 轮、第 3 轮；之后是设计第 4 节剩下的（秒杀和混合读写的阶梯、空表对照、扩容系数、弹性测试和重定 HPA、长稳、容量结论）。
- **集群现在的状态**（和 2026-11-18 那一段相比变了的）：镜像 tag `7980e19-dirty-20261119152743`；五个 MySQL 实例 2026-11-19 15:2x UTC 滚动过；压测 SKU 用到 7063，**下一批从 7064 起**；库里压测留下的订单约 48 万单。按地址的限流仍然是关的，gateway、order 仍然钉在 2 个副本，做完 3b 要恢复。
- 压测时原来要记住的两件事变成一件：**支付的速率不用再压在每秒 15 笔以下**；每轮压完仍然要等 15 分钟以上（支付超时关单）才能对账，但不用再等从库。
- 新的用法：`scripts/cluster/loadtest-round.sh stages <RUN_ID>` 给阶梯的每一档各取一份服务端指标。

### 3b 做到哪（2026-11-18 更新，这一段在最前面，比下面 2026-11-17 写的新）

**断点（2026-11-18 19:50 UTC，夜间那一段做完时写的）。新会话先读这一小段，再读 `docs/notes/3b-rounds/round-0-first-bottleneck.md`。**

- 做完的：3a 收尾；3b 设计第 4 节的第 1 到 6 步（脚本和工具、灌 1000 万单、备份恢复、基线三个场景、可观测开销对照、普通下单场景的阶梯加压、第一个瓶颈的定位）。所有轮次对账通过。全部已提交并推送到 `origin/phase-3b`，没有开 PR，所以没有 CI 结果；**这一夜没有跑过 `make lint test`**（改的是脚本、values 和文档，脚本过了 `shellcheck -x`，没有改 Go 代码）。
- **等主人定的**：第 1 轮优化做哪几条（记录第 7 节的七条；建议从库放松刷盘加修支付的串行循环）。夜里按约定没有改业务代码。
- **集群现在的状态（和平时不一样的地方，做完 3b 要恢复）**：
  - gateway 按地址的限流是关的（`LOADTEST=1` 部署的）。恢复：不带 `LOADTEST=1` 再 `make deploy`。
  - gateway、order 钉在 2 个副本（KEDA 暂停）。恢复：`scripts/cluster/loadtest.sh unpin gateway order`。
  - 镜像 tag `61bb6be-dirty-20261118173536`（工作区不干净时构建的，内容等于提交 `61bb6be` 加当时没提交的备份 Job 内存那一行 values，Go 代码没有差别）。
  - MySQL 五个实例的上限 2560 Mi、缓冲池 1 G；备份 Job 1 Gi。都已入库。
  - 库里：1000 万造数订单，加压测留下的约 31 万单（用户号 2^39 段）。压测 SKU 用到 7053，**下一批从 7054 起**；`verify` 要的初始库存在本机 `tmp/loadtest/sku-initial.txt`（不入库，丢了就用 `VERIFY_INITIAL` 手工给，库存都是 10 亿，7011 是 1 万）。
  - 灌数之后的备份（回到基准状态用）：NAS 上 `mysql-order-0/20261118T173525Z`、`mysql-order-1/20261118T173606Z`、`mysql-main/20261118T173407Z`。**定时备份只留 7 份，一周后会被轮掉，要留就在 NAS 上另存。**注意这份 `mysql-main` 的备份是在商家索引追平之后做的，三份之间差几十秒，恢复后要让索引按 CDC 追一下。
  - server 节点的系统盘剩 42 GB（夜里的压测写了约 5 GB 的日志和链路）。
- **压测时必须记住的两件事**：支付的速率压在每秒 15 笔以下（`PAY_RATIO`），否则量到的是 payment 的串行循环，而且会产生退款；每轮压完要等 15 分钟以上才能对账，从库追平还要更久（阶梯那一轮追了 36 分钟）。
- 还没有做的（设计第 4 节）：秒杀和混合读写场景的阶梯；空表上的对照；1、4 个副本的扩容系数；三轮优化；弹性测试和重定 HPA；长稳测试；容量结论、`GOALS.md` 的修正、笔记。夜里发现、没有处理的小事：Tempo 在过载时被 OOMKilled（上限 2 Gi）；落库延迟的直方图只到 10 秒；支付单出结果的时延没有指标；k6 只给下单接口留了客户端的分位数（别的接口要加阈值才有）；入口没有 `BackendTrafficPolicy`；备份写 NFS 的页缓存问题只是把上限抬高了；灌数的 Pod 里订单库的密码出现在进程参数上（`loadtest.sh seed-orders`）；k6 的 remote write 输出已弃用。
- 用法速查：`make k6 SCENARIO=...`；`RUN_ID=<名字> SKUS=... RATE=... scripts/cluster/loadtest-round.sh <场景>`（预检、条件、k6、pprof、服务端指标存档）；`make loadtest ARGS="preflight|conditions|skus|pin|unpin|wait-quiet|recon|verify|seed-orders"`；`LOADTEST_IMAGE_TAG=42c5fd6` 免去重建 chaos 镜像。结果在 `tmp/k6/<运行号>/`（不入库）。

以下是这一天里按时间写的各段，细节以设计文档 3b 第 6 节为准。


- **3a 收尾了**：空转一天后的数量完（设计文档 3a 第 8 节最后一段“空转一天后的数”），阶段计划里最后一项已勾。保留期不改；Grafana 的 requests 调到 384 Mi；agent 没有加内存上限，改的是发送队列（1000 批改成 20 批），改完重做了一次“停 Collector gateway 2 分 30 秒”，日志不丢、agent 的内存峰值从 204 Mi 降到 94 Mi。etcd 的 fsync p99 仍是 16 到 18 毫秒，和负载无关，先接受。**这些改动在 `phase-3b` 的工作区里，不在 `phase-3a` 和 PR #15 里**：PR #15 合并时 3a 的计划那一项还是没勾的样子，等 `phase-3b` 合并才带上。
- **Mac mini**：`ssh mini` 能登录，k6 v2.4.0 核对过。三件要主人处理的事：
  1. **它走的是 Wi-Fi，有线口没插线**。量基线之前要插网线，否则客户端的延迟不可信（现在查一次库存 11 到 15 毫秒，大半是无线这一段）。
  2. 空闲 1 分钟就睡眠（`pmset` 的 `sleep 1`），没有免密的 sudo。脚本用 `caffeinate -i` 顶着；长稳测试之前最好在 Mac mini 上执行 `sudo pmset -a sleep 0`。
  3. 默认文件描述符上限 256，脚本里调高，不用主人管。
- **Prometheus 的写入地址已暴露**：入口上 `Host: prom-write.oppenheimer.test`，只放行 `POST /api/v1/write`（`deploy/cluster/monitoring/prometheus-write-route.yaml`，`cluster.env` 的 `PROM_WRITE_HOST`，`monitoring.sh kps` 会应用）。
- **主人 2026-11-18 确认：压测的订单不删**（设计文档 3b 第 6 节第一条、ADR-0031 第 4 项）。下面 2026-11-17 那段里“主人还没有看过”“请主人过目”两处已经过时。
- **第 3 步做完了**（2026-11-18，提交 `abb92f3`、`e822c39`、`43f2c26`，后两个之后的没有推送，见 git log）：`make k6 SCENARIO=...`（`scripts/cluster/k6.sh`，在 Mac mini 上原生跑 k6，结果拉回 `tmp/k6/<运行号>/`，指标 remote write 进 Prometheus，标签 `testid=<运行号>`，一轮约 200 条序列；结束后序列带 stale 标记，查的时候用 `last_over_time(...[10m])`）；`scripts/cluster/loadtest.sh`（`make loadtest ARGS=...`：`preflight`、`conditions`、`skus`、`pin`/`unpin`、`seed-orders`、`wait-quiet`、`recon`、`verify`）；`LOADTEST=1 make deploy` 关掉 gateway 按地址的限流（**还没有部署**）。冒烟：order、mixed 从 Mac mini 各跑通一次；`loadtest.sh` 的 `wait-quiet`、`recon`、`verify` 在 300 单上通过（SKU 6901、6902，冒烟用掉了）。chaos 镜像 tag `42c5fd6`（`LOADTEST_IMAGE_TAG=42c5fd6` 可以免去重建）。
  - Mac mini 上 k6 起初连不上局域网（macOS 的“本地网络”权限），主人在 Mac mini 的终端里跑过一次 k6 并允许之后，经 ssh 启动的也通了。重装 k6 或升级系统后可能要再授权一次。
  - k6 2.4 的 remote write 输出设不了 Host 头，写入的路由改成了不限 Host（`http://192.168.50.51/api/v1/write`）。这个输出已标弃用（v3 移除），以后换 OpenTelemetry 输出。
  - 每轮压完要等 15 分钟（没付款的订单等支付超时关单）`wait-quiet` 才过；`preflight` 的消费延迟不算 `order.order-close-delay` 这个组。
  - `verify` 需要的初始库存记在本机 `tmp/loadtest/sku-initial.txt`（`skus` 子命令写的，不入库）。
  - 两个子 Agent 的 worktree 和分支已删（2026-11-18，主人同意）。
- **第 4 步（灌数）停在这里，等主人定**：`LOADTEST_IMAGE_TAG=42c5fd6 scripts/cluster/loadtest.sh seed-orders -orders 10000000 -from 2026-08-20 -to 2026-11-18`（**参数必须原样**，指纹记在库里，换了会被拒绝；可以另加 `-rate`、`-timeout`）。试跑 30 秒灌进去 1064235 单（约 4.3 万单/秒），**把 `mysql-order-0-0` 打到 1 Gi 的内存上限被 OOMKilled 一次**（16:18:51 UTC），7 秒后自己恢复；两个分片主从的 GTID 一致，`seed_progress` 的合计等于工具报的数，没有丢已提交的数据。另外三个订单库实例当时也在 990 到 1014 Mi。缓冲池是 512 MB。**续灌之前要先定 MySQL 的内存**（建议订单库四个实例的上限提到 2 Gi，并给灌数加 `-rate`）；商家索引的消费者当时积压 35 万条，在追。 **主人同意后已做**：四个订单库实例 2 Gi、缓冲池 1 G（已滚动）；16:54 UTC 起以 `-rate 5000` 续灌（`LOADTEST_OUT=tmp/loadtest/seed-10m`）。灌到 35% 时订单库各实例 1.3 Gi 上下；**`mysql-main-0` 到了 1482 Mi，上限 1536 Mi**，要不要提它的上限在等主人定（见设计文档 3b 第 6 节“第 4 步”）。灌完之后：等从库和商家索引追平（`loadtest.sh preflight`）、`seed-report`、做一次备份并量备份和恢复的耗时。
- **1000 万历史订单灌完了**（2026-11-18 17:24 UTC，两个分片 5023016 加 4976984，主从 GTID 一致，消费延迟 0，`preflight` 通过）。`mysql-main` 灌数期间也被 OOMKilled 过一次，主人同意后提到 2560 Mi、缓冲池 1 G。**订单库四个实例灌完后在 1854 到 1902 Mi，上限 2048 Mi，又贴着上限**：缓冲池 1 G 之外有约 400 MB 固定开销加约 400 MB 分配器留着的内存（设计文档 3b 第 6 节“续灌和结果”的表）。**等主人定**：订单库也提到 2560 Mi（建议），还是把缓冲池降到 768 M。定了之后再做：备份并量备份和恢复的耗时、`seed-report`、`LOADTEST=1 make deploy`、基线。`mysql-main` 的 values 和这段文档已在本地提交，**没有推送**。
- **夜间（2026-11-18 到 19 日，主人睡前授权）**：范围是“基线、阶梯加压、定位第一个瓶颈并写记录”，不改业务代码；中间件撞资源上限可以自己调并滚动（改动入库、记文档）；每个阶段提交并推送。
- 订单库四个实例已提到 2560 Mi（主人同意）。备份 Job 在灌数后的订单库上被 OOMKilled（输出文件在 NFS 上的页缓存），内存提到 1 Gi 后三个实例备份成功，恢复演练 177 秒（设计文档 3b 第 6 节“第 4 步的后一半”）。`LOADTEST=1 make deploy` 已部署（**按地址的限流现在是关的，压测做完要不带它再部署一次**）；gateway、order 用 `loadtest.sh pin` 钉在 2 个副本（**做完要 `unpin`**）。
- **基线第一批做完并对账通过**（设计文档 3b 第 6 节“第 5 步”）：普通下单 100 单/秒受理 p99 23 毫秒、落库延迟 p99 1.3 秒，但订单库的从库已经追不上（延迟 134 秒）、agent 节点 CPU 七成；混合读写 500 请求/秒各接口 p99 10 到 20 毫秒；秒杀 2000 请求/秒零超卖，但入口 Envoy 的默认熔断上限答了 12 万个 `503 UO`。运行一轮用 `scripts/cluster/loadtest-round.sh`。压测 SKU 用到 7031，下一批从 7032 起。
- **阶梯加压和第一个瓶颈做完了，对账通过**（2026-11-18 19:11 UTC），记录在 `docs/notes/3b-rounds/round-0-first-bottleneck.md`：落库上限约 143 单/秒，拐点 125；瓶颈是同步提交的刷盘（宿主机的 NVMe 一次同步写 5.3 毫秒，虚拟机里 11 到 13 毫秒）；从库延迟同因；**支付出结果只有每秒约 17 笔（processor 串行、一笔一个事务），阶梯那一轮 22351 笔支付排队超过 15 分钟被退款**；入口默认熔断上限 1024 在过载时答 `503 UO`。七条优化方案在记录第 7 节，**等主人选第 1 轮做哪条**（建议：从库放松刷盘加修支付的串行循环）。阶段计划里“阶梯加压找拐点，定位第一个瓶颈”已勾。Tempo 在阶梯期间被 OOMKilled 一次（上限 2 Gi），没有调。压测 SKU 用到 7041。
- **Mac mini 已接千兆有线**（2026-11-18）：有线口的地址是 192.168.50.19，到入口的路由走有线（`en0`，1000baseT），`ssh mini` 仍然连 Wi-Fi 的 .14；休眠已关；重启之后“本地网络”的授权还在（经 ssh 的 k6 20 个请求 0 失败，中位数 3.4 毫秒）。重启后要在本机登录一次，否则 ssh 提示 `This system is locked`。
- 细节在设计文档 3b 第 6 节“第 2、3 步”。

### 3b 做到哪（2026-11-17）

设计文档 [design/3b-load-testing.md](design/3b-load-testing.md)，**主人已确认第 2 节的十项，都按建议**（2026-11-17）；另外确认不实做影子库表、不从 OpenAPI 生成客户端（计划里那一项已勾）。决定在 ADR-0031。实现顺序在设计第 4 节。

- **时间上的约束：2026-11-18 15:00 UTC 之前不往集群上压流量、不滚动监控的组件**（3a 的“空转一天”窗口）。在那之前只做不碰集群的第 1 步。
- **确认之后改了一处**（设计第 6 节第一条、ADR-0031 第 4 项，已记进“计划变更记录”）：压测的订单**不删**。原因是订单表的 DELETE 在这个系统里只表示归档，CDC 和商家索引按这个约定处理，按号段删会在索引里留下冷库里没有的“已归档”订单。改成每轮用新的 SKU 号、记下数据量、要回到基准状态时从灌数后的备份恢复。**这一处主人还没有看过，要请主人过目。**
- **Mac mini 已由主人用 brew 装了 k6 2.4.0**（2026-11-18，主人说的，主会话没有核对到）。但从 MacBook `ssh mini` 还登录不了：2026-11-17 是 22 端口超时，2026-11-18 变成 `Connection closed by 192.168.50.14 port 22`（连得上，被对端关掉）。第 3 步之前要先弄清原因（远程登录的开关、允许的用户、密钥），再用 `ssh mini 'k6 version'` 核对版本。
- **第 1 步（不碰集群的部分）做完了**，三样都由子 Agent 写、主会话读过 diff 后合进工作区，细节在设计第 6 节：
  - `test/k6/`（四个场景 `order`、`flash`、`mixed`、`ratelimit` 和 `lib/`）、`scripts/k6-local.sh`（用 Docker 镜像 `grafana/k6:2.4.0` 在本机跑，依赖 `jq`）。Sonnet 写；四个场景在本机冒烟通过，主会话自己又跑了一次 `order`。
  - `pkg/redisx/metrics.go`：Redis 命令耗时进 `db.client.operation.duration`（和 MySQL 同名，`db.system.name=redis`）。Opus 写。
  - `load verify` 的限定范围模式（`-scope-skus`、`-orders-jsonl`，`test/load/internal/verify/scope.go`、`internal/ledger/orderlist.go`）。Opus 写，Fable 逐行读过采集的代码；`Check` 没有改。
  - 全量 `make lint test`：lint 11 个模块 0 issues；test 第一次失败 1 项（`services/order/internal/service` 的 `TestSweeperSlotTimeoutSkipsHungDB`，健康的 slot 在 100 毫秒的时限里没关成单；当时本机的六个服务也在跑），原样重跑一次全部通过（`tmp/lint-test-3b-step1.log`、`tmp/lint-test-3b-step1b.log`）。这次没有改 order 的代码，判断是负载下的偶发，没有证实；再出现时要看。
- 本机的六个服务验证完已停掉；compose 的中间件还在运行。
- 三个子 Agent 的 worktree 和分支已删（2026-11-18，删之前核对过：各自提交改到的文件和 `phase-3b` 上的逐字节相同）。还留着一个没有 worktree 的旧分支 `worktree-agent-a97714952afd11fa3`（1b 时期的提交，不在 `phase-3b` 的历史里），没有动，要不要删由主人定。
- **下一步，新会话从这里开始**：
  1. 请主人过目上面“确认之后改了一处”（不删订单、用户号段 2^39）。提交已做（2026-11-18，分支 `phase-3b`，已推送，没有开 PR，所以没有 CI 结果）。主人说 2026-11-18 15:00 UTC 之后再继续。
  2. 2026-11-18 15:00 UTC 之后：量 3a 最后一次内存、磁盘、etcd 的 fsync，勾掉计划里那一项，3a 收尾；再做 Grafana 的 requests（384 Mi）和 agent 的内存上限。
  3. 设计第 4 节的第 3 步：Mac mini 上核对 k6（已装 2.4.0，先要 `ssh mini` 能登录）、把 Prometheus 的写入地址暴露给局域网、`scripts/cluster/loadtest.sh preflight`、`make k6`、对长期运行的那一套跑 `recon` 的入口、压测 SKU（7001 起）的准备脚本、把 gateway 的 `RATE_LIMIT_IP` 关掉的 values。
  4. 然后灌数（1000 万）、基线。
- 第 1 步留下、上集群时要看的：k6 里库存不足的 409、`BACKLOGGED`、`READ_BUSY` 三条分支没有触发过；`console.log` 输出订单号清单在上千次/秒时的开销没有量；k6 的落库耗时精度受轮询间隔限制，以服务端的指标为准；超时的错误码 1050 没有核对；Redis 指标在真实的 Cluster、Sentinel 下的测试被跳过了（本机没起 `make redis-modes-up`），上集群后对着指标看一眼有没有重复计数；Redis 指标分不出是哪一个 Redis（闸门、缓存、限流），建议定位瓶颈之前给 `redisx.Config` 加一个固定的名字当标签；限定范围的对账看不到“客户端没收到响应、又在预占之前就失败”的订单，第 17 条（outbox 为空）数的是全库。
- 这些改动在 2026-11-18 提交到 `phase-3b`（从 `phase-3a` 的 `cc21ae2` 开出）并推送；提交前 `make lint` 11 个模块 0 issues，`make test` 只跑了改到的 `pkg` 和 `test/load`，都通过（是缓存的结果）。`phase-3a` 和 PR #15 没有动。

### 3a 做到哪（2026-11-17）

设计文档 [design/3a-observability.md](design/3a-observability.md)，**主人已确认第 2 节的十项，都按建议**（2026-11-17），决定在 ADR-0030。实现顺序在设计第 6 节，每一步的结果和量到的数在第 8 节。分支 `phase-3a`（从 `phase-2d` 开出，带着那个不在 `main` 里的文档提交）。前三步已提交，**第 4 步的改动没有提交，也没有推送**。

**做完的：**

- 三台虚拟机的内存改成各 14 GiB（ADR-0030 第 5 项），逐台重启，集群自己恢复。重启时 Sentinel 切了一次，**闸门 Redis 的主库现在是 `redis-gate-1`**。`cluster.env`、`hardware.md` 已改。
- 设计第 6 节的第 2 步，kube-prometheus-stack：`make monitoring-up`（`scripts/cluster/monitoring.sh`，values 在 `deploy/cluster/monitoring/`），命名空间 `monitoring`，release 名 `kps`。Prometheus 是 LTS 的 v3.13.5，OTLP 接收端和 exemplar 已打开；全部 Pod 和三个卷在 k3s-server 上；没有不健康的抓取目标。Grafana 经入口访问（`Host: grafana.oppenheimer.test`，地址 192.168.50.51；密码 `scripts/cluster/monitoring.sh password`）。`scripts/cluster/monitoring.sh promql '<查询>'` 直接查 Prometheus。版本已登记到 `versions.md`。
- k3s 的 scheduler、controller-manager、kube-proxy 的指标只监听本机，抓不到，先关了（设计第 8 节）。
- 设计第 6 节的第 3 步，Loki 3.7.10 和 Tempo 3.1.1（单体模式、本地盘，都在 k3s-server 上）：`scripts/cluster/monitoring.sh loki|tempo`，`make monitoring-up` 现在依次装三样。Grafana 里 Loki、Tempo、Prometheus 三个数据源健康检查通过，互相跳转已配（`deploy/cluster/monitoring/datasources.yaml`）。Tempo 的 metrics-generator 已打开，写 Prometheus 的 remote write。**还没有任何日志和链路进来**：跳转、metrics-generator 生成的指标都要等第 4 步接上 Collector 才能验证。版本已登记。

- 设计第 6 节的第 4 步，Collector 和服务接入（细节和量到的数在设计第 8 节“第 4 步”）：
  - `scripts/cluster/monitoring.sh collector`（`make monitoring-up` 现在依次装四样）。Collector contrib 0.165.0，两个 release：`otel-gateway`（Deployment，在 k3s-server 上，收 OTLP 分发到 Tempo、Prometheus、Loki）和 `otel-agent`（DaemonSet，只读容器日志）。**尾部采样、`spanmetrics`、`servicegraph` 还没有加**（第 6 步）；agent 不开 `kubeletstats`（和设计 3.3 不一致，设计已改）。
  - service chart：默认导出到 `http://otel-gateway.monitoring:4317`，指标 15 秒导出一次，资源属性里有命名空间、Pod、节点和 `service.version`（镜像 tag），出站排除加了 4317。
  - 验证 1 到 3 用接口查过，都成立：六个服务的指标在 Prometheus 里；一单的三条链路在 Tempo 里（按订单号查要写成数字，不加引号）；按 `trace_id` 在 Loki 里查到同一条链路三个服务的日志；exemplar 查得到。**Grafana 界面上点一遍（大盘到链路到日志）还没有做，要主人或下个会话在浏览器里看一次。**
  - **查出并修了一个埋点缺陷**：三分之一的 MySQL span 被标成 Error（`driver.ErrSkip`）。改了 `pkg/mysqlx`（两个 otelsql 选项）加一个用例。全量 `make lint test` 通过（输出在 `tmp/lint-test-3a-step4.log`）。
  - 集群现状：六个服务的镜像 tag 是 `69030ff-dirty-20261117101712`（含上面的修复）；两次滚动期间探针 0 失败；没有不正常的 Pod。本机的 compose 中间件这次 `make up` 起来了，还在运行。

**要回头看的：**

- etcd 的 WAL fsync p99 在服务接上之后是 15.9 毫秒（装完时的 29.6 是启动期间的）。仍然**空转一天后再量**：`histogram_quantile(0.99, sum by (le) (rate(etcd_disk_wal_fsync_duration_seconds_bucket[5m])))`。
- `local-path` 不限制卷的大小，PVC 上写的容量不起作用；server 的系统盘只有 60 GB，靠保留期兜着，3c 的磁盘告警要盖住它。
- 序列数 16.5 万（业务服务只占 4600，kubelet 7.2 万、apiserver 4.7 万），比设计第 4 节估的多。
- 第 4 步留下的小事（设计第 8 节末尾）：metrics-generator 的 exemplar 标签名是 `traceID`，数据源里配的是 `trace_id`，从它的图上跳不到链路；`evalsha` 遇到 `NOSCRIPT` 的 span 被标成 Error；Tempo 的搜索接口带时间范围时查不到刚写入的数据；旧的一个 gateway Pod 滚动前重启过 26 次，原因没有查；本机 `pkg/cache` 的 `TestHitDoesNotLoad` 在 `make up` 刚起来时连 Redis 超时失败过两次，之后三次通过。

**下一步，新会话从这里开始**（设计第 6 节）：

1. 到 exporter 和 pprof 为止的改动都已提交并推送到 `origin/phase-3a`（最后一个是 `4ff3ca0`），**还没有开 PR，所以 CI 没有跑**。之后的大盘（`deploy/cluster/monitoring/dashboards/` 四个 JSON、`monitoring.sh` 的 `dashboards` 子命令）和文档已提交，**没有推送**。
2. `redis-cache-5` 的复制断过约 3 小时（连的是主库重启前的旧地址），主人确认后已用 `cluster replicate` 修好（设计第 8 节最后一段）。根因没有查，主库下次换地址多半还会断，记给了阶段 4；3c 要加告警。
3. 四个大盘已加载到 Grafana 的文件夹 Oppenheimer（业务总览、单服务、中间件、Collector，设计第 8 节有表）。**界面上的样子没有人看过，要主人在浏览器里看一遍**，顺便走一次“大盘上的 exemplar → 链路 → 日志”。地址 `http://grafana.oppenheimer.test/d/opp-business`（`/etc/hosts` 要有 `192.168.50.51 grafana.oppenheimer.test`）。
4. “新 order Pod 一就绪就 503”已解决（改答 `429 READ_BUSY`，设计第 8 节）。
5. **尾部采样已上线，对比做完**（设计第 8 节“第 6 步”有表）：同样的故障，出错的链路头部 10% 只留下 10%，尾部全留；Collector 多 100 Mi 内存。“慢的全留”没有验证到。尾部采样之后 Tempo 的 metrics-generator 的指标是偏的，只用来看拓扑，RED 用 SDK 的指标。
   **Istio 的指标已接上**（设计第 8 节最后一段）：标签在源头从 25 个裁到 9 个，新增 532 条序列；网格大盘 `opp-mesh` 已加载；gateway 到 order 一次调用 6.5 毫秒里有 2.7 毫秒在边车和网络上。
   这一批（尾部采样、Istio 的指标和网格大盘、文档）已提交，**没有推送**（`origin/phase-3a` 停在 `ea3db7c`，还没有开 PR，CI 没有跑过）。新会话从“3a 剩下的”第一项（KEDA）开始。
   **3a 剩下的**：按 QPS 的 HPA（第 7 步，KEDA，装上先验证它在 Kubernetes 1.37 上能用）；实测内存和磁盘定保留期（Tempo 的内存在压测里涨到 1.2 Gi，上限 2 Gi，要看）、从空集群重建一次、ADR 的补充、笔记（第 8 步）；验证 5（停掉 Collector gateway）、6（`make cluster-fault` 抽几个场景对指标）、7（停从库复制、停消费者看图）没有做；五个大盘在界面上的样子、“大盘 → 链路 → 日志”仍然要主人在浏览器里看一遍。做大盘时发现的指标缺口在设计第 8 节，其中“Redis 没有命令耗时的指标”建议在 3b 之前补（`pkg/redisx`）。
6. **第 7 步，按请求速率的 HPA，做完了（2026-11-17 晚）**，细节和数在设计第 8 节“第 7 步”、ADR-0030 的补充：
   - KEDA 2.21.1 装在命名空间 `keda`（`scripts/cluster/monitoring.sh keda`，已并入 `make monitoring-up`），在 Kubernetes 1.37 上实测可用。版本主会话在 GitHub 的发布页核对过，已登记。
   - service chart：`autoscaling.requestRate.query` 非空就渲染 `ScaledObject`（请求速率加 CPU 两个触发器），gateway 每副本 180 请求/秒、order 100，是用 CPU 70% 反推的初值，**3b 压测后重定**。集群上现在是 `keda-hpa-gateway`、`keda-hpa-order` 两个 HPA，旧的两个已手工删掉（旧 HPA 在时 KEDA 的 webhook 会拒绝，`helm upgrade` 过不去）。
   - 验证过：507 请求/秒下两个服务各扩到 3（gateway 48 秒、order 79 秒，order 是请求速率触发的），122025 个请求 0 失败，停压 5 分 19 秒后缩回 2；Prometheus 连不上时不缩容、CPU 仍然能扩。
   - **一键部署的顺序变了**：`make cluster-up && make middleware-up && make mesh-up && make monitoring-up && make deploy && make deploy-seed`（KEDA 要先于业务服务）。没有从空集群走过，重建时验证。
   - 顺带的发现：405 请求/秒时 gateway、order 的 CPU 已到 71% 到 75%（2c 是约 810 请求/秒才扩容），3b 做基线时要拆开看多出来的 CPU 花在哪。
   - 这一步的改动**没有提交**：`deploy/cluster/monitoring/keda.yaml`、`deploy/charts/service/`（新模板 `scaledobject.yaml`、`hpa.yaml`、`values.yaml`）、`deploy/cluster/services/gateway.yaml` 和 `order.yaml`、`scripts/cluster/monitoring.sh`、`cluster.env`、`Makefile`、文档。没有改 Go 代码；脚本过了 `shellcheck -x`，chart 过了 `helm lint`，没有跑 `make lint test`。
   - 记给 3c 的：KEDA 取数失败的告警（`keda_scaler_detail_errors_total`、HPA 的 `ScalingActive`）。记给阶段 4 的：KEDA 不在时的行为、聚合 API 不可用时删命名空间会不会卡住。没有做的：按 Kafka 消费延迟伸缩 order；大盘上的副本数图；扩到 4 个副本没有压到。
   - **同一个会话接着做的（都没有提交）**：
     - 实测内存和磁盘（设计第 8 节“第 8 步的前一半”，`hardware.md` 的预算表已改）：保留期不改；盘是被压测写满的，不是被保留期（2000 请求/秒压一小时，Tempo 和 Loki 各约 1.5 GB）；**日志的 60% 是入口 Envoy 的访问日志，3b 之前定要不要关**；活跃序列 21.8 万；etcd 的 fsync p99 是 19.1 毫秒。空转一天后的数没有量。
     - 验证 5（停 Collector gateway 5 分钟，做了三轮）：业务 0 影响、内存不涨；指标断图但累计值不丢；链路丢；**日志原来会丢约 6 万行，查出三处并修了**（`otel-agent.yaml` 的重试、队列、批次上限，`otel-gateway.yaml` 的 `batch/logs`，`loki.yaml` 的写入限额），第三轮不丢。已应用到集群（Loki、Collector 都滚动过）。
     - 验证 7（停 `mysql-order-0-1` 的 SQL 线程、把 orderindex 缩到 0，各 2 分钟）：都已恢复；消费延迟的图有反应；**复制停掉时主从延迟的指标是断掉而不是变大**，3c 的告警要按运行状态写。
     - 没有解释的：验证 5 第三轮有 15 个查库存请求同时超时，入口和 gateway 都没有收到过它们，卡在 MacBook 到入口之间，原因没有查到；入口的两个 Envoy Pod 各重启过 26 次（43 小时里），原因没有查。
     - 留给 3b 之前的小事：Grafana 主容器的内存（峰值 357 Mi）超过 requests（256 Mi），调到 384；agent 的内存在队列堵满时到过 204 Mi（上限 256 Mi）。
     - **主人 2026-11-17 的四个答复**：同意从空集群重建；五个大盘和“大盘 → 链路 → 日志”在浏览器里看过，没有问题（“互相跳转”已勾）；提交并推送；同意降入口的访问日志，已改成只留 5xx 和带 response flags 的（`deploy/cluster/envoy-gateway/gateway.yaml`，已应用并实测）。全量 `make lint test` 通过（输出在 `tmp/lint-test-3a-step7.log`）。
     - **从空集群重建做完了（2026-11-17 14:00 到 14:45 UTC）**，设计第 8 节“第 8 步：从空集群重建”：新的一键顺序走通，约 35 分钟；探针 0 失败，`make cluster-e2e` 通过，三种信号、Istio 的指标、KEDA、大盘都在。中途 `make deploy` 失败一次：提交号 `7390796` 全是数字，`helm --set` 把镜像 tag 当成整数，`services.sh` 已改成 `--set-string`。
     - **集群现状（重建之后）**：全新的集群，六个服务的镜像 tag 是 `7390796`；库里只有示例 SKU 和验收时探针、e2e 留下的几千单；MySQL 的密码、Grafana 的密码都是新生成的；主库都是序号 0；监控的数据从重建时重新开始攒；没有不正常的 Pod。上面“集群现状”那一段写的是重建之前的。
     - PR #15 第二次推送（`c0fcda1`）的 CI 27 项全部通过（`test (./services/order)` 4 分 15 秒、e2e 3 分 36 秒，没有变慢）。
     - **验证 6 和笔记做完了（2026-11-17，已提交并推送：`ac2e357`、`e58ef3c`、`18d6c41`）**：
       - 验证 6：`FAULT_TELEMETRY=1 E2E_KEEP=1 FAULT_SCENARIOS="A B D E" make cluster-fault` 通过，脚本、Istio 的指标、服务自己的指标三边对得上（设计第 8 节“验证 6”）。`FAULT_TELEMETRY` 和 `deploy/cluster/services/fault-telemetry/` 是新加的。**查出并修了**：并发上限的拒绝记在 `upstream_rq_active_overflow` 上，全网格保留的和网格大盘画的是另外两项（`istiod.yaml`、`istio-monitors.yaml`、`mesh.json`、`fault.sh` 已改，五个带边车的服务滚动过，Pod 模板上多了 `restartedAt` 注解，无害）。Pod 注解里的 `proxyStatsMatcher` 是覆盖不是合并。熔断器和降级开关的指标没有对（要对中间件做故障注入，阶段 4）。
       - 尾部采样之后 exemplar 大半跳不到链路（40 个里查得到 14 个），没有改，记在设计第 8 节最后一段和 ADR-0030 的补充里。
       - 笔记 `docs/notes/3a-observability.md`（Sonnet 写，640 行上下；主会话抽查了数字、补了 14a 一节、按它报的不一致改了 ADR 和设计）。它提的、没有处理的质疑：`READ_BUSY` 那件事的样本很小、16 个读名额为什么会满只是推断；etcd 的 fsync 三次都高于 10 毫秒却一直没有查；“监控自己看不见的故障”（推送的指标没有 `up`、KEDA 取数失败、复制停了延迟指标消失）在 3c 之前没有任何告警兜着。
       - 没有改 Go 代码；脚本过了 `shellcheck -x`，没有重新跑 `make lint test`。`oppenheimer-e2e` 命名空间已清掉，没有不正常的 Pod。
   **3a 现在剩下的（新会话从这里开始）**：空转一天后（从重建算起，即 2026-11-18 15:00 UTC 之后；注意重建之后又做过一轮故障注入和两次短的探针）再量一次内存、磁盘和 etcd 的 fsync，量完勾掉“监控数据保留策略与资源占用评估”，3a 就收尾了；PR #15 还是 draft，3a 收尾后请主人转成正式的并决定合并。3a 收尾后是 3b（压测，需要 Mac mini，还没有装 k6）。3b 之前的小事：Grafana 主容器的 requests 调到 384 Mi；agent 的内存上限；拆开看 405 请求/秒时 CPU 就到 70% 的原因。
7. 记给阶段 4 的：滚动重启 Kafka 的 broker 期间有 31 个下单超时；Redis Cluster 的从库在主库换地址之后不会自己重连。

集群现状：六个服务的镜像 tag 是 `c79f81c-dirty-20261117110013`；命名空间 `keda` 里有 KEDA 的三个 Pod；Loki、Collector（gateway 和三个 agent）按新配置滚动过；gateway、order 的伸缩由 `ScaledObject` 管（各 2 个副本）；`monitoring` 里多了 Collector（gateway 加三个 agent）、mysqld-exporter、redis-exporter，`kafka` 里多了 kafka-exporter，三个 broker 滚动过一次；MySQL 三个实例上多了账号 `exporter`；没有不正常的 Pod。本机的 compose 中间件在运行。

没有查证的（设计 1.3）：除 kube-prometheus-stack、Prometheus、Loki、Tempo 之外的版本都是 Haiku 子 Agent 查的，主会话没有核对；mysqld_exporter 对 MySQL 9.x 的支持（`versions.md` 里记了实测可用）。KEDA 的版本和在 1.37 上能用已经核对过。

### 2c 做到哪（2026-11-15）

设计文档 [design/2c-services.md](design/2c-services.md)，**主人已确认八项**：第 3 到 8 项按建议；第 1 项节点号用 MySQL 租约（ADR-0028）；第 2 项分两步，2c 用手写的 Envoy 边车，**2d 迁到 Istio**（已写进阶段计划的 2d，是必做）。实现顺序在设计第 13 节（八步），每一步的结果记在同一节末尾。

**做完的（设计第 13 节的第一、二、三步），全部没有提交：**

- 第一步，镜像：`deploy/docker/Dockerfile`、`.dockerignore`、`scripts/cluster/images.sh`、`make images`。
- 第二步，chart 和部署：`deploy/charts/service`、`deploy/cluster/services/*.yaml`、`scripts/cluster/services.sh`、`make deploy`、`make deploy-status`。**六个服务已经在集群上跑着**（命名空间 `oppenheimer`）：order、payment 各 2 个副本，其余 1 个；镜像 tag `65adb85-dirty-20261115105112`。从入口走通了查库存、下单、落库、支付、商家索引。访问方式：`curl -H 'Host: api.oppenheimer.test' -H 'X-User-Id: 7' http://192.168.50.51/api/v1/skus/1001/stock`。示例 SKU（1001 到 1003、2001）是手工插的；库里有一笔测试订单（用户 7，已支付，SKU 1001 的库存因此是 98）。
- 第三步，节点号的租约：`pkg/idgen/lease.go`、`lease_sql.go` 和测试，`idgen.Open`；order、payment 的装配（没设 `WORKER_ID` 就租，设了就固定，本机脚本都设了所以行为不变）；order 受理在租约过期时按降级拒绝（有测试）。Fable 写。`make lint` 0 issues，`make test` 全部通过，`make e2e` 通过（19 个，完整输出在 `tmp/e2e-2c-lease.log`）。集群上四个 Pod 各租到不同的号，滚动时新旧 Pod 各占一个。
  - **实现比设计草稿更强，改了方案**：唯一性不靠“持有者在本地判断租约有效”，而靠每一任持有者只在一段互不重叠的毫秒区间里发号（表里的 `last_ms` 是上界）。见设计 5.3、ADR-0028。
  - **一个要记住的后果**：订单库 0 不可用超过约 20 秒后，所有用户的下单都被拒绝（原来只影响一半）。ADR-0028 的后果一节。阶段 4 做订单主库故障的实验时要算进预期。
  - 顺带修了 `Generator.Next` 里序列号在检查之前就被改掉的隐患。

**前三步已提交（2026-11-15，没有推送）**：`2da007c`（租约的代码）、`d4f30d9`（镜像、chart、脚本；`Makefile` 的两处改动拆不开，所以合在一个提交里）、`3d8f2a6`（文档）。

**第四步做完了**（结果和数在设计第 13 节末尾）：

- 除 cdc 外都是 2 个副本；集群上现在跑的镜像 tag 是 `3d8f2a6-dirty-20261115112619`。
- 新脚本 `scripts/cluster/probe.py`：经入口持续发请求，记下每个失败和时刻。验证滚动发布、删 Pod 都用它。
- 端点传播实测 1.7 到 9.5 秒，`DRAIN_DELAY` 定为 15 秒（没有改值，现在有依据了）。`DRAIN_DELAY=0` 的对照：每删一个 Pod 失败约 15 个请求。
- 滚动发布零报错：干净状态下整体滚动两次，28500 个请求 0 失败。
- gRPC 负载均衡的对照：直连 ClusterIP 时请求全部落在一个 Pod 上，经边车各一半。
- 内存 requests 按实际用量调低了（服务 64 Mi、order 96 Mi、边车 32 Mi）。
- 2c 的面试考点已补进阶段计划。
- 实验留下的东西都清掉了：临时的 Service `inventory-lb-exp` 已删，values 里临时改的两处已恢复并重新部署。量传播时间的一次性脚本在会话的临时目录里，没有入库。测试订单留在库里（探针用的用户号从 900000 起，SKU 3001，几千单，没付款的会超时关单）。

**第四步已提交**（见 git log，`phase-2c` 上的第四、五个提交；没有推送）。

**主人定了业务服务避开 server 节点**（2026-11-15，加内存太贵）：chart 加了 `avoidControlPlane`，副本分散改成软约束。已部署，11 个业务 Pod 都在两个 agent 上，滚动期间 0 失败。留下两个没有处理的问题（设计第 13 节）：

- inventory 和 payment 的两个副本现在都在 k3s-agent-1 上（软约束加滚动时多开的结果）。agent-1 一挂这两个服务就没有副本了。阶段 4 关节点的实验之前要定怎么办（descheduler、`maxSurge: 0`、或 3 个副本）。
- server 节点的内存在业务 Pod 走了之后仍然是 82%，主要来源是 etcd、k3s、Kafka、Redis。阶段 3 装监控时它没有余量。

**第五步的前一半、第六步、第七步的前一半做完了（2026-11-15 第二个会话），都没有提交。** 结果和数在设计第 13 节末尾的三段：

- **e2e 上集群**：`make cluster-e2e`（`E2E_ARGS="-run TestX"` 只跑部分，`E2E_KEEP=1` 保留现场，之后 `scripts/cluster/services.sh e2e-down` 清）。在命名空间 `oppenheimer-e2e` 起一套隔离的服务（`e2e_*` 库、每次不同的 Kafka 和 Redis 前缀），测试是一个 Job，跑完删命名空间并清中间件。第一次执行就通过，全量 19 个用例，连构建带清理 203 秒。Opus 写，主会话验收并执行。`test/e2e` 只多了一个可选的 `E2E_WAIT_READY`。e2e 的 cdc 用 server id 1012、1022。**同一时刻只能有一次 e2e 在跑；跑的时候不要同时压测和对账。**
- **HPA**：gateway、order，2 到 4 个副本，CPU 目标 70%（chart 的 `autoscaling`，`templates/hpa.yaml`）。已部署。压到约 810 请求/秒时两个都从 2 扩到 4，新 Pod 约 5 秒就绪并均分流量；发压到第一次扩容约 55 秒（指标滞后）；停压 5 分钟后缩回 2。**开了 HPA 的服务，Deployment 不再写 `replicas`**；去掉字段的那一次部署副本数会先掉到 1 再被拉回（记在设计里）。
- **静态成员：结论是不做**（ADR-0007 的补充）。滚动 payment 时每个分区只停 0.5 到 3.2 秒；franz-go 的静态成员关闭时不离组，用了反而要等会话超时。
- 集群现状：长期运行的那一套镜像 tag 仍是 `3d8f2a6-dirty-20261115112619`（gateway、order 带 HPA，现在各 2 个副本）；payment 被 `kubectl rollout restart` 过一次（Pod 模板上多了一个 `restartedAt` 注解，下次 `make deploy` 会再滚一次，无害）。`oppenheimer-e2e` 命名空间已删，中间件里的 e2e 数据已清。库里多了约 2 万单探针的订单（用户号 900000 和 910000 起）。

**这个会话发现、没有处理的两件事（阶段 3 压测之前要处理）：**

- **扩容时新的 order Pod 一就绪就出错**：约 810 请求/秒时，第 3 个 order Pod 就绪后的 20 秒里有 14 个查订单返回 `503 STOCK_GATE_UNAVAILABLE`。推断是新 Pod 到闸门 Redis 的连接池是冷的，撞上读闸门的 300 毫秒时限（只有时间上的吻合）。可选的修法：就绪前预热连接池（go-redis 的 `MinIdleConns`），或在调用方边车上开 Envoy 的慢启动。属于 order 的代码，由 Fable 改。
- 缩容时相隔 15 秒被删的两个 order Pod：前一个交出的分区被分给了后一个（它正在摘流），这些分区挪了两次。代价是多停一两秒，没有处理。

**第五步的后一半也做完了（2026-11-15）**：`make cluster-chaos`，压测期间随机删 Pod，静止后 `verify` 和 `recon` 无差异。正常删除和强制删除（`CHAOS_FORCE=1`）各通过一次，四次运行的数据都对得上（设计第 13 节的表）。它和 e2e 共用命名空间 `oppenheimer-e2e`，靠标签互斥；日志在 `tmp/cluster-chaos-logs/<运行号>/`。

- **它查出一个真缺陷，已修**：`pkg/kafkax` 的消费者启动时查死信 topic，可能问到元数据还没更新的 broker，服务启动时退出再被重启（只有多 broker 才出现）。`checkDLQTopics` 现在最多等 10 秒。`pkg` 的 lint 和测试通过。**长期运行的那一套还是旧镜像，没有重新部署**（第八步从空集群重建时会带上）。
- 一次没有解释的重启：某次运行里一个 inventory 容器启动阶段重启过一次（用的是修过的镜像），日志没留下，之后两次没有再出现。
- PR #11（draft）已开；CI 第一次失败 1 项（`pkg/idgen` 的 `TestSQLLeaseTakeoverAfterExpiry`，测试用 300 毫秒的租期占满 64 个号，CI 上慢，最早的在断言前就过期了；改成先用长租期占满），第二次（`aa94c07`）27 项全部通过。

**第八步做了一部分（2026-11-15）**，都在设计第 13 节“第八步里做了的”：

- `make deploy-seed`（示例 SKU）、`make deploy-degrade ARGS="<服务> [<开关> off|auto]"`（逐个 Pod 改降级开关，验证过）。
- 切换手册的验证：带着探针对 `mysql-order-1` 计划内切换来回两趟，写入中断 12 到 14 秒，只有落在这个分片的下单被拒绝（设计内的 503），服务自己恢复，cdc 和商家索引照常。**主库都已切回序号 0，values 没有改动。** 真正的 cdc 换源没有验证（阶段 4）。
- MySQL 恢复耗时重量了（4 到 5 秒，备份才 1.2 MB，数据太少，不算数）。**发现恢复演练在服务跑着时必然报不一致**（节点号租约的续租、outbox 的清理一直在写），没有修；阶段 4 之前要补（把从库停在备份的 GTID 上再比）。三个实例各多了一份手工备份。
- 2c 的笔记 `docs/notes/2c-services.md` 已写（Sonnet，613 行）；它没有包含第五步后一半和第八步的结果，要补。它报的文档不一致里改了三处（阶段计划节点号那条、设计 5.2 的一句、设计第 4 节的估算），没改的：ADR-0028 没提正常退出时 `last_ms` 会降回实际发过的最后一毫秒；设计 7.4 说要确认 `GOMAXPROCS` 在 cgroup 限额下的行为，没有确认；设计 7.6 说第一次部署要盯迁移的偶发失败，没有记录有没有出现。
- PR #11 的第三次 CI（`aeaabf0`）28 项全部通过（最长的是 `test (./services/order)` 4 分 26 秒、e2e 3 分 50 秒）。

**2c 的八步都做完了（2026-11-15）。** 最后做的是从空集群重建一遍（主人确认）：删三台虚拟机后 `make cluster-up`、`make middleware-up`、`make deploy`、`make deploy-seed`，17 分 34 秒，没有人工步骤；重建后没有容器重启，探针 0 失败，`make cluster-e2e` 19 个用例通过。吞吐对比的结论是**没有数量级的提升**（1a 约 400 单/秒，1b 落库约 200、受理在默认背压下约 365、关背压约 1900 到 2500），细节在设计第 13 节。

**集群现状（重建之后）**：全新的集群，六个服务的镜像 tag 是 `178fe99`（含 `kafkax` 的修复），gateway、order 带 HPA；库里只有示例 SKU 和验收时探针下的几百单；MySQL 的密码是新生成的；主库都是序号 0。NAS 上旧集群的备份和 etcd 快照还在，对新集群没有意义。新集群还没有做过手工备份（定时的 CronJob 每天 19:00 UTC 起跑）。

### 2d 做到哪（2026-11-16）

**2d 必做的部分做完了**：服务之间的流量治理从手写的 Envoy 边车迁到了 Istio。设计文档 [design/2d-mesh.md](design/2d-mesh.md)（主人确认了第 2 节的八项，都按建议；九步的结果和量到的数在第 10 节，没做的在第 11 节），决定在 ADR-0029，笔记 [notes/2d-mesh.md](notes/2d-mesh.md)，面试考点已补进阶段计划。分支 `phase-2d`，PR 见“当前状态”。

**集群现状（2026-11-16 从空虚拟机重建之后）**：全新的集群，一键部署的顺序是 `make cluster-up && make middleware-up && make mesh-up && make deploy && make deploy-seed`（`make mesh-check` 可以在 `mesh-up` 之后验证注入）。

- istiod 1.32.0 在 `istio-system`（1 个副本，在 agent 节点上）。chart 和 istioctl 来自 GitHub 的发布包，`scripts/cluster/mesh.sh fetch` 放在 `./bin/istio-1.32.0/`；排查用 `scripts/cluster/mesh.sh istioctl proxy-config ...`。
- 六个服务在命名空间 `oppenheimer`，镜像 tag 是重建时的提交号。五个有 gRPC 的服务各带一个注入的 `istio-proxy`（按 Pod 的标签 `sidecar.istio.io/inject` 注入，命名空间不打标签），cdc 不注入。入站只接管 gRPC 端口，到 MySQL、Redis、Kafka 的出站不过边车。
- 流量规则在 `deploy/cluster/mesh/`（每条调用边的超时、不重试、并发上限、摘除；`make deploy` 先于 release 应用），mTLS 严格模式和按身份的访问控制在 `deploy/cluster/mesh/security/policies.yaml`（release 滚动完之后应用）。
- `DRAIN_DELAY` 是 5 秒（2c 是 15 秒）。
- **手写边车的形态已经从 chart 和集群脚本里删掉**，集群上只有 Istio 一种形态；本机 compose 和 CI 仍然用 `deploy/envoy/` 的手写边车。同一跳的超时写在三处（Go 的常量、`deploy/envoy/`、`deploy/cluster/mesh/`），有测试核对。
- 库里只有示例 SKU 和验收时探针下的几百单；MySQL 的密码是新生成的；主库都是序号 0；新集群还没有做过手工备份。

**验证过的**：`make cluster-e2e`（19 个用例）、`make cluster-chaos`（正常删除、强制删除、严格模式下各一次）、`make cluster-fault`（八个场景，含“把重试打开”的对照）、不停机切换（e2e 上来回演练十次滚动 0 失败；长期运行的那一套七次部署 18770 个请求 0 失败）、访问控制的反向验证、从空集群重建。本机全量 `make lint test e2e` 通过。

**这个阶段的几个发现**（细节在设计第 10 节）：

- Istio 默认对 `UNAVAILABLE` 重试 2 次，必须显式关；关没关掉要有“打开时”的对照才说明得了。
- 摘除在触发之后 0.5 到 1 秒才生效（也解释了 1d 里那个没有解释的现象）；原因和 Envoy 的 `update_merge_window` 吻合，没有直接证实。
- 摘除时间在间隔几秒的重复故障下不累加，不需要 EnvoyFilter。
- 边车超时时调用方看到的可能是 503 也可能是 504（被调用方的入站边车也在计时）：1d 的“503 是边车超时、504 是边车不在”在集群上不成立了。
- 回退路径上有只在退回时出现的缺陷（Pod 模板残留的 ServiceAccount），演练时发现并修掉；随后整条回退路径随手写边车的分支一起删了。
- 重建时 `make cluster-up` 因为经代理的下载断了三次，重跑才完成：脚本里的下载没有重试。

**下一步，新会话从这里开始：**

1. 请主人看 2d 的 PR，决定合并。2d 其余几项（Argo CD、cert-manager、灰度发布）这次不做（ADR-0029 第 8 项）；阶段 2 到此可以收尾，下一个是阶段 3（可观测，需要 Mac mini，先提醒主人开机）。
   - 2d 留下、适合在阶段 3 压测时一起做的：HPA 的扩容点重量（边车的 CPU requests 从 20m 变成 50m）；经边车和直连的延迟对比；边车和 istiod 的内存 requests 各降一半（实际 32 Mi、66 到 76 Mi）；接监控时定 Istio 边车保留哪些统计项、要不要开访问日志。
   - 适合放进阶段 4 的：istiod 不在时的行为（已有的边车继续工作、新 Pod 建不出来，没有实测）；入站并发上限没有压到过；设计第 4 节计划的两个对照没有做。
   - **CI 上一个和 2d 无关的偶发失败**：PR #12 第一次推送时 `test (./pkg)` 失败 1 项，`pkg/redisx` 的 `TestCommandWithoutParentProducesNoSpan`（“没有父 span 的命令不出 span”，多出 2 个 span）。这个 PR 没有改 `pkg/`，本机没有复现。span 的记录器是进程级的，推断多出来的是别的用例留下、这时才结束的 span，没有证实；已让它失败时把多出来的 span 的名字和父 span 打出来，下次出现时先看那几行。
   - 小的欠账：一键部署里经代理的下载没有重试；e2e 的 OTLP 接收端端口名不符合 Istio 的约定；server 节点的内存偏高（重建后 73%），来源没有查。
2. **阶段 3 压测之前要处理的**（都是 2c 发现、没有修的）：
   - 1b 在集群上的落库只有约 200 单/秒，是本机的四分之一左右，原因没有查（线索：最忙的是订单库 0 的从库，约 1 核）。
   - 扩容时新 order Pod 一就绪就有十几个 503（推断是冷连接池，Fable 改）。
   - 恢复演练的比对在服务跑着时必然报不一致（租约续租、outbox 清理），要改成把从库停在备份的 GTID 上再比；恢复耗时要等有百万行数据再量。
   - inventory 和 payment 的两个副本可能挤在同一个节点上（软约束），阶段 4 关节点之前要定怎么办；server 节点的内存没有余量。
3. 小的欠账：2c 的笔记没有包含删 Pod 测试、切换验证、吞吐对比和重建的结果（派 Sonnet 补）；本机 `scripts/bench.sh` 的 `BENCH_ARCH=1a` 是坏的；ADR-0028 没提正常退出时 `last_ms` 会降回实际发过的最后一毫秒；设计 7.4 的 `GOMAXPROCS` 没有确认；一次没有解释的 inventory 容器启动时重启（chaos 第二次运行）。
4. 没有验证的：真正的 cdc 换源；在节点上直接杀进程；探针不发支付请求；租约所在的库不可用时受理被拒绝再恢复（集群上）。都适合放进阶段 4。
5. 提交和推送：2c 的改动都在已合并的 PR #11 里；2d 的改动都已提交在 `phase-2d` 上，没有推送。

### 2a 做到哪（2026-11-15）

已完成：

- 摸清了三台设备的现状，地址规划写进了 `scripts/cluster/cluster.env` 和 [infra/hardware.md](infra/hardware.md) 的“网络”。
- 宿主机（`ssh ubt`，xinyu 已有免密 sudo）装好了 KVM 和 libvirt（qemu 10.2.1、libvirt 12.0.0、virtinst、cloud-image-utils、nfs-common），xinyu 已加进 `libvirt`、`kvm` 组。
- `scripts/cluster/host-setup.sh`（装包、建网桥 `br0`、登记 libvirt 网络 `host-bridge`、停掉默认的 NAT 网络）和 `on-host.sh`（从 MacBook 把脚本推到宿主机执行）已跑通，重复运行没有变化。宿主机的有线网卡现在接在 `br0` 上，地址是静态的（写在 `/etc/netplan/10-cluster-bridge.yaml`），默认路由走 `br0`；Wi-Fi 没有动，`ssh ubt` 仍然走它。
- Ubuntu 26.04 的 cloud image 已下到宿主机的 `/var/lib/libvirt/images/base/`，SHA256 核对过。
- 换盘方案写在 `tmp/disk-migration.md`（本地文档，不入库）。

- **（2026-11-17 已按主人的要求删除，宿主机现在没有整机备份；重建靠仓库的脚本。）** 宿主机整机备份曾在 NAS 的 `/volume1/homes/me/backups/ubt-20261115/`（2026-11-15）：`metadata.tar`（分区表、LVM 元数据、fstab、启动项、软件包清单）和 `rootfs.tar.zst`（`/`、`/boot`、`/boot/efi`，16.8 GB，两端 SHA256 一致；没有做过恢复演练）。
- **网线拔掉时宿主机从 IPv4 连不上的问题已修**：`br0` 是静态地址，没有载波时路由还在而且比 Wi-Fi 的优先。修法是 `ignore_routes_with_linkdown=1`（已写进 `host-setup.sh`）。再遇到 IPv4 连不上时可以走 Wi-Fi 网卡的 IPv6 链路本地地址（在 MacBook 上 `ping6 ff02::1%en0` 找邻居）。
- 板载网口确认是硬件坏了（只通告千兆时链路起不来，别的电脑接同一个路由器口是千兆）。主人已把网线拔掉，现在只走 Wi-Fi；长期方案是加一块 PCIe 网卡，显示器（U2723QX）的 USB 网口可以临时顶替。**网线没插时 `host-setup.sh` 会因为没有链路而退出**（`ALLOW_NO_LINK=1` 可以跳过）。
- 换盘定了走方案 A（没有硬盘盒）：主人拔 Windows 盘装 1TB，主会话经 ssh 做迁移，主人再换回。

- **换盘（方案 A）的软件步骤已做完并重启验证过**（2026-11-15）：1TB 分成 EFI 1G、`/boot` 2G、LVM 928G；根分区用 `pvmove` 在线搬到新盘，旧盘已移出卷组；`/boot`、EFI 复制过去，`/etc/fstab` 换了两个 UUID（原件 `/etc/fstab.before-migration` 2026-11-17 已删）；GRUB 装到新盘，主板启动项 `Boot0001` 指向新盘的 EFI 分区。卷组里还有约 698G 没分配。250GB 的旧盘按主人要求已清空（分区表和签名都抹掉，整盘 discard），指向它的启动项已删。虚拟机磁盘的卷已建：`/dev/ubuntu-vg/vmpool`，600G，ext4，挂在 `/var/lib/libvirt/images`（fstab 里带 `nofail`），cloud image 已挪进去并重新核对过；卷组还剩约 98G 没分配。**剩下的**：主人关机拔掉 250GB、把 Windows 盘装回 `M2_1`，开机后主会话确认启动顺序和 Windows 的启动项。
- `br0` 在 netplan 里加了 `optional: true`：没插网线时开机不再等 2 分钟（等待期间 ssh 会以 pam_nologin 拒绝登录）。

- **换盘全部完成**：Windows 盘已装回 `M2_1`，开机自动进 Ubuntu，启动顺序是 Ubuntu、Windows Boot Manager（Windows 没有实际启动验证过）。
- **三台虚拟机已建好**（`scripts/cluster/vm.sh`，经 `scripts/cluster/on-host.sh vm.sh create` 调用；规格和地址见 [infra/hardware.md](infra/hardware.md)）。从 MacBook 用 `ubuntu` 用户直接 ssh 到节点地址能登录；三台都能经 R2S 访问 GitHub、Docker Hub、get.k3s.io，互相按名字能 ping 通。还没有装 k3s。

- **2a 的集群已建成并做过一次完整的重建演练**（2026-11-15）。设计文档 [design/2a-cluster.md](design/2a-cluster.md)（主人确认了第 2 节的六项，都按建议），实现中的七个发现在第 11 节，结果和实测的数在第 12 节。现状：
  - k3s v1.37.2+k3s1，1 server 加 2 agent，内嵌 etcd，flannel；MetalLB（二层模式）；镜像仓库（集群里，数据在 NAS 的 `/volume1/k8s/registry`，明文 HTTP）；Envoy Gateway v1.10.0，`Gateway` 叫 `public`，在 `envoy-gateway-system`，监听 80。固定地址见 hardware.md 的“网络”和 `cluster.env`。
  - 用法：`make cluster-up`（从空宿主机到可用集群，可重复执行，重建实测 5 分 28 秒）、`make cluster-status`、`make cluster-smoke`。kubeconfig 在 `~/.kube/oppenheimer.yaml`，**不在默认的 `~/.kube/config` 里**，手工操作要 `export KUBECONFIG=~/.kube/oppenheimer.yaml`。删虚拟机是 `scripts/cluster/on-host.sh vm.sh destroy <名字...>`。
  - NAS 的共享文件夹 `k8s` 主人已建好（NFS 权限按设计文档 6.2），NAS 不需要 root。子目录要直接挂载，并且建好后要 `chmod` 一次（设计文档第 11 节的发现 2）。
  - 宿主机加了 `br_netfilter` 的 udev 规则（`host-setup.sh`），做过实验。
  - 仓库里留着一个测试镜像 `test/busybox:1.37.0`，可以不管。

停在哪、在等什么：

1. 2a 没有在等的事。重启宿主机的检查已做（2026-11-15）：不做任何操作，104 秒后集群全部恢复。宿主机上有主人自己的一个 traefik 容器，以后重启宿主机前仍然先说一声。
2. **PCIe 网卡已装好，有线是千兆，宿主机出网正常（2026-11-17 晚些时候验证：`curl https://github.com` 返回 200）。网卡后来挪到了 `PCI_E4`，接口名现在是 `enp41s0`，见会话日志同日最后一段；下面写的 `eno1` 是挪之前的名字。R2S 上 `.19` 的静态绑定仍然认板载口的 MAC（板载口插线后 DHCP 拿到的也是 `.19`），要不要清掉由主人定。** 以下是当时的记录：PCIe 网卡装好后宿主机一度出不了外网，等主人改 R2S。新网卡是 `eno1`（RTL8126），`cluster.env` 的 `HOST_NIC` 已改、`host-setup.sh` 已重跑，`br0` 的 MAC 跟着变成了新网卡的。R2S 上 192.168.50.19 的静态绑定和代理白名单还认旧 MAC，所以宿主机解析域名拿到假地址、连接超时（虚拟机不受影响，实测正常）。主人在 R2S 上把这两处换成新 MAC 之后，在宿主机上 `curl -m 8 https://github.com` 验证。同一天还有两件没处理的事，见会话日志 2026-11-17：NAS 没开机（镜像仓库的 Pod 挂不上 NFS）；`redis-gate-1` 的 AOF 损坏，在 CrashLoopBackOff。以下是装之前写的说明，留作记录。原文：PCIe 网卡还没装，有线仍是 100 Mbit/s。装好之后：确认设备在线，把 `scripts/cluster/cluster.env` 的 `HOST_NIC` 改成新网卡的名字（`ip -br link` 看，板载的是 `enp39s0`，建议插 `PCI_E5`），重跑 `scripts/cluster/on-host.sh host-setup.sh`，用 `ethtool` 确认协商到千兆。改网桥时 ssh 走 Wi-Fi（`ssh ubt`），不要走有线的地址。阶段 3 压测之前要解决；在那之前往仓库推镜像约 11 MB/s。2026-11-16 补充：主人选了一块最高 5G 的 PCIe 网卡（型号和芯片装好后用 `lspci -nn` 确认并记到 `hardware.md`）；新网卡的 MAC 和板载的不同，R2S 上 192.168.50.19 的静态绑定和代理白名单要跟着改。板载网口（RTL8168h，r8169）不支持 `ethtool --cable-test`；水晶头在这个口上卡不住，重新插入并用力按住仍然降到百兆，所以是插座内部的线对坏了，不用再查。
3. 出网的白名单由主人管，**不要去改 R2S**。R2S 经路由通告下发的 IPv6 DNS 不走代理、返回被污染的地址，节点一侧已经绕开（`accept-ra: false`）；局域网里其他在白名单内的设备可能有同样的问题，已告诉主人。

下一步：

1. **2b 的中间件已全部部署并验证（第一个会话）。** 分支 `phase-2b`。PR #9（2a）已合并进 `main`（2026-11-15）；2b 到目前为止的改动已分提交推送，在 draft PR 里（见“当前状态”）。设计文档 [design/2b-middleware.md](design/2b-middleware.md)，实现中的九个发现、量到的数和“还没做的”都在它的第 10 节。
   - **集群在 2026-11-15 重建过一次**（主人确认）。原因是节点上的 AppArmor 拦掉了容器里进程之间的全部信号：官方的 MySQL 镜像起不来，所有 Go 程序的抢占信号被拒绝。根因是 Ubuntu 26.04 默认的 `kernel.apparmor_restrict_unprivileged_unconfined=1`，主人在一个节点上手工验证后同意写进 `vm.sh` 的 cloud-init 并重建。重建后三个节点没有任何信号被拒绝的记录。
   - 现在集群上有：两套 Redis（`redis-gate` 主从加 Sentinel、`redis-cache` Cluster）、Strimzi 1.3.1 加 Kafka 4.3.2（3 个 broker）、MySQL 9.7.4（`mysql-order-0`、`mysql-order-1` 各一主一从，`mysql-main` 单实例）。`make cluster-up` 之后 `make middleware-up`（或 `scripts/cluster/middleware.sh redis|kafka|mysql`），都可重复执行。MySQL 的密码是脚本在集群里随机生成的 Secret（`<release>-auth`），重建后会变。
   - MySQL 的主从由 values 里的 `primaryOrdinal` 决定（不是 Pod 的标签，发现 2）；`middleware.sh mysql` 最后只读地核对实际状态，不一致时非零退出。
   - Go 代码：`pkg/redisx` 三种模式（地址写法 `sentinel://gate@...`、`cluster://...`）、`pkg/ratelimit` 在 Cluster 下按桶调用（ADR-0019 的补充）、调用方改成 `redis.UniversalClient`、`kafkax` 建 topic 的副本数用 broker 的默认值。全部 11 个模块 `make lint` 0 issues、`make build` 通过；`pkg`、四个服务和 `test/load` 的 `make test` 通过；`make e2e` 通过（都是 2026-11-15）。第一次推送时漏改了 `test/load`（它也用旧的客户端类型），CI 有 3 项编译失败，已修。
   - CNI 已改定为保留 flannel（ADR-0026 末尾）。
   - 本机：OrbStack 配了 `insecure-registries`，可以直接 `docker push 192.168.50.50:5000/...`；多了一个可选的测试环境 `make redis-modes-up`（11 个容器，现在在运行，不用时 `make redis-modes-down`）。
2. **2026-11-15 第二个会话做完的（都没有提交）**，细节在设计文档 2b 第 10 节的发现 10 到 16：
   - etcd 快照同步到 NAS：server 节点上的 systemd 定时器（`scripts/cluster/k3s.sh etcd-backup install|run|list`，模板在 `deploy/cluster/k3s/etcd-snapshot-sync.*`，已进 `make cluster-up`；`make etcd-backup` 立刻拍一份）。连 server 的 token 一起备份。NAS 上现在有一份按需快照。
   - MySQL 备份：chart 里的 CronJob（三个实例每天 19:00、19:30、20:00 UTC）、`backup` 账号、NFS 的 PV 和 PVC；`make middleware-backup`、`make mysql-restore-drill RELEASE=<实例>`。三个实例的恢复演练都通过（各 1000 行记号数据，表叫 `drill_marker`，留在库里没有删；`mysql-order-1` 里还有 490 行切换探针写的）。Sonnet 写，主会话读过 diff 并在集群上执行，修了一个等 Job 完成的判断。
   - MySQL 切换手册 `docs/infra/mysql-switchover.md`，计划内切换在 `mysql-order-1` 上演练了两趟（切过去再切回来），写入各中断约 5 秒，490 次确认的写入没有丢。现在主库都是序号 0，values 没有改动。
   - 两个 Redis 的 chart 加了 `priorityClassName`。顺带发现并修掉两个问题：`redis-cache` 有一对主从在同一个节点（新脚本 `scripts/cluster/redis-cache-pairing.sh check|fix`，`middleware.sh redis` 结尾会调）；`redis-cache` 的 PDB 因为选中了 hook Job 的 Pod 一直允许 0 个中断（Job 的 Pod 换了标签）。
   - 入口的 Envoy 改成 2 个副本加 PDB（`deploy/cluster/envoy-gateway/gateway.yaml`），现在在 k3s-server 和 k3s-agent-1 上。
   - e2e 的偶发失败已修：`test/e2e/e2e_test.go` 的 `getOrder` 对带 `Retry-After` 的 503 重试（最多 10 秒）。本机 `make e2e` 通过，`make lint MODULES=$PWD/test/e2e` 0 issues。没有改服务。
   - 这一轮没有跑全量的 `make lint test`（Go 代码只改了 e2e 的一个函数）；脚本过了 shellcheck，chart 过了 `helm lint`。提交前按 AGENTS.md 跑一遍。
3. **etcd 的恢复演练和重启宿主机的检查都做了**（主人 2026-11-15 确认后做的，设计文档 2b 第 10 节的发现 17、18）：用 NAS 上的快照副本原地恢复，控制面中断 8 秒，快照之后的记号消失、之前的对象和所有 Pod 原样；重启宿主机后 240 秒内全部中间件自己恢复。演练的痕迹已清理（命名空间 `etcd-drill`、server 上的 `etcd-old-*`）。重启之后 Redis 主从的主库是 `redis-gate-0`。
   **2b 的收尾（已全部做完，PR #10 已合并；新会话从上面的“2c 做到哪”开始）**：
   - 笔记已补（Sonnet 写，主会话抽查了切换一章；现在 752 行，覆盖备份、恢复演练、切换、etcd、发现 10 到 18）。它报告的几处文档不一致已经改掉（设计文档里 Redis 的 `priorityClassName`、发现 5 的“恰好”、3.3 的标签描述；切换手册主库故障一节补了“提升前等从库把收到的事务应用完”的判据，那条语句在线上从库跑过）。
   - 文档已提交并推送，PR #10 已合并，最后一次推送的 CI 27 项全部通过。
   - 2c 的设计文档草稿已写（见“2c 做到哪”）。
4. 2b 留下、不在这个阶段做的（设计文档第 10 节“还没做的”）：换一台机器从快照加 token 恢复 etcd 没有演练；server 节点的内核日志里有少量 `profile="who"` 读 `/etc/passwd` 被 AppArmor 拒绝的记录（和信号无关，来源没有查）；备份失败没有告警（阶段 3）；备份脚本和 MySQL 共用 ConfigMap，改它会滚动重启 MySQL；主库故障时的切换和“停掉两个 broker”（阶段 4）；恢复耗时要在 2c 灌数据后重量；有流量时的恢复比对要用对账工具（2c）。没有查到的：Oracle MySQL Operator 的最新版本。
5. 2b 里要回头看的：etcd 的 fsync 延迟（空集群上 p99 在 8 到 16 毫秒，超过 10 毫秒的参考线，原因没有查）；CNI 已定为 flannel；空集群三个节点已经用了约 3.1 GiB，比预算里的 2 GB 多。入口多了一个 Envoy 副本（requests 128 Mi）。
6. 2c 要定的：服务的镜像怎么构建和推送（MacBook 已经可以直接 `docker push` 到集群的仓库）；各服务连中间件的地址（Redis 用 `sentinel://`、`cluster://` 的写法，MySQL 连 `<release>-primary`、`<release>-replica` 两个 Service，Kafka 连 `kafka-kafka-bootstrap.kafka.svc:9092`）。
7. 2a 的面试笔记已写：`docs/notes/2a-cluster.md`（Sonnet 写，主会话抽查并补了 DNS 排查的步骤和重启宿主机一节）。标了“通用知识”的段落面试前要对照官方文档核对。

2a 用到的版本都已登记到 `versions.md`。已查证、还没有用到的：Cilium v1.20.4。

主人已定的（2026-11-15；前三项已写成 ADR-0025、ADR-0026）：

- **入口控制器用 Envoy Gateway**（查到的最新版 v1.10.0，Gateway API 按它的兼容性表用 v1.6.1），取代已停止维护的 ingress-nginx；负载均衡用 MetalLB（地址池见 hardware.md）。k3s 自带的 Traefik 和 ServiceLB 要禁用（`--disable=traefik --disable=servicelb`）。
- **k3s 的数据存储用内嵌 etcd**（server 加 `--cluster-init`）。
- **CNI 由主会话按内存决定**：先用自带的 flannel，**2b 的中间件第一次部署完、量过内存之后、灌数据之前**回来改定（ADR-0026 的后果一节），够再换 Cilium（查到的最新版 v1.20.4）。换 CNI 等于重建集群。
- **换盘要同时保留 Windows 和 Ubuntu**（已按方案 A 做完，见上）。

## 上次做到哪

1c 分五步（设计文档 [design/1c-sharding.md](design/1c-sharding.md) 第 1 节）：分片、主从、CDC 索引表、跨分片查询与扩容、冷热归档。

**步骤一（分片）已完成**，阶段计划里前两个 checkbox 已勾：

- 布局（ADR-0012）：8 个 slot，`slot = user_id & 7`，`db = slot % 2`，表名带全局 slot 号（`orders_3`），每库一张 `outbox`。只拆 order。
- `pkg/sharding`（主会话写）：纯计算的路由。
- order 服务（Opus 子 Agent）：`mysqlx.MigrateTemplate` 按 slot 渲染迁移，迁移历史压缩成一个文件；store 的事务绑定 slot，跨 slot 返回 `ErrWrongShard`；每库一个中继（`order-0`、`order-1`）；sweeper 逐 slot 扫；落库消息基因不一致当毒消息。`MYSQL_DSNS` 取代 `MYSQL_DSN`（设了旧变量拒绝启动）。
- 脚本、Makefile、`test/load` 的 verify 和 wait-quiet（Sonnet 子 Agent）：建两个库，按 slot 汇总。
- 验证：`make lint test e2e` 通过，`make chaos` 两次 PASS。
- 实现中发现的问题记在设计文档 2.6（CHECK 约束名库级唯一、`payment_results` 主键只在 slot 内唯一、chaos 回收数偏高待复核）。

**Fable 评审（2026-11-13）**：1c 步骤一和 1b 遗留的背压代码没有一致性缺陷。发现并修掉了四个问题，细节在设计文档 1c 的 2.6.1 和 ADR-0011：

- 迁移没有锁（注释说有），也不可重入。`mysqlx` 用 `GET_LOCK` 实现了 goose 的会话锁，order 的建表改成 `IF NOT EXISTS`。
- order 两个连接池合计 100，加上其他服务超过 `max_connections`（151）。新增 `MYSQL_MAX_OPEN_CONNS`，每库默认 25。
- sweeper 没有按 slot 的时限，一个库挂起会拖住另一个库。新增 `SlotTimeout`，默认 10 秒。
- 背压在落库完全停住时周期性重新放行。`Claim` 记心跳，落库超过 `ADMIT_MAX_WAIT` 没有动静时只放进 `ADMIT_MAX_STALE + 1` 个探测订单。`Gate.Claim` 多了一个 `now` 参数。
- 分片布局改为只在 `pkg/sharding` 定义（`OrderSlots`、`OrderDBs`）。

前三项和布局由 Opus 子 Agent 实现，背压由 Opus 子 Agent 实现、Fable 逐行评审。合入后 `make lint test e2e` 通过，`make chaos` 两次 PASS（回收数都是 0）。

**chaos 回收数偏高与分片无关。** 旧日志显示分片前就有过 180 条（order 被杀 6 次）；回收只出现在 order 被杀之后，而且集中在其中一两次击杀上，和击杀次数的相关性弱。order 从启动到监听在分片前后都是冷启动约 1.4 秒、再启动 0.1 到 0.4 秒。推断的原因（没有直接量到）：`kill -9` 不会退出消费组，新实例要等旧成员的会话过期（10 秒）才分到分区，这段时间受理照常进行，而 chaos 把回收宽限期设成了 5 秒。chaos 的负载只有约 10 单/秒，队列到不了探测名额，所以这次的背压修复对它没有影响。chaos 里大量的 `claimed hold outlived twice the payment timeout` 是支付时限设成 5 秒造成的，对账通过。

## 下一步

**1c 步骤三（CDC 加商家索引表）已完成并推送（5 个提交，`400a97e` 到 `4b34bb7`，另有一个修 e2e 的提交）。** 设计在设计文档 1c 第 4 节，决定在 ADR-0014，实现中的发现在 4.13，结果在 4.14。`make lint test e2e` 通过，`make chaos` 两次 PASS。步骤一、二已推送，在 draft PR #6（`phase-1c`）里。

1. 下一步：
   - 步骤四已分 6 个提交推送（`baa80e3` 到 `e2a01c8`，在 draft PR #6 里），CI 的 24 项检查全部通过（2026-11-14）。
   - **CI 偶发失败，根因没有找到**：文档提交 `9a219ff` 的那次运行里，`test (./pkg)` 的 `TestMigrateConcurrentOnEmptyDB` 失败一次，8 个并发迁移中的一个报 `missing zero version migration`（goose 在版本表存在但没有 0 号记录时报这个错）。这次提交只改了文档，前一次运行（`e2a01c8`，代码相同）是通过的；本机带 `-race` 连跑 150 次没有复现。已经排除的：迁移锁的加锁、解锁和锁名（`pkg/mysqlx` 的 `getLockLocker`）读下来没有问题；goose v3.30.0 的 `up` 是先拿锁再建版本表、再读版本。一个没有证实的怀疑：MySQL 的建表会隐式提交，goose 把“建版本表”和“插入 0 号记录”放在一个事务里并带重试，如果插入那一步失败过一次，重试时看到表已存在就直接往下走，读到的是空表。再出现时先看失败的是不是第一个拿到锁的迁移。这关系到多实例同时在空库上启动，不只是测试问题。
   - 步骤四的结果在设计文档 5.11。最后一次 `make lint test e2e` 通过（2026-11-14，扩容之后）。`make chaos` 这次没有跑。
   - 商家索引这一半的造数报告已补（索引追平后量的，设计文档 5.11 的“商家索引”一段）。
   - **1c 步骤五（冷热归档）已完成并推送**（7 个提交，在 draft PR #6 里）。设计在设计文档 1c 第 6 节，决定在 ADR-0016，实现中的发现在 6.13，实测结果在 6.14。推送前 `make lint test e2e` 通过（2026-11-14），`make chaos` 没有跑。
   - 步骤五的要点：冷的定义是终态且订单号里的时间早于 90 天；读路径靠常量 `MinHotAge`（30 天）决定要不要查冷库，一律先热后冷；归档是读热、写冷、读回校验、在订单行锁里重读比对后删热；冷库是 `mysql` 实例上的 `order_archive_db`，压缩表按年分区。
   - **上线顺序是硬约束**：先给所有 order 实例配上 `ARCHIVE_DSN`，再第一次运行归档（设计文档 6.13）。`scripts/run-local.sh` 已经默认带上。
   - 步骤五留下、没有做的（设计文档 6.14 末尾）：
     - 大批量归档后冷库有碎片（多个 slot 并行、主键交错插入），这次手工 `REBUILD PARTITION` 了一遍，没有做进归档任务。
     - `GetOrder` 查冷库没有自己的时限，冷库不可用时没有降级（建议 1d）。
     - 归档任务没有进 `scripts/chaos.sh`；限速和等从库的路径没有自动化测试。
     - 用户列表落到冷库的比例没有量。
     - `load verify` 的第 4 到 15 项只读热库，长期运行的环境里会把已归档的真实订单当成不存在。
     - 面试笔记（`docs/notes/`）仍然没有写，考点已经补进阶段计划。
   - 步骤五推送后 CI 的 24 项检查全部通过（2026-11-14）。
   - 1c 的验收项都已满足。
   - **1d 步骤一（吞吐的欠账）进行中，改动都没有提交。** 设计在 [design/1d-protection.md](design/1d-protection.md) 第 2 节，决定在 ADR-0017（已采纳）。主人确认了：按步骤的顺序做、落库用分区内按 key 并发、商家索引先复用并发消费（达不到 1 万行/秒再做按批写）。
     - 主线已完成：`pkg/kafkax` 的 `Concurrency` 和 `MaxUncommitted`（`inflight.go`、`concurrent_test.go`）；order 的 `fetch` 改成先读占用再读订单行（`fetch_order_test.go`）。order 模块和 kafkax 的测试、lint 通过。
     - 三个子 Agent 的工作已验收合入：order 落库消费者开并发（`SUBMIT_CONCURRENCY`，默认 8）和并发落库测试，产品代码没有发现并发缺陷；orderindex 开并发（`INDEX_CONCURRENCY`，默认 16），测试库上约 7600 到 9000 条/秒见顶，瓶颈是每次提交的刷盘；压测工具的开环模式（`load run -mode open`）、`load poll` 子命令和 `scripts/bench.sh` 的 `BENCH_OPEN`、`BENCH_POLL`。
     - 共享的 `mysql` 实例的 `max_connections` 在 compose 里提到了 400（各服务的池加起来 164，超过默认的 151，服务开着时跑测试会报 1040）。
     - `make lint test e2e` 通过（2026-11-14）。以上改动已提交到本地的 `phase-1d`，**没有推送**。
     - 实测已做完，结果在设计文档 1d 的 2.10。`make chaos` 两次 PASS。要点：落库吞吐随并发度从 316 升到 686 单/秒就平了，`performance_schema` 显示瓶颈是 inventory 预占时 `sku_stock` 热门行的行锁（平均等 48 毫秒）加上每次提交约 7 毫秒；有查询压力时落库是没有时的 75%；商家索引在测试库上 7600 到 9000 行/秒见顶，瓶颈是提交刷盘。
     - 步骤一的前半已推送：draft PR #7（`phase-1d`），CI 的 29 项检查全部通过（2026-11-14，到提交 `9697914`）。
     - **步骤一的延续，进行中，都没有提交**（主人 2026-11-14 的决定）：
       - 商家索引按批写：设计在 1d 设计文档 2.11，ADR-0017 末尾有补充。要做 `kafkax.NewBatchConsumer`（Fable）和 orderindex 的改造（Opus）。
       - MySQL 侧库存扣减改成异步（方案 A：`stock_pending` 增量表加单个应用者合并更新）：设计在 1d 设计文档第 3 节，ADR-0018（已采纳）。主人接受了“MySQL 从阻止超卖变成发现超卖”的代价；方案 B（经 Kafka、order 不再调 Reserve）等 A 做完量过再定。产品代码由 Fable 写，测试铺开派 Opus，脚本和 verify 派 Sonnet（设计文档 3.6）。
       - 现状（2026-11-14）：两项都已实现、验收并实测，已提交并推送到 PR #7。`make lint test e2e` 通过，`make chaos` 两次 PASS（22 项）。
         - 商家索引按批写：测试库上批大小 500 时 4.8 万行/秒（带 `-race`），结果在设计文档 2.11。`INDEX_CONCURRENCY` 换成了 `INDEX_MAX_BATCH`。kafkax 的按批消费在暂时性错误时先整批重试 3 次再退回逐条。
         - 异步扣减：热点行的锁消失，落库吞吐在并发度 8 时从 642 到 789、16 时从 686 到 870；新的瓶颈是提交（每个订单三次，每次约 9 毫秒）。放宽刷盘后停在约 1050 单/秒，剩下的时间不在 MySQL 里，原因没有找到。结果在设计文档 3.9，测试铺开时发现并修掉的两个缺陷（超卖时预热失败、对账失效）在 3.8。
         - `load verify` 现在有 22 项，第 22 项是“库存增量都已应用”；wait-quiet 把 `stock_pending` 为空加进了静止条件。
         - 主人的决定（2026-11-14）：这一批提交并推送到 PR #7；**方案 B 先不做**，等有了埋点（步骤六）或上了集群再判断。步骤一的延续已分 6 个提交推送（`84b58d5` 到 `7b37be2`），PR #7 的 CI 30 项全部通过。
         - **1d 步骤二（网关限流）已实现、验收并实测，已提交并推送到 PR #7。** 设计在设计文档 1d 第 4 节，ADR-0019（已采纳），发现在 4.15、4.16，结果在 4.17。`make lint test e2e` 通过（e2e 18 个），`make chaos` 一次 PASS。
           - `pkg/ratelimit`（Fable）：进程内、Redis（Lua）、`Fallback`。网关的中间件、配置、客户端地址、规范（Opus，Fable 评审）。脚本、`bench.sh` 的 `BENCH_RATELIMIT`、e2e 用例（Sonnet）。
           - 各脚本的限流配置：`make run` 用默认值；e2e 关掉按 IP 的；chaos 和 bench 整体关闭。**从本机压网关而忘了关限流，现象是吞吐上不去或随机 429。**
           - 做限流时发现并修掉两个旧问题（设计文档 4.16）：受理的 `Hold` 被 Redis 客户端重试后不发落库消息、订单凭空消失（1b 起就有，`hold_retry_test.go` 钉住）；`redisx` 的客户端不遵守 ctx 的截止时间（已开 `ContextTimeoutEnabled`）。没改的：分布式锁的加锁脚本被重试时调用方以为没拿到锁。
           - 推送后 CI 失败两项，都已做根因分析并修掉（设计文档 3.10、4.16）：inventory 的应用者把“取到的增量先后不全”误报成超卖（设计漏洞：按 id 加锁扫描不是快照；改成被拒绝时用该 SKU 的全部增量重算，再用快照判断）；`pkg/lock` 的测试租期太短（`redisx` 的超时生效之后续期只有 100 毫秒）。应用者那个机制是推断的，本机没有复现。修复后的一轮 CI 里这两处都通过了，另有一个 kafkax 测试因为辅助函数查位点时遇到 `COORDINATOR_NOT_AVAILABLE` 而失败（测试辅助函数没有重试，不是被测逻辑），已改成重试到时限。
           - 步骤二最终的 CI：提交 `4db959f`，24 项检查全部通过。
         - **1d 步骤三（服务侧保护、超时与重试规范）已实现、验收并实测，改动没有提交。** 设计文档 1d 第 5 节，ADR-0020（已采纳），实测的 Envoy 行为和三个坑在 5.3，发现在 5.13，结果在 5.14。`make lint test e2e` 通过（e2e 18 个，经边车），`make fault` 五个场景全过，`make chaos` 一次 PASS。
           - 主人定的：不用 sentinel-go（停更五年）；服务之间的熔断和并发隔离用 **Envoy v1.40.2 边车，每个服务一个**；保护范围是全部下游调用；客户端断开时 gateway 返回 499。
           - 做了什么：五个边车的配置（`deploy/envoy/`）和 compose、`make envoy-validate`（进了 CI）；`grpcx.WithHopTimeout` 和 `grpcx.SidecarMargin`（进程里的时限 = 边车的路由超时 + 500 毫秒，三个服务各有测试核对）；`pkg/bulkhead`；order 的展示读并发上限（`READ_MAX_CONCURRENCY`，默认每库 16）、冷库上限和 1 秒时限、闸门调用 300 毫秒、从库读 500 毫秒；order 和 payment 的取消与超时映射；gateway 的热点参数限流（`RATE_LIMIT_HOT`、`RATE_LIMIT_HOT_ITEMS`）、499、按跳时限；`kafkax` 退避加抖动；`worker` 的 Guard 时限；`scripts/lib/sidecar.sh`（run-local、chaos、bench、fault 共用，`SIDECAR=off` 直连）；`scripts/fault.sh`（`make fault`）。
           - 实测：inventory 挂住时查库存前 5 次各约 3 秒、之后毫秒级 503，查订单和下单受理不受影响，解除后 5.5 秒恢复；同时 300 个请求时恰好 172 个被并发上限立刻拒绝；边车让受理延迟的 p50 多 0.7 毫秒、p99 多约 15 毫秒，落库吞吐不变。
           - 没做的（设计文档 5.13、5.14）：受理超时后清理和受理乱序留下的占用（要改闸门脚本）；`pkg/grpcx` 的日志拦截器对超时仍记 ERROR；预热被 1 秒封顶是否够用；按错误比例和慢调用比例的熔断（Envoy 单实例下没有）；`make fault` 没进 CI。
           - **本机现在多了五个 Envoy 容器**（`make up` 启动）。`make run`、e2e、chaos、bench 默认经过它们，它们没起来时脚本会提示先 `make up`。管理端口 19901 到 19905。
           - 已提交并推送（PR #7，HEAD `17cd38b`，五个提交），CI 25 项全部通过（新的 `envoy` job 17 秒；e2e 带五个边车 3 分 47 秒，是最长的 job）。第一次运行因私有仓库的 Actions 额度用完没有启动，主人把仓库改为公开后重跑通过。
         - **1d 步骤四（降级预案和优雅启停）已实现、验收并实测，已分 6 个提交推送到 PR #7（`5d30230` 到 `98ca954`），CI 25 项全部通过（2026-11-14；最长的是 e2e 3 分 42 秒，没有明显变慢的 job）。** 设计文档 1d 第 6 节，ADR-0021（已采纳），发现在 6.16，没做的在 6.17，结果在 6.18。`make lint test e2e` 通过（e2e 18 个），`make fault` 14 个场景全过，`make chaos` 一次 PASS（都是 2026-11-14）。
           - 主人定的：闸门的 Redis 不可用时**拒绝下单，不退回数据库扣减**（改了原计划）；开关在进程内、运行中可改；熔断器自己写；落库健康和列表的部分结果都做。
           - 做了什么：`pkg/breaker`、`pkg/degrade`、`pkg/app` 的 `/readyz`、`DRAIN_DELAY` 和第二次信号退出、`redisx.OpenLazy`、`mysqlx.OpenLazy`、`pkg/cache` 的熔断器和读时限、`lock.FailOpen` 的冷却、`errx.Degraded`；order 的闸门熔断器、落库健康（按订单库，受理时拒绝落不了库的订单）、回收任务在闸门恢复后等半个宽限期、从库健康探测、冷库的后台迁移和熔断、用户列表的部分结果（`archive_unavailable`）；order、inventory、payment 消费者对普通错误不设重试上限；inventory、payment 的启动和错误码；gateway 的降级错误映射（503 加 `Retry-After`）、三个开关和管理端口；`scripts/fault.sh` 的场景 F 到 N。
           - **新的约定（已写进 AGENTS.md）**：依赖坏了时返回 `errx.Degraded`（`FAILED_PRECONDITION` 加原因码），不返回 `UNAVAILABLE`，否则调用方的边车会把整个服务摘掉；消费者不设重试上限，handler 里重试无用的错误必须标 `Permanent`。
           - **本机的变化**：gateway 多了管理端口 `127.0.0.1:8089`（`ADMIN_ADDR`），降级开关在上面；其他服务的开关在各自的内部端口（8081 到 8084）的 `/degrade`。新的环境变量：`DEGRADE`（开关初始值，名字写错拒绝启动）、`DRAIN_DELAY`、`ORDER_READ_MAX_LAG`。`make fault` 会暂停本机共享的 Redis 和订单库的从库（结束时恢复，中途失败也会兜底恢复），跑它之前不要有别的东西在用。
           - 没做的见设计文档 6.17、6.18 末尾。要点：闸门脚本出错和 Redis 连不上分不开、payment 的扫描错误，这两种在数据损坏时会永久挡住分区；inventory 进程里的闸门没有熔断器；`Retry-After` 固定 5 秒；“Redis 暂停时还没被认领的订单恢复后落库”和回收任务的等待只有按模块的测试；`make fault` 没进 CI。
           - 一次没有解释的偶发失败：`pkg/cache` 的 `TestConcurrentColdKeyLoadsOnceInProcess` 在整包运行时失败过一次（回源 2 次，耗时 1.15 秒），之后重跑十几轮没有复现；耗时特征像本机容器网络的 1 秒停顿让一次读超时。order 的 `TestSubmitConcurrentWarmUpIsCoalesced` 也失败过一次，当时的输出没有留下，之后没有复现。
           - **1d 步骤五（对账）已实现、验收并实测，已分 4 个提交推送到 PR #7（`c8e8c9c` 到 `b09a74e`）**（2026-11-14）。设计文档 1d 第 7 节，ADR-0022（已采纳），实测结果在 7.12，发现在 7.15，没做的在 7.16。`make lint test e2e` 通过（e2e 18 个），`make chaos` 一次 PASS（`load verify` 和 `recon` 都通过）。
             - 主人定的（7.14）：摘要在数据库里算；节点摘要用行哈希的和，不是教科书式的 H(子节点)；闸门和 MySQL 的库存不纳入，维持 inventory 的 `reconcile`；只有商家索引可以自动修复。
             - 做了什么：`pkg/merkle`（`Digest`、规范编码和对应的 SQL 片段、订单号上的基数树、自上而下的 `Diff`）；新模块 `services/recon`（已进 `go.work`）：五对的投影（`replica`、`index`、`payment`、`stock`、`archive`，在 `internal/recon/pairs.go`）、数据源、比对、复查、报告、索引修复、命令行；`scripts/recon.sh`、`make recon`；`scripts/chaos.sh` 在 `load verify` 之后加跑 `recon -pairs payment,stock`，有差异时判失败。`pkg/merkle`、投影由 Fable 写，引擎由 Opus 写、Fable 评审后改了三处（修复改成按主键删、全量模式的分段、`archive` 改读主库）。
             - 用法：`ORDER_DBS=4 make recon`（默认比 `replica`、`index`、`payment`、`stock`，只读）；`RECON_ARGS="-pairs index -repair"` 修商家索引；归档的前后对比是三步：`RECON_ARGS="-pairs archive -snapshot-out F"`、归档、`RECON_ARGS="-pairs archive -against F"`，都要在归档任务不在跑的时候做。退出码 0 无差异、1 有差异、2 出错。
             - 实测要点（490 万行的 `index`）：无差异时树 4.8 秒、传约 1.7 万个摘要，全量归并 7.8 秒、545 MB；1 行差异多 0.3 秒；9612 行均匀散布的差异打中 95% 的节点，树比归并慢。交叉点取决于差异落在多少个节点里。
             - 推送后的 CI（`b09a74e`）：27 项里 26 项通过，新模块的 `lint (./services/recon)` 30 秒、`test (./services/recon)` 1 分 39 秒。失败的一项是 `envoy`（`make envoy-validate`）：前三个配置 OK，`deploy/envoy/orderindex.yaml` 那一次什么也没输出就失败了。这次推送没有改 `deploy/envoy/`，本机五个配置都通过。**根因没有找到**：原来的写法是 `docker run ... | tail -1 | grep OK`，失败时把校验器的输出丢掉了，日志里没有可看的东西。已把 `envoy-validate` 改成失败时打印 docker 的退出码和完整输出并重新推送；再出现时看那段输出（怀疑是 `docker run` 本身偶发失败，没有证据）。
             - 重新推送后（`1ac7c1c`）CI 27 项全部通过，`envoy` 这次也过了，所以那次失败的原因仍然不知道（最长的是 e2e 3 分 52 秒，其次 `test (./services/order)` 3 分 44 秒）。
             - **增量对账（窗口的下界）已加，已分 2 个提交推送到 PR #7**（2026-11-14，主人提出）：`recon -since 26h` 只比最近 26 小时的订单，和 `-settle` 合成一个窗口；`archive` 不支持。设计文档 7.17，ADR-0022 有补充。`make lint` 和 recon 模块的测试通过；本机 `index` 全量 4.7 秒、`-since 26h` 0.27 秒。改的文件：`services/recon/` 下的 `sql.go`、`source.go`、`rows.go`、`compare.go`、`report.go`、`cmd/recon/main.go` 和测试，`Makefile` 的帮助，三份文档。
             - 这次推送（`4e0f4ca`）的 CI 27 项全部通过（最长的是 `test (./services/order)` 4 分 1 秒和 e2e 4 分；`envoy` 没有再失败）。
             - **1d 步骤六（埋点）已实现、验收并实测，已分 6 个提交推送到 PR #7（`7826840` 到 `493e983`），CI 27 项全部通过**（2026-11-14；最长的是 `test (./services/order)` 4 分 27 秒、e2e 3 分 21 秒、`test (./services/gateway)` 3 分 13 秒，没有明显变慢的 job）。设计文档 1d 第 8 节，ADR-0023（已采纳），发现在 8.15，结果在 8.16，没做的在 8.17。`make lint`、`make test`（55 个包）、`make e2e` 通过，`make chaos` 一次 PASS（杀进程 18 次，`load verify` 和 `recon` 都通过）。
               - 主人定的（8.14，四项都按建议）：本机后端用 `grafana/otel-lgtm`（compose 的 `obs` profile）；`outbox` 加一列 `trace_context` 传递链路；异步一跳以父子为主，延迟关单和按批消费用 link；指标经 OTLP 推。
               - 做了什么：`pkg/otelx`（`Setup`、采样器、建指标和给 span 加属性的小工具）；`logx` 的日志带 `trace_id`、`span_id`；`grpcx`（`otelgrpc`）、`mysqlx`（`otelsql`）、`redisx`（`redisotel`）；`kafkax/telemetry.go`（生产和消费的 span、排队时长、重试和死信的计数）和分到分区的日志；`outbox` 的列加 order 的 `00004`、payment 的 `00003` 迁移（可重跑）；`breaker`、`degrade`、`cache` 的指标；gateway 入口的中间件（没有用 `otelgin`，原因在 8.4）、`X-Trace-Id`、限流和降级的指标；order、inventory、payment、orderindex、cdc 的业务指标和 span 属性；payment 的处理器在出结果处开一条链路（`payment.complete`）；e2e 的 `TestTraceSpansAllServices`（测试进程里起 OTLP 接收端）；`scripts/lib/obs.sh`、`make obs-up`、`make obs-down`。公共库和 payment 的那一处由 Fable 写；gateway、order、inventory 等四个服务派 Opus，order、inventory、payment 的 diff 由 Fable 逐行评审（只加观测，没有改行为）；公共库的测试、本机后端和脚本、e2e 用例派 Sonnet。
               - 实测要点：1% 采样的开销量不出来，全量采样落库吞吐低约 7%；一个订单是三条链路（下单到落库约 70 个 span、发起支付、支付结果），靠 span 上的订单号关联。**3.9 的悬案有了答案**：那 54 毫秒不在 order 进程里（子 span 没覆盖的时间中位数 0.15 毫秒），是“一次落库约 30 次往返乘以本机每次往返的代价”加上偶发的 200 毫秒 TCP 重传造成的长尾（中位数 27 毫秒、平均 55 毫秒）。
               - **新发现、没有查原因**：过载（开环每秒 2000 个请求）时，12 次运行里 5 次出现闸门的 Redis 调用成批超过 300 毫秒、熔断器打开、几百条消息重试，那一轮落库吞吐低 15% 到 25%，订单没有丢。和埋点无关（`always_off` 时也出现）。
               - **本机的变化**：`make obs-up` 启动 `otel-lgtm`（Grafana 在 http://127.0.0.1:3000 ，admin/admin；OTLP 4317/4318；内存上限 1.5 GiB，全量采样压测后到过 1.33 GiB）。它在运行时 `make run`、`bench.sh` 自动导出（`OBS=off` 关闭；`bench.sh` 默认 1% 采样）；e2e 把链路发给测试进程自己的接收端（14317），chaos、fault 不导出。**当前 `otel-lgtm` 容器在运行**，不用时 `make obs-down`。查链路和指标的命令在 8.10 之外另记在这里：Tempo 用 `curl -s -u admin:admin -G localhost:3000/api/datasources/proxy/uid/tempo/api/search --data-urlencode 'q={ resource.service.name = "order" }'`，Prometheus 用同样的前缀换成 `uid/prometheus/api/v1/query`。拆落库时间的脚本没有入库（在会话的临时目录里），压测的原始输出在 `tmp/bench/otel-overhead/`。
               - 新的约定（已写进 AGENTS.md 的“可观测”）：带 ctx 的日志；业务代码不手工建 span；指标的标签只能是有限的枚举；只有服务端故障标 Error；加指标不改行为、不新增查询。
               - 没做的见设计文档 8.17。要点：`sloglint` 没有打开；`order.gate.unclaimed_holds` 和 `inventory.stock_pending.backlog` 两个指标没有做；`make fault` 里指标对不对得上没有抽查；后端长时间不在时的表现没有跑过。
             - **阶段 1 收尾（2026-11-14，分支 `phase-1-wrapup`，从合并后的 `main` 开出）。已分 4 个提交推送（`a829857` 到 `7686015`），draft PR #8，CI 27 项全部通过（最长的是 `test (./services/order)` 4 分 20 秒、e2e 4 分 18 秒、`test (./pkg)` 3 分 14 秒；e2e 比 PR #7 的 3 分 21 秒慢了约 1 分钟，只看了这一次，没有查是波动还是变慢）。** PR #7 已合并进 `main`（合并提交 `d04e118`）。主人要求的四件事：
               - 合并 PR #7：已做。
               - 阶段计划里 1d 的四项旧条目（orderindex 按批写、落库性能、开环模式、1b 遗留的小问题）逐项在代码里核对过，已勾。1c 里有两项没勾的是有意留着的（读模式的定量对比等阶段 3；归档后自动重建分区没做）。
               - 面试笔记：`docs/notes/1c-sharding.md`（824 行）和 `docs/notes/1d-protection.md`（约 800 行，覆盖 104 条考点）由 Sonnet 写、主会话抽查。两份里按通用知识补充的段落都标了“通用知识”，设计文档里没有出处，面试前要对照官方文档核对。写笔记时查出的几处文档问题已改：**AGENTS.md 里边车时限的规则原来写反了**（“两处要一致”，实际是进程里的时限等于边车的数加 500 毫秒）；设计 1c 的 5.7、设计 1d 的 4.6 和 5.7、ADR-0014 里过时的内容加了注。
               - 8.17 里没做的，按收益做了三项：
                 - **过载时闸门调用成批超时，查到原因并修了**（设计文档 1d 的 8.18，ADR-0021 末尾的补充）。两个原因：查询把闸门的 Redis 连接池（原来 32）占满，排队的超时被算成闸门失败；一次约 300 毫秒的停顿让十几个在途调用同时失败，“连续 5 次”一下满足，熔断器打开 5 秒。修复：展示读读闸门占用要先占名额（`READ_GATE_MAX_CONCURRENCY`，默认 16），连接池可配置（`REDIS_POOL_SIZE`，默认 128）；`pkg/breaker` 加可选的 `MinFailureSpan`，闸门的熔断器设 1 秒。修后 6 轮压测熔断器没有再打开，重试从每轮 380 到 750 次降到 0 到 5 次；`make fault` 14 个场景全过。**那个约 300 毫秒的停顿本身来自容器网络还是 order 进程没有查到。**
                 - 两个积压量的指标（`order.gate.unclaimed_holds`、`inventory.stock_pending.backlog`），Opus 写、Fable 逐行评审。后者是全表扫描，见设计 8.17。
                 - `sloglint` 的 `context: scope` 已打开，作用域里有 ctx 的日志调用（约 66 处，九个模块）由 Sonnet 换成了带 ctx 的方法，主会话核对过 diff 里只有方法名和第一个参数的变化，没有加 `nolint`。
               - 一次没有解释的失败：`make e2e` 紧跟在 `make fault` 之后跑失败过一次，之后连跑 5 次通过。失败的输出被过滤掉了，不知道是哪个用例。再出现时把完整输出留下来。
               - 新的环境变量（order）：`REDIS_POOL_SIZE`、`READ_GATE_MAX_CONCURRENCY`。
               - 8.17 里仍然没做的：`make fault` 里抽查指标、后端长时间不在时的表现、压测工具用 `X-Trace-Id`、cdc 发 Kafka 没有 span、orderindex 的 `ListMerchantOrders` 错误映射。
               - 收尾的全部改动之后：`make lint` 各模块 0 issues，`make test` 全部通过，`make e2e` 通过，`make fault` 14 个场景全过（`make fault` 是在加指标和改日志之前跑的）。`make chaos` 这一轮没有跑。`pkg/lock` 的 `TestExperimentNaiveDelDeletesOthersLock` 在整仓库的 `make test` 里失败过一次，单独重跑通过，原因没有看。
               - 2026-11-15 进阶段 2 之前主人定的三件事（都只改了文档，见“计划变更记录”）：链路后端从 SkyWalking 换成 Tempo（ADR-0024）；Agent 的 Kubernetes 只读工具提前到 3d、pprof 工具提前到阶段 4；阶段 6 加 Gemini Decision 的对比实验。
               - 下一步：PR #8 已合并进 `main`（见“当前状态”）。阶段 1 的完成标准都满足了，可以进阶段 2（要先请主人开 Ubuntu 3900X 和 DS218+，装 helm）。
             - 动工前量过的数（本机冷库 482 万行）：行读到客户端 276 MB、7.3 秒；库里按 18.6 小时一个桶聚合 SHA-256 是 3115 个桶、8.5 秒；`CRC32` 5.0 秒；只 `COUNT(*)` 3.1 秒。MySQL 9.7 没有 `MD5()` 函数。
   - 步骤四留下、没有做的（都记在设计文档 5.11 末尾）：
     - 按用户的列表固定读主库，没有分流到从库（要在写路径上按用户再打一份读己之写标记）。
     - 造数的进度表不跟着 slot 走，扩容后不能用原参数续灌。
     - 多个 order 实例滚动切换没有验证（阶段 2）；CI 和 e2e 仍只跑 2 个库。
     - orderindex 消费者改成按批写，挂到了 1d。
     - 支付结果改成无限重试只有 `fence_test.go` 在 handler 层守着（返回可重试的错误），消费者配置本身（`MaxAttempts`）没有测试。
     - 面试笔记仍然没有写，考点已经补进阶段计划。
   - 步骤三留下、没有做的：
     - 把 CDC 和 orderindex 加进 `scripts/chaos.sh` 的击杀名单，并在对账前等索引追上（verify 的第 20 项现在在 chaos 里是跳过的）。“杀掉 CDC 再启动”目前只有 cdc 模块自己的集成测试覆盖。
     - 延迟的指标、告警和源库心跳，挪到阶段 3（见计划变更）。
     - `GET /orders/{id}` 不返回失败原因，多商家订单的 `MIXED_MERCHANTS` 只能从库里看到。
     - 多商家的订单在受理时不拒绝，落库预占时才失败（闸门里没有商家信息）。
     - 1c 的面试笔记（`docs/notes/`）还没有写，考点已经记在阶段计划里。
   - 步骤五（归档）动工前必须先定的两件事，记在设计文档 4.13：DELETE 在压缩的 topic 里怎么表示；行删除后同主键重插，版本从 0 重新开始。
   - 阶段 4 做故障切换时要验证：行版本号的正确性依赖 CDC 的源就是被提升的那个从库（ADR-0014 后果一节）。
2. 步骤一评审的遗留，没有做：
   - inventory 的 `00001_init.sql`（3 条建表）和 payment 的 `00002_outbox.sql`（建表后跟 `ALTER`）中途被杀后不可重入。并发启动已经由迁移锁解决。
   - 背压的第三项检查只管落库完全停住。消费者活着但落不了库（部分分区停住、极慢、认领后写库失败）时，原来的“触发、回收、重新放行”仍会出现，见 ADR-0011 的后果一节，建议 1d 处理。
   - `ListStaleCreating` 没有 `(status, updated_at)` 索引，决定不加：MySQL 没有 `ADD INDEX IF NOT EXISTS`，而 CREATING 的行很少。
   - 消费者没有“加入消费组、分配到分区”的日志，order 启动时也没有迁移耗时的日志，排查恢复耗时只能靠推断。
3. 步骤二（主从与读写分离）的结果见设计文档 3.10，决定见 ADR-0013。遗留：
   - CI 的按模块测试 job 改用 `make up-core`（只起 `mysql`、Redis、Kafka），只有 e2e job 用 `make up` 起主从。加主从后这些 job 各慢了 1 到 3 分钟，这是为了把时间降回去；本机 `make test` 同样只需要 `make up-core`。
   - 已知局限，建议 1d 和熔断降级一起做：从库整体落后时 `gtid` 模式的每次展示读先等满 100 毫秒再读主库；从库一直挂起时 sweeper 每轮约 1 分钟。
   - 定量的读模式对比挪到阶段 3（见计划变更）。

1b 步骤三遗留、仍未处理的两点（记在这里不丢）：

- `closed` 结局在正常时限下几乎不出现，CI 覆盖不到“支付对关单”的竞争，要稳定覆盖需要单独写一个用例。
- 支付时限从受理时刻起算，落库排队会挤占支付窗口。背压把排队限制在约 25 秒以内，影响变小了，但没有消除。

挂到后续阶段的（已写进对应计划）：落库串行与轮询拖垮落库、压测工具开环模式、几个小问题（1d）；集群上重测吞吐、Kafka 静态成员（阶段 2）。

## 本机开发环境

- `make tools` 安装 buf、proto 插件和 `oapi-codegen` 到 `./bin`（已装）。
- `make up-core` 只启动现有的 `mysql`、Redis、Kafka，够跑 `make test`。
- `make up` 启动依赖：现有的 `mysql`（inventory_db、payment_db、商家索引库和测试用的临时库）、order 的两主两从（端口见 `docs/infra/versions.md`）、Redis、Kafka，并建立复制。`make down` 停止并保留数据。
- **本机的订单库已经扩成 4 个**（2026-11-14 的演练）：`order_db_0` 到 `order_db_3`，各在自己的实例上，各带一个从库。500 万条造出来的历史订单（用户号从 2^40 起）**已经归档**：热库只剩约 16.7 万单（近 80 天的；1d 步骤五的演练又归档了 20717 单），其余 484 万单在 `mysql` 实例的 `order_archive_db` 里；热表已重建回收过空间。要重新得到“归档前”的状态只能清库重灌。多出来的 4 个实例属于 compose 的 `expand` profile，`make up` 不会启动它们：
  - 启动依赖要 `make up && make expand-up`。
  - 启动服务要 `ORDER_DBS=4 make run`。直接 `make run` 会因为 `shard_meta` 不符而拒绝启动，这是有意的。
  - 想回到干净的 2 库环境：`make down-volumes && make up`（会清掉全部数据，包括造的数）。
  - `make e2e`、`make test` 不受影响，它们用旧的两个实例上独立的库。
- 依赖在运行，六个服务**已停止**（2026-11-14 跑 1d 的压测前停的）。要起来用 `ORDER_DBS=4 make run`。停止时按进程号杀 `scripts/run-local.sh`，不要用 `pkill -f`（见会话日志的教训）。
- 共享的 `mysql` 实例的 `max_connections` 是 400（compose 里设的）。
- 对账：`ORDER_DBS=4 make recon`（只读，约 6 秒；增量用 `RECON_ARGS="-since 26h"`；不带 `ORDER_DBS=4` 会报 `orders_2` 不存在）。实测的原始输出和归档前的快照在 `tmp/recon/`。2026-11-14 最后一次运行四对加 `archive` 都没有差异。
- 归档：`ORDER_DBS=4 make archive`（本机是 4 个库，不带 `ORDER_DBS=4` 会被 `shard_meta` 校验拒绝），`make archive-dry-run` 只数不搬，`make archive-reclaim` 重建热表回收空间，参数用 `ARCHIVE_ARGS` 传。这次实跑的原始输出在 `tmp/archive/`。
- Kafka 的 Web 界面：`make kafka-ui` 启动 kafbat-ui（compose 的 `ui` profile，只读，http://127.0.0.1:8090），`make kafka-ui-down` 停止。看消费组的 lag 要点进具体的消费组。消息体是 protobuf，没有配置解码，看到的是原始字节。当前处于运行状态。
- 造数：`make seed-orders`（可中断续跑）、`make seed-report`。扩容：`scripts/expand.sh`（不带参数看用法），演练的状态文件在 `tmp/expand/`。
- order 的读模式由 `ORDER_READ_MODE` 控制（`primary`、`window`、`gtid`），`make e2e` 默认 `gtid`，`make run` 会连上从库但默认是 `primary`，要读从库时自己设这个变量。
- `make test` 的集成测试连接 `make up` 启动的 MySQL，每个测试自建临时库。
- `make run` 启动六个服务（订单库是 4 个时加 `ORDER_DBS=4`；gateway 8080、order 8081、inventory 8082、payment 8083、orderindex 8084、cdc 8085；gRPC 端口是 HTTP 端口加 1000），`make seed` 写入示例 SKU（1001 到 1003 属于商家 1，2001 属于商家 2），`make e2e` 在独立的 `e2e_*` 库上跑端到端测试。日志在 `./tmp/`。

## 阻塞项与待确认

- **`TestLandConcurrentRedeliveredShuffled` 在 CI 上偶发失败过一次**（2026-11-22，`0db0499`，重跑通过）：主人定了暂时不改，再出现时再说，见“3b 收尾”一段。

- （已定）1 亿数据量：主人 2026-11-22 定了不做，理由在 `docs/notes/3b-capacity.md` 第 8 节。

- 内存测试已做完：memtest86+ 完整跑了两轮没有错误（2026-11-17，主人在开机菜单里跑的），之后已卸载。

## 设备状态（上次已知）

| 设备 | 状态 |
|---|---|
| MacBook Pro | 备用；只用来远程连 Mac mini |
| Ubuntu 3900X | 开机（2026-11-17），内存 48GB（虚拟机各 14 GiB），k3s 三个节点 Ready，Ubuntu 26.04.1，有线经 PCIe 网卡 `enp41s0` 接在网桥 `br0` 上（千兆，192.168.50.7），Wi-Fi（.8，`ssh ubt`）和板载口（.6，插着一根线）是管理通道；三台虚拟机在运行并设了自启 |
| Mac mini | 开机，k6 2.4.0，有线接入；2026-11-21 起是开发机和集群操作端（见 `docs/infra/hardware.md`） |
| DS218+ | 开机（2026-11-17） |

## 计划变更记录


| 日期 | 变更 | 原因 |
|---|---|---|
| 2026-11-11 | 监控与压测提前到故障注入之前 | 对应的 ADR 已由主人删除 |
| 2026-11-11 | 只保留 `AGENTS.md`，不建 `CLAUDE.md` | 主人决定 |
| 2026-11-11 | 最小诊断 Agent 提前到阶段 3d，阶段 4 同步迭代，阶段 6 改为评测与完善 | 让 Agent 尽早积累真实故障下的运行记录 |
| 2026-11-11 | 埋点改用 OpenTelemetry，SkyWalking 作为链路后端 | 见 ADR-0004 |
| 2026-11-11 | 1b 增加分布式锁；1c 增加 binlog CDC 和冷热分离归档；1d 对账改用 Merkle Tree | 主人要求 |
| 2026-11-11 | 阶段 4 增加 Go 代码缺陷类故障，细化 MySQL 和 Redis 故障 | 覆盖常见生产故障 |
| 2026-11-11 | 版本选择规则：LTS 优先，其次最新稳定版 | 见 `docs/infra/versions.md` |
| 2026-11-11 | 1a 就实现了支付与关单的并发处理（条件更新加退款答复）和 Release 的空回滚 | 这两点是主流程正确性的一部分，无法推迟到 1b；1b 的对应任务改为在引入 Kafka 后复核 |
| 2026-11-11 | 设计修正：Release 改用 `ON DUPLICATE KEY UPDATE` 加锁并增加死锁重试；Reserve 被业务拒绝后一律 Release | 原设计在并发下死锁、泄漏库存，由测试和子 Agent 的评审发现 |
| 2026-11-11 | `app.Run` 传给组件的 context 不再随退出信号取消 | 原行为与“优雅停止时做完在途工作”矛盾 |
| 2026-11-11 | 1a 与 1b 之间增加「1a 补充：对外接口契约（OpenAPI）」，gateway 改为规范先行 | 见 ADR-0005 |
| 2026-11-11 | ADR 重新编号：原 0005（OpenTelemetry）改为 0004，新增的 OpenAPI 为 0005、金额表示为 0006 | 主人删除了原 ADR-0004（可观测先于故障注入），序号补齐 |
| 2026-11-11 | 「1a 补充」的 gateway 重构由主会话完成，没有派给子 Agent | 验证 ADR-0005 的待验证细节需要先把重构做出来，改动很小 |
| 2026-11-11 | 「从 OpenAPI 规范生成客户端」从「1a 补充」移到阶段 3b | 和压测脚本一起评估，避免做两次 |
| 2026-11-12 | 步骤三设计修正：受理失败用 `ReturnIfUnclaimed`；认领的占用单独成列表；落库时已过期的订单直接失败；创建超时从落库时刻算起 | 都是实现时由子 Agent 的评审和测试发现的，见设计文档 6.3、6.4 |
| 2026-11-12 | 1b 验收项“吞吐相对 1a 有数量级提升”移到阶段 2，在集群上重测；落库性能问题和压测工具开环模式移到 1d | 本机实测只有 1.7 到 3.7 倍，测量疑似受 OrbStack 端口转发干扰；主人决定 1b 只修积压与回收的冲突 |
| 2026-11-12 | 1b 增加受理端背压（ADR-0011） | 压测发现积压超过回收宽限期会丢掉已受理的订单 |
| 2026-11-12 | 背压修复由 Opus 编写和自评审，没有经过 Fable | Fable 没有额度，主人决定先由 Opus 完成；Fable 可用时补一次评审 |
| 2026-11-13 | 1c 拆成五步，步骤一只拆 order；payment、inventory 不分库 | 见设计文档 1c 第 1 节、ADR-0012 |
| 2026-11-13 | 1c 步骤一由 Opus 主持并派子 Agent 实现，订单状态相关代码待 Fable 评审 | Fable 不可用，主人决定可用后一并评审 |
| 2026-11-13 | 背压增加第三项检查（落库心跳加探测名额），ADR-0011 补充 | 评审发现原来的第二项检查会被回收任务抹掉信号 |
| 2026-11-13 | 步骤二的读模式定量对比（读主库比例、落库吞吐）挪到阶段 3 | 现在没有指标能统计读走了哪里，本机吞吐测量不可靠；端到端用例已量到两种模式的行为差异 |
| 2026-11-13 | 步骤三：CDC 同步延迟的指标、告警和源库心跳挪到阶段 3；两个可选项（CDC 替换 outbox 中继、ES）不做 | 现在没有指标体系；主人确认 |
| 2026-11-13 | 步骤三的并行任务没有用 worktree，三个子 Agent 在主工作目录里各改一个目录 | 主线的共享改动没有提交，worktree 里看不到 |
| 2026-11-13 | `super_read_only` 改由复制脚本在复制建立后设置，不写在 compose 启动参数里 | 官方镜像首次初始化会失败 |
| 2026-11-14 | 步骤四新增“按用户列出订单”接口；订单号 Epoch 提前到 2020-01-01 并清空本机开发库；订单库数由 `MYSQL_DSNS` 个数决定（修改 ADR-0012 第 4 条） | 见设计文档 1c 第 5 节、ADR-0015；主人确认 |
| 2026-11-14 | order 消费支付结果改为无限重试 | 读代码发现默认 5 次重试（约 1.5 秒）后支付结果进死信，订单库短暂不可用就会丢；扩容的围栏窗口必然触发 |
| 2026-11-14 | 路线图增加阶段 5：eBPF（可选），计划文件只到里程碑粒度 | 主人要求 |
| 2026-11-14 | 步骤四的三个并行任务仍在主工作目录里各改各的目录，没有用 worktree | 主线的共享改动没有提交 |
| 2026-11-14 | 步骤五：冷热按订单号里的受理时间划分，不按完结时间；用 Merkle 工具校验归档挪到 1d | 订单号决定冷热时读路径是纯计算，本系统受理到终态只隔一个支付时限；Merkle 工具 1d 才有。见 ADR-0016，主人确认 |
| 2026-11-14 | order 落库拒绝订单号早于 30 天的受理消息；针对已归档订单的支付结果从冷库推出答复 | 归档引入的两个一致性问题：旧订单号重新插入热库会盖住冷库里的订单；重放的支付结果会对付清的订单要求退款 |
| 2026-11-14 | orderindex 对 DELETE 从“忽略”改为“把删除前的映像做带版本号的 upsert” | 归档在热库上是 DELETE；商家索引保留归档订单的行，索引重建时这条消息是它唯一的来源 |
| 2026-11-14 | 步骤五的五个并行任务仍在主工作目录里各改各的目录，没有用 worktree | 主线的共享改动没有提交 |
| 2026-11-14 | 1d 分六步，先还吞吐的欠账；步骤一增加“MySQL 侧库存扣减改成异步”和“商家索引按批写” | 限流和熔断的阈值要对着实际容量定；实测发现瓶颈是热门 SKU 的行锁。见设计文档 1d 第 1 到 3 节、ADR-0017、ADR-0018 |
| 2026-11-14 | 1d 的服务侧保护不用 sentinel-go，服务之间的熔断和并发隔离改用 Envoy | sentinel-go 最新正式版是 2021 年的；主人不想引入停更的库，并提出这类组件在生产里通常是独立的。见 ADR-0020 |
| 2026-11-14 | 1d 步骤四：闸门的 Redis 不可用时拒绝下单，不退回数据库扣减；增加落库健康（受理时拒绝落不了库的订单）和列表的部分结果；落库和订单事件的消费者不设重试上限 | 退回数据库扣减在多实例下会超卖、切回要重新装载闸门，而 ADR-0018 之后 MySQL 已不阻止超卖。见设计文档 1d 第 6 节、ADR-0021；主人确认 |
| 2026-11-14 | 步骤四的三个并行任务（order 读路径、inventory 和 payment、gateway）仍在主工作目录里各改各的目录，没有用 worktree；故障注入脚本在它们验收之后才派 | 主线的共享改动没有提交；故障注入要暂停共享的 Redis，不能和别的测试同时跑 |
| 2026-11-14 | 1d 步骤五：闸门（Redis）和 MySQL 的库存不纳入 Merkle 对账，维持 inventory 现有的 `reconcile`；树的节点摘要用行哈希的和，不是教科书式的 H(子节点) | SKU 很少、数一直在变，逐个比就是对的做法；两边分片方式不同，摘要要能跨分片相加。见设计文档 1d 第 7 节、ADR-0022；主人确认 |
| 2026-11-14 | 1d 步骤六：`outbox` 表加 `trace_context` 列；本机多一个可选的 `otel-lgtm` 容器（`obs` profile）；步骤六的五个并行任务仍在主工作目录里各改各的目录，没有用 worktree | 链路要穿过 outbox 只能跟着数据走；埋点要被真实的后端收过才算数。见设计文档 1d 第 8 节、ADR-0023，主人确认。主线的共享改动没有提交，worktree 里看不到 |
| 2026-11-15 | 链路后端从 SkyWalking 换成 Grafana Tempo，不部署 SkyWalking | 本机已经在用 Tempo；三种信号统一成 PromQL、LogQL、TraceQL，Agent 只对接一套；省约 3 GB 内存；去掉 ADR-0004 的待验证项。见 ADR-0024，主人提出并确认 |
| 2026-11-15 | 阶段 6 增加“封闭式判断交给决策模型（Gemini Decision 1）”的对比实验，挂在模型分层下面；3d 和阶段 4 不接，主循环保持 Claude | 它只做是否、多选、打分，不能做主循环；适合判分、裁剪工具输出、告警分诊，接不接由评测数据决定。主人提出并确认 |
| 2026-11-15 | 诊断 Agent 的 Kubernetes 只读工具从阶段 6 提前到 3d，pprof 工具提前到阶段 4（Go 代码缺陷实验之前） | 阶段 4 的 Pod 和节点类故障、Go 缺陷类故障没有这两个工具基本诊断不了，运行记录也就没有评测价值。主人提出并确认 |
| 2026-11-15 | 2b 的 Redis 从“Redis Cluster 3 主 3 从”改为按用途分两套；Kafka 用 4.3.2 | 闸门的脚本不能在 Cluster 上运行；Strimzi 还不支持 4.4。见设计文档 2b 第 1、2 节；主人确认 |
| 2026-11-15 | 2a 的入口控制器不再考虑 ingress-nginx；虚拟机等宿主机换上 1TB 的盘再建 | ingress-nginx 已于 2026 年 3 月停止维护；现有系统盘只剩 170 GB，放不下原规划的 360 GB，主人不想先建小规格再重建 |
| 2026-11-15 | 2c：按 QPS 的 HPA 挪到阶段 3a；2c 只做按 CPU 的 | 集群上没有 Prometheus，没有自定义指标的来源。主人确认 |
| 2026-11-15 | 2c 增加“节点号的租约”（`pkg/idgen`，ADR-0028）；2d 的“服务网格（Istio）”从没有变成必做 | 读代码发现两个副本会发出重复的订单号；主人希望服务间的流量治理贴近真实环境，定为 2c 先用手写边车、2d 迁到 Istio |
| 2026-11-11 | 设计修正：支付服务退款的前置条件增加 `notified = 0`（第一次答复为准） | 原设计下矛盾的答复会把已付清订单的钱退掉，由子 Agent 发现 |
| 2026-11-11 | inventory 不建 outbox 表 | 步骤二里它只消费不发布，空表加没有中继容易误导；主人决定去掉 |
| 2026-11-11 | 设计修正：订单不存在且支付失败时答复 `ACCEPTED` 而不是 `REFUND_REQUIRED` | 没有收钱，不应要求退款 |
| 2026-11-16 | 2d 的 NetworkPolicy 一条改用网格的 mTLS 严格模式加 `AuthorizationPolicy` 实现（只管服务之间的 gRPC）；Argo CD、cert-manager、灰度发布这次不做 | 主人确认（ADR-0029 第 8 项）：网格自带，按身份比按地址准；其余几项留到有需要时 |
| 2026-11-17 | 三台虚拟机的内存从 10、16、16 改成各 14 GiB；监控的后端优先放在 k3s-server | 阶段 4 关 agent 节点时监控不能跟着没。见 ADR-0030，主人确认 |
| 2026-11-17 | 2c 留下的“新 order Pod 一就绪就 503”不按“冷连接池”去改，等监控装好后重现再定 | 读代码发现连接池在就绪前已经建满，原推断只有时间上的吻合 |
| 2026-11-17 | 3b 的“压测数据清理工具”不做成删数据的工具：压测的订单留在库里，每轮用新的 SKU 号，要回到基准状态时恢复备份 | 订单表的 DELETE 在系统里只表示归档（设计文档 1c 的 6.7），按号段删会在商家索引里留下冷库里没有的“已归档”订单。见设计文档 3b 第 6 节、ADR-0031。**主人还没有看过这一处** |
| 2026-11-17 | 3b 的“从 OpenAPI 规范生成客户端”评估的结论是不生成 | 主人确认。手写的结构是对规范的第二次独立解读，见 ADR-0031 |
| 2026-11-18 | 路线图增加两个可选阶段：阶段 10 LBS、阶段 7 推荐，计划文件只到里程碑粒度；`GOALS.md` 核心目标加第 8 条 | 主人要求：都是现实系统里比较核心的功能方向。场景和边界（LBS 定为同城配送、推荐只做工程不做算法、数据都是模拟的）是主会话拟的，**主人还没有看过** |
| 2026-11-18 | 没做的阶段按关联重排并**重新编号**（计划文件跟着改名，文档里的引用全部改成新号）：eBPF 9→5，诊断 Agent 5→6（5a、5b、5c 变成 6a、6b、6c），推荐 11→7，优惠券 6→8，风控 7→9，LBS 仍是 10，Rust 8→11。阶段 0 到 4 不变。推荐拆成 7a 入门和 7b 按需深入；6b 的检索按可复用来设计，推荐接着用；LBS 的场景维持同城配送，不做打车 | 主人要求：推荐先做入门，Agent 和推荐重合的部分复用，关联紧的阶段放在一起，编号和顺序一致。**这一天之前的提交信息、PR 和会话里说的阶段 5 到 11 是旧号**，本文件里更早的记录已经改成新号。理由见 `ROADMAP.md` 的顺序说明、阶段 7 和 10 的计划文件 |
| 2026-11-20 | 3b 的第 2 轮原定是“落库消费者的应用层组提交”（第 0 轮记录第 7 节第 2 条），改成“调大落库的并发加三个串行消费者改并发”；应用层组提交建议不做，等主人确认 | 不改代码的对照显示 MySQL 的组提交在并发够时已经把提交合并了，第 0 轮“单独调大并发没有用”的判断是错的。见 `docs/notes/3b-rounds/round-2-concurrency-and-consumers.md` 第 1 节 |
| 2026-11-21 | 应用层的组提交确定不做（主人确认）；3b 多了一项“闸门里的幂等键只留 1 小时”（ADR-0033）；“减少每单的提交次数”成为候选的下一个方向，还没有定 | 在宿主机上验证了盘变慢是这块盘自身的稳态行为，不是负载触发的（第 2 轮记录第 10 节）；压测把闸门 Redis 写满过 |
| 2026-11-21 | 接受 3b 现在的写入拐点（慢的一档下持续 125 到 150 单/秒）：不做“每单的提交次数减半”，不换盘；容量结论按这个上限写 | 主人确认。瓶颈和再往上的代价已经清楚，再做只是让这块盘上的数字翻倍。见第 2 轮记录第 10 节末尾 |
| 2026-11-21 | 开发机和集群操作端从 MacBook 换成 Mac mini（它同时还是发压机）；k6 在跑时不在它上面跑测试和构建 | 主人要求：Mac mini 一直开着，可以远程连上去做。见 `docs/infra/hardware.md` |
| 2026-11-21 | 3b 第 3 轮不做“202 的响应里给轮询的建议间隔”；“重定 HPA”从第 3 轮挪到扩容系数的那几轮之后 | 前者：服务端给不出比文档更准的数（要 order 把当时的积压带回来），改成把退避规则写进接口规范、429 带 `Retry-After`。后者：量到每个请求的 CPU 随请求组合变化两倍多，现在的 CPU requests 只有实际用量的四到六成，放开后两个服务在拐点的负载下会直接扩到上限；先要知道多副本有没有用（节点 CPU 和盘才是限制），再定 requests，最后才是每副本的请求数。见第 3 轮的记录第 1.1、6 节 |
| 2026-11-21 | 3b 第 3 轮多做了一项：落库健康的熔断器加“连续失败至少持续 1 秒” | 压测里它被一次停顿打开（在途的上百个落库同时超时），每轮白白拒绝一千个上下的下单；闸门的熔断器在 1d 加过同样的条件。见第 3 轮的记录第 4 节 |
| 2026-11-21 | 3b 的空表对照：不另起一套（`oppenheimer-e2e`），也不清库恢复；把长期运行的那一套临时指到同一批实例上的 `e2e_*` 空库（`loadtest.sh empty-up`、`empty-down`，values 在 `deploy/cluster/services/loadtest-empty/`） | 另起一套时副本数、资源、HPA、监控标签都和平时不同，还和原来那一套抢节点，对照里混进别的变量；清库恢复是破坏性的。指过去只改“数据在哪”，正式库一行不动，切回去就恢复。局限不变：空库和大库共用实例的缓冲池、redo 和盘 |
| 2026-11-22 | 3b 的弹性测试从“阶梯加压，看扩容跟不跟得上”改成“验证 HPA 扩容期间不丢请求”；gateway、order 的 HPA 上限从 4 收到 3；容量结论里“各服务的单副本能力”改成“每个请求的 CPU 开销”加“水平扩容在这套硬件上没有收益” | 扩容系数量到的是：节点不变时加副本不加能力，4 个副本比 2 个还差（记录第 3、5 节）。扩完不会变快，原来的问法量不出东西；单个副本从来不是瓶颈。主人 2026-11-22 同意 |
| 2026-11-22 | 3c：加三项（补四个 SLI 缺的指标、常驻的低速探针、`recon` 做成 CronJob）；CDC 源库心跳不做；SLO 的窗口用 7 天而不是 30 天；runbook 只给自己写的规则 | 设计文档 3c 第 1、2 节：平时没有流量时按比例的规则算不出来；支付处理时延、outbox 等待、最老的未消费消息、对账差异都没有指标；探针让源库一直有写入，心跳要解决的问题不存在了；Prometheus 只留 15 天。主人 2026-11-22 确认 |
| 2026-11-22 | 加 3e“对账方式按场景拆”（钱对笔数和金额、库存对守恒式、派生数据按 CDC 的变更核对），放在 3d 之后、阶段 4 的故障实验之前；其中“钱的笔数和金额做成指标”提前到 3c | 主人提出对账该按场景用合适的方式；现在的 `recon` 一律逐行摘要。全量对账量过（1473.7 万单 1 分 55 秒），不急，所以整体放在 3d 之后；阶段 4 的每次实验要用对账当裁判，所以在它之前。笔数和金额能做成指标，让钱的差异几分钟内报警，属于 3c 的业务告警。主人 2026-11-22 定 |
| 2026-11-22 | `recon-daily` 从比最近 26 小时改成全量（不带 `-since`，加 `ARCHIVE_DSN`） | 主人定的。全量量过只要 1 分 55 秒，每天一次的代价可以不计；全量能发现订单创建很久之后才出现的差异，窗口式的看不到。耗时随总量线性增长，告警里盯它的耗时 |
| 2026-11-23 | 3c 告警质量检查的登记表：第 9 项从“1.3 倍、1.15 倍”改成“用量除以 0.75、除以 0.82，只改主库”；第 5 项要 27 分钟而不是 15 分钟；第 3、4 项挪到其余几项之后做 | 1.15 倍是 87%，越过 order 停止受理的 85%，探针在跑会真的拒单；`ConsumerNotConsuming` 实际约 21 分钟才响；第 3、4 项会烧 SLO 的预算，中、慢档在恢复后还要响半小时到两小时，放在前面会和后面几项的告警混在一起 |

## 会话日志

### 2026-11-11

制定规划并落成文档。本机工具现状：Go 1.27.3、kubectl 1.33、Docker 29.8（OrbStack）已装；helm 未装，阶段 2 前安装。

同日完成阶段 0 并合并；开始 1a，完成契约、表结构、公共库和设计文档，四个服务派给子 Agent 并行实现；评审、合入、端到端测试和笔记全部完成，待推送。

同日讨论了金额用整数分的理由和对外接口是否用 OpenAPI。决定采用规范先行（`oapi-codegen`），写了 ADR-0005，在阶段 1 计划里加了「1a 补充」一节。另外补记了 ADR-0006（金额用最小货币单位的整数），这是 1a 已经执行的决定，不涉及代码和计划变更。本次只改文档。

同日完成 1a 收尾：文档改动作为独立提交进入 1a 分支，推送、开 PR #2、CI 全绿后合并到 `main`。

同日实施「1a 补充」：写了 OpenAPI 规范，引入 `oapi-codegen`，gateway 改为实现生成的接口，本机 lint、测试、端到端全部通过。随后提交、推送、开 PR 并合并到 `main`（git 操作派给 Haiku 子 Agent）。生成客户端的可选任务移到阶段 3b。

同日开始 1b：写了设计文档和 ADR-0007、ADR-0008，完成步骤一的公共库（三个子 Agent 并行实现，主会话评审合入，outbox 由主会话编写）。步骤二改了 proto 和脚本后暂停，服务改造尚未开始。

同日继续 1b：重新派出第二批四个子 Agent，全部完成。主会话评审合入，修了 outbox 的两处缺陷，`make lint test e2e` 通过，复核结论写入设计文档第 5.6 节。步骤二完成，已推送并开 draft PR #5；CI 的 e2e 首次失败（topic 创建后不可见），修复后全绿。

### 2026-11-12

完成 1b 步骤三。先细化设计文档第 6 节并写 ADR-0009，主会话写库存闸门和契约，order、inventory 派给子 Agent（中途因用量限制中断过一次，恢复后继续）。评审合入后修了两个设计问题，`make lint test e2e` 通过。

同日提交推送步骤三，CI 的 e2e 失败（支付与取消竞争的用例超时）。派 Opus 排查并修复：根因是测试造数耗尽了支付时限，产品代码没有一致性缺陷。主会话评审 diff 后推送修复，CI 全绿。随后细化了步骤四的设计（设计文档第 7 节、ADR-0010），主人确认后定稿（订单只缓存终态）。步骤四的代码尚未动工。

同日完成步骤四。主会话（Fable）写了 `lock.FailOpen`、`pkg/cache` 和 `redis-cache`；order、inventory、payment 的接入、压测工具、杀进程脚本、面试笔记、吞吐对比派给 Opus 和 Sonnet 子 Agent，主会话评审后合入。主人中途要求尽量把具体任务派给便宜的模型。杀进程测试发现消费者会话超时和下单重试两个问题，已修。吞吐没有达到数量级提升，主人决定挂到阶段 2。主人随后把会话切到 Opus，由 Opus 完成受理端背压（第一版只看排队时长，压测证明不够，改为按队列长度）。

同日按主人要求修了 e2e 的偶发失败（冷连接池遇上突发流量，新连接丢包要等 1 秒的初始重传超时），推送 `phase-1b` 并合并 PR #5。

### 2026-11-13

开始 1c。Opus 主持：写了设计文档 1c 和 ADR-0012、`pkg/sharding`；order 服务派给 Opus 子 Agent，脚本和对账工具派给 Sonnet 子 Agent，两者并行。合入后 `make lint test e2e` 通过，`make chaos` 两次 PASS。根据子 Agent 的报告补了设计文档 2.6，旧 `MYSQL_DSN` 改为拒绝启动。待 Fable 评审。

同日 Fable 评审步骤一和 1b 遗留代码，结论和修复见“上次做到哪”。修复派给两个 Opus 子 Agent 并行实现，e2e 和 chaos 的验证与回收数调查派给 Sonnet 子 Agent。提交和推送派给 Haiku 子 Agent，随后合入 `main` 并开 draft PR。

同日完成步骤二：设计（设计文档 1c 第 3 节、ADR-0013）经主人确认后，compose 拓扑和延迟用例派给 Sonnet 子 Agent，从库读和读模式派给 Opus 子 Agent，Fable 评审后返工一次。`make up` 现在起 9 个容器，本机依赖处于运行状态。

同日收尾：步骤二分三个提交推送，CI 全绿；加了 `make up-core`，CI 的按模块测试 job 改用它。下一个会话从步骤三的设计开始。

同日写了步骤三的设计（设计文档 1c 第 4 节、ADR-0014），主人确认后开始实施：主线完成了商家模型、行版本号和共享契约，修了容器重建后复制起不来的问题，三个子 Agent 并行实现 cdc、orderindex 和 gateway 的接口（中途因用量限制中断过一次），Fable 评审后合入；e2e 用例和对账项派给 Sonnet。步骤三完成并推送；CI 的 e2e 有一个竞争用例因取消窗口太窄失败，加宽后重新推送。

### 2026-11-14

确认 PR #6 的 CI 全绿。写了步骤四的设计（设计文档 1c 第 5 节，原第 5 节的步骤五要点挪到第 6 节）和 ADR-0015，主人确认了四项（Epoch 提前到 2020、改表名做围栏、散发加归并只放在造数工具里、默认 500 万单）。

主线（Fable）做了共享部分：库数由 DSN 个数决定加 `shard_meta` 校验、00003 迁移、`ListUserOrders` 的 proto、Epoch，清空了本机开发库。然后三个子 Agent 并行：按用户列出订单（Opus）、造数工具（Opus）、扩容脚本和 compose（Sonnet）。主线同时读代码确认围栏期间的行为，发现支付结果的消费者 1.5 秒后进死信并修掉，给 cdc 加了 `-print-checkpoint`。

评审后合入。扩容脚本改了三处（等待超时自动撤围栏、建从库移出窗口、校验不怕新写入）。灌了 500 万单并出报告，带流量演练了 2 库扩 4 库：窗口 9.3 秒，8181 个订单全部落库，对账和数据校验通过，结果在设计文档 5.11。做完后分 6 个提交推送（git 操作和看 CI 派给 Haiku），CI 全绿。

同日按主人要求在路线图里加了可选的阶段 5（eBPF），并回答了两个设计问题，要点记进了阶段计划的考点：为什么用 CHECK 而不是 UNSIGNED，`SKIP LOCKED` 适合什么场景。

同日商家索引追平，补了索引这一半的造数报告；按主人要求加了 kafbat-ui（Sonnet 子 Agent）。CI 在一次只改文档的提交上偶发失败过一次（并发迁移用例），重跑通过，根因未找到，记在“下一步”里。

同日写了步骤五的设计（设计文档 1c 第 6 节、ADR-0016），主人确认了四项（按订单号时间划分、压缩表加按年分区、列表冷热归并、商家索引保留）。主线（Fable）写了冷库迁移、冷库访问层、`MinHotAge`、落库拒绝和支付结果的冷库路径；五个子 Agent 并行：归档任务（Opus）、读路径（Opus）、orderindex（Opus）、脚本和 e2e（Sonnet）、对账和报告（Sonnet）。Fable 逐行评审了归档任务和读路径，补了归档任务的互斥锁和扫描语句的固定索引。

在本机 500 万单上实跑：482 万单 8 分 8 秒搬完，行数守恒，对账通过，从库延迟最大 1 秒；热库磁盘占用从 2273 MB 到 217 MB（要重建表才回来）；冷库刚归档完几乎没有压缩效果，重建分区后 906 MB。结果在设计文档 6.14。

同日主人同意提交，步骤五分 7 个提交推送，CI 全绿；随后合并 PR #6 进 `main`，开了 `phase-1d`。写了 1d 的分步方案和步骤一的设计（`docs/design/1d-protection.md`、ADR-0017），主人确认后开始实现：主线写了 kafkax 的分区内并发消费和 `fetch` 的新读顺序，三个子 Agent 并行做落库并发、orderindex 并发和开环压测。

同日继续 1d 步骤一的延续（主人定了 MySQL 异步扣减、商家索引按批写、推送并开 PR #7）。Fable 写了 kafkax 的按批消费和 inventory 的异步扣减（`stock_pending` 加应用者）；orderindex 按批写、inventory 测试的铺开派给 Opus，对账和脚本派给 Sonnet。测试铺开时发现超卖时预热和对账失效的两个缺陷，已修。实测见设计文档 2.11、3.9。

同日做 1d 步骤二（网关限流）：设计经主人确认后，Fable 写 `pkg/ratelimit`，网关接入派 Opus、脚本和 e2e 派 Sonnet。网关的子 Agent 提醒了 Redis 客户端的两个默认行为，核实后修掉了一个 1b 起就有的缺陷（受理脚本被重试后订单消失）。实测见设计文档 4.17。

同日做 1d 步骤三。查到 sentinel-go 停更五年，主人不想引入，并提出这类组件在生产里通常是独立的；最终定为每个服务一个 Envoy 边车。先在本机用可控的 gRPC 上游把 Envoy 的行为逐项验证（发现三个默认配置下不生效的坑），再写设计。Fable 写边车的配置和进程里的公共部分，gateway、order 和 payment 的接入派 Opus，脚本和故障注入派 Sonnet。实测见设计文档 5.14。

教训：引入一个没用过的组件，先用最小的探测把它的行为跑出来再写设计，文档里的默认值和“应该会怎样”靠不住（单实例下摘除不生效、慢下游不计入摘除，都是跑出来才知道的）；即使跑过，设计时也可能没把两个事实连起来（“调用方先放手不计入”和“两处时限相同”），子 Agent 的独立检查补上了这一环。

教训：并发测试本机通过不说明什么，CI 更慢、交错不同；论证“不会发生”时要分清一致性读（快照）和加锁读（不是快照）。

教训：客户端库的默认行为（会不会重试、时限从哪来）要读源码确认；子 Agent 报告里“待核实”的提醒值得逐条查，这次查出的问题比任务本身重要。

教训：一个值的取值范围变了（真实可售数可以为负），要把它所有的读者过一遍；改坏验证时测试没有失败，说明那条性质没有被断言（`SKIP LOCKED` 的害处是误报而不是算错，第一版测试只看了最终的数）。

教训：`pkill -f <脚本路径>` 会连带杀掉命令行里出现这个路径的其他 shell（这次杀掉了启动归档的外层 shell，归档进程本身没事）。要按进程号杀。MySQL 9.7 的客户端在 `-e` 里不认 `\G`，用 `-E`。

教训：直接在模块目录里跑 `go test` 时集成测试会因为没有环境变量而全部跳过，看起来是通过。要用 `make test MODULES=$PWD/<模块>`。

同日做 1d 步骤四。先读代码摸清每个依赖坏掉时的现状（发现闸门不可用超过约 26 秒时已受理的订单会进死信后消失、可选依赖会挡住启动等），写设计和 ADR-0021，主人确认四项都按建议（闸门不可用时拒绝下单，不退回数据库扣减）。Fable 写公共库和 order 的受理、落库、回收；order 读路径、inventory 和 payment、gateway 派 Opus 并行，Fable 评审；故障注入的场景派 Sonnet。实测见设计文档 6.18。

教训：一层保护的输出是另一层保护的输入。降级时返回的 `UNAVAILABLE` 会被调用方的边车当成服务故障、把整个服务摘掉；设计降级的错误时要把步骤三的熔断规则一起看。

教训：把测试输出过滤得太狠会把编译失败也滤掉。这次有两轮“通过”其实是测试没编译过，过滤条件里要留着 `build failed` 和每个包的 `ok`。

同日做 1d 步骤五（对账）。先在本机冷库上量了“行读到客户端”和“库里聚合摘要”的耗时，再写设计和 ADR-0022，主人确认四项都按建议。Fable 写 `pkg/merkle` 和五对的投影，引擎、修复、命令行派 Opus，Fable 评审后改了三处；脚本和 chaos 的接入很小，主会话直接写了。在 500 万单上实测并做了验收演练，结果在设计文档 7.12。

教训：`DELETE` 走不了索引时是带锁的全表扫描，会挡住这张表的所有写入；先用不加锁的读找到主键，再按主键删。评审时对每条写语句都要问它走哪个索引。

教训：性能对比的基线要先确认它自己没有被写慢（全量归并的第一版发了 2.9 万条大多读不到行的语句）；量之前先想清楚自变量是什么（这次是“差异落在多少个节点里”，不是“差异有多少行”）。

教训：给“什么算成功”下定义要想反例。落库健康第一版把“消息处理完了”当成功，而依赖坏着时被 sweeper 置为失败的订单也会“处理完”。

### 2026-11-15

阶段 2a 开工。摸清三台设备的现状后发现四件和计划不符的事：宿主机只连着 Wi-Fi；系统盘只剩 170 GB；R2S 的代理有白名单；ingress-nginx 已停止维护。主人插了网线、开了免密 sudo、给 NAS 加了公钥，决定换上 1TB 的盘再建虚拟机，并定了 Envoy Gateway、内嵌 etcd、CNI 按内存决定（ADR-0025、ADR-0026）。

同日做了：宿主机装 KVM 和 libvirt、建网桥（`host-setup.sh`）；整机备份到 NAS；换盘（方案 A：`pvmove` 在线迁移根分区，复制 `/boot` 和 EFI，重装 GRUB，中间重启验证，旧盘清空）；建 600G 的 `vmpool` 卷；写 `vm.sh` 并建好三台虚拟机。

同日新开一个会话：把上面的工作分两个提交存在本地（没有推送），写了 2a 的设计文档草稿。写之前对三台设备做了只读的探测并核对了版本，查出两处和之前记录不符的：Envoy Gateway v1.10 配的 Gateway API 是 v1.6.1；NAS 的 NFS 服务本来就开着。还没有装 k3s。

主人确认设计并建好 NAS 的共享文件夹之后，同一个会话里把 2a 做完：装 k3s、MetalLB、仓库、Envoy Gateway，最后删掉三台虚拟机用 `make cluster-up` 重建了一遍。`k3s.sh` 派给 Sonnet 写，其余清单和脚本很小，主会话直接写了；集群上的执行都是主会话做的。

教训：一项检查“通过了”要问它是不是碰巧通过。建虚拟机时验证过节点能访问镜像源，装 k3s 时却拉不到镜像：两个 DNS 上游一好一坏，第一次恰好用的是好的那个。有多个上游、多个副本、多条路径时，要逐个验证，不能只看整体的一次结果。

教训：Helm chart 的默认值要先 `helm show values` 看一遍再写 values。MetalLB 这次按记忆关了一个已经弃用的开关，真正的开关是另一个，结果多装了一套用不到的组件。

教训：本机的 `rm` 是 `trash`，不认 `-f`。在 `&&` 串起来的命令里它失败会让后面的步骤全部跳过；脚本和一次性命令里用 `command rm`。

教训：改公共库的类型之后要对**全部**模块跑 `make lint` 和 `make build`，不能只跑自己想到的那几个。这次按“谁在用 `*redis.Client`”搜了 `pkg` 和 `services`，漏了 `test/load`，推送后 CI 才发现。

教训：新环境装完要看内核日志，不能只看功能。2a 里所有 Go 程序的抢占信号一直在被 AppArmor 拒绝，但什么都“正常”，直到 2b 碰上一个靠信号退出的程序（mysqld）才暴露。排查的顺序是：先用不带任何自定义配置的官方镜像复现，排除自己写的 chart；再逐个去掉可疑的保护层（这里是 AppArmor）；确认之后去内核日志里找具体被拒绝的操作。

教训：子 Agent 报告里“我认为设计有问题”的那一条（主从角色记在 Pod 标签上，Pod 重建就丢）是对的，而且是主会话写设计时没有想到的。设计里凡是“把状态记在会被重建的对象上”的地方都要问一句：它重建之后谁来恢复这个状态。

教训：静态地址的网桥在没有载波时地址和路由都还在，而且比备用网卡的路由优先。说“留了一条备用通道”之前要想清楚主通道以各种方式坏掉时内核会怎么选路；这次是拔网线后整台机器从 IPv4 连不上，靠 IPv6 的链路本地地址才进去修。

教训：`set -o pipefail` 下不要写 `命令 | grep -q`，grep 匹配到就退出，前面的命令收到 SIGPIPE，整条管道算失败。这次让“停掉默认网络”那一步悄悄没有执行。

教训：链路只协商到百兆时，让网卡只通告千兆是个决定性的测试：起不来就是物理层少了线对，不用再猜驱动和设置。

### 2026-11-17

主人给宿主机加了两条 8GB 内存（威刚 3200，插 A1、B1；原有两条金士顿 16GB 在 A2、B2），共 48GB，XMP 关着，跑默认的 2400MHz；又装了 PCIe 网卡并把网线换到它上面。没有动业务代码，改了 `scripts/cluster/cluster.env`（`HOST_NIC="eno1"`）和 `host-setup.sh`（检查链路前先把接口打开并等自协商），都没有提交。

- 开机后有线和 Wi-Fi 的 IPv4 都连不上，从 Wi-Fi 的 IPv6 链路本地地址进去的。原因：网线换到了新网卡，新网卡没有任何 netplan 配置；`br0` 还绑着没有网线的板载口，但三台虚拟机的 `vnet` 口让 `br0` 仍然有载波，它的路由没有被标成 linkdown，发往局域网的包照样进了网桥。重跑 `host-setup.sh` 后恢复，`eno1` 协商到 1Gbps 全双工（对端只通告到千兆）。
- 经 IPv6 跑 `on-host.sh` 的写法：`HOST_SSH_OPTS="-o HostName=IPv6地址%%en0 -o HostKeyAlias=192.168.50.14" scripts/cluster/on-host.sh host-setup.sh`（`%` 要写成 `%%`，ssh 会展开它）。
- 宿主机上手工加了 Windows 的启动项（不在仓库的脚本里）：`/etc/grub.d/40_custom` 里一条按 EFI 分区 UUID `2014-FDA8` 链式加载 `bootmgfw.efi` 的菜单项，`/etc/default/grub.d/50-boot-menu.cfg` 把菜单设成显示 5 秒，默认仍是 Ubuntu。`grub-script-check` 通过，**没有重启验证过**。改之前的备份在宿主机的 `/root/40_custom.bak-20261117` 和 `/root/grub.cfg.bak-20261117`。
- `redis-gate-1`（从库）启动时报 `Bad file format reading the append only file appendonlydir/appendonly.aof.3.incr.aof at offset 63837`，已重启 23 次；主库 `redis-gate-0` 正常，`connected_slaves:0`。损坏在文件中间而不是末尾，所以 `aof-load-truncated` 救不了。多半是今天宿主机几次断电时虚拟机没有正常关机，没有证实。没有修。
- 内存没有跑过测试（memtester 或 memtest86+）。混插的内存在灌数据和压测之前应该测一遍。

教训：`ignore_routes_with_linkdown` 只在网桥自己没有载波时起作用。网桥上只要还有虚拟机的端口，上联口断了它也有载波，Wi-Fi 这条“备用通道”在 IPv4 上仍然是死的。上联断开时可靠的入口只有 Wi-Fi 的 IPv6 链路本地地址。

教训：给机器加一块网卡不等于它会自己联网。netplan 只配置写了名字的接口，新接口是关着的，路由器上也就看不到它的租约；这时读 `/sys/class/net/<网卡>/carrier` 会报错，和“没插网线”不是一回事。

同一天的后续（主人确认后做的）：

- 主人在 R2S 的代理名单里加了新网卡的 MAC，宿主机出网恢复（`curl https://github.com` 返回 200）。“停在哪”第 2 条里等 R2S 的那部分已经不用等了。
- `redis-gate-1` 修好了。损坏的样子：`appendonly.aof.3.incr.aof` 共 63976 字节，从 63837 起的最后 139 字节全是 0，之前的内容是完整的命令；文件的最后修改时间是 2026-11-16 23:55（北京时间）。这是断电的典型痕迹：文件长度已经落盘，数据块还没有。Redis 把它当成格式错误而不是截断，所以拒绝启动。修法：在 k3s-agent-1 上把 `appendonlydir` 和 `dump.rdb` 移到 `/home/ubuntu/redis-gate-1-appendonlydir.bad-20261117`、`/home/ubuntu/redis-gate-1-dump.rdb.20261117`（没有删），删 Pod 重建；从库向 Sentinel 问到主库后全量同步，主从各 17 个键，三个 Sentinel 都报 `slaves=1`、`master-link-status ok`。那两份备份看完可以删。
- 这件事值得记成面试考点（还没有写进阶段计划）：`appendfsync everysec` 在断电时除了丢最后一秒，还可能留下补零的尾巴；`aof-load-truncated` 只管“读到一半没了”，不管补零；`redis-check-aof --fix` 能截掉；从库的数据可以直接丢掉重新同步，主库不行。

同一天再往后（主人确认后做的）：

- NAS 开机后镜像仓库自己恢复了（`http://192.168.50.50:5000/v2/` 返回 200）。
- 网络测试：从 k3s-server 挂 NAS 的 NFS，直读 2GiB 是 113 MB/s（千兆跑满）；带缓冲写 77 MB/s，`oflag=direct` 每 1MB 落盘的写只有 20 MB/s（NAS 机械盘的同步写，不是网络）。`eno1` 接收错误为 0，向网关连发 200 个包 0 丢失。
- 内存测试：宿主机上 `memtester 22G 1` 跑了 1 小时 20 分，0 个 FAILURE，内核日志里没有硬件错误。它只测得到当时空闲的内存。装了 `memtest86+`（8.00），开机菜单的 `Advanced options for Ubuntu` 下有 `Memory test (mt86+x64)`，完整的一轮没有跑。
- 三台虚拟机的内存改成 10、16、16 GiB（主人同意的方案）：`vm.sh` 加了 `resize` 子命令（正常关机、`virsh setmaxmem` 和 `setmem --config`、开机），`cluster.env` 已改。按 agent-1、agent-2、server 的顺序一次一台，每台开机后 40 到 70 秒集群全部恢复。改完节点内存占用是 server 34%、两个 agent 24% 和 25%。
- 关 agent-2 时 Sentinel 做了一次切换，**Redis 主从现在的主库是 `redis-gate-1`**（上面“2a 做到哪”里写的 `redis-gate-0` 已过时）。
- 当天的改动（`scripts/cluster/cluster.env`、`host-setup.sh`、`vm.sh`、`docs/infra/hardware.md`、本文件）已在当天提交并推送到 `phase-2d`。

同一天最后（主人挪了网卡的插槽之后）：

- 主人把 Wi-Fi 和有线网卡从 `PCI_E4`、`PCI_E5` 挪到了 `PCI_E3`、`PCI_E4`（`PCI_E5` 的位置不好插拔网线）。两块网卡的接口名都变了（有线 `eno1` 变成 `enp41s0`，Wi-Fi `wlp41s0` 变成 `wlp35s0`），netplan 里的旧名字对不上，宿主机开机后两条路都不通，虚拟机在跑但没有上联。
- 恢复的过程：网线临时插到板载口（名字没变，安装器的 DHCP 配置还在），从 MacBook 用 `ping6 ff02::1%en0` 找邻居、按主机密钥认出宿主机，经板载口的 IPv6 链路本地地址登录；先把 `/etc/netplan/00-installer-config-wifi.yaml` 里的接口名改成 `wlp35s0`（原件在宿主机的 `/root/00-installer-config-wifi.yaml.bak-20261117`），Wi-Fi 起来后网线换回 PCIe 网卡，`cluster.env` 的 `HOST_NIC` 改成 `enp41s0`，经 Wi-Fi 的 IPv6 重跑 `host-setup.sh`。
- 恢复后验证过：`ssh ubt` 通，`enp41s0` 协商到 1Gbps 全双工，三台虚拟机和镜像仓库可达，三个节点 Ready，宿主机 `curl https://github.com` 返回 200。`redis-gate-1` 仍在 CrashLoopBackOff（上面记的 AOF 损坏，还没处理）。
- 没有做的：让 netplan 按 MAC 认网卡（挪插槽就不会再断）。向主人提过，主人还没定。
- 内存测试做完了：主人在开机菜单里跑 memtest86+，两轮没有错误。之后 `apt purge memtest86+`，菜单里的内存测试项没有了（`memtester` 还装着）。
- 启动菜单：Windows 那一项从 `/etc/grub.d/40_custom` 挪到了新建的 `/etc/grub.d/15_windows`，名字改成 `Windows`，排在 `Ubuntu`、`Advanced options for Ubuntu` 之后（这两项是 `10_linux` 一起输出的，中间插不进去）；`40_custom` 恢复成原样。`update-grub` 之后 `grub-script-check` 通过，默认项仍是 Ubuntu。**改完没有重启验证过，Windows 这一项也仍然没有实际启动过。** 改之前的备份在宿主机的 `/root/grub.cfg.bak-20261117-menu` 和 `/root/40_custom.bak-20261117-menu`。
- **宿主机的三个地址改成了连续的（主人要求）：板载 192.168.50.6、有线（`br0`）192.168.50.7、Wi-Fi 192.168.50.8。本文件更早的记录里的 .19 和 .14 指的是改之前的有线和 Wi-Fi。** 主人在 R2S 上改了三条静态绑定和代理白名单，把 MacBook 的 `~/.ssh/config` 里 `ubt` 改成了 .8；主会话把 `cluster.env` 的 `HOST_IP` 改成 .7 并重跑了 `host-setup.sh`，让 Wi-Fi 重新拿了地址，把核对过的主机公钥登记到 MacBook 的 `known_hosts` 里这三个地址下。验证过：`ssh ubt` 和 .7 都能登录，.19 和 .14 不再应答，三个节点 Ready，镜像仓库可达，宿主机 `curl https://github.com` 返回 200，脚本再跑一次没有变化。三块网卡的 MAC 不写进仓库，要用时在宿主机上 `ethtool -P <接口名>` 读。R2S 的白名单里旧的 .19 有没有删由主人处理。
- **板载口配成了兜底通道**（主人要求）：`cluster.env` 加了 `HOST_FALLBACK_NIC`、`HOST_FALLBACK_IP`、`HOST_FALLBACK_TABLE`、`LAN_NET`，`host-setup.sh` 生成的 netplan 里给板载口写静态的 `/32` 地址、一张单独的路由表和一条按源地址的策略路由（为什么这样写在脚本的注释里）。验证过：板载口插线后从 .6 能登录；把 `enp41s0` 关掉 25 秒，期间 .19 不通而 .6 能新建 ssh 连接；宿主机到虚拟机的流量仍然走 `br0`。板载口现在插着一根线（只协商到百兆，水晶头卡不住）。Wi-Fi 随后也做了同样的处理（主人要求）：`cluster.env` 加了 `HOST_WIFI_NIC`、`HOST_WIFI_IP`、`HOST_WIFI_TABLE`，地址仍由 DHCP 给，脚本只在安装器已经配好这个 Wi-Fi 接口时才补策略路由；同样关掉 `enp41s0` 25 秒验证过，期间 `ssh ubt` 能新建连接。`host-setup.sh` 还加了一道保护：新的 netplan 通不过 `netplan generate` 时把旧文件放回去。
- `docs/infra/hardware.md` 按主人的要求精简过：只写现状，过程性的记录（百兆的排查、换网卡、改地址的经过）不再放在那里，要查看本文件的会话日志。
- 这一段的改动（`scripts/cluster/cluster.env`、`scripts/cluster/host-setup.sh`、`docs/infra/hardware.md`、本文件）已提交在 `phase-2d` 上，没有推送。
- **`redis-gate-1` 又坏了一次，又修好了**。这次的经过：上午修好之后它在关 agent-2 时被切成了主库；下午主人搬机器、宿主机断电，它的 `appendonly.aof.4.incr.aof` 尾部又是 142 字节的 0（文件 70503 字节，从 70361 起；最后修改 14:56 北京时间），起不来；Sentinel 把主库切回了 `redis-gate-0`。修法和上午一样：在 k3s-agent-1 上把 `appendonlydir` 和 `dump.rdb` 移到 `/home/ubuntu/redis-gate-1-appendonlydir.bad-20261117-2`、`/home/ubuntu/redis-gate-1-dump.rdb.20261117-2`，删 Pod 重建，它作为从库全量同步，主从各 17 个键，三个 Sentinel 都报 `master-link-status ok`。它坏的时候是主库，断电前最后那一点没有同步出去的写入丢了（内容是限流和租约这类会过期的键，没有去比对）。`/home/ubuntu` 下的四份备份已经按主人的要求删了。
- **同一天坏了两次，说明宿主机每次断电都可能让某个 Redis 起不来，要人工处理。** 没有修，留给阶段 4（故障恢复）定：可选的做法是启动前用 init 容器跑 `redis-check-aof --fix`，或者从库起不来时自动清空数据重新同步；主库不能这样做，会丢数据。面试考点仍然没有写进阶段计划。
- 主人说明了断电的原因：调试内存和网卡时没有接显示器，只能强制关机；现在硬件都就绪了，以后不太会再发生。所以上一条说的“每次断电都要人工处理”不急，仍然留给阶段 4。
- 清理备份（主人要求）：宿主机 `/root` 下 Wi-Fi 配置的备份和上午那两份 GRUB 备份删了。**留着的**：`/root/grub.cfg.bak-20261117-menu` 和 `/root/40_custom.bak-20261117-menu`（改启动菜单之后宿主机还没有重启过，重启一次能正常进 Ubuntu 就可以删）；`/etc/fstab.before-migration` 和 NAS 上的整机备份 `ubt-20261115`（16G，换盘之前的）随后主人确认，也删了。
