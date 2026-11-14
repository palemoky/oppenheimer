# 阶段 9：eBPF（可选）

**目标**：学会用 eBPF 从内核一侧观察这套系统，补上应用埋点看不到的部分（网络、调度、磁盘、系统调用），并弄清它和 OpenTelemetry 埋点各自适合什么。
**说明**：可选阶段，只到里程碑粒度，动工前再细化。eBPF 只能跑在 Linux 上，实验都在 Ubuntu 的集群节点上做；MacBook 上的 OrbStack 虚拟机不作为目标环境。依赖阶段 3 的可观测体系，最好在阶段 4 之后做：手里有真实的故障场景，才知道内核视角多看到了什么。

## 候选方向（动工时按兴趣和阶段 4 的遗留问题选 2 到 3 个）

- **现成工具排查问题**：bpftrace 和 bcc 工具集（`tcpretrans`、`tcplife`、`offcputime`、`biolatency`、`runqlat`）。拿项目里已经遇到过的现象做练习，例如 1b 的“冷连接池遇上突发流量，新连接丢包要等 1 秒的初始重传超时”，当时是推断出来的，用 `tcpretrans` 可以直接看到。
- **不改代码的可观测**：Grafana Beyla 或 Pixie 从内核抓 HTTP、gRPC、SQL 的请求，和现有的 OpenTelemetry SDK 埋点对比，看各自缺什么。
- **持续剖析**：Parca 或 Pyroscope 的 eBPF 采集器，全集群的 CPU 火焰图，对比 Go 自带的 pprof。
- **网络层**：把集群的 CNI 换成 Cilium，用 Hubble 看服务间的流量；了解它怎样替代 kube-proxy 的 iptables 规则。
- **自己写一个小程序**：用 cilium/ebpf（Go）写一个 kprobe 或 tracepoint 程序，例如统计每个服务的 TCP 重传或连接建立耗时，把结果作为指标导出。
- **接入诊断 Agent**：把上面某一种数据作为 Agent 的一个新工具，在阶段 4 的评测集上看诊断有没有变准。

## 任务

- [ ] 选定方向，写 ADR 说明选择依据，以及节点内核版本、BTF、权限上的前提
- [ ] 在集群节点上搭好环境，所有部署用清单文件或 Helm 入库
- [ ] 每个方向做一次“用 eBPF 看到了原来看不到的东西”的实验，按阶段 4 的模板留记录
- [ ] 量采集的开销（CPU、延迟的变化）
- [ ] 写总结：eBPF 与应用埋点各自的边界，什么时候该用哪个

## 验收标准

- 至少有一个问题是靠 eBPF 的数据定位或证实的，并且说得清为什么应用埋点看不到。
- 有采集开销的数据。

## 面试考点

eBPF 是什么、验证器保证了什么；kprobe、tracepoint、uprobe、XDP、tc 这些挂载点的区别；map 和 ring buffer；CO-RE 和 BTF 解决了什么问题；对 Go 程序挂 uprobe 的坑（uretprobe 与 goroutine 栈的移动冲突、调用约定是寄存器传参）；eBPF 可观测与 SDK 埋点的取舍（不改代码、看得到内核，但拿不到业务语义，TLS 加密的流量要另想办法）；Cilium 怎样替代 kube-proxy；采集开销从哪里来。
