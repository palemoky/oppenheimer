# s006 的答案

## 标准根因

gateway 的一个副本的业务容器里被放进了一个占内存的进程（Chaos Mesh 的 StressChaos：1 个 worker，要 640 MB），
容器的内存 limit 是 512 Mi，所以整个容器因为超过 limit 被内核杀掉（OOMKilled），kubelet 把它重新拉起来。
gateway 自己的进程没有内存泄漏。

## 定位必需的关键证据

- 这个 Pod 的业务容器上一次退出的原因是 OOMKilled，`restartCount` 增加；`ContainerOOMKilled` 告警。
- 容器的内存（cAdvisor 的 working set）在几十秒里从平时的水位直线涨到 limit，**不是**几小时慢慢爬上去的；
  同一时间 Go 运行时报的堆（在用的字节数、对象数）是平的：涨的那部分不在 Go 的堆里。
- 另一个副本的内存正常；请求量和请求的大小没有变。
- 重启之后如果抓得到 heap 剖析，里面没有异常的大户。
- 被杀的那一刻，入口到这个副本的在途请求断掉（入口的 5xx 有一小簇）。

## 容易误判成的原因

- gateway 的内存泄漏（Go 的堆是平的；泄漏是慢慢涨，这里是直线）。
- limit 设小了（平时的水位离 limit 很远）。
- 大请求或者流量突增把内存撑爆（请求量和大小没有变，另一个副本正常）。
- goroutine 泄漏（goroutine 数没有涨；别和 s001 混了）。

## 备注

B1（2026-11-24，`oppenheimer-e2e`，没有负载）试过：注入后 10 秒内被选中的副本的业务容器 `OOMKilled`、`restartCount` 0 → 1，整个容器被杀（不是只杀占内存的那个进程）。
**容器重启之后 Chaos Mesh 没有再注入**：之后的 50 秒里它一直正常运行，所以这个场景是“OOM 一次”，不是反复 OOM。
没有量的（要有负载）：入口的 5xx 有多少，`ContainerOOMKilled` 多久响。
长期运行的那一套里 gateway 平时的内存水位比 e2e 高，640 MB 仍然超过 limit，结论不变。
