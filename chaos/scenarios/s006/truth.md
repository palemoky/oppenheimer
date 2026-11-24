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

没有核实的几件事，第一次用之前在 `oppenheimer-e2e` 里试：

- 超过 limit 时被杀的是整个容器还是只有那个占内存的进程。按设计推是整个容器（cgroup v2 下 Kubernetes 默认把一个容器当成一组一起杀），
  如果只杀了那个 worker，业务进程不受影响，这个场景就不成立，要换写法。
- 容器重启之后 Chaos Mesh 会不会对新的容器再注入一次（会的话 5 分钟里会反复 OOM，`restartCount` 加好几次，进入 CrashLoopBackOff 的退避）。
- 冒烟只在探测 Pod 上试过 96 MB、没有超过 limit 的情形。
