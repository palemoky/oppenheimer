# 容器内存：被 OOMKilled 和接近上限

覆盖的告警：`ContainerOOMKilled`（warning）、`ContainerMemoryNearLimit`（warning）

## 这条告警在说什么

两条都排除命名空间 `oppenheimer-e2e`（那里的 OOM、内存高是故意制造的故障注入）。

**`ContainerOOMKilled`**：某个容器最近一次终止的原因是 `OOMKilled`（`kube_pod_container_status_last_terminated_reason{reason="OOMKilled"} == 1`），并且它的重启次数在 10 分钟内有增长（`increase(kube_pod_container_status_restarts_total[10m]) > 0`），两个条件一起，持续 1 分钟。两个条件一起是免得“很久以前 OOM 过一次”一直响；代价是告警最多保持 10 分钟，之后自己恢复，不代表问题没了。意思：容器内存超过 limit，被内核杀了并重启。

**`ContainerMemoryNearLimit`**：容器的常驻内存（`container_memory_rss`）超过内存 limit（`kube_pod_container_resource_limits{resource="memory"}`）的 90%，持续 10 分钟。只算有内存 limit 的容器，除法左右按 `namespace, pod, container` 对齐。**按 RSS（匿名页）算，不按工作集（working set）**：工作集里有活跃的页缓存，内存紧时内核会回收，不会导致 OOM。2026-11-22 实测 Kafka 三个 broker 的工作集是 limit（1.5 GiB）的 87% 到 96%，RSS 只有 67% 到 71%，按工作集写这条会一直响。MySQL 的 buffer pool、Redis 的数据、Go 的堆都是匿名页，RSS 量得到。出处：[设计 3c 第 8 节“第 4 步的另一半”](../design/3c-slo-alerting.md)、[3b 设计第 6 节第 4 步](../design/3b-load-testing.md)。设计里记着：`OOMKilled` 的序列长什么样，集群上没有样例，标签是推断的。

## 影响

被杀的容器重启，重启期间这个副本不可用：MySQL 主库被杀就是几秒到几十秒写不了（3b 灌数时 `mysql-order-0-0` 被杀，7 秒后重启完成）；服务的副本被杀，边车会摘除它（见 [mesh-ejection.md](mesh-ejection.md)）。MySQL 被杀后崩溃恢复，数据没有丢（3b 灌数那次主从的 `gtid_executed` 一致，没提交的事务回滚）。从库被 OOMKill 不需要重建（ADR-0032：数据已经在页缓存里）。

接近上限是预警：再涨就会被杀。`ContainerOOMKilled` 响了，当天处理；反复重启要尽快。

## 先看什么

1. 哪个容器、是 OOM 还是接近上限：

   ```sh
   scripts/cluster/monitoring.sh alerts
   scripts/cluster/monitoring.sh promql 'kube_pod_container_status_last_terminated_reason{reason="OOMKilled",namespace!="oppenheimer-e2e"} == 1'
   ```

2. 常驻内存占 limit 的比例，和工作集对照：

   ```sh
   scripts/cluster/monitoring.sh promql 'topk(10, container_memory_rss{container!="",container!="POD",namespace!="oppenheimer-e2e"} / on (namespace, pod, container) kube_pod_container_resource_limits{resource="memory",namespace!="oppenheimer-e2e"})'
   scripts/cluster/monitoring.sh promql 'container_memory_working_set_bytes{namespace="oppenheimer",container!="",container!="POD"}'
   ```

3. 重启次数和 Pod 事件：

   ```sh
   scripts/cluster/monitoring.sh promql 'increase(kube_pod_container_status_restarts_total{namespace!="oppenheimer-e2e"}[1h]) > 0'
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n <命名空间> describe pod <pod> | grep -A8 'Last State'
   ```

4. 上一个容器的日志，和这个容器的内存是什么在占（要换成实际的命名空间、Pod 名）：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n <命名空间> logs <pod> -c <container> --previous --tail=100
   ```

5. 有没有压测、灌数、备份在跑（项目里的 OOM 都和这类大负载一起出现），Go 服务看堆（pprof 只开在回环上，见 [3a 笔记第 13.2 节](../notes/3a-observability.md)）。

## 常见的原因和各自的处理

1. **MySQL 灌数时内存涨。** 记录：[3b 笔记第 9.1 节](../notes/3b-load-testing.md)：1000 万单灌数，分片 0 主库撞 1 Gi 上限被杀；`mysql-main` 在商家索引追数时撞 1536 Mi 被杀。处理：订单库提到 2560 Mi、缓冲池 1 G，`mysql-main` 提到 2560 Mi（当时是主人同意后改的，改了要滚动实例）。内存要算缓冲池、约 400 MB 的固定开销、分配器的碎片约 400 MB 三样，不是只给缓冲池。灌完之后订单库仍然在 1854 到 1902 Mi，贴着 2048 的上限（那是在提到 2560 之前，见 `docs/PROGRESS.md`）。
2. **备份 Job 写 NFS，页缓存算进 cgroup。** 记录：3b 笔记第 9.2 节：上限 256 Mi，开始十几秒就被杀；`mysqldump` 加 `zstd` 的匿名内存只有 41 Mi，占内存的是输出文件的页缓存（脏页回写慢）。提到 1 Gi 后成功。**这类 OOM 是 RSS 看不见的**，`ContainerMemoryNearLimit` 对它没有预警；而且 Job 的 Pod 不重启，`ContainerOOMKilled` 要求重启次数增长，可能也不响（我的判断，没有验证，见报告里的规则问题）。
3. **监控的后端在大写入量下涨。** 记录：Tempo 在过载压测时被 OOMKilled（round-0 的 18:30，round-1 的 15:46，上限 2 Gi，后来提到 3 Gi），内存跟着写入量走，不是泄漏（3a 笔记第 12 节）。处理：3b 不要关尾部采样压测。
4. **对账的 `verify` 一次性 Pod。** 记录：round-9 的第 4 节：180 万单的 `verify` 30 分钟没跑完、输出是空的，内存上限从 1 Gi 提到 6 Gi 后一分半钟跑完；**没有直接看到 OOM 记录，是推断的**。
5. **Go 服务的堆涨到 limit。** `GOMEMLIMIT` 设成 limit 的约 90%（[2c 设计 7.4](../design/2c-services.md)），所以 Go 服务的 RSS 正常也可能贴近 90%，`ContainerMemoryNearLimit` 对它们可能边缘上响（推断，没有遇到过）。`gateway` 的堆缓慢上涨 3b 没查原因（设计 3c 第 7 节）。
6. **Redis 内存满。** 不是容器的 OOM：round-2 里闸门 Redis 报 `OOM command not allowed when used memory > 'maxmemory'` 是 Redis 自己的 maxmemory，见闸门内存的告警，不在这份里。

## 怎么确认恢复了

`ContainerOOMKilled` 在最后一次重启 10 分钟后自己恢复，**不代表不会再被杀**：确认重启次数不再增长，且内存占比有余量：

```sh
scripts/cluster/monitoring.sh promql 'increase(kube_pod_container_status_restarts_total{namespace!="oppenheimer-e2e"}[30m]) > 0'
```

没有结果，并且第 2 步里的占比低于 0.9。

## 怎么人为触发它

设计 3c 第 5 节第 2 层：内存这类不方便真做的，临时把规则的阈值改到当前值以下（比如 `0.9` 改成 `0.3`），响了再改回来；**这只验证了“路是通的”**。这一招只适用于 `ContainerMemoryNearLimit`。

`ContainerOOMKilled`：设计里的 3c 第 8 节写明集群上没有样例。**待定**，建议在一个不是 `oppenheimer-e2e` 的临时命名空间里起一个内存 limit 很小、会分配内存的 Pod（没有验证过），看序列的标签和告警的行为，验完删掉。不要对真实的 MySQL 或 Kafka 这样做。

故障注入动手前要先登记到 `docs/plan/phase-3-observability.md`；重置节点、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
