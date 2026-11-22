# 入口和网格的并发上限溢出

覆盖的告警：`IngressRejectingRequests`（warning）、`MeshConcurrencyLimitOverflow`（warning）

## 这条告警在说什么

**`IngressRejectingRequests`**：入口 Envoy（`job="monitoring/ingress-envoy"`）到后端的三种溢出里任何一个在 5 分钟窗口里有增长，持续 2 分钟，按 `envoy_cluster_name` 和 `signal` 分：

- `signal="active"`：在途请求到了上限（`maxParallelRequests` 1024），`envoy_cluster_upstream_rq_active_overflow`。多出来的直接答 503，访问日志里是 `UO`。round-6 7.3 查明过载时主要是这一条：2384 请求/秒那一档每秒 530 个 503，而排队溢出是 0。
- `signal="queue"`：排队也满了，`envoy_cluster_upstream_rq_pending_overflow`，同样是 503 `UO`。
- `signal="conn"`：连接数到了上限，`envoy_cluster_upstream_cx_overflow`，新请求开始排队（还没有拒绝）。

表达式的后一半抓“5 分钟前没有这条序列、现在有而且大于 0”，免得计数器第一次出现的那次增长被 `increase()` 漏掉；窗口 5 分钟配 `for` 2 分钟，窗口要比 `for` 长。`active` 这一条是 2026-11-23 才开始抓的（之前不在 PodMonitor 的保留列表里），更早的时间段查不到。

**`MeshConcurrencyLimitOverflow`**：服务之间的边车（`job="monitoring/istio-proxy"`，命名空间 `oppenheimer`）的 `envoy_cluster_upstream_rq_active_overflow` 在 5 分钟里有增长，持续 2 分钟，按目标服务（`cluster_name`）汇总。这个计数是“边车对这个下游的在途请求数到了上限、多出来的被直接拒绝”的次数。上限在 `deploy/cluster/mesh/` 的 `DestinationRule` 里：gateway 到 inventory、order 到 inventory、gateway 到 payment、orderindex、payment 到 order 是每个调用方 Pod 128；gateway 到 order 是 512。出处：[round-4](../notes/3b-rounds/round-4-mixed-and-flash-ladders.md) 的 1.2，[1d 设计的 5.3](../design/1d-protection.md)，入口一侧见 [round-3 的 2.2](../notes/3b-rounds/round-3-overload-behavior.md)、[round-6 的第 7 节](../notes/3b-rounds/round-6-scaling-factor.md)。两条都是 warning：是“某个保护机制在起作用”，用户有没有受影响看 SLO 的燃烧率（[设计 4.3](../design/3c-slo-alerting.md)）。

## 影响

被拒的请求立刻（几十到一百多毫秒）返回 503，不会排队拖慢别的接口，这正是上限的用处。用户看到下单或查询偶尔 503，客户端带幂等键重试。数据没有风险。当天看，不用半夜处理；如果同时有 `IngressGateway5xxHigh` 或 SLO 告警，就是在失败，按那边的优先级。

## 先看什么

1. 先弄清是入口还是边车，哪个目标服务：

   ```sh
   scripts/cluster/monitoring.sh alerts
   scripts/cluster/monitoring.sh promql 'sum by (cluster_name) (increase(envoy_cluster_upstream_rq_active_overflow{job="monitoring/istio-proxy",namespace="oppenheimer"}[10m]))'
   ```

2. 入口：看在途请求数离上限有多远，和排队、连接数溢出：

   ```sh
   scripts/cluster/monitoring.sh promql 'max(envoy_cluster_upstream_rq_active{job="monitoring/ingress-envoy",envoy_cluster_name="httproute/oppenheimer/gateway/rule/0"})'
   scripts/cluster/monitoring.sh promql 'sum by (envoy_cluster_name) (increase(envoy_cluster_upstream_rq_active_overflow{job="monitoring/ingress-envoy"}[10m]))'
   scripts/cluster/monitoring.sh promql 'sum by (envoy_cluster_name) (increase(envoy_cluster_upstream_cx_overflow{job="monitoring/ingress-envoy"}[10m]))'
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n envoy-gateway-system logs -l app.kubernetes.io/name=envoy,gateway.envoyproxy.io/owning-gateway-name=public -c envoy --tail=50 | grep UO
   ```

   在途接近 1024，或日志里有 `UO`，就是入口在拒绝。

3. 下游为什么慢：这个服务自己的延迟和节点 CPU。

   ```sh
   scripts/cluster/monitoring.sh promql '1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m]))'
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer top pods --containers
   ```

4. 有没有同时发生摘除、熔断：

   ```sh
   scripts/cluster/monitoring.sh promql 'sum by (cluster_name) (increase(envoy_cluster_outlier_detection_ejections_enforced_total{job="monitoring/istio-proxy",namespace="oppenheimer"}[10m]))'
   ```

5. 有没有在压测。k6 在跑时溢出是预期里的事，对一下 `docs/notes/3b-rounds/` 里相同档位的数。

## 常见的原因和各自的处理

1. **过载，节点 CPU 到顶之后下游变慢。** 记录：round-4 的 2384 档，inventory 的 `GetStock` p99 从几毫秒到 0.49 秒，gateway 两个 Pod 到 inventory 的 `upstream_rq_active_overflow` 合计 7870 次，被拒的请求约 100 毫秒返回。结论是 128 这个数在 CPU 到顶之后才触发，不用改。处理：降负载；这个上限是在保护，别先去放大它。
2. **副本数少，按 Pod 给的上限总量就少。** 记录：round-6 的 4.2：1 个副本时 2384 档查库存的 503 是 157/秒，2 个副本 118/秒。现在 gateway、order 的 HPA 上下限都是 2，缩容不会发生；但滚动更新或有一个 Pod 不就绪时，总量会短暂减半。
3. **入口：在途上限 1024 被打满。** 记录：round-6 的 7.3，上面讲了。处理同过载；上限的数要等“在途到多少时 gateway 延迟开始不可接受”的数据才能改，round-3 的 2.2 有算法。
4. **入口连接数被空闲连接占满。** 记录：round-6 的 4.1，已经把 `maxConnections` 放到 4096、加了 30 秒空闲超时；4096 在 2384 档用到了 3716，余量不大（7.2）。gateway 副本数或入口线程数涨了要重算。
5. **下游挂住、没有被摘除，在途请求一直占着。** 边车的路由超时是 3 秒（gateway 到下游）、1 秒（order 到 inventory、payment 到 order）。摘除生效前有 0.5 到 1 秒的延迟（[2d 笔记发现一](../notes/2d-mesh.md)），这期间并发上限管着（场景 D）。
6. **上限偏低、本来就不够用。** 推断，没有遇到过：所有上限都是按“单实例、几毫秒一次”定的，没有一个是按真实流量定的。

## 怎么确认恢复了

两条告警都在 5 分钟窗口内没有新增长之后恢复（最长约 7 分钟）。确认：

```sh
scripts/cluster/monitoring.sh promql 'sum(increase(envoy_cluster_upstream_rq_active_overflow{job="monitoring/istio-proxy",namespace="oppenheimer"}[5m]))'
```

结果为 0 或没有数据。

## 怎么人为触发它

设计 3c 第 5 节第 2 层：用压测打过拐点。round-4 和 round-6 的阶梯在 2384 档会让 gateway 到 inventory 的边车溢出；入口一侧在 round-6 的 7.2 里也在 2384 档。要发压，先 `make down`，k6 在跑时本机不跑测试和构建。

`make cluster-fault` 的场景 D（同时 400 个请求）不能用来验这条：它在 `oppenheimer-e2e` 里，规则只看 `namespace="oppenheimer"`。

`IngressRejectingRequests`：同一次压测的 2384 档入口的在途上限会溢出（`signal="active"`）。不想发压时的办法**待定**，建议把入口的 `maxPendingRequests`、`maxConnections` 临时调低（`deploy/cluster/services/gateway.yaml` 的 `route.circuitBreaker`），再用小压测触发，之后改回。

故障注入动手前要先登记到 `docs/plan/phase-3-observability.md`；重置节点、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
