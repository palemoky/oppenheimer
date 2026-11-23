# 边车摘除下游实例

覆盖的告警：`MeshEndpointEjected`（warning）

## 这条告警在说什么

服务之间的边车（`job="monitoring/istio-proxy"`，命名空间 `oppenheimer`）的离群检测摘除了某个服务的实例：`envoy_cluster_outlier_detection_ejections_enforced_total` 在 10 分钟窗口里有增长，持续 1 分钟，按目标服务（`cluster_name`）汇总。用 `enforced`（真正执行了的摘除），不用 `detected`（检测到但没执行）。后一半表达式抓“10 分钟前没有这条序列、现在有而且大于 0”的第一次。摘除是离散事件，所以窗口放宽到 10 分钟，一次摘除之后告警会保持约 10 分钟。

摘除的规则在 `deploy/cluster/mesh/` 的每个 `DestinationRule` 里：连续 5 次 5xx（`consecutive5xxErrors: 5`）或网关错误（路由超时、连接失败）就把这个实例摘掉 5 秒（`baseEjectionTime: 5s`），检查间隔 1 秒，`maxEjectionPercent: 100`（整个服务都坏时全部摘除，这是故意的）。业务错误（比如 404）不计入。出处：round-3；机制和三个坑见 [1d 设计的 5.3](../design/1d-protection.md)，在 Istio 上的实测见 [2d 笔记的第 5、6 节](../notes/2d-mesh.md)，决定见 [ADR-0029](../adr/0029-istio-sidecar-mesh.md)。告警是 warning：摘除本身是保护在起作用，用户有没有受影响看 SLO（[设计 4.3](../design/3c-slo-alerting.md)）。

## 影响

被摘除的实例在 5 秒内不接请求。两个副本里摘一个，另一个继续接，用户基本无感；两个都被摘（整个服务在失败），所有走这个下游的请求立刻（约 0.2 毫秒到几十毫秒）返回 503，直到恢复。摘除生效前，发往坏实例的请求各等满路由超时（gateway 到下游 3 秒，order 到 inventory、payment 到 order 1 秒）。数据没有风险（失败的请求没有被受理，客户端带幂等键重试）。看到先判断是单个实例还是整个服务：整个服务的话按 SLO 告警的优先级处理，否则当天看。

## 先看什么

1. 哪个服务、几次，现在有没有 SLO 告警同时在响：

   ```sh
   scripts/cluster/monitoring.sh alerts
   scripts/cluster/monitoring.sh promql 'sum by (cluster_name) (increase(envoy_cluster_outlier_detection_ejections_enforced_total{job="monitoring/istio-proxy",namespace="oppenheimer"}[10m]))'
   ```

2. 这个服务的 Pod 在不在、是不是重启过、有没有被删：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get events --sort-by=.lastTimestamp | tail -20
   ```

3. 容器是不是被 OOMKilled 或在反复重启，见 [container-memory.md](container-memory.md)：

   ```sh
   scripts/cluster/monitoring.sh promql 'increase(kube_pod_container_status_restarts_total{namespace="oppenheimer"}[30m]) > 0'
   ```

4. 这个服务自己的日志，找 5xx 或超时的原因（Loki 里 `{service_name="inventory"}`，换成目标服务名）：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs deploy/inventory -c inventory --tail=100
   ```

5. 调用方边车眼里这个端点的状态（`FAILED`、`UNHEALTHY` 说明正在被摘），容器名和服务名按实际换：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer exec deploy/gateway -c istio-proxy -- pilot-agent request GET clusters | grep inventory | grep -E 'health_flags|outlier'
   ```

## 常见的原因和各自的处理

1. **下游慢，路由超时到期，连续 5 次。** 项目里实测过：对 inventory 发 `SIGSTOP`，8 个请求都等满 3 秒返回 503 或 504，第二到三轮之后在 12 到 19 毫秒返回 503，边车里端点 `UNHEALTHY`、异常检测 `FAILED`；`SIGCONT` 后再过 5 秒恢复（[2d 笔记第 5 节](../notes/2d-mesh.md)）。处理：找下游为什么慢，节点 CPU（[node-resources.md](node-resources.md)）、数据库、Redis。
2. **下游进程没了或被删。** 记录：2d 场景 C，强制删除两个 inventory，40 个请求全部 503、最长 13 毫秒，中断 4.6 秒，新 Pod 就绪即恢复。处理：看 Pod 为什么没了（OOM、节点、被手工删）。
3. **下游被 OOMKilled 重启。** 记录：3b 里 MySQL 被 OOMKilled（[3b 笔记第 9 节](../notes/3b-load-testing.md)）。业务服务被 OOMKilled 没有记录（推断，没有遇到过）。
4. **过载。** 节点 CPU 到顶时下游超时，摘除和并发上限溢出（[mesh-overflow.md](mesh-overflow.md)）常一起出现。
5. **部署或滚动更新。** 推断，没有遇到过：新 Pod 起来之前旧 Pod 已经不接请求时会有几次连接失败。2d 的第 11 节把 `DRAIN_DELAY` 降到 5 秒就是为了让端点更新赶上，正常滚动不该连续失败 5 次。
6. **摘除的统计和实际不同步。** 2d 的发现一：摘除统计里记了，但负载均衡要晚 0.5 到 1 秒才用上；发现二：摘除时间没有逐次加长，每次都是 5 秒；发现三：超时的请求一部分是 504（被调用方的入站边车也在计时），两种都计入摘除。排查时别靠 503 还是 504 来判断是谁超时。

## 怎么确认恢复了

摘除在 5 秒后自己放回，告警在最后一次摘除之后约 10 分钟恢复。恢复的实质判断是服务本身：

```sh
scripts/cluster/monitoring.sh promql 'sum(increase(envoy_cluster_outlier_detection_ejections_enforced_total{job="monitoring/istio-proxy",namespace="oppenheimer"}[5m]))'
```

为 0，且 SLO 的燃烧率告警没有在响。

## 怎么人为触发它

设计 3c 第 5 节第 2 层：把 inventory 缩到 0 一段时间（3a 用过），或用 `make cluster-fault` 的场景（对 inventory 发 `SIGSTOP`，2d 的场景 B）。

但注意：`make cluster-fault` 在 `oppenheimer-e2e` 里跑，而这条规则只看 `namespace="oppenheimer"`，所以它不会让这条响。要在长期运行的那一套上验证，只能真的对 `oppenheimer` 里的服务动手：**待定**，建议把 inventory 缩到 0（用 `scripts/cluster/loadtest.sh pin` 或直接 `scale`，inventory 没有 KEDA），发几个查库存的请求（探针每秒有几个），约 2 分钟看到告警，再恢复。这会让查库存和下单失败，只在计划好的时间做。

故障注入动手前要先登记到 `docs/plan/phase-3-observability.md`；重置节点、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
