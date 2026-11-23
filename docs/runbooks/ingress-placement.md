# 入口 Envoy 落到了 k3s-server 上

覆盖的告警：`IngressEnvoyOnServerNode`（warning）

## 这条告警在说什么

入口（Envoy Gateway 的数据面）的 Envoy Pod 在命名空间 `envoy-gateway-system` 里，Pod 由名字形如 `envoy-envoy-gateway-system-public-*` 的 ReplicaSet 创建。规则：`kube_pod_info` 里有这样的 Pod 的 `node="k3s-server"`，并且它的 `kube_pod_status_phase{phase="Running"}` 是 1（已经终止的旧 Pod 不报），持续 5 分钟。

入口应该在两个工作节点（agent）上各一个：k3s-server 只有 4 个 vCPU，还跑着 etcd、控制面和整套监控后端。这条规则是 [round-8 的 6.4](../notes/3b-rounds/round-8-cpu-requests.md) 遇到过的事：滚动入口时多出来的那个 Pod 落到了 k3s-server，滚完变成 k3s-server 和 agent-2 各一个，当时是手工删掉 server 上的那个、让它重新调度到 agent-1。规则的出处是 [设计 3c 的 4.5](../design/3c-slo-alerting.md)，第 8 节“第 4 步的另一半”没有单独讲它。

## 影响

用户不会直接看到什么，这是“资源不该这样放”的预警。风险在压测或流量高的时候：入口的 Envoy 用掉的 CPU 不小（round-4 秒杀阶梯里接流量的那个 Pod 用了 2.08 核，requests 700m），落在 4 个 vCPU 的 server 上会和 etcd、控制面抢 CPU，入口一满整个集群的控制面跟着慢（规则注释里的说法，没有单独量过）。没有数据风险。当天看；有压测计划的话压测前必须处理。

## 先看什么

1. 现在入口的 Pod 在哪些节点上：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n envoy-gateway-system get pods -l gateway.envoyproxy.io/owning-gateway-name=public -o wide
   scripts/cluster/monitoring.sh promql 'kube_pod_info{namespace="envoy-gateway-system",node="k3s-server"}'
   ```

2. **哪个节点持有入口的地址（接流量的是哪个 Envoy）。** MetalLB 的二层模式下地址同一时刻只在一个节点上，加上 `externalTrafficPolicy: Local`，流量全部落在那个节点上的 Envoy（[ADR-0025](../adr/0025-envoy-gateway-and-metallb.md)）。如果 server 上的恰好是接流量的那个，删它会让地址漂移一次：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n envoy-gateway-system describe svc -l gateway.envoyproxy.io/owning-gateway-name=public | grep -iA6 events
   ```

   看事件里 MetalLB 最近一次是从哪个节点宣告的（字段写法以实际输出为准）。

3. 为什么落到 server：是不是刚滚动过、哪个节点被隔离或不可调度：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl get nodes
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n envoy-gateway-system get events --sort-by=.lastTimestamp | tail -15
   ```

4. 看一眼 server 现在的 CPU，判断紧不紧迫（[node-resources.md](node-resources.md)）：

   ```sh
   scripts/cluster/monitoring.sh promql '1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m]))'
   ```

## 常见的原因和各自的处理

1. **滚动更新入口 Envoy 时，多出来的那个 Pod 落到了 server。** 记录：round-8 的 6.4，那次是改了入口的 CPU requests 触发的滚动。原因：入口的 Envoy 不像业务 Pod 那样被禁止上控制面节点；`deploy/cluster/envoy-gateway/gateway.yaml` 里用的是 `topologySpreadConstraints`（`maxSkew: 1`，按节点），而不是“必须不同节点”的反亲和，这是故意的：滚动时多出来的那个 Pod 可以落到第三个节点，不会因为两个 agent 各有一个而卡住。代价就是这个。
   处理：确认第 2 步后，删掉 server 上的那个 Pod，让它重新调度（round-8 就是这么做的，调度到了 agent-1）：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n envoy-gateway-system delete pod <server 上的 Pod>
   ```

   `envoyPDB` 设了 `maxUnavailable: 1`，一次只会带走一个。删完再看第 1 步，两个 Pod 在两个 agent 上。以后每次滚入口都要看一眼 Pod 落在哪（round-8 的原话）。
2. **一个 agent 被 cordon、不可调度或没有资源。** 推断，没有遇到过：调度器退到 server。这时 agent 恢复后也不会自动回来，要手工删。
3. **根治的办法没有做。** 给 `EnvoyProxy` 的 Pod 加节点亲和，排除 k3s-server。这是个建议，没有验证过会不会影响滚动时的调度，改之前要写进 ADR-0025 的后续或新增 ADR。

## 怎么确认恢复了

告警在 Pod 离开 server 后恢复。确认：

```sh
KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n envoy-gateway-system get pods -l gateway.envoyproxy.io/owning-gateway-name=public -o wide
scripts/cluster/monitoring.sh promql 'kube_pod_info{namespace="envoy-gateway-system",node="k3s-server"}'
```

前一条两个 Pod 在 agent-1、agent-2 上，后一条没有入口的 Pod。再确认入口地址上有流量：`IngressGateway5xxHigh` 不在响，探针在通。

## 怎么人为触发它

设计 3c 第 5 节第 2 层没有单列。**待定**。两个办法，都没有验证过：

- 重现 round-8：对入口做一次滚动重启（`kubectl -n envoy-gateway-system rollout restart deploy -l gateway.envoyproxy.io/owning-gateway-name=public`），多出来的那个 Pod 是否落到 server 由调度器决定，不一定复现。
- 更可控：先 `cordon` 一个 agent，再删这个 agent 上的 Envoy Pod，让它调度到 server（`topologySpread` 允许），响了之后 `uncordon` 并删掉 server 上的那个。如果被删的 Pod 恰好持有入口地址，会有一次地址漂移，先看第 2 步，只在计划好的时间做。

故障注入动手前要先登记到 `docs/plan/phase-3-observability.md`；重置节点、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
