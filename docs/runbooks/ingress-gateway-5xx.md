# 入口到 gateway 的 5xx

覆盖的告警：`IngressGateway5xxHigh`（critical）

## 这条告警在说什么

入口 Envoy（`job="monitoring/ingress-envoy"`）上，发往 gateway 这个上游（`envoy_cluster_name="httproute/oppenheimer/gateway/rule/0"`）的请求，5xx 的比例在 5 分钟窗口里超过 1%，并且持续 5 分钟（`for: 5m`）。两个 Envoy 的数加在一起算。

另有一个门槛：这个上游的请求速率要不低于每秒 0.2 个，否则不算（一两个失败就是很高的比例）。常驻探针保证平时每秒有几个请求。

5xx 里包括 gateway 自己返回的，也包括 gateway 整个不在时入口替它生成的 503。这是下单可用性 SLO 的补充：SLO 的指标是 gateway 自己报的，gateway 不在或者指标报不出来（Collector 坏了）时那边没有数据、不响，只有入口的指标看得见。出处和设计见 [设计 3c 的 4.1](../design/3c-slo-alerting.md)，以及第 8 节“第 4 步的另一半”。1% 这个数没有单独的实测来源，是按“99.9% 的目标下，平时 5xx 只有 0.063%”定的（设计 1.2）。

## 影响

用户在下单、查询时直接拿到 503。已经拿到 202 的订单不受影响：它们在 Kafka 里排队，等落库。没有拿到 202 的请求没有被受理，客户端带同一个幂等键重试即可，不会重复下单。数据没有风险。

这是 critical，发 Telegram。看到就处理，目标是几分钟内弄清是“gateway 不在”还是“过载”。

## 先看什么

1. 对照 SLO：只有这条响、SLO 的燃烧率不响，说明 gateway 不在或者它的指标没报上来；两边都响，是 gateway 在、但在失败。

   ```sh
   scripts/cluster/monitoring.sh alerts
   ```

2. 看入口答的 5xx 速率、请求速率，和有没有溢出：

   ```sh
   scripts/cluster/monitoring.sh promql 'sum by (envoy_response_code_class) (rate(envoy_cluster_upstream_rq_xx{job="monitoring/ingress-envoy",envoy_cluster_name="httproute/oppenheimer/gateway/rule/0"}[5m]))'
   scripts/cluster/monitoring.sh promql 'sum(increase(envoy_cluster_upstream_rq_pending_overflow{job="monitoring/ingress-envoy"}[5m]))'
   scripts/cluster/monitoring.sh promql 'sum(envoy_cluster_upstream_rq_active{job="monitoring/ingress-envoy",envoy_cluster_name="httproute/oppenheimer/gateway/rule/0"})'
   ```

3. gateway 的 Pod 在不在、是不是就绪：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -l app.kubernetes.io/name=gateway -o wide
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get hpa,scaledobject
   ```

4. 入口自己的日志（只记 5xx 和带 response flags 的请求）。`UO` 是上限溢出，`UF` 连不上上游，`UH` 没有健康的上游：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n envoy-gateway-system logs -l app.kubernetes.io/name=envoy,gateway.envoyproxy.io/owning-gateway-name=public -c envoy --tail=50
   ```

5. gateway 的日志，看它返回 5xx 时的原因码。Loki 里：`{service_name="gateway"} | json | level="ERROR"`（字段名以 Loki 里实际的为准）。

## 常见的原因和各自的处理

按项目里遇到过的排序。

1. **过载，入口拒绝（`503 UO`）。** 入口到 gateway 的在途请求上限是 1024（`maxParallelRequests`），连接上限 4096。记录：[round-6 的 7.3](../notes/3b-rounds/round-6-scaling-factor.md)：2384 请求/秒这一档入口每秒答 530 个 503，在途正好停在 1024；[round-4 的秒杀阶梯](../notes/3b-rounds/round-4-mixed-and-flash-ladders.md)：4000 次/秒时每秒 20 个。处理：这是入口在保护还能被处理的请求，先降发压或限流，不是放大上限。上限的来历见 [round-3 的 1.2](../notes/3b-rounds/round-3-overload-behavior.md)。看节点 CPU（见 [node-resources.md](node-resources.md)）。
2. **gateway 下游的边车并发上限溢出。** gateway 到 inventory 每个 Pod 128 个在途，超出的立刻 503。记录：round-4 的 2384 档，k6 记到 9137 个 5xx。处理见 [mesh-overflow.md](mesh-overflow.md)。
3. **下游被边车摘除，请求快速失败。** 见 [mesh-ejection.md](mesh-ejection.md)。
4. **节点 CPU 到顶，所有东西一起慢。** 记录：round-4、round-6 里的拐点都是两个 agent 节点 CPU 在 90% 以上。见 [node-resources.md](node-resources.md)。
5. **gateway 整个不在（比如缩到 0、滚动卡住）。** 入口对每个请求返回 503。3a 的验证里缩过 inventory，缩 gateway 没有记录（推断，没有遇到过）。处理：`kubectl -n oppenheimer rollout status deploy/gateway`，看事件。
6. **入口自己的问题：持有入口地址的那个 Envoy 的节点出事。** MetalLB 二层模式下地址同一时刻只在一个节点上（ADR-0025），切换期间请求会失败（推断，没有量过，阶段 4 的计划里有“入口地址漂移要多久”）。

## 怎么确认恢复了

告警在比例回到 1% 以下之后自己恢复。再确认：

```sh
scripts/cluster/monitoring.sh promql 'sum(rate(envoy_cluster_upstream_rq_xx{job="monitoring/ingress-envoy",envoy_cluster_name="httproute/oppenheimer/gateway/rule/0",envoy_response_code_class="5"}[5m]))'
```

返回接近 0。再确认探针和 SLO 的燃烧率告警都不在响。

## 怎么人为触发它

设计 3c 第 5 节第 2 层没有为这条单列办法，和 SLO 那条一起用“缩到 0”的思路：**待定**。建议：用 `scripts/cluster/loadtest.sh pin gateway=0`（gateway 用 KEDA 的暂停注解钉副本数，直接 scale 会被 HPA 改回来）让 gateway 不在，等约 5 到 6 分钟看这条响，之后 `unpin gateway`。`pin` 能不能接受 0 没有验证过，先读 `scripts/cluster/loadtest.sh` 的 `cmd_pin`。这会让下单全部 503，只在计划好的时间做。

另一个办法是用压测打过拐点（round-6 的 2384 档），入口会自己拒绝。

故障注入动手前要先登记到 `docs/plan/phase-3-observability.md`；重置节点、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
