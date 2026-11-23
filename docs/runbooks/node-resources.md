# 节点 CPU 持续高

覆盖的告警：`NodeCPUHigh`（warning）

## 这条告警在说什么

某个节点（node-exporter 的 `instance`）的 CPU 使用率，即 1 减去所有核空闲时间占比的 5 分钟平均，超过 85%，持续 10 分钟。三个节点各自算：server 4 vCPU，agent-1、agent-2 各 8 vCPU（每个可分配 7.5 核）。

加了 `keep_firing_for: 3m`（2026-11-23；先取的 10 分钟，主人嫌恢复的通知晚得太久，改成 3 分钟）：条件不成立之后再多响 3 分钟，过载时在 85% 上下晃的节点不再多发一轮“恢复”和“触发”；掉下去不到 3 分钟又回来的算同一次，超过 3 分钟再回来的算新的一次（要重新等 `for` 的 10 分钟）。代价：真的降下来之后“恢复”的通知晚 3 分钟到。

阈值的来历：混合读写的拐点在九成上下，85% 是提前量；3b 量到过 95% 时延迟会晃，所以 `for` 不短于 10 分钟。出处：[round-4 的 1.2](../notes/3b-rounds/round-4-mixed-and-flash-ladders.md)（1907 请求/秒的拐点，两个 agent 节点 87% 和 95%，2384 档 92% 和 99%）、[3b 容量笔记](../notes/3b-capacity.md)、[设计 3c 的 4.5](../design/3c-slo-alerting.md)。这是原因一级的线索：用户有没有受影响看 SLO 的燃烧率，不看这条。

## 影响

到顶的是节点，不是哪一个服务，所有东西一起慢：读写接口的 p99 一起涨五到八倍，入口开始 503，边车的并发上限溢出，限流的 Redis 超时增多，落库延迟变长（round-4、round-6、round-8 的拐点以上都是这样）。数据没有风险：落库欠账由背压（队列上限 2500）接住。

平时没有压测时这条不该响。压测时在拐点附近响是预期内的，正式发压前给它加静默（`scripts/cluster/monitoring.sh silence 'alertname=NodeCPUHigh' 2h '正式压测'`）。没有压测却响了，要查，当天处理；同时有 SLO 告警就按 SLO 的优先级。

## 先看什么

1. 哪个节点，是不是在压测（k6 在 Mac mini 上跑，看有没有人在发压）：

   ```sh
   scripts/cluster/monitoring.sh promql '1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m]))'
   scripts/cluster/monitoring.sh alerts
   ```

2. 这个节点的 CPU 花在哪种模式上（用户态、系统、iowait、steal）。iowait 高说明是盘，steal 高说明宿主机上的别的虚拟机在抢：

   ```sh
   scripts/cluster/monitoring.sh promql 'sum by (instance, mode) (rate(node_cpu_seconds_total{mode!="idle"}[5m]))'
   ```

3. 谁在吃：按 Pod 的 CPU 排序（含边车）：

   ```sh
   scripts/cluster/monitoring.sh promql 'topk(10, sum by (namespace, pod) (rate(container_cpu_usage_seconds_total{container!="",container!="POD"}[5m])))'
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl top pods -A --sort-by=cpu | head -15
   ```

4. 请求量是不是比平时高（用入口的请求速率）：

   ```sh
   scripts/cluster/monitoring.sh promql 'sum(rate(envoy_cluster_upstream_rq_total{job="monitoring/ingress-envoy",envoy_cluster_name="httproute/oppenheimer/gateway/rule/0"}[5m]))'
   ```

5. 这个节点上有没有不该在的 Pod（入口 Envoy 落到 server 上，见 [ingress-placement.md](ingress-placement.md)）：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl get pods -A -o wide --field-selector spec.nodeName=k3s-server
   ```

## 常见的原因和各自的处理

1. **压测打过拐点。** 记录：round-4（读多写少 1907 请求/秒，agent 节点 87% 到 99%）、round-6（扩容系数）、round-8 的 q1、q3 阶梯（2384 档节点 CPU 93%、99%）。处理：降发压，等 CPU 回落；不是要“修”的东西。
2. **加副本不加能力。** 记录：[round-6 的 3.1、3.2](../notes/3b-rounds/round-6-scaling-factor.md)：这套系统到顶的是节点，同样的请求量 2 个副本比 1 个多用三成半到五成的 CPU，4 个比 2 个再多一成多。所以 CPU 高时不要靠加副本解决（HPA 上限也已经收到 2）。
3. **秒杀有货、轮询把读流量推高。** 记录：round-4 的 2.2：有货时每个订单都要等落库，等的人数 = 受理速率 × 落库延迟，节点 CPU 到 90% 和 96%。
4. **requests 写得不合理，抢 CPU 时权重不对。** 记录：[round-8 的 1、6.2](../notes/3b-rounds/round-8-cpu-requests.md)：节点满时按 requests 的比例分 CPU，原来入口只有 100m 对实际 0.73 核，过载时入口多拒请求；调完之后好了很多。
5. **链路、日志导出占 CPU。** 记录：3b 容量笔记：全量导出链路占业务容器 CPU 的 14% 上下，头部采样 10% 拿回几乎全部。处理：确认采样配置没被改回全量。
6. **监控自己在 server 上吃 CPU。** server 只有 4 个 vCPU，上面有控制面、etcd、Prometheus、Tempo、Loki。Tempo 在压测时到过 1 核（round-4）。server 的 CPU 高没有单独的记录（推断，没有遇到过）。
7. **宿主机上虚拟机互相抢。** 三台虚拟机合计 20 个 vCPU，宿主机 24 线程，推断，没有遇到过。看第 2 步的 steal 模式。

## 怎么确认恢复了

CPU 低于 85% 之后告警恢复（5 分钟窗口，会比实际回落晚几分钟）。确认：

```sh
scripts/cluster/monitoring.sh promql '1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m]))'
```

三个节点都在 0.85 以下；再看 SLO 的燃烧率告警不在响，入口 5xx 回到 0。

## 怎么人为触发它

设计 3c 第 5 节第 2 层：用压测打过拐点（round-4 的混合读写阶梯，1907 以上）。要发压，先 `make down`，k6 在跑时本机不跑测试和构建。

不想发压时：在要验证的节点上跑一个吃满 CPU 的负载（例如 `ssh ubuntu@192.168.50.41 'yes > /dev/null'` 起 8 个，用完 `pkill yes`）。**没有验证过这个办法**，待定；它会影响这个节点上的所有业务，只在计划好的时间做。

或者临时把规则阈值改到当前值以下，响了再改回来，说明这只验证了“路是通的”。

故障注入动手前要先登记到 `docs/plan/phase-3-observability.md`；重置节点、清数据这类破坏性操作要先问主人。

## 历史

- 2026-11-23 02:47 UTC，`NodeCPUHigh` 第一次真的响（压测 280 单/秒，设计 [3c](../design/3c-slo-alerting.md) 第 8 节“第 14 项”）：agent-2 02:47 响（93% 到 96%），agent-1 02:49 响，之后 agent-1 在 85% 上下抖：02:57 恢复又进 `pending`，多发了一轮通知。CPU 主要在用户态（agent-2 约 4.9 核，iowait 0.3 核），吃得最多的是 order、gateway 的 Pod，其次 `mysql-main-0`、`tempo-0`、入口 Envoy。停压后约 3 分钟恢复。chart 自带的 `NodeSystemSaturation`（按负载均值）同时响了两条，说的是同一件事；自带的 `NodeCPUHighUsage` 只进过 `pending`。同日加了 `keep_firing_for: 10m`，这种抖动不再多发一轮通知。
