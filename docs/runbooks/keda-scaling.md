# KEDA 伸缩失灵

覆盖的告警：`KedaScalerErrors`（warning）、`KedaHPAScalingInactive`（warning）

## 这条告警在说什么

gateway、order 由 KEDA 按请求速率伸缩（加 CPU 兜底）：KEDA 照 `ScaledObject` 建一个普通的 HPA（`keda-hpa-gateway`、`keda-hpa-order`），请求速率在里面是 External 指标，HPA 控制器每 15 秒经聚合 API 向 KEDA 取数，KEDA 当场去 Prometheus 执行查询。见 [3a 笔记第 9 节](../notes/3a-observability.md)。

**`KedaScalerErrors`**：`keda_scaler_detail_errors_total{exported_namespace="oppenheimer"}` 在 10 分钟窗口里有增长，持续 1 分钟，按 `scaledObject` 汇总。`exported_namespace` 是被伸缩的对象所在的命名空间（`namespace` 标签是 KEDA 自己的）。意思是 KEDA 查伸缩指标失败。后一半表达式抓计数器第一次出现的情形。

**`KedaHPAScalingInactive`**：`kube_horizontalpodautoscaler_status_condition{namespace="oppenheimer",condition="ScalingActive",status="true"}` 的值是 0，持续 5 分钟。这个序列的值是 1 才正常，0 表示 HPA 的 `ScalingActive` 条件不是 True（取不到指标，或者伸缩对象有问题）。它在 scaler 取不到数时会变 False，比错误计数更直接，所以两条都留。

出处：3a 第 7 步，[3a 笔记 9.4](../notes/3a-observability.md)（实测 Prometheus 连不上时，`ScaledObject` 的 `READY` 仍然是 `True`，所以不能看它，要看这两个）。规则见 [设计 3c 的 4.3](../design/3c-slo-alerting.md)，KEDA 的选择见 [ADR-0030](../adr/0030-observability-backends.md) 第 10 项。

## 影响

**现在对容量几乎没有影响**：gateway 和 order 的 HPA 上下限都是 2（round-8 的 6.1，加不上第三个副本，上限收到 2 是主人定的），不扩也不缩。副本数不会变，用户看不到任何东西。

如果以后加了节点、上限放开：取不到数时 HPA 不会缩容（任何一个指标取不到就不缩），取得到的指标仍然能扩（CPU 那一项）。所以最坏是“负载上来时请求速率那一项不参与扩容、只剩 CPU”，扩得更晚。没有数据风险。当天看，不急。

## 先看什么

1. 哪个对象、HPA 现在是什么状态：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get scaledobject,hpa
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer describe hpa keda-hpa-gateway | tail -30
   ```

   看 Conditions 里的 `ScalingActive`，原因 `FailedGetExternalMetric` 就是取不到数。

2. 错误计数和 HPA 条件：

   ```sh
   scripts/cluster/monitoring.sh promql 'sum by (scaledObject) (increase(keda_scaler_detail_errors_total{exported_namespace="oppenheimer"}[10m]))'
   scripts/cluster/monitoring.sh promql 'kube_horizontalpodautoscaler_status_condition{namespace="oppenheimer",condition="ScalingActive"}'
   ```

3. KEDA 自己在不在（命名空间 `keda`）：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n keda get pods
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n keda logs deploy/keda-operator --tail=50
   ```

4. Prometheus 在不在（ScaledObject 里的查询打的是它）：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring get pods | grep prometheus
   scripts/cluster/monitoring.sh promql 'up{job="monitoring/ingress-envoy"}'
   ```

   最后一条能返回数，说明 Prometheus 能查。

5. 查询本身有没有结果：`kubectl -n oppenheimer get scaledobject gateway -o yaml` 里取出 `query`，用 `monitoring.sh promql` 执行。

## 常见的原因和各自的处理

1. **Prometheus 连不上。** 实测过（3a 的 9.4）：HPA 条件变成 `ScalingActive=False FailedGetExternalMetric`，7 分半没有缩容，`keda_scaler_detail_errors_total` 在涨，`ScaledObject` 的 `READY` 仍是 `True`；取得到的指标仍然能扩（30 秒内 1 扩到 4）。Prometheus 是单副本，它不在时伸缩是悄悄停掉的。处理：把 Prometheus 弄回来（它在 k3s-server 上，节点重启时会短暂不在，见 [node-reboot.md](node-reboot.md)）。
2. **KEDA 的 operator 或 metrics-apiserver 不在。** 3a 的 9.4 写明没有实测，推断和 Prometheus 连不上一样；另外聚合 API 不可用时删命名空间可能卡住（`make cluster-e2e` 每次都删命名空间）。处理：看第 3 步，重启 deployment。
3. **operator 第一次启动时自己重启一次。** 不是故障：它生成了 webhook 和聚合 API 用的证书，退出好让新证书被加载（3a 9.1）。
4. **同一个 Deployment 上有两个 HPA。** 旧的 HPA 还在时建 `ScaledObject` 会被 webhook 拒绝（3a 9.5）。从空集群部署不会遇到。
5. **压测时用 `loadtest.sh pin` 钉了副本数。** gateway、order 用 KEDA 的暂停注解。暂停期间 HPA 条件和错误计数是什么样，没有核对（推断，没有遇到过），出现这条告警时先查有没有忘记 `unpin`。
6. **ScaledObject 里查询的写法有问题或指标没了。** 查询没有结果时按 0 算（`ignoreNullValues`），不会报错；报错才计数。推断，没有遇到过：改了指标名之后查询会静默变成 0，两条告警都不响。

## 怎么确认恢复了

`KedaScalerErrors` 在最后一次错误 10 分钟后恢复；`KedaHPAScalingInactive` 在条件回到 True 后恢复。确认：

```sh
scripts/cluster/monitoring.sh promql 'kube_horizontalpodautoscaler_status_condition{namespace="oppenheimer",condition="ScalingActive",status="true"}'
```

gateway、order 两个都是 1。

## 怎么人为触发它

3a 的 9.4 用过：在临时命名空间里建一个 `ScaledObject`，`serverAddress` 指向不存在的地址，`keda_scaler_detail_errors_total` 涨到 3。**但规则只看 `exported_namespace="oppenheimer"`，临时命名空间触发不了。** 待定，建议：在 `oppenheimer` 里建一个无关紧要的空 Deployment（不要碰 gateway、order），配一个 `serverAddress` 错误的 `ScaledObject`，约 1 到 2 分钟 `KedaScalerErrors` 进入 pending，约 5 分钟后 `KedaHPAScalingInactive` 响；验完删掉。没有验证过。

不要为了触发它去停 Prometheus：那会把所有告警一起停掉。

故障注入动手前要先登记到 `docs/plan/phase-3-observability.md`；重置节点、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
