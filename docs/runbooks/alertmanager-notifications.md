# 告警送不出去

覆盖的告警：`AlertDeliveryStalled`（critical）、`AlertmanagerNotificationsFailing`（warning）

## 这条告警在说什么

告警的链路：Prometheus 求值 → Alertmanager（配置在 `deploy/cluster/monitoring/alertmanager/alertmanager.yaml`，做成 Secret `alertmanager-oppenheimer`）→ webhook 接收端 `alertsink`（`monitoring` 命名空间的 Deployment，只打日志）；`severity=critical` 的另外发 Telegram（token 和会话号在仓库根目录不入库的 `.env` 里）。两条告警盯这条链路的两个位置。

**`AlertmanagerNotificationsFailing`**：`alertmanager_notifications_failed_total` 按接收端（`integration`，`webhook` 或 `telegram`）汇总，15 分钟窗口里有增长，持续 10 分钟。Alertmanager 往这个接收端发通知持续失败，告警可能没有送达。后一半表达式抓计数器第一次出现的情形。

**`AlertDeliveryStalled`**：`time() - max(alertsink_watchdog_last_received) > 15 * 60`，或者这个指标不存在（`absent`），持续 10 分钟。`Watchdog` 一直在响，Alertmanager 每 5 分钟把它发给 alertsink 一次（`alertmanager.yaml` 的第一条路由），alertsink 把最后一次收到的时刻报成指标，15 分钟（三次）没更新就算断了。覆盖整条路：Prometheus 不求值、Prometheus 到 Alertmanager 不通、Alertmanager 的路由或配置坏了、alertsink 不在。从断开到响大约 25 分钟（15 分钟加 `for` 10 分钟）。

出处：[设计 3c 的 4.6、3.5、第 7 节、第 8 节“第 6 步”](../design/3c-slo-alerting.md)。

## 影响

没有用户可见的影响，但**这期间别的告警可能同样没有送到**，监控等于瞎了；3d 之后诊断 Agent 也收不到。`AlertDeliveryStalled` 是 critical，看到就处理；`AlertmanagerNotificationsFailing` 当天看。数据没有风险。

**盲区，要清楚这两条各自看不到什么：**

1. **坏的是 Telegram 时，`AlertmanagerNotificationsFailing`（`integration="telegram"`）只能从 webhook 那边看到。** 它是 warning，路由只去 webhook，不发 Telegram，所以主人不会被推送，要自己看 alertsink 的日志或 `monitoring.sh alerts`。此时 critical 告警实际上没有送到主人（规则注释把这点列为不能当唯一保障的原因）。`AlertDeliveryStalled` 不会响：它只看 webhook 这条路（Watchdog 到 alertsink）。
2. **坏的是 alertsink 时，`AlertmanagerNotificationsFailing`（`integration="webhook"`）这条 warning 自己也没有地方送**（warning 只去 webhook）；只有 critical 的 `AlertDeliveryStalled` 能走 Telegram。
3. **坏的是 Prometheus 或 Alertmanager 自己时，两条都发不出来**：规则没人求值、通知没人发。集群外没有东西盯着 `Watchdog`（dead man's switch），这是有意不做的（设计第 7 节，记成考点）。chart 自带的 Alertmanager、Prometheus 的告警也在同一个集群里，同样发不出来。
4. **`OtelGatewayDown` 在响时 `AlertDeliveryStalled` 被抑制**（`alertmanager.yaml` 的抑制规则）：alertsink 的指标经 Collector 进 Prometheus，Collector 不在时这个指标会消失，会误报成“送不出去”。所以 Collector 坏的时候，要自己看 alertsink 的日志确认告警到底有没有送到。

## 先看什么

1. 现在有什么告警，Watchdog 在不在，有没有 `OtelGatewayDown`：

   ```sh
   scripts/cluster/monitoring.sh alerts
   scripts/cluster/monitoring.sh promql 'ALERTS{alertname="Watchdog",alertstate="firing"}'
   ```

   `ALERTS` 里没有 Watchdog，说明 Prometheus 没有在求值，问题在更上游。

2. 两个接收端各自失败了多少、总共发了多少：

   ```sh
   scripts/cluster/monitoring.sh promql 'sum by (integration) (increase(alertmanager_notifications_failed_total[15m]))'
   scripts/cluster/monitoring.sh promql 'sum by (integration) (increase(alertmanager_notifications_total[15m]))'
   scripts/cluster/monitoring.sh promql 'time() - max(alertsink_watchdog_last_received)'
   ```

   第三条返回空说明 alertsink 的指标没有了（它自己不在，或者指标送不出来），直接看第 3 步。

3. alertsink 在不在、有没有收到 Watchdog（它每 5 分钟一次，日志是每条告警一行 JSON）：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring get pods -l app.kubernetes.io/name=alertsink -o wide
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring logs deploy/alertsink --tail=30
   ```

4. Alertmanager 的日志，找发送失败的原因（`api.telegram.org` 超时、证书、401、webhook 连不上）：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring logs alertmanager-kps-alertmanager-0 -c alertmanager --tail=100
   ```

5. 配置本身：这份 Secret 里有没有 Telegram 那一段。`.env` 里没有 `TELEGRAM_BOT_TOKEN`、`TELEGRAM_CHAT_ID` 时，`monitoring.sh alertmanager` 渲染出的是“只发 webhook”的配置，此时 Telegram 这条路根本不存在：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring get secret alertmanager-oppenheimer -o jsonpath='{.data}' | grep -o telegram-token
   make alertmanager-test
   ```

## 常见的原因和各自的处理

项目里这两条都还没有真的响过，下面除第 1 条外都是推断。

1. **Telegram 根本没配。** 设计 8 的“第 6 步”记录：截至 2026-11-23 `.env` 里没有 Telegram 的两个变量，集群上是“只发 webhook”的配置，真的发一条到 Telegram 还没有做过；从 Alertmanager 的 Pod 访问 `api.telegram.org` 是通的（302，经 R2S 的透明代理）。也就是说 critical 现在大概率只进了 alertsink 的日志，没有推送到主人。处理：写好 `.env` 后重跑 `scripts/cluster/monitoring.sh alertmanager`（改完不用重启，一分钟内重新加载）。
2. **alertsink 不在（推断，没有遇到过）。** Deployment 偏好 k3s-server，节点重启（见 [node-reboot.md](node-reboot.md)）或镜像拉不到时会不在。`monitoring.sh alertsink` 会重新部署（镜像是 chaos 镜像里的 `load alertsink`，仓库里没有就构建）。
3. **Telegram 的 token、会话号错了，或 `api.telegram.org` 不通（推断）。** 拉取外网经 R2S 代理，超时先怀疑网络（AGENTS.md）。看第 4 步的日志。
4. **Alertmanager 的配置坏了、重载失败（推断）。** 改过 `alertmanager.yaml` 之后先 `make alertmanager-test`，再 `monitoring.sh alertmanager`。
5. **Collector 的 gateway 不在，指标没了（见盲区 4）。** `AlertDeliveryStalled` 被抑制，不报。看 `OtelGatewayDown`；Collector 停机各丢什么见 [3a 笔记第 10 节](../notes/3a-observability.md)。
6. **Prometheus 或 Alertmanager 的 Pod 不在、k3s-server 出问题（推断）。** 两条都发不出来。k3s-server 空间满了（[node-disk.md](node-disk.md)）也会这样。处理：直接 `kubectl -n monitoring get pods`，看它们。

## 怎么确认恢复了

`AlertmanagerNotificationsFailing` 在 15 分钟窗口里没有新的失败后恢复；`AlertDeliveryStalled` 在 alertsink 又收到 Watchdog 之后恢复（最长 5 分钟）。确认：

```sh
scripts/cluster/monitoring.sh promql 'time() - max(alertsink_watchdog_last_received)'
```

返回小于 300 左右的数。Telegram 这条路要真的收到一条才算：往 Alertmanager 里塞一条合成的 critical 告警（设计 8 的验证用过：标签带 `synthetic=true`，几分钟后自己过期）：

```sh
KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring exec alertmanager-kps-alertmanager-0 -c alertmanager -- amtool --alertmanager.url=http://localhost:9093 alert add alertname=RunbookTest severity=critical synthetic=true --annotation=summary='runbook test'
```

确认手机上收到，alertsink 的日志里也有。

## 怎么人为触发它

设计 3c 第 5 节第 2 层没有单列。**待定**，建议，没有验证过：

- `kubectl -n monitoring scale deploy/alertsink --replicas=0`。下一个 Watchdog（最多 5 分钟）发送失败，`AlertmanagerNotificationsFailing{integration="webhook"}` 在约 10 到 15 分钟后进入 firing（它自己送不到任何地方，只能在 `monitoring.sh alerts` 和 Prometheus 里看到，这正是盲区 2）；`AlertDeliveryStalled` 在约 25 分钟后响，并且只有配了 Telegram 才推送得到。验完 `scale --replicas=1`（或重跑 `monitoring.sh alertsink`）。
- Telegram 这条路：把 `.env` 里的 token 临时写错再重跑 `monitoring.sh alertmanager`，发一条合成的 critical；验完改回。
- 盲区 3（Prometheus 或 Alertmanager 自己坏）没有办法触发验证，那是设计上承认的缺口。

故障注入动手前要先登记到 `docs/plan/phase-3-observability.md`；重置节点、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
