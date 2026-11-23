# 降级开关被关着忘了开

覆盖的告警：`DegradeSwitchLeftOff`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.protection` 组。出处：[ADR-0021](../adr/0021-degradation-and-lifecycle.md)、[1d 笔记](../notes/1d-protection.md) 里“降级开关怎么设计”一节。

## 这条告警在说什么

`pkg/degrade` 给非核心功能设了有名字的开关，取值 `auto`（默认，是否降级由熔断器或探测决定）和 `off`（强制降级）。`degrade_switch_off{service_name,switch}` 等于 1 的开关持续超过 1 小时。自动降级一般几分钟就恢复，关这么久多半是实验做完忘了开回来。

开关：order 的 `admission`、`cache`、`replica_read`、`archive_read`；inventory 的 `cache`；gateway 的 `merchant_orders`、`user_orders`、`stock_query`（被关时返回 `503 FEATURE_DISABLED`）。初始值从 `DEGRADE` 环境变量读，运行中通过服务内部端口的 `GET/PUT /degrade` 修改，**只在本进程生效、不持久化**，重启回到环境变量的值，多实例要逐个改。gateway 的开关接口在只监听 Pod 回环的管理端口（`127.0.0.1:8089`）上，没有鉴权。

## 影响

对应的功能处于降级状态：对应的读走不了缓存或从库，或者接口直接返回降级的响应。`admission` 关了就是不受理新订单（用户下不了单）；`stock_query` 等关了接口返回 503。数据没有风险。是有意的就加静默并写明原因，不是有意的马上恢复。

## 先看什么

1. 哪个服务哪个开关：
   `scripts/cluster/monitoring.sh promql 'max by (service_name, switch) (degrade_switch_off{k8s_namespace_name="oppenheimer"})'`
2. 每个 Pod 上的当前值（两个副本可能不一致）：`scripts/cluster/degrade.sh <服务>`（列出这个服务每个 Pod 的全部开关）。
3. 是谁关的：是不是在做实验（`docs/plan/phase-3-observability.md`、`phase-4-chaos.md` 里登记的故障注入）；服务有没有重启过（重启后回到环境变量的值）：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods`，环境变量 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get deploy <服务> -o yaml | rg -A1 DEGRADE`。
4. 受理的结局里有没有对应的拒绝：`scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(order_admissions_total{k8s_namespace_name="oppenheimer"}[5m]))'`。
5. 如果开关是自动降级留下的（对应的熔断器还打开着）：见 [breaker.md](breaker.md)。

## 常见的原因和各自的处理

- **实验做完忘了开回来（规则的本意）。** 处理：`scripts/cluster/degrade.sh <服务> <开关> auto`（对每个 Pod 改一遍，再打印各 Pod 的当前值）。
- **有意关着。** 加静默并写原因：`scripts/cluster/monitoring.sh silence 'alertname=DegradeSwitchLeftOff switch=<开关>' 24h '<原因>'`。
- **`DEGRADE` 环境变量里写死了 `off`**，重启后又回来了：改 `deploy/cluster/services/<服务>.yaml` 里的配置并重新部署（推断，没有遇到过）。
- **只改了一个 Pod。** 开关只在本进程生效，多副本要逐个改；`degrade.sh` 会对每个 Pod 各改一遍。
- **自动降级和人工开关走同一条代码路径**（ADR-0021 决定 6），所以关着的开关和熔断器打开造成的降级在行为上是一样的；告警只看 `degrade_switch_off`，熔断器打开的降级由 `BreakerOpen` 管。
- 这条告警还没有真的响过，3b 和 3a 的记录里没有“忘了开回来”的实例。

## 怎么确认恢复了

- `scripts/cluster/monitoring.sh promql 'max by (service_name, switch) (degrade_switch_off{k8s_namespace_name="oppenheimer"})'` 为 0 或者没有这条序列。
- `scripts/cluster/degrade.sh <服务>` 里每个 Pod 的这个开关都是 `auto`。
- 对应的功能恢复：`order_admissions_total` 里没有对应的拒绝；探针周期回到 `ok`（`probe_cycles_total`）。
- 关着期间如果受理被拒过，不需要对账收尾；但如果是 `replica_read`、`cache` 关了期间有读到旧数据的可能，没有写入，也不需要。涉及订单、库存的实验之后照例以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）。

## 怎么人为触发它

设计第 5 节没有专门列。待定，建议：`scripts/cluster/degrade.sh gateway stock_query off` 关一个无害的开关（gateway 的 `stock_query`，被关时返回 503，仅限在 `oppenheimer-e2e` 或确认没人用的时候），然后等 1 小时；等不起就临时把规则的 `for` 改成 1 分钟、`make rules-test` 之后下发，用完改回，并写明这只验证了“路是通的”。没有验证过。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
