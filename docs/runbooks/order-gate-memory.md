# 闸门 Redis 内存到水位，下单被拒

覆盖的告警：`OrderGateMemoryFull`（critical）。同一件事的提前量告警 `RedisGateMemoryHigh`（70%，warning）、`RedisGateMemoryCritical`（80%，critical）在 [middleware.yaml](../../deploy/cluster/monitoring/rules/middleware.yaml)，它们的 `runbook_url` 指向 `redis-gate-memory.md`（另一份，不在这里）；响 `OrderGateMemoryFull` 说明已经过了那两条。

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.admission` 组。出处：[ADR-0034](../adr/0034-gate-memory-watermark.md)、[ADR-0033](../adr/0033-idempotency-key-ttl.md)、[round-2](../notes/3b-rounds/round-2-concurrency-and-consumers.md) 第 6 节、[round-9](../notes/3b-rounds/round-9-soak.md) 2.2。

## 这条告警在说什么

`order_admissions_total{outcome="gate_memory_full"}` 在 5 分钟内有增长（已有序列又涨了，或 5 分钟前没有、现在有了）。没有 `for`，第一次拒绝就响。这个结局的意思是：闸门已用内存到了 `maxmemory` 的 85%（`GATE_MEMORY_WATERMARK_PERCENT`），每个 order 实例每秒采一次 `INFO memory`，到水位后在 `Hold` 之前拒绝所有新的受理，对外 `503 STOCK_GATE_UNAVAILABLE` 加 `Retry-After`。

为什么有这个水位：闸门的淘汰策略是 `noeviction`（占用被淘汰就是库存凭空变多），满了之后所有写入都失败，包括落库的认领、回收、分布式锁。3b 第 2 轮这样写满过，5672 个已受理订单卡住且不会自己恢复。85% 时先拒绝新受理，把剩下的内存留给“处理完已经受理的订单”（约 115 MB）。

## 影响

所有新的下单都被拒（503），用户直接受影响，现在就要看。已受理的订单照常落库。水位以上，带着已有幂等键的重放也会被拒（检查在 `Hold` 之前，分不出新请求和重放），客户端应当查询订单。`noeviction` 下不处理不会自己好，要等键过期或人来扩容。

## 先看什么

1. 内存用了多少，在涨还是在跌（这是 middleware.yaml 里的指标）：
   `scripts/cluster/monitoring.sh promql 'redis_memory_used_bytes{cluster="redis-gate"} / redis_memory_max_bytes{cluster="redis-gate"}'`
   `scripts/cluster/monitoring.sh promql 'redis_memory_used_bytes{cluster="redis-gate"}'`
2. 被拒的速率和受理的结局：
   `scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(order_admissions_total{k8s_namespace_name="oppenheimer"}[5m]))'`
3. 内存被什么占着：键数和幂等键、占用各多少。直连主库看（主库是哪个先查，round-2 之后是 `redis-gate-1`，可能已变）：
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis get pods`
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-gate-1 -- redis-cli info memory`（可能需要密码，以集群清单为准，没有核对）
   `... redis-cli dbsize`
4. 占用有没有在被回收、落库有没有跟上（占用不回收会一直占内存）：
   `scripts/cluster/monitoring.sh promql 'max(order_gate_unclaimed_holds{k8s_namespace_name="oppenheimer"})'`
   `scripts/cluster/monitoring.sh promql 'sum(rate(order_persists_total{k8s_namespace_name="oppenheimer"}[5m]))'`（见 [order-backlog.md](order-backlog.md)）
5. order 的日志（水位状态变化时各记一条）：`{service_name="order"} | json | level=~"WARN|ERROR"`。

## 常见的原因和各自的处理

- **幂等键没过期，累积到写满（真遇到过）。** 3b 第 2 轮：TTL 原来 24 小时，一个键约 220 字节，两天攒 82 万个把 192mb 写满。ADR-0033 改成 1 小时，768mb 够约 900 单/秒持续一小时；长稳 125 单/秒 4 小时里键数在 1 小时处走平（52 万个键、184 MiB，约 24%），内存公式：闸门内存 ≈ 幂等键 TTL 内受理的订单数 × 220 字节（round-9 2.2，约每持续 1 单/秒占 1.5 MiB，约 500 单/秒是上限，线性外推没有实测）。处理：确认 `IDEMPOTENCY_TTL` 是 1 小时没被改过；受理速率是不是远超 125 到 150 单/秒；必要时临时提 `maxmemory`（`deploy/cluster/redis/redis-gate.yaml`，`scripts/cluster/middleware.sh redis` 滚动，round-2 就是这样处理的，滚动时 Sentinel 会切主）。提上限属于改集群，先和主人说。
- **占用没有被回收或认领（落库停了）。** 占用随落库和支付结果释放，落库停住时堆在闸门里；处理落库（[order-backlog.md](order-backlog.md)、[consumer-blocked.md](consumer-blocked.md)）。
- **内存在键数走平后还在涨（约 20 MiB）。** round-9 2.2：可能是分配器碎片，也可能是别的结构在长，4 小时分不清，没有查。
- **受理速率远超设计**（推断，没有遇到过）：压测或误操作灌入；看第 2 步。
- 采不到数时放行（ADR-0034 决定 3）：所以这条响了说明采样是通的；采样循环自己停了的话水位不起作用，那种情况只能靠 `RedisGateMemoryHigh`。

## 怎么确认恢复了

- 使用率回到 85% 以下并稳定：`scripts/cluster/monitoring.sh promql 'redis_memory_used_bytes{cluster="redis-gate"} / redis_memory_max_bytes{cluster="redis-gate"}'` 低于 0.7（`RedisGateMemoryHigh` 也恢复）。
- `scripts/cluster/monitoring.sh promql 'sum(increase(order_admissions_total{k8s_namespace_name="oppenheimer",outcome="gate_memory_full"}[5m]))'` 为 0；告警在最后一次拒绝之后保持 5 分钟才自己恢复。
- 探针的周期回到 `ok`（`probe_cycles_total`）。
- 已受理的订单全部落库、没有被回收：对账差异为 0（`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`，[recon.md](recon.md)），库存对闸门无漂移（`InventoryGateDrift` 没在响，[inventory-anomaly.md](inventory-anomaly.md)）。

## 怎么人为触发它

设计第 5 节第 2 层：**临时把 `maxmemory` 调低**，而不是真的灌满。例如 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec <主库 Pod> -- redis-cli config set maxmemory <当前已用的 1.1 倍>`（运行时改，不入库；做完 `config set maxmemory 768mb` 还原，或滚动重启；命令没有验证过）。水位 85% 的单元测试已有（水位以上拒绝受理、降回去恢复、采不到数放行）。在压测里验证不了：幂等键只留 1 小时之后受理速率写不满 768mb（round-3 第 1.4 节）。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过。3b 第 2 轮写满过一次，那时还没有这条规则。）
