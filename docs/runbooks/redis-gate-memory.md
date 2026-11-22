# 闸门 Redis 内存

覆盖的告警：`RedisGateMemoryCritical`（critical）、`RedisGateMemoryHigh`（warning）

## 这条告警在说什么

闸门（`cluster="redis-gate"`，库存预扣、幂等键、分布式锁、读己之写标记）的 `redis_memory_used_bytes / redis_memory_max_bytes`：

- `RedisGateMemoryHigh`：超过 0.7，持续 5 分钟。
- `RedisGateMemoryCritical`：超过 0.8，持续 2 分钟。

`redis_memory_max_bytes` 现在是 `maxmemory` 768 MiB（`deploy/cluster/redis/redis-gate.yaml`），策略是 `noeviction`：满了不淘汰，**所有写命令**报 OOM。order 在到 85% 时自己停止受理新订单（`503 STOCK_GATE_UNAVAILABLE`，指标里是 `gate_memory_full`，[ADR-0034](../adr/0034-gate-memory-watermark.md)），所以 70% 是提前量、80% 是“离停止受理只剩 5 个百分点（约 38 MB）”。出处：[round-9 第 2.2 节](../notes/3b-rounds/round-9-soak.md)、[ADR-0033](../adr/0033-idempotency-key-ttl.md)、ADR-0034。

抑制：`RedisGateMemoryCritical` 在响时 `RedisGateMemoryHigh` 不通知（同名抑制之外这是单独配的）。

## 影响

- 70% 到 85%：用户无感，余量在变少。
- 85% 以上：新订单被拒（503 加 `Retry-After`），客户端用同一个幂等键重试；带着已有幂等键的重放也会被拒。
- 真的写满：比拒绝受理糟。落库认领不了占用、回收和分布式锁写不进去，已拿到 202 的订单卡住，直到有键过期才恢复（3b 第 2 轮，5672 单卡住，[round-2 第 6 节](../notes/3b-rounds/round-2-concurrency-and-consumers.md)）。
- 数据风险：占用卡住期间库存数偏差，靠 inventory 的 `reconcile` 和对账纠正。critical 现在就看，warning 当天看。

## 先看什么

1. 用了多少、哪个实例：

   ```
   scripts/cluster/monitoring.sh promql 'redis_memory_used_bytes{cluster="redis-gate"} / redis_memory_max_bytes{cluster="redis-gate"}'
   scripts/cluster/monitoring.sh promql 'redis_memory_max_bytes{cluster="redis-gate"}'
   ```
2. 是在涨还是已经走平（幂等键在 TTL 处走平；稳态时每持续 1 单/秒占约 1.5 MiB）：`scripts/cluster/monitoring.sh promql 'deriv(redis_memory_used_bytes{cluster="redis-gate"}[30m])'`。
3. 谁占的：键的前缀分布（只读）：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-gate-0 -c redis -- redis-cli --scan --pattern 'gate:idem:*' --count 1000 | head -3
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-gate-0 -c redis -- redis-cli dbsize
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-gate-0 -c redis -- redis-cli info memory
   ```
   换成当前主库的 Pod（`redis_instance_info{role="master"}`）。不要对大库用 `KEYS *`。
4. 幂等键的 TTL 是不是 1 小时：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get deploy order -o yaml | rg IDEMPOTENCY_TTL`，没有就是默认 1 小时。
5. 受理是不是已经被拒：order 日志里状态变化时各记一条（进入和离开 `gate_memory_full`）：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs deploy/order --since=30m | rg -i 'gate.*memory'`；用户一侧是 503 `STOCK_GATE_UNAVAILABLE`。

## 常见的原因和各自的处理

1. **幂等键累积。** 内存约等于“TTL 内受理的订单数 × 220 字节”。3b 第 2 轮 82 万个 24 小时 TTL 的键把 192mb 写满；上限提到 768mb，ADR-0033 把 TTL 缩到 1 小时。**注意：改完部署后，已经在闸门里的键仍按写入时的 24 小时过期**，内存要一天才降下来。
2. **持续高于设计的受理速率。** 768mb 在 1 小时 TTL 下约撑 900 单/秒，4 小时 125 单/秒的长稳里停在 24%（184 MiB，键数 52 万，在 1 小时处走平，round-9）。压测打得更猛时会上到 70%，那是预期：处理是降速或等键过期。
3. **有人把 TTL 调长了、或 `IDEMPOTENCY_TTL` 配置被改。** order 在它比 `PAYMENT_TIMEOUT` 短时拒绝启动，但调长没有上限（ADR-0033）。看 order 的环境变量。
4. **键不过期的东西在涨**（推断，没有遇到过）：占用随落库和支付结果释放，不随时间累积；如果落库停住（消费者被挡住），占用不释放。先看落库的消费延迟，再看库存对账。
5. **要真的扩容。** 改 `deploy/cluster/redis/redis-gate.yaml` 的 `maxmemory` 和容器上限，`scripts/cluster/middleware.sh redis` 滚动；滚动会触发一次 Sentinel 切换，写入短暂中断。上次（2026-11-20）事先没问主人，现在先问。

## 怎么确认恢复了

- 比值回到 0.7 以下，warning 5 分钟、critical 2 分钟内消失。内存降不下来时，键过期要等一个 TTL，不会马上降。
- 受理没有 `gate_memory_full`，order 日志里没有 `OOM command not allowed`：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs deploy/order --since=15m | rg 'OOM command'` 无输出。
- 出过写满或卡单的，以对账为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%H%M)`。

## 怎么人为触发它

按[设计 5 节第 2 层](../design/3c-slo-alerting.md)：临时把 `maxmemory` 调低，不真的灌满。注意用 `noeviction` 时已用量超过 `maxmemory` 所有写都会失败，所以只能调到“略高于当前用量”：取 当前 `used_memory` 除以 0.75 触发 `RedisGateMemoryHigh`，除以 0.82 触发 `RedisGateMemoryCritical`（用量约 184 MiB 时分别是约 245 MB 和 224 MB），对主库和从库都 `redis-cli config set maxmemory <字节数>`，确认告警先后响，然后改回 `768mb`。这个改动不入库，Pod 重启就复位。越过 85% order 会拒绝受理，所以要在没有流量时做。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
