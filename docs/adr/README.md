# 架构决策记录（ADR）

每个影响架构或难以回头的决定写一篇，文件名 `NNNN-简短标题.md`。

模板：

```markdown
# ADR-NNNN：标题

- 状态：提议 / 已采纳 / 已废弃（被 ADR-XXXX 取代）
- 日期：YYYY-MM-DD

## 背景
要解决什么问题，有哪些约束。

## 决定
选了什么。

## 备选方案
考虑过什么，为什么没选。

## 后果
带来的好处、代价，以及什么情况下应该重新评估。
```

## 索引

| 编号 | 标题 | 状态 |
|---|---|---|
| [0001](0001-k3s-on-kvm.md) | 在 KVM 虚拟机上用 k3s 搭集群 | 已采纳 |
| [0002](0002-app-level-sharding.md) | 应用层实现分库分表 | 已采纳 |
| [0003](0003-no-heavy-framework.md) | 不使用重型微服务框架 | 已采纳 |
| [0004](0004-opentelemetry.md) | 用 OpenTelemetry 做埋点标准 | 已采纳，细节待验证 |
| [0005](0005-openapi-spec-first.md) | 对外 HTTP 接口用 OpenAPI 规范先行 | 已采纳 |
| [0006](0006-money-as-integer-minor-units.md) | 金额用最小货币单位的整数表示 | 已采纳 |
| [0007](0007-kafka-and-redis-clients.md) | Kafka 客户端用 franz-go，Redis 客户端用 go-redis | 已采纳 |
| [0008](0008-outbox-and-events.md) | 用本地消息表（outbox）加 Kafka 做服务间的最终一致 | 已采纳 |
| [0009](0009-delayed-close.md) | 超时关单用固定时长的延迟 topic，扫表兜底 | 已采纳 |
| [0010](0010-query-cache.md) | 查询缓存用旁路模式，只缓存不会再变或旧了也无所谓的数据 | 已采纳 |
| [0011](0011-admission-backpressure.md) | 异步下单的受理端按队列长度做背压 | 已采纳 |
| [0012](0012-shard-layout.md) | 订单分片布局为 8 个逻辑分片、2 个库，表名带全局分片号 | 已采纳 |
| [0013](0013-read-write-split.md) | 读写分离按“展示读走从库”划分，读己之写用写前标记加 GTID 等待 | 已采纳 |
| [0014](0014-cdc-merchant-index.md) | 商家维度查询用自研的 Go CDC 同步到索引表，从从库读，靠行版本号做幂等 | 已采纳 |
| [0015](0015-shard-expansion.md) | 成倍扩容用从库提升，切换时改表名做围栏；库数由配置决定并由库内元数据校验 | 已采纳 |
| [0016](0016-cold-archive.md) | 冷订单按订单号里的时间归档到独立的压缩库，先写冷、校验、再删热 | 已采纳 |
| [0017](0017-keyed-concurrent-consume.md) | 消费者在分区内按 key 并发，提交连续前缀 | 已采纳 |
| [0018](0018-async-stock-deduction.md) | MySQL 侧的库存计数改为异步合并更新，闸门是防超卖的唯一关口 | 已采纳 |
| [0019](0019-gateway-rate-limiting.md) | 网关用令牌桶限流，按 IP 在进程内、按用户和接口在 Redis 里 | 已采纳 |
| [0020](0020-service-protection.md) | 服务之间的熔断和并发隔离用 Envoy 边车，时限逐跳收紧，同步链路不重试 | 已采纳 |
| [0021](0021-degradation-and-lifecycle.md) | 闸门不可用时拒绝下单而不退回数据库扣减；依赖不可用时消息不进死信；就绪和存活分开 | 已采纳 |
| [0022](0022-reconciliation-hash-tree.md) | 对账用订单号上的基数哈希树，摘要是行哈希的和，在数据库里算 | 已采纳 |
| [0023](0023-instrumentation.md) | 埋点：链路过 outbox 靠表里的一列，异步一跳以父子为主，指标走 OTLP | 已采纳 |
| [0024](0024-tempo-for-traces.md) | 链路后端用 Grafana Tempo，不部署 SkyWalking（修改 ADR-0004） | 已采纳 |
| [0025](0025-envoy-gateway-and-metallb.md) | 集群入口用 Envoy Gateway 加 MetalLB，不用 ingress-nginx 和 k3s 自带的入口 | 已采纳 |
| [0026](0026-k3s-datastore-and-bundled-components.md) | k3s 用内嵌 etcd；保留 flannel（暂定）、CoreDNS、local-path，禁用 Traefik 和 ServiceLB | 已采纳 |
| [0027](0027-mysql-statefulset-not-operator.md) | 集群上的 MySQL 用手写的 StatefulSet，不用 Operator | 已采纳 |
| [0028](0028-worker-id-lease.md) | 发号器的节点号从 MySQL 租，唯一性靠互不重叠的时间区间 | 已采纳 |
| [0029](0029-istio-sidecar-mesh.md) | 服务之间的流量治理迁到 Istio，用边车模式；默认的重试必须关 | 已采纳 |
| [0030](0030-observability-backends.md) | 可观测后端的部署：两层 Collector、单体后端加本地盘、放在 server 节点、头部加尾部采样 | 已采纳 |
| [0031](0031-load-testing-approach.md) | 压测的做法：k6 开环发压、压长期运行的那一套、号段标记不做影子库表、不从 OpenAPI 生成客户端 | 已采纳 |
| [0032](0032-replica-relaxed-durability.md) | 订单库的从库提交时不逐次刷盘；落盘方式跟着角色走，主库永远每次落盘；从库的节点掉电后重建 | 已采纳 |
| [0033](0033-idempotency-key-ttl.md) | 闸门里的幂等键只留 1 小时：它是捷径不是依据，保留时长是一条容量公式，下限是支付时限 | 已采纳 |
| [0034](0034-gate-memory-watermark.md) | 闸门内存到水位（85%）时停止受理：留出余量给认领和回收；每个实例在进程里每秒采样，不放进 `Hold` 的脚本 | 已采纳 |
