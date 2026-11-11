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
