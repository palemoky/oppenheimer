# 路线图

状态标记：⬜ 未开始 · 🟡 进行中 · ✅ 完成。当前断点见 [PROGRESS.md](PROGRESS.md)。

| 阶段 | 内容 | 运行位置 | 计划文件 | 状态 |
|---|---|---|---|---|
| 0 | 文档与仓库骨架 | MacBook | [phase-0-bootstrap.md](plan/phase-0-bootstrap.md) | ✅ |
| 1 | Go 订单系统（1a 正确性 → 1b 高并发 → 1c 分库分表 → 1d 限流降级） | MacBook | [phase-1-order-system.md](plan/phase-1-order-system.md) | 🟡 1c |
| 2 | Kubernetes 集群与部署 | Ubuntu | [phase-2-kubernetes.md](plan/phase-2-kubernetes.md) | ⬜ |
| 3 | 可观测（OpenTelemetry）、压测基线、SLO 与告警、最小诊断 Agent | Ubuntu + Mac mini | [phase-3-observability.md](plan/phase-3-observability.md) | ⬜ |
| 4 | 故障注入与复盘（Agent 同步参与并迭代） | Ubuntu + Mac mini | [phase-4-chaos.md](plan/phase-4-chaos.md) | ⬜ |
| 5 | 诊断 Agent 的评测与完善 | Mac mini | [phase-5-agent.md](plan/phase-5-agent.md) | ⬜ |
| 6 | 优惠券 | 集群 | [phase-6-coupon.md](plan/phase-6-coupon.md) | ⬜ |
| 7 | 风控 | 集群 | [phase-7-risk.md](plan/phase-7-risk.md) | ⬜ |
| 8 | Rust 优化（可选） | 集群 | [phase-8-rust.md](plan/phase-8-rust.md) | ⬜ |
| 9 | eBPF（可选）：内核视角的可观测与排查 | Ubuntu | [phase-9-ebpf.md](plan/phase-9-ebpf.md) | ⬜ |

## 顺序说明

- 诊断 Agent 分三步走：阶段 3 末尾做只接 Prometheus 和日志的最小版本，阶段 4 跟着故障实验一起跑和迭代，阶段 5 做正式评测和完善。
- 阶段 4 的故障场景会作为阶段 5 诊断 Agent 的评测集，所以每次实验都要按模板留下完整记录。
- 阶段 6、7 是业务扩展，依赖阶段 1 的服务，可以在阶段 5 之后做，也可以在等待设备或想换换脑子时穿插。
- 阶段 8、9 是可选的，互相没有依赖。阶段 9 的 eBPF 只能在 Linux 上做，依赖阶段 3 的可观测体系，放在阶段 4 之后收益最大：可以拿真实的故障场景来比较内核视角和应用埋点。
- 各阶段的详细任务在动工前会再细化一次，后面阶段的计划现在只到里程碑粒度。

## 系统总览

```
            k6 (Mac mini)
                 │
           ┌─────▼─────┐
           │  gateway  │  HTTP，限流、鉴权、路由
           └─────┬─────┘
                 │ gRPC
     ┌───────────┼────────────┐
┌────▼────┐ ┌────▼─────┐ ┌────▼────┐
│  order  │ │inventory │ │ payment │（模拟）
└────┬────┘ └────┬─────┘ └────┬────┘
     │           │            │
     ├── MySQL：2 库 × 4 表，每分片一主一从；冷数据归档库
     │     └── binlog → CDC → Kafka → 商家维度索引表
     ├── Redis Cluster：库存预扣、幂等、限流、分布式锁
     └── Kafka：异步下单、outbox 事件、延迟关单

可观测：OpenTelemetry SDK → Collector → Prometheus（指标）· Loki（日志）· Tempo（链路）
        Grafana + Alertmanager
对账：Merkle Tree 定位差异
故障注入：Chaos Mesh        诊断：Agent（Go + Claude API）
```
