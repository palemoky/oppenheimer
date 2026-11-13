// Package cdc 是 CDC 服务的模块根：伪装成 MySQL 从库读 binlog，把行变更发到 Kafka。
// 设计见 docs/design/1c-sharding.md 第 4 节和 ADR-0014。
package cdc
