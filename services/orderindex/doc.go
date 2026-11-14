// Package orderindex 是订单索引服务的模块根：消费 CDC 发出的订单行变更，写入按商家分片的索引表，
// 并提供商家维度的订单查询。设计见 docs/design/1c-sharding.md 第 4 节和 ADR-0014。
package orderindex
