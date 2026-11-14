// Package recon 是对账工具所在的模块：在系统运行中比对几份应当一致的数据，定位到不一致的行。
// 设计见 docs/design/1d-protection.md 第 7 节，决定见 ADR-0022。命令在 cmd/recon。
//
// 它要连所有服务的库，所以不属于任何一个服务。
package recon
