# ADR-0040：诊断 Agent 的模型后端可换，OpenAI 兼容协议手写一个薄客户端

- 状态：已采纳
- 日期：2026-11-24

## 背景

3d 的 Agent 只接了 Claude。主人要它能换成 DeepSeek、OpenAI 等（2026-11-24；当时没有 Claude 的密钥，试跑卡着）。阶段 6 的“模型分层”本来也要拿同一批故障比不同的模型。

ADR-0038 已经把循环和模型隔开了：循环只认 `internal/llm` 的 `Client` 和 `Conversation`，历史收在实现里。所以这里要定的只是另一侧怎么接。查到的事实见[设计文档第 9 节](../design/3d-diagnosis-agent.md)。和决定直接相关的几条：

- DeepSeek、OpenAI 和大多数别的厂商、本地的推理服务都说 OpenAI 的 Chat Completions 协议，但各自加了字段。DeepSeek 的思考内容在 `reasoning_content` 里，**带工具的请求必须在之后的每一轮把它原样带回去，少了回 400**；缓存命中有它自己的两个计数。
- 这个协议里 `prompt_tokens` 含缓存命中的部分，Claude 的 `input_tokens` 不含。
- DeepSeek 的严格模式只在 beta 地址上有；OpenAI 的推理模型不接受 `max_tokens`，要用 `max_completion_tokens`。

## 决定

1. **后端按配置选，一次运行固定一个。** `-provider`（`anthropic`、`deepseek`、`openai`、`openai-compatible`）加 `-model`、`-base-url`。接口不动，循环、工具、报告、留痕一行不改。
2. **除 Claude 外只有一个适配器**（`internal/llm/openai.go`），`deepseek`、`openai`、`openai-compatible` 是它的三个预设（地址、放密钥的环境变量、默认模型、输出上限的字段名、开不开严格模式）。差异只允许出现在预设这张表里，适配器里不写“如果是某一家”。
3. **不用任何一家的 SDK，手写一个薄客户端**（`net/http` 加 `json/v2`，重试用仓库自己的 `pkg/retry`）。历史按原始 JSON 存：assistant 的那条消息是回复里的原文，原样放回。这样 `reasoning_content` 和别家加的、我们不认识的字段都不会丢，“回复原样放回历史”和 Claude 那一侧是同一条规矩。
4. **用量换成一个口径再往上交**：`Input` 一律不含缓存命中的部分（适配器里减出来），循环和价格表不需要知道是哪一家。
5. **价格表按模型名查，表里没有的模型必须用 `-price` 显式给**（本地的写 `0,0`），不猜；表里有的不接受 `-price`。DeepSeek 分高峰和平时两个价，表里填高峰的：上限宁可卡早。
6. **各家特有的能力不抹平，也不假装都有。** 拒绝回退只有 Claude 有；DeepSeek 不开严格模式，报告合不合格由循环已有的校验把关；OpenAI 的 Chat Completions 不给思考内容，运行记录里就是空的。

## 备选方案

- **用 `openai-go` SDK**：省掉请求、重试的代码。没选：要用的恰恰是它的类型里没有的字段（`reasoning_content` 要读出来、还要在下一轮带回去），得绕着类型走；协议本身只有一个端点，手写两百多行，测试对着 `httptest` 就能把请求的每个字节看清楚。代价见“后果”。
- **走 DeepSeek 的 Anthropic 兼容地址**（`https://api.deepseek.com/anthropic`），代码一行不改、只换地址：没选。它忽略 `anthropic-beta`、`cache_control`、`is_error`，自适应思考当成普通的开，等于现有适配器里一半的行为悄悄不生效；而且解决不了 OpenAI 和别家。
- **LangChainGo、Eino 一类的通用抽象层**：没选。接口已经有了，再包一层拿不到缓存、思考这些要学的细节，也是规范里不引入的重框架。
- **一次诊断里中途换后端（一家失败了换另一家接着答）**：没选。历史是各家的原生格式，思考内容不能跨家带；要做只能整次重跑，那是触发那一层的事。
- **OpenAI 用 Responses API**：它能带回加密的思考内容。没选：只有 OpenAI 有，DeepSeek 试跑用不上；等阶段 6 真要比 OpenAI 的模型时再看。

## 后果

- 好处：加一个说这个协议的后端是在预设表里加一行；换后端不动循环，阶段 6 的模型对比可以跨厂商。
- 好处：密钥没到的后端不挡别的后端，3d 的试跑可以先用 DeepSeek 做。
- 代价：协议的变化要自己跟（SDK 会替你跟）。**OpenAI 的预设只有单元测试，没有对真的 API 跑过。**
- 代价：各后端的运行记录不完全可比：缓存写的计数只有 Claude 有；思考内容 Claude 是摘要、DeepSeek 是原文、OpenAI 没有；费用按各自的表估，DeepSeek 平时的时段会高估一倍。运行记录里记了后端的名字（`meta.json` 的 `provider`）。
- 代价：这个协议没有“工具出错”的标记，出错的结果在内容开头加 `ERROR: `，模型看到的和 Claude 那一侧差这几个字。
- 要知道的：诊断时查到的日志、指标、Pod 的状态会发给所选的那一家（ADR-0038、设计第 2 节第 10 项原来只写了 Anthropic）。
- 重新评估的时机：要接的后端不说这个协议（比如 Gemini 的原生接口）；或者要用 OpenAI 的思考内容。那时加第三个适配器，接口仍然不动。
