# ADR-0005：对外 HTTP 接口用 OpenAPI 规范先行

- 状态：已采纳
- 日期：2026-11-11

## 背景

服务间的 gRPC 接口有机器可读的契约（`api/proto`，buf 生成代码），先定 proto 再写实现。对外的 HTTP 接口没有：1a 的 gateway 用 gin 手写路由和 DTO，契约只存在于 `docs/design/1a-order-flow.md` 第 6 节的表格和 handler 代码里。

这带来三个问题：

- 文档和实现各写一遍，没有任何机制保证两者一致。
- 客户端、端到端测试、后面阶段的压测脚本都要照着文档手写请求和响应结构。
- 接口的破坏性变更（删字段、改类型）只能靠人评审发现。

约束：

- 不引入重框架（ADR-0003），对外 HTTP 继续用 gin。
- gateway 是独立的一层：对外接口允许和内部 RPC 长得不一样（ID 用字符串、枚举用小写、错误结构统一、幂等键走请求头）。
- JSON 编解码用 `encoding/json/v2`，错误响应的格式和状态码映射由 gateway 自己控制。

## 决定

对外 HTTP 接口采用**规范先行**：手写 OpenAPI 文档作为唯一事实来源，用 `oapi-codegen` 生成 Go 代码，handler 实现生成出来的接口。

具体做法：

- **规范文件**放在 `api/openapi/gateway.yaml`，用 OpenAPI 3.1。和 `api/proto` 并列，表示它同样是契约。
- **生成工具**用 `oapi-codegen` v2.8.2（2026-11-11 在官方发布页查证），和 buf 一样由 `make tools` 装到 `./bin`，版本写在 `Makefile` 里并登记到 `docs/infra/versions.md`。
- **生成内容**是类型（models）和 gin 的服务端接口（`gin-server`），放在 gateway 模块内，和 `api/gen` 一样提交入库。新增 `make openapi` 目标。
- **不用 strict-server 模式**。strict 模式由生成代码接管请求解码和响应编码，gateway 就无法用 `encoding/json/v2`，也无法统一控制错误响应。普通模式下生成代码只负责路由注册和路径、查询、请求头参数的绑定，请求体的解码和响应的编码仍由 handler 自己完成。
- **暂不加运行时请求校验中间件**。入参校验沿用 handler 里现有的手写逻辑，行为保持不变。
- **防止漂移**：CI 里重新生成一次，生成结果与入库的不一致就失败。接口签名变了而 handler 没跟上，编译不过。
- 接口变更的流程改为：先改 `gateway.yaml`，运行 `make openapi`，再改 handler。

## 备选方案

- **代码先行（`swaggo/swag` 从注释生成规范）**：改动最小。但规范只是注释的产物，对实现没有约束力，注释写错或忘改不会有任何报错，没有解决“文档和实现不一致”这个核心问题。
- **从 proto 生成（`grpc-gateway` 加 `protoc-gen-openapiv2`）**：只维护一份 proto。但对外接口会被内部 RPC 的形状绑定，gateway 这一层的存在意义被抹掉；现有的错误映射、幂等键、鉴权头、ID 转字符串都要用注解或自定义 marshaler 绕着实现；基本等于放弃 gin。生成的还是 OpenAPI 2.0。
- **`ogen`**：同样是规范先行，生成的代码类型更严格、性能更好。但它自带路由和编解码，不能和 gin 一起用，也接管了 JSON 处理，和上面 strict 模式是同样的问题。
- **`huma` 这类以 OpenAPI 为中心的框架**：从 Go 类型生成规范并自动校验。属于代码先行，而且框架接管了请求处理流程，和 ADR-0003 的取向不符。
- **保持手写**：接口只有 6 个，现在的成本确实不高。但后面每个阶段都会加接口（异步下单的结果轮询、限流响应、优惠券），越晚改越贵。

## 后果

好处：

- 契约有了唯一来源，偏离契约会在编译期或 CI 里暴露。
- 可以从规范生成客户端，端到端测试和压测脚本不用手写请求结构。
- 以后可以在 CI 里对规范做破坏性变更检测，和 `buf breaking` 对 proto 做的事一样。
- 内外两套契约的工作方式一致：都是先定契约，再生成，再实现。

代价：

- 多了一个生成步骤和一个工具依赖，gateway 多一个运行时依赖（`github.com/oapi-codegen/runtime`，做参数绑定）。
- 生成的 DTO 和 proto 类型之间仍然要手写转换。这是保留 gateway 独立一层的固有成本。
- 1a 的 gateway 要重构一次：handler 改成实现生成的接口，手写的 `dto.go` 换成生成的类型。
- 规范只约束形状，不约束行为。状态码映射、幂等语义这些仍然要靠测试保证。

## 验证结论（2026-11-11，实现时补记）

决定时留了四个待验证的细节，实现后的结论如下。四项都没有推翻方案。

- **生成的类型与 `encoding/json/v2` 兼容**。生成的是普通结构体加 `json` 标签，不依赖 v1 的特有行为。可选字段生成为指针加 `omitempty`：v2 的 `omitempty` 按“编码结果是否为空的 JSON 值”判断，nil 指针编码为 `null`，会被省略，和重构前手写的 `omitzero` 效果相同。
- **ID 的表达**：规范里 `Id` 是带正则的字符串，并用 `x-go-type: ID` 指向手写的 `oas.ID` 类型（`int64`，自带 JSON 编解码）。出参是字符串；入参除字符串外仍接受 JSON 整数，这是服务端的宽容处理，规范里只在描述中说明，不要求客户端依赖。没有收紧接口。
- **中间件分组和 404、405 都能保留**。生成的 `RegisterHandlersWithOptions` 接受 `gin.IRouter`，传入一个前缀为空、挂了 `requireUser` 和 `withDeadline` 的分组即可。`X-User-Id` 在规范里建模为安全方案（apiKey）而不是请求头参数，这样生成代码不会去绑定它，仍由中间件返回 401。
- **与 gin v1.12 兼容**，生成代码只用到 `gin.Context` 和 `gin.IRouter` 的基础接口。

实现中另外确定的几点：

- **生成位置**：`services/gateway/internal/httpapi/oas/oas.gen.go`，配置在 `api/openapi/oapi-codegen.yaml`。运行时依赖 `github.com/oapi-codegen/runtime` v1.9.0。
- **路径参数按字符串生成**，是否为正整数由 handler 校验，错误消息和重构前一致。
- **枚举常量一律带类型名前缀**（`always-prefix-enum-values`）。默认只在重名时加前缀，以后新增枚举可能让已有常量改名。
- **参数绑定错误走统一的错误结构**：给生成代码传了自定义的 `ErrorHandler`，否则它会输出自己的 `{"msg":...}` 格式。
- **唯一的对外行为差异**：缺少 `Idempotency-Key` 时，状态码（400）和错误码（`INVALID_ARGUMENT`）不变，但 `message` 的文字变成了生成代码的措辞（`Header parameter Idempotency-Key is required, but not found`）。规范里已写明客户端不应依赖 `message` 的内容。另外，同一个请求里重复出现多个 `Idempotency-Key` 头现在会被拒绝（400），以前是取第一个。
- **`/healthz` 不进规范**：它由 `pkg/app` 给所有服务统一提供，不是 gateway 的业务接口。

出现以下情况时重新评估：

- 生成代码和 `encoding/json/v2` 或现有约定冲突到需要大量绕过，这时退回手写 handler，只保留规范文件加契约测试。
- 对外接口需要流式响应或 WebSocket，OpenAPI 表达不了的部分单独处理。
