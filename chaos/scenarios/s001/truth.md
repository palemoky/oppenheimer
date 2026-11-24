# s001 的答案

## 标准根因

gateway 查库存的接口给下游调用套了一个 10 毫秒的时限：另起一个 goroutine 去调 inventory 的 `GetStock`，
主流程等结果或者等定时器。结果用的是**没有缓冲的 channel**，定时器先到时主流程已经返回，
那个 goroutine 之后往 channel 里发结果时没有人收，永远阻塞。每一次超过 10 毫秒的库存查询泄漏一个 goroutine，
同时这次请求返回 504。

代码：`services/gateway/internal/httpapi/stock_variant.go` 的 `stockWithin`，开关 `f01`。

## 定位必需的关键证据

- gateway 的 goroutine 数（运行时指标）只涨不降，增长的速度和 `GET /api/v1/skus/{id}/stock` 的 504 的速率一致。
- goroutine 剖析里有大量 goroutine 停在同一处的 `chan send`，调用栈在 `httpapi.(*server).stockWithin` 起的那个匿名函数里。
- 这个接口的 504 集中在下游耗时刚过 10 毫秒的请求上，而 inventory 自己的延迟和错误率没有变。

## 容易误判成的原因

- inventory 变慢或者出错（它是正常的：平时 p99 约 9 毫秒，有约 1% 的请求本来就超过 10 毫秒）。
- 内存泄漏（内存确实在涨，但那是结果：每个泄漏的 goroutine 占着自己的栈和一份响应）。
- 网格里的超时或者熔断（边车没有摘除任何端点，504 是 gateway 自己返回的）。

## 备注

平时 `GetStock` 的 p50 约 3.6 毫秒、p99 约 9.2 毫秒（2026-11-24 量的，两天的窗口，只有探针每秒约 1 次的流量），
所以开关打开后大约每 100 次库存查询泄漏一次：只有探针时每小时几十个，有压测时很快。
给 inventory 注入延迟（另一个场景）和它叠加时，泄漏的速度会成倍上去。
