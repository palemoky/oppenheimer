# s017 的答案

## 标准根因

网格里多了一条配置（目标命名空间里的一个 EnvoyFilter，选中 gateway 的边车）：gateway 调 payment 的出站请求里有 20% 被 gateway 自己的边车直接回了
gRPC `UNAVAILABLE`，请求根本没有发出去。payment、它的库、它依赖的 order 都是好的。对应真实世界里“有人下发了一条错的流量治理配置”。

## 定位必需的关键证据

- 发起支付（`POST /api/v1/orders/{id}/pay`）的 503 比例在某一刻突然到约 20%，之后是平的，不随流量变；别的接口没有变化。gateway 的两个副本一样。
- payment 一侧看不到任何失败：它的服务端指标没有错误，延迟不变，**收到的 `CreatePayment` 少了约两成**（稳态约每秒 11 次变成约 9 次）；
  payment 调 order 的 `GetOrder` 跟着少。失败只存在于调用方。
- 边车的指标里，gateway 作为来源（`reporter="source"`）到 payment 的请求有 `response_flags="FI"`（fault injected）的那一份，`grpc_response_status="14"`；
  payment 作为目的地（`reporter="destination"`）没有对应的失败。
- 没有摘除（`envoy_cluster_outlier_detection_ejections_enforced_total` 不涨，`MeshEndpointEjected` 不响），没有并发上限溢出：
  边车自己造的中断不算下游的失败（设计 4b 第 9 节）。
- 链路上：失败的那些请求，gateway 的客户端 span 立刻（约 0 毫秒）以 `UNAVAILABLE` 结束，下面没有 payment 的服务端 span。
- `kubectl -n <命名空间> get envoyfilter` 有一个创建时间和症状开始时间对得上的对象，内容里写着 `envoy.filters.http.fault`、去 payment 的出站 cluster、20%。

## 容易误判成的原因

- payment 挂了或者过载（它的副本都在、没有重启、服务端没有错误，只是收到的请求少了）。
- payment 的库或者它同步调的 order 有问题（那样 payment 回的是带原因码的降级错误，gateway 的 503 带原因码和 `Retry-After`；这里没有原因码）。
- 边车的熔断：摘除或者并发上限（那是 `UH`、`UO` 一类的标志和对应的告警，这里是 `FI`）。
- 网络丢包（丢包的表现是变慢和超时的 504，不是立刻返回的 503；比例也不会这么整）。
- gateway 自己重试放大了流量（同步调用链上不重试，payment 收到的请求是少了不是多了）。

## 备注

- 以上证据是按设计 4b 第 9 节的实测（延迟、HTTP 503、另一个下游）和代码推的，这个场景本身（gRPC 状态码的中断、去 payment、按比例）还没有在集群上跑过。
  要核对的：`grpc_status: 14` 这种写法生效；`response_flags="FI"` 这个标签在 Prometheus 里真的有（边车的访问日志是关着的，只能靠指标）；
  20% 的中断多久让 `SLOOrderPayAvailabilityBurnFast`（critical，5 分钟和 1 小时两个窗口都要超过 1.44%）响，按算的是 5 到 7 分钟。
- 支付在稳态里每秒只有约 11 次，20% 是每秒约 2 次失败：入口整体的 5xx 比例约 0.4%，不到 `IngressGateway5xxHigh` 的 1%。
- 数据上的后果：没付成的订单到支付时限后关单、退回库存，是正常路径；压测的客户端不重试。
