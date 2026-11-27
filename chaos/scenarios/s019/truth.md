# s019 的答案

## 标准根因

inventory 的所有副本发往调用方（gateway、order）的包被加了 4 秒延迟（Chaos Mesh 的 NetworkChaos，delay，只对着调用方的地址）。
inventory 自己是好的（Ready、处理速度正常、它的库和 Redis 正常、它的 Kafka 消费者照常），但每个调用方都在路由的时限里等不到它。
下单的失败是连带的：落库要调 inventory 的预占，预占一直失败，order 的落库熔断器打开，在受理时就拒绝。

## 定位必需的关键证据

- 查库存全部失败：开头约 3 秒是 504（等满路由的时限），之后绝大多数是立刻返回的 503，隔一阵有一小段 504。
- 下单在注入后十几秒内变成几乎全部 503，带原因码 `ORDER_LANDING_UNAVAILABLE`，立刻返回（毫秒级）；探针的下单同样失败。
- 查订单、订单列表、商家列表、发起支付的成功率和延迟基本不变，gateway 和 order 没有重启、内存和 goroutine 没有失控：故障被圈在“要用 inventory 的功能”里。
- 因果的顺序在指标上看得出来：先是 gateway、order 到 inventory 的出站 cluster 上超时和摘除（`MeshEndpointEjected`，两个端点都被摘），
  开头那几秒在途请求顶到上限（`MeshConcurrencyLimitOverflow`）；然后 order 的落库消费者调预占连续失败、落库熔断器打开（`OrderLandingBreakerOpen`）；然后才是受理拒绝。
- inventory 自己的指标全部正常，而且收到的请求大幅减少（调用方把它摘了）：服务端耗时不变、CPU 降了、没有重启。问题在它和调用方之间，不在它里面。
- 恢复是 duration 到时发生的；之后约一分钟内摘除期满、熔断器合上、积压的订单落库。

## 容易误判成的原因

- inventory 挂了、过载或者它的库慢了（它 Ready、服务端耗时正常、库的耗时正常）。
- order 的订单库或者闸门 Redis 坏了（原因码是落库不可用，容易往订单库上想；订单库的写入耗时正常，失败的是落库里调预占那一步）。
- gateway 过载或者被限流（429 没有增加；gateway 调别的下游是好的）。
- 把“下单失败”当成第一个故障去查（它是结果；最早出问题的是到 inventory 的调用）。
- 把摘除和熔断当成故障本身（它们是保护：没有它们，每个请求要等满 3 秒，gateway 的在途请求会堆到上限）。

## 备注

- 都是按 1d 的设计和本机 `make fault` 的结果（设计 1d 的 5.14 的 B 行、6.18 的 L 行）推的，没有在集群上、带负载跑过。要核对的：
  - 注入的写法（同 s018）：探针不失败、副本不掉 Ready。
  - 两个端点都被摘除之后调用方拿到的是不是立刻的 503（`maxEjectionPercent: 100`、`minHealthPercent: 0`，没有恐慌模式）；摘除期满的那几秒有多少请求等满 3 秒、并发上限（每个 gateway 副本 128）有没有顶住。
  - 落库熔断器打开之前被受理的那几百个订单的去向：已经认领、停在 CREATING 的由兜底扫描处理；没有被认领的占用约 60 到 70 秒后被回收（幂等键一起删掉），对应的消息之后落库时被丢弃。
    恢复后三个常驻核对和逐行对账应当没有差异。**这是这个场景真正要验的东西。**
  - 背压（原因码 `BACKLOGGED`，429）会不会比落库熔断器先拦；`ConsumerBlocked`、`OrderQueueNearWaitLimit`、`SLOOrderPersistDelayBurn*` 响不响。
  - 恢复要多久：10 分钟里摘除时间一次比一次长，结束时那一次可能有一分钟上下。
- 会响 critical 的告警（入口 5xx、下单可用性、探针），会发到 Telegram、触发集群里的 Agent 诊断。
- 支付的结果消息（order.events）inventory 照常消费：它到 Kafka、到闸门 Redis 的流量不受影响。
