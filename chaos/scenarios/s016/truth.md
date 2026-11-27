# s016 的答案

**全部是按设计推的，还没有在集群上试过（2026-11-27 写）。试过之后按实测改，改之前不进池。**

## 标准根因

两件事叠在一起，单独哪一件都不出事：

1. inventory 的 `GetStock` 把整次查询放在一把进程级的 `sync.Mutex` 里做（开关 `f03`，和 s010 是同一处：
   `services/inventory/internal/service/stock_lastknown.go` 的 `stockOrLastKnown`，`Lock()` 加 `defer Unlock()`
   包住了读缓存 Redis、未命中时的回源和写回）。每个副本上查库存的查询排成一队，一次只过一个，吞吐上限是 1 / 锁里一次的时间。
2. inventory 到缓存 Redis 的往返慢了 5 毫秒。平时查库存是并行的，每次慢 5 毫秒谁也不会注意。

锁里一次从约 0.7 毫秒变成约 6 到 8 毫秒，每个副本的上限降到 125 到 170 次/秒，而到达约 100 次/秒：锁的占用率 0.6 到 0.8，
排队时间远大于那 5 毫秒本身。**根因是锁的范围（把网络往返包进了临界区），Redis 的延迟只是把它放大出来的条件**：
只修延迟，下次任何让锁里那次 I/O 变慢的事（Redis 抖动、一个副本没了流量压到另一个上）都会再来一次。

## 定位必需的关键证据

- `GetStock` 的服务端耗时（和 gateway 的 `GET /api/v1/skus/{id}/stock`）的 p99 到几十至几百毫秒，**远大于 Redis 多出来的 5 毫秒**；
  同一个进程里的 `Reserve` 不变，inventory 的 CPU 不变。
- 链路：`GetStock` 的服务端 span 里，Redis 的子 span 每个只多约 5 毫秒；大头在服务端 span 开始到第一个子 span 开始之间，那一段没有 I/O（在等锁）。
  能说出“延迟解释不了这么多”是关键的一步。
- block 剖析（常开）隔一段时间抓两次做差值：`sync.(*Mutex).Lock` ← `stockOrLastKnown` ← `GetStock` 排在前面；
  mutex 剖析的差值里 `sync.(*Mutex).Unlock` ← `stockOrLastKnown`。goroutine 剖析能抓到多个 goroutine 停在 `sync.runtime_SemacquireMutex`。
  （单独开 `f03` 时这些几乎采不到，见 s010 的实测；这里占用率高了一个量级，应当明显，待实测。）
- 对照：inventory 进程里别的用缓存 Redis 的路径没有；gateway 的限流也用 `redis-cache`，但延迟只加在 inventory 的 Pod 上，gateway 到 Redis 不慢。

## 容易误判成的原因

- **缓存 Redis 慢了（最容易停在这里）**：Redis 确实慢了 5 毫秒，客户端的命令耗时指标和子 span 都看得到，于是把它当成全部原因。
  但 `redis-cache` 自己的指标（服务端命令耗时、CPU、连接数）正常，别的客户端（gateway 的限流）不慢，而且 5 毫秒解释不了几百毫秒的 p99。
  对照实验（只有延迟、不开开关）：查库存每次慢约 5 毫秒，没有排队。
- 网络或节点的问题：只有 inventory 到 Redis 这一段慢，inventory 到 MySQL、order 到 inventory 都正常。
- MySQL 慢或连接池不够：同一时刻最多一个查库存的查询在用连接，在用数反而更低。
- `pkg/cache` 的重建锁：另一把锁（Redis 里的 `cache:lock:*`），延迟会让它等得久一点，不是根因。
- 网格的并发上限（gateway 每个 Pod 对 inventory 128 个在途）或 3 秒时限：排队严重时才出现，是结果。

## 备注

- **参数**：`f03` 20 分钟；NetworkChaos `delay` 5 毫秒、出方向、目标是注入那一刻 `redis-cache` 6 个 Pod 的地址，20 分钟。先开开关再建对象，恢复相反。
- **`stock-cache` 的熔断器不会打开（读代码得出，e2e 里核对）**：`pkg/breaker` 默认连续 5 次失败才开，缓存读一次的时限是 200 毫秒（`pkg/cache` 的 `defaultReadTimeout`），
  5 毫秒的延迟不算失败。要是它开了，查库存绕过缓存直接查库，锁里的时间变成一次查库，现象就不是这里写的了。
- **不连带扣减路径**（同 s010）：Reserve、Confirm、Release 不走这把锁、不用缓存 Redis（闸门是另一个实例）。预期对账没有差异。
- **不要把延迟加到 10 毫秒以上**：每个副本的上限低于到达速率，队列只涨不降，查库存基本不可用（504、503 `MeshConcurrencyLimitOverflow`）。
- Pod 地址是快照：注入之后重建的 Redis Pod 不受影响（Redis Cluster 的客户端会连到新地址），故障会变弱。
