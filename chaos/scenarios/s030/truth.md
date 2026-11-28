# s030 的答案

## 标准根因

缓存 Redis 里有一个大 key：一个 hash，20 万个字段、约 30 MB（脚本造的，名字是 `cache:export:orders:all`，对应“把一整个列表塞进一个 hash 当缓存”），并且有客户端每 2 秒把它整个读一遍（`HGETALL`）。
这个分片的内存比另外两个多出 30 MB（上限是 128 MB），每次全量读要在主线程里序列化 20 多 MB，占住这个节点几十毫秒，同时往外发 20 多 MB。

## 定位必需的关键证据

- 三个主节点的 `used_memory` 明显不均：一个比另外两个高约 30 MB；这个节点的出流量（`instantaneous_output_kbps`、`total_net_output_bytes`）高出一个量级。
- `redis-cli --bigkeys`（或者 `--memkeys`）在那个节点上找到这个 key；`MEMORY USAGE` 约 2900 万字节，`HLEN` 20 万，`TYPE` hash。
- `SLOWLOG GET`：反复出现 `HGETALL cache:export:orders:all`，每次几十毫秒，来源 127.0.0.1。
- `INFO commandstats` 的 `cmdstat_hgetall`：调用次数不多，`usec_per_call` 很高。
- 如果这个分片因此到了内存上限：`evicted_keys` 开始涨，订单缓存的命中率下降（只在这个分片上）。
- 处置：先停掉读它的一方，再用 `UNLINK` 删（或者 `HSCAN` 分批删）；不要 `DEL`，不要为了看它的内容再 `HGETALL` 一次。

## 容易误判成的原因

- 流量上涨导致缓存变多（键的数量没有明显变化，多出来的内存在一个 key 上）。
- 热 key（它被读得并不频繁，每 2 秒一次；问题是每次读的量）。
- 慢命令本身的问题（s029 那一类：这里慢是因为 key 大，命令是普通的 `HGETALL`）。
- 内存碎片（`mem_fragmentation_ratio` 正常）。

## 备注

- **对业务的影响是推的，没有在集群上跑过。** 脚本在本机的 Redis 上验过：key 约 28.6 MB，一次 `HGETALL` 在 slowlog 里是 48 毫秒，撤销后 key 没有了。
- 多半是安静的场景（每 2 秒卡几十毫秒，缓存读的时限是 200 毫秒），所以带一句 `report`。分片本来就接近上限时才会有淘汰：注入前看三个主节点的内存。
- **容器的内存上限是 192 MB、`maxmemory` 128 MB**：key 加上复制给从节点的缓冲不能把容器顶到上限（那样是 OOM、主从切换，是另一个故障）。所以读它的循环不经过 `redis-cli`（它会把整个应答攒在内存里），注入前内存已经高的话把 key 改小。
- key 的名字带着 order 的 `REDIS_PREFIX`（长期运行的那一套是空的，e2e 是每次运行的前缀）。
- 删除卡顿没有做成故障（`UNLINK` 是对的做法）；要练 `DEL` 大 key 的卡顿，可以在复盘时手工对一个同样的 key 试一次。
