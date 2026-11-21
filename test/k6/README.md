# k6 压测脚本

对网关的 8 个对外接口做压测。k6 固定 v2.4.0，本机用 Docker 镜像 `grafana/k6:2.4.0` 跑，不需要装 k6。

## 运行

```sh
# 本机（网关在 127.0.0.1:8080，容器里自动换成 host.docker.internal）
RATE=30 DURATION=20s scripts/k6-local.sh order
FLASH_SKU=1001 RATE=50 DURATION=20s scripts/k6-local.sh flash
RATE=50 DURATION=20s scripts/k6-local.sh mixed
A_RATE=200 B_RATE=50 DURATION=20s scripts/k6-local.sh ratelimit

# 阶梯：每档 3 分钟
STAGES=100:3m,125:3m,156:3m scripts/k6-local.sh order

# 额外参数原样传给 k6
scripts/k6-local.sh order --quiet
```

## 在集群上跑（发压机）

```sh
SKUS=3001 RATE=30 DURATION=20s make k6 SCENARIO=order     # 等价于 scripts/cluster/k6.sh order
```

`scripts/cluster/k6.sh` 把本目录 rsync 到发压机（Mac mini，ssh 别名 `mini`）的 `~/oppenheimer-k6/scripts/`，在上面原生运行 k6（调高 `ulimit -n`，用 `caffeinate` 防睡眠），`BASE_URL` 默认 `http://$LB_GATEWAY_IP`、`HOST_HEADER` 默认 `api.oppenheimer.test`，其余环境变量和上面一致。结果拉回 `tmp/k6/<RUN_ID>/`，结构相同，`conditions.txt` 里多记发压机的主机、系统、`ulimit -n` 和网络接口。发压机走 Wi-Fi 时终端会有醒目提示：客户端延迟不可信。Ctrl-C 会让发压机上的 k6 收尾退出，结果照常拉回。

指标同时 remote write 到集群的 Prometheus（`-o experimental-prometheus-rw`，经入口的 `/api/v1/write`），每条序列带标签 `testid=<RUN_ID>`；趋势类指标按 `K6_PROM_TREND_STATS`（默认 `p(50),p(95),p(99),max`）各输出成一条序列，如 `k6_http_req_duration_p99`。`K6_PROM=off` 关掉。

结果在 `tmp/k6/<RUN_ID>/`：

- `summary.json`：k6 的完整汇总（含自定义指标、阈值结果、`load_config`）
- `orders.jsonl`：每个被受理的订单一行，`{"run","user_id","order_id","idempotency_key","sku_id","quantity","accepted_at"}`，行数等于受理数
- `conditions.txt`：这次运行的环境变量、k6 版本、开始和结束时间（UTC）、k6 退出码
- `k6.log`：终端输出（含简短汇总）

阈值不通过时 k6 退出码是 99，包装脚本原样返回。

## 环境变量（都有默认值）

| 变量 | 默认 | 含义 |
|---|---|---|
| `BASE_URL` | `http://127.0.0.1:8080` | 网关地址 |
| `HOST_HEADER` | 空 | 要带的 `Host` 头，集群上用 `api.oppenheimer.test` |
| `RUN_ID` | `<开始时间UTC>-<场景名>`，如 `20261117T150600-order` | 运行号；进幂等键、`X-Load-Run` 头、结果目录名 |
| `USER_BASE` | 549755813888（2^39） | 用户号 = `USER_BASE + [0, USER_COUNT)` |
| `USER_COUNT` | 1000000 | 用户数 |
| `SKUS` | `1001,1002` | 逗号分隔的 SKU |
| `MERCHANTS` | `1,2` | 逗号分隔的商家号（混合场景查商家订单列表用） |
| `RATE` | 20 | 每秒到达（开始）的迭代数 |
| `DURATION` | 30s | 持续时间 |
| `STAGES` | 空 | 阶梯，如 `100:3m,125:3m,156:3m`（速率:时长）；给了就用 ramping-arrival-rate，`RATE`/`DURATION` 不再起作用 |
| `MAX_VUS` | 峰值速率 x 2（至少 50） | 虚拟用户上限 |
| `PRE_VUS` | 峰值速率 / 2（至少 20） | 预分配的虚拟用户数 |
| `POLL_INTERVAL_MS` | 200 | 轮询订单状态的第一个间隔 |
| `POLL_BACKOFF` | 2 | 每次轮询之后间隔乘以它；1 是固定间隔（2026-11-21 之前的轮次是固定 100 毫秒，要和它们比时用 `POLL_INTERVAL_MS=100 POLL_BACKOFF=1`） |
| `POLL_MAX_INTERVAL_MS` | 2000 | 间隔的上限。收到 429 时另外至少等 `Retry-After` |
| `POLL_TIMEOUT_MS` | 10000 | 轮询超时，从受理算起 |
| `PAY_RATIO` | 0.5 | order 场景：落库成功后支付的比例 |
| `CANCEL_RATIO` | 0.2 | order 场景：取消的比例（其余不动，等超时关单） |
| `REQ_TIMEOUT` | 10s | 单个请求的超时，超时算失败 |
| `FLASH_SKU` | 必填 | flash 场景抢的 SKU |
| `RAMP` | 5s | flash 场景速率从 0 升到 `RATE` 的时间 |
| `ZIPF_S` | 1.1 | mixed 场景的 Zipf 指数 |
| `A_USERS` / `A_RATE` / `B_RATE` | 10 / 500 / 50 | ratelimit 场景：A 组用户数、A 组合计速率、B 组速率 |

**用户号范围限制**：`USER_BASE + USER_COUNT` 不能超过 2^40（1099511627776）。对账工具把用户号 >= 2^40 的订单当作造数排除，超出的话压测的订单不会被对账。脚本（`lib/config.js` 和 `k6-local.sh`）会直接报错退出。

## 约定

- 全部是开环：按到达速率发起迭代，虚拟用户只是上限。如果虚拟用户不够，k6 会丢弃迭代（`dropped_iterations`），汇总第二行会显示，并标出“结果不可当作目标速率下的结果”。出现时调大 `MAX_VUS`。
- 幂等键 `lt-<RUN_ID>-<虚拟用户号>-<序号>`，全局唯一。每个请求带 `X-Load-Run: <RUN_ID>`。
- 失败 = 5xx、超时、连接错误，以及意外的 400/401/404。2xx、409、429 是预期结果，不进 `http_req_failed`，按原因码计入 `lt_reason`。
- 请求的 `name` 标签是 operationId（`createOrder` 等），URL 不进标签。
- 阈值：`http_req_duration{name:createOrder}` p99 < 200ms；`http_req_failed` 比例 < 0.1%。
- 带 `count>=0` 的阈值是占位：k6 只为有阈值的子指标（带标签的指标）统计，加上它们才能在汇总里看到按原因码、按接口的计数。它们永远通过，汇总输出里不显示。

## 自定义指标

| 指标 | 含义 |
|---|---|
| `lt_accept_ms` | 受理耗时：下单请求本身（只记 202） |
| `lt_landing_ms` | 落库耗时：从下单返回到轮询看到订单离开 `creating`。精度受轮询间隔限制（偏大；间隔退避之后落库越慢偏得越多，最多 2 秒），落库耗时以服务端的指标为准 |
| `lt_create` | 下单请求总数（不论结果） |
| `lt_reason{reason}` | 非 2xx 或失败的原因码计数；取值是 `lib/metrics.js` 里 `REASONS` 的有限集合，不认识的记 `OTHER` |
| `lt_accepted` / `lt_landed` / `lt_order_failed` | 受理 / 落库成功（`pending_payment`）/ 落库失败（`failed`） |
| `lt_paid` / `lt_cancelled` | 支付被受理（202）/ 取消成功（200） |
| `lt_poll_timeout` | 轮询超时仍是 `creating` |

## 场景量的是什么

- **order 普通下单**：一次购买的完整链路：受理耗时、落库耗时、落库成功率，支付和取消的可用性。每个迭代含轮询，迭代时间约等于落库耗时，所需虚拟用户数约为 速率 x 迭代时间。
- **flash 秒杀**：单个 SKU 上的热点：先查库存再下单，受理和落库的数量，库存不足（`INSUFFICIENT_STOCK`）的数量，闸门和落库在突发流量下的表现。汇总末尾有“秒杀”一行。
- **mixed 混合读写**：读写混合权重（文件顶部常量：查库存 50、查订单 25、订单列表 10、商家订单列表 5、下单 8、支付 2），用户和 SKU 按 Zipf 分布，少数用户和商品被反复访问，看热点上的限流（`RATE_LIMITED`，包括 `hot` 维度）和读名额（`READ_BUSY`）。不轮询。查订单、支付用本虚拟用户最近受理的 20 个订单；没有可用的时退化成下单。支付只选受理超过 1 秒的订单，避免打到还没落库的订单上。Zipf：SKU 这类小集合预先算好累积分布；用户（默认 100 万）用连续分布的反函数近似，免得每个虚拟用户背一张几 MB 的表。
- **ratelimit 限流验证**：A 组（`A_USERS` 个用户合计 `A_RATE`/s）远超单用户限额，B 组（`B_RATE`/s，大量用户）正常。指标按 `load_group=A/B` 拆开。汇总里每组有：下单请求数、受理比例、`RATE_LIMITED` 比例、`BACKLOGGED` 比例、下单耗时和受理耗时。看 A 组被限流的比例，以及 B 组的耗时和成功率是否被 A 组拖累。两组都只下单、不轮询。

## 订单号清单怎么出来的

k6 脚本不能写文件（`handleSummary` 只能在结束时写一次）。办法：下单被受理时 `console.log` 一行 JSON，运行时用 `--console-output=/out/console.log` 把 console 输出写到文件（用 `--log-output=file=...` 也行，但它混着 k6 自己的日志，这里用专门的 console 输出）。k6 2.4 写出的每行是 logfmt：`time=... level=info msg="<转义的 JSON>"`，包装脚本用 `jq` 取出 `msg` 还原，写成 `orders.jsonl`；清单以外的 console 输出（如果有）留在 `console-other.log`。

开销：每个受理的订单一次 `console.log`，由 k6 写文件，每条约 250 字节，本机 50 次/秒的冒烟里测不出差别。高速率（上千/秒）下的开销没有实测（推断：日志写入在 VU 的 JS 线程之外，但会占一些 CPU 和锁）；要测的话对比有无 `console.log` 时的 `lt_accept_ms` 和 `dropped_iterations`。受理数和 `orders.jsonl` 行数不一致时以 `lt_accepted` 为准，并怀疑日志丢失。

## 内存

一个虚拟用户占几 MB 是官方说法。本脚本在本机实测（`PRE_VUS=MAX_VUS=50` 对 500，`RATE=1`，看 `docker stats`）：每个虚拟用户约 0.33MB（空闲时容器内存 35MB -> 183MB）。负载下会高一些（响应体、GC）。迭代里的轮询会拉长迭代时间、增加需要的虚拟用户数；不需要响应体的请求（列表、查库存、支付、取消、查支付）用 `responseType: 'none'` 丢弃。
