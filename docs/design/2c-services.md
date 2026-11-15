# 2c 设计：服务上集群

本文是阶段 2c 的实现规格：把六个业务服务（gateway、order、inventory、payment、orderindex、cdc）做成镜像，部署到 2a、2b 建好的集群上，每个服务多个副本，配好探针、启停、滚动发布和自动伸缩，并让阶段 1 的端到端测试和一致性测试对着集群入口通过。

已有的决定，本文不重复理由：入口是 Envoy Gateway 加 MetalLB（ADR-0025）；服务之间的熔断和并发隔离在每个服务自己的 Envoy 边车里（ADR-0020）；降级和优雅启停的语义（ADR-0021）；中间件的拓扑和连接方式（设计文档 [2b-middleware.md](2b-middleware.md)）。

状态：主人已确认（2026-11-15）。第 3 到 8 项按建议；第 1 项用 MySQL 租约（ADR-0028，实现时把方案改得更强，见 5.3）；第 2 项分两步：2c 用手写的边车，**2d 迁到 Istio**（主人希望贴近真实环境，见 6.5）。第 7 项改了阶段计划的写法。

## 1. 动工前查到的事实（2026-11-15）

写之前读了各服务的装配代码、边车的配置、启停的公共库和本机的脚本。到现在为止每个服务在任何环境里都只跑过一个实例，下面这些是“变成多个实例”会碰到的。

**1. 订单号和支付单号里的节点号现在是手工写死的。** `pkg/idgen` 的号是“毫秒时间、6 位节点号、10 位序列号、6 位基因”，唯一性靠“同一时刻没有两个进程用同一个节点号”。本机靠 `run-local.sh` 里写 `WORKER_ID=2`（order）、`WORKER_ID=3`（payment）。用到它的只有 order 和 payment，两者发的是不同的号（订单号、支付单号），所以只要求同一个服务内部不重复。一个 Deployment 的所有 Pod 环境变量相同，两个副本用同一个节点号，同一毫秒里各自从序列号 0 开始发，**必然重号**。节点号只有 6 位，一共 64 个。这是 2c 里唯一一处不改代码就会出一致性问题的地方（见第 5 节）。

**2. 周期任务已经为多实例做过准备，逐个核对的结果：**

| 任务 | 多实例下靠什么 | 结论 |
|---|---|---|
| order 的 outbox 中继（每库一个）、sweeper、reaper | Redis 上的 leader 锁，`lock.FailOpen` | 可以多副本。锁的 Redis 不可用时所有副本都会跑（有意的，1b 的设计），靠各自语句的幂等 |
| payment 的 outbox 中继、处理器 | 同上 | 可以多副本 |
| inventory 的闸门对账 | leader 锁 | 可以多副本 |
| inventory 的库存增量应用者 | 没有锁，靠增量行上的行锁互斥，偶尔死锁后重试（`store/pending.go` 的注释，有并发测试） | 可以多副本 |
| 各服务启动时的迁移 | `mysqlx` 的 `GET_LOCK` 会话锁 | 可以同时启动。**有一个没查到根因的偶发失败**（`TestMigrateConcurrentOnEmptyDB`，CI 上一次，PROGRESS 里记着）：它对应的正是“多个副本同时在空库上第一次启动”，2c 的第一次部署会真的走到这条路径，要盯着 |
| cdc | 没有任何互斥；每条流水线用一个固定的 `server-id` 伪装成从库 | **只能有一个实例。** 两个实例用同一个 `server-id` 连同一个源，MySQL 会把先连上的那个踢掉，两边轮流重连 |

**3. 进程内的状态乘以副本数。** gateway 按 IP 的限流和热点参数限流是进程内的令牌桶（`ratelimit.NewLocal`），N 个副本就是 N 倍的限额；按用户加接口的限流在 Redis 里，不受影响。降级开关（`pkg/degrade`）在每个进程自己的内存里，经各自的管理端口改，重启后回到 `DEGRADE` 环境变量的初始值：N 个副本要改 N 次，新起来的副本不知道别人被改过。各服务的连接池（order 每个库默认 25，主库和从库各一套）也乘以副本数，而 `mysql-order-N` 的 `max_connections` 是 200、`mysql-main` 是 400。

**4. 边车的配置是为“每个下游一个实例”写的。** `deploy/envoy/*.yaml` 里下游的地址是 compose 的容器名（`envoy-inventory:15001`），本机进程的地址是 `host.docker.internal`。集群类型已经是 `STRICT_DNS`：这个类型会把域名解析出来的**每一个地址**当成一个独立的上游，定期重新解析。`max_ejection_percent: 100` 和 `healthy_panic_threshold: 0` 是 1d 里为单实例设的（5.3 的第一个坑），多实例下要重新看。Go 的测试会读这几个文件，核对出站路由的超时和代码里的时限（差 500 毫秒）。

**5. 启停的公共部分已经有了，但只在单实例下跑过。** `pkg/app` 有 `/healthz`、`/readyz`、`DRAIN_DELAY`（收到 SIGTERM 后先变成未就绪、继续服务一段时间再停）、停止总时限 20 秒、gRPC 健康检查在摘流开始时置为不可用。`DRAIN_DELAY` 在本机一直是 0，该设多少取决于“一个 Pod 被摘掉这件事传到每个调用方要多久”，这个数只能在集群上量。

**6. 端到端测试不只是访问入口。** `test/e2e` 还会：直连 5 个 MySQL 实例读数据做核对；在订单从库上停复制线程（主从延迟的用例）；在测试进程里起一个 OTLP 接收端收各服务的链路；直接运行 `archiver` 的可执行文件。服务用的是一套缩短的时限（`PAYMENT_TIMEOUT=3s` 等）和带前缀的库、topic、Redis 键。所以“对着集群入口跑 e2e”需要：一套单独配置的服务，和一个能连到集群里 MySQL 的测试进程。

**7. 集群上还没有指标系统。** k3s 自带 metrics-server（`kubectl top` 可用），够做按 CPU 的 HPA。按 QPS 伸缩要有地方查 QPS：Prometheus 加适配器，或者 KEDA 加 Prometheus，都是阶段 3a 才有的东西。

**8. 资源。** 三个节点还能承诺的内存约 10.9 Gi（2b 的“量到的数”），预算里给业务服务的是 3 GB、监控 5.5 GB。服务的二进制每个 32 到 52 MB（arm64 的本机构建）。往仓库推镜像约 11 MB/s（百兆链路）。

## 2. 主人确认的八项

| # | 问题 | 建议 | 主要的另一种做法 |
|---|---|---|---|
| 1 | 节点号怎么分（**要改代码**） | 启动时从本服务的 MySQL 里租一个号，定期续租，续不上就停止发号（第 5 节） | order、payment 改成 StatefulSet，节点号取 Pod 的序号；不写代码，但没有滚动发布时的多开，节点挂了 Pod 不会自动换地方 |
| 2 | 服务之间的边车 | 沿用 1d 的 Envoy 配置，作为原生 sidecar 容器进 Pod；找下游靠 headless Service 加 `STRICT_DNS`（第 6 节） | 换成 Istio 或 Linkerd，配置由控制面下发 |
| 3 | chart 怎么组织 | 一个公共 chart `deploy/charts/service`，每个服务一份 values，装六次（7.1） | 每个服务一个 chart |
| 4 | 镜像怎么构建 | 一个 Dockerfile 用参数选服务；在 MacBook 上交叉编译成 `linux/amd64`；运行镜像用 distroless 的静态基础镜像、非 root；tag 用 git 提交号（第 4 节） | 基础镜像用 alpine（有 shell，排查方便）；或者在集群里构建 |
| 5 | e2e 和一致性测试怎么对着集群跑 | 单独的命名空间 `oppenheimer-e2e` 里装一套缩短时限的服务，测试作为 Job 在集群里跑，经入口的地址访问（第 10 节） | 测试在 MacBook 上跑，MySQL 经 `kubectl port-forward` 连 |
| 6 | 副本数和资源 | gateway、order、inventory、payment、orderindex 各 2 个，cdc 1 个；内存 requests 合计约 1.9 Gi（7.4） | 先都跑 1 个，验收前再加 |
| 7 | HPA 做到哪（**改计划**） | 2c 只做按 CPU 的（gateway 和 order），按 QPS 的挪到阶段 3a，等有了 Prometheus（第 9 节） | 2c 里为它单独装一个 Prometheus 加适配器 |
| 8 | 一键部署 | `make deploy`：一个脚本构建、推送、`helm upgrade --install`，和 `middleware.sh` 同样的写法；Argo CD 留在 2d（第 11 节） | 现在就上 Argo CD |

## 3. 总体的样子

```
MacBook ──► 192.168.50.51:80（MetalLB，入口的 Envoy ×2）
              │  HTTPRoute（命名空间 oppenheimer）
              ▼
        gateway ×2 ──► [边车出站] ──► headless Service ──► [边车入站] order ×2 / inventory ×2 / payment ×2 / orderindex ×2
                                                               order ──► inventory，payment ──► order（同样经边车）
        cdc ×1 ──► 订单从库的 binlog ──► Kafka ──► orderindex

        MySQL（mysql 命名空间）、Redis（redis）、Kafka（kafka）：2b 已有，服务按 2b 定的地址连
```

- 命名空间 `oppenheimer`（长期运行的那一套）和 `oppenheimer-e2e`（测试时才有，跑完删）。
- 各服务连中间件的地址（Service 名是 2026-11-15 从集群里读的）：
  - Redis 闸门和锁：`sentinel://gate@redis-gate-sentinel.redis.svc:26379`（主库组的名字 `gate` 实现时对一下 chart）。
  - Redis 缓存和限流：`cluster://redis-cache.redis.svc:6379`（headless，解析出 6 个 Pod）。
  - MySQL：`mysql-order-0-primary`、`mysql-order-0-replica`、`mysql-order-1-primary`、`mysql-order-1-replica`、`mysql-main-primary`，都在 `mysql` 命名空间，3306。`mysql-main` 没有 `-replica`。
  - Kafka：`kafka-kafka-bootstrap.kafka.svc:9092`。
- Pod 网段是 `10.42.0.0/16`（三个节点各一个 /24）。
- cdc 的源是从库 Pod 的稳定名字（`mysql-order-0-1.mysql-order-0.mysql.svc`），不是 `-replica` Service：它要认准一个实例的 binlog 位点。**这个名字和 `primaryOrdinal` 绑着**，主从切换之后要跟着改（切换手册里已经有“CDC 换源”这一步，2c 把它验证掉）。

## 4. 镜像

- **一个 Dockerfile，两个阶段。** 构建阶段用官方的 Go 镜像，`--platform=$BUILDPLATFORM`（在 MacBook 上就是 arm64 原生运行），靠 `GOOS=linux GOARCH=amd64 CGO_ENABLED=0` 交叉编译。不用 QEMU 模拟 amd64 来编译：Go 自己会交叉编译，模拟要慢一个数量级。运行阶段只有一个静态的二进制。
- **构建上下文是整个仓库**：多模块的 workspace，服务依赖 `pkg/` 和 `api/`，靠 `go.work` 解析。`.dockerignore` 排掉 `bin/`、`tmp/`、`.git`、`docs/`。依赖下载和编译缓存用 BuildKit 的缓存挂载，改一行代码不用重新下载模块。
- **基础镜像用 distroless 的 static（nonroot）**：没有 shell、没有包管理器，只有 CA 证书、时区数据和一个非 root 用户。服务不需要别的。代价是 `kubectl exec` 进不去，排查用 `kubectl debug` 挂一个临时容器，这本身是值得练的。order 的镜像里同时放 `order` 和 `archiver` 两个二进制（归档任务用同一个镜像，换命令）。
- **tag 是 git 的短提交号**，工作区有没提交的改动时加 `-dirty-<时间>`。不用 `latest`，也不重用 tag：Deployment 里的镜像没变，`helm upgrade` 就不会滚动；节点上缓存着同名的旧镜像时也不会重新拉。
- **版本号**（Go 镜像、distroless）在写 Dockerfile 时去官方页面查证，登记到 `versions.md`。
- 镜像名 `192.168.50.50:5000/oppenheimer/<服务>:<tag>`。`scripts/cluster/images.sh build|push`，`make images`。六个镜像共用基础层，每个只多一层二进制。（动工前估的是每层约 40 MB、全推一遍约半分钟；实测镜像 4 到 18 MB，七个构建加推送 2 分 20 秒，大头是构建，见第 13 节第一步。）

要讲得清楚的：多阶段构建为什么能把镜像从约 1 GB 降到几十 MB；镜像层和缓存失效的顺序（先复制 `go.mod` 再复制代码）；静态链接和 `CGO_ENABLED=0` 的关系；为什么不用 root 运行。

## 5. 节点号

### 5.1 问题

见第 1 节的第 1 条。要的性质是：**任何时刻，一个服务里用同一个节点号发号的进程最多一个**，包括滚动发布时新旧 Pod 同时存在、节点失联后旧 Pod 还在跑而新 Pod 已经起来的时候。

### 5.2 三种做法

**A. StatefulSet 的序号。** `order-0`、`order-1`，节点号取 Pod 名的序号。Kubernetes 保证同一个序号同一时刻最多一个 Pod，一行代码不用写。代价都来自这个保证本身：

- 滚动发布是原地一个一个换（先删 `order-1` 再建 `order-1`），没有“先起新的再停旧的”，发布期间少一个副本的容量。
- 节点失联时，上面的 Pod 停在 Terminating，StatefulSet **不会**在别的节点上建替代的（它不能确认旧的已经死了），要人工强制删除。阶段 4 的“关掉一个节点”实验里，order 会一直少一个副本直到人介入。
- 节点号的个数等于副本数的上限，这倒没有问题。

**B. 从 Redis 租。** `SET NX` 一个带过期时间的键。Redis 的主从复制是异步的：主库挂了、Sentinel 提升从库，刚写的租约可能丢，另一个进程能租到同一个号，而原来的持有者要到下一次续租才发现。可以靠“租到之后先等一个续租周期再用”堵上，但正确性建立在闸门那套 Redis 的故障切换行为上。

**C. 从 MySQL 租（建议）。** 每个发号的服务在自己的库里有一张 64 行的小表，一行一个号：

```sql
CREATE TABLE IF NOT EXISTS idgen_worker (
  worker_id   TINYINT UNSIGNED NOT NULL PRIMARY KEY,   -- 0..63
  owner       VARCHAR(128) NOT NULL DEFAULT '',        -- Pod 名加启动时生成的随机串
  expires_at  DATETIME(3) NOT NULL,                    -- 库的时钟
  last_ms     BIGINT NOT NULL DEFAULT 0                -- 这个号发出过的最大毫秒时间
);
-- 租：挑一个过期的，条件更新，看影响行数
UPDATE idgen_worker SET owner = ?, expires_at = NOW(3) + INTERVAL ? SECOND
 WHERE worker_id = ? AND expires_at < NOW(3);
-- 续：只有自己还是持有者才成功
UPDATE idgen_worker SET expires_at = NOW(3) + INTERVAL ? SECOND, last_ms = GREATEST(last_ms, ?)
 WHERE worker_id = ? AND owner = ?;
```

要点：

- **过期与否只由库的时钟判断**（`NOW(3)`），不比较两个 Pod 各自的时钟。
- **持有者自己设限**：进程记下最后一次续租成功的本地单调时刻，超过“租期减一个余量”还没续上就停止发号（`Next` 返回错误，受理按降级处理），不等库告诉它。这样即使进程卡住很久（虚拟机暂停、长 GC），醒来后的第一件事是发现自己过期了，而不是接着用一个可能已经被别人租走的号。租期 30 秒、每 10 秒续一次、余量 5 秒是起点，实现时再定。
- **`last_ms` 防两台机器的时钟差**：正常退出时把租约立刻让出来，下一个持有者可能在另一个节点上、时钟慢几毫秒，会发出旧持有者已经用过的“毫秒加序列号”。新持有者租到后等自己的时钟超过 `last_ms` 再发号（通常是 0 等待）。进程内的时钟回拨处理（`idgen` 已有）不变。
- **放在哪个库**：order 放在第一个订单库（`order_db_0`），payment 放在 `payment_db`。order 启动时本来就要连订单库做迁移，没有多出新的硬依赖；租约的库不可用时 order 也落不了库，受理已经会被“落库健康”拒绝（ADR-0021）。（这里原来写“停止发号没有扩大故障面”，不对，见 5.3。）
- **主从切换时**：手册的顺序是“旧主只读、等从库追平、再提升”，租约表跟着复制走，不丢。主库故障的切换（异步复制可能丢最后几个事务）可能丢一次续租，丢的是“延长”，不是“持有者是谁”，持有者下一次续租照常；丢的如果恰好是一次“租到”，新主库上这一行是过期的，另一个进程可以租走，而原持有者下一次续租会因为 `owner` 不符而失败并停止发号。两者之间有最多一个续租周期的重叠。和 B 一样用“租到之后先等一个续租周期再发号”堵上，区别是这里只在主库故障切换时才用得上，而那是阶段 4 才做的人工操作。
- 64 个号用完时启动失败并说清楚原因。它同时是 order、payment 各自副本数的硬上限（HPA 的 `maxReplicas` 远小于它）。
- 本机和 CI：`WORKER_ID` 设了就用它、不租（现状不变）；没设才租。集群上不设。

建议 C 的理由：Deployment 的滚动发布和节点故障后的自动替换都保住了，这两件事是 2c 的验收项和阶段 4 的实验对象；“号段和节点号怎么分配”本身是分布式 ID 的标准考点，A 把它绕过去了。代价是多一段一致性代码（由 Fable 写，带并发和假时钟的测试），以及受理路径多一个“租约过期”的失败原因。

已写成 ADR-0028。

### 5.3 实现时改掉的地方（2026-11-15）

上面 C 的写法是“持有者在本地判断租约还有没有效，过期前停手”。动手时发现它的正确性要靠比较两台机器的时钟（持有者的钟不能比数据库慢、进程不能在判断和使用之间卡住），于是把 `last_ms` 的含义从“发出过的最大毫秒”改成了**上界**，唯一性改由它来保证：

- 持有者每次续租把上界抬到“自己的时钟加一个租期”，数据库确认之后才用到那么远；毫秒时间超过已确认的上界就拒绝发号。
- 接手的人读到上一任留下的上界，只发比它大的毫秒（另加一个续租间隔的保护，为主库故障切换丢掉最后一次续租留的）。
- 上一任的区间在上界以内，下一任的在上界以外。不再需要“持有者自己设限”那一条本地的计时，租期和续租间隔只影响可用性。

实现：`pkg/idgen/lease.go`、`lease_sql.go`，装配入口 `idgen.Open`。表由 `NewSQLLeaseStore` 自己建，没有放进迁移文件（order 的迁移每个订单库都跑一遍，这张表只该在一个库里）。测试里有“上一任被接手之后还在发号”“两边时钟差 20 秒”“去掉这条规则测试会失败”（手工改坏验证过）。

写的时候顺带发现 `Generator.Next` 一个原有写法的隐患：序列号在检查之前就被改掉，新加的“拒绝发号”路径会把“这一毫秒用到哪了”冲掉，时钟再回到同一毫秒时会重号。已改成检查都通过才写回，有测试。

**一处和 5.2 说的不一样、要记住的后果**：5.2 写的是“停止发号没有扩大故障面”，不对。落库健康是按库判断的，订单库 0 不可用时原来只拒绝一半用户；现在租约在订单库 0 上，它不可用超过约 20 秒之后**所有**用户的下单都被拒绝。见 ADR-0028 的后果一节。

## 6. 边车进 Pod

### 6.1 形态

每个有 gRPC 的服务的 Pod 里两个容器：服务本身，和它的 Envoy 边车。边车写成**原生 sidecar**（`initContainers` 里 `restartPolicy: Always` 的容器），解决的是顺序问题：

- 启动：边车先起来并就绪，服务才启动。否则服务启动时的预热调用（order 预热闸门要调 inventory）会打在还没监听的出站端口上。
- 停止：服务容器全部退出之后，kubelet 才给边车发 SIGTERM。服务在 `DRAIN_DELAY` 和停止阶段还要处理在途请求、还要调下游，边车必须活到最后。普通的多容器 Pod 里两个容器同时收到 SIGTERM，Envoy 先退出的话在途请求全部失败。

cdc 没有 gRPC 调用，没有边车。gateway 的 HTTP 入站不经过边车（入口的 Envoy 直接连 gateway 的 8080），它的边车只管四个出站。

### 6.2 找下游：headless Service 加 STRICT_DNS

每个服务两个 Service：

- 普通的 ClusterIP Service（`order`）：给不经过边车的访问用（入口到 gateway、调试）。
- headless Service（`order-headless`，`clusterIP: None`）：DNS 返回所有**就绪** Pod 的地址。调用方边车的出站集群指向 `order-headless.oppenheimer.svc.cluster.local:15001`，`STRICT_DNS` 把每个地址当一个上游，按请求轮流。

这同时回答了计划里的“gRPC 在 K8s 里的负载均衡问题”：

- gRPC 跑在 HTTP/2 上，一条 TCP 连接上复用所有请求，而且连接是长的。ClusterIP 的负载均衡（kube-proxy）是按**连接**做的：一个调用方进程对一个 Service 只建一条连接，这条连接落在哪个 Pod，之后所有请求都去那个 Pod。下游有两个副本，一个满载一个空闲；扩容出来的新 Pod 一个请求也收不到，直到调用方重连。
- 解法都是把均衡从连接层挪到请求层：客户端自己做（gRPC 的 `dns:///` 加 `round_robin`，要 headless Service），或者中间放一个懂 HTTP/2 的代理按请求分发。这里的边车就是后者：服务进程到自己的边车是一条本地连接，边车对每个下游 Pod 各维持连接、按请求选。
- **先看到问题再解决**：部署跑通后做一个对照实验，`SIDECAR=off` 式的直连（`INVENTORY_ADDR` 指向 ClusterIP Service），压一轮，看两个 inventory 副本各收到多少请求；再换回边车看一遍。数字记进第 13 节。

### 6.3 配置的来源只有一份

`deploy/envoy/*.yaml` 仍然是唯一的来源（本机 compose 在用，Go 的测试在核对）。部署脚本把两处地址替换掉再交给 chart（`helm --set-file`），不在 chart 里另存一份：

- `host.docker.internal` → `127.0.0.1`（同一个 Pod 里）。
- `envoy-<服务>` → `<服务>-headless.<命名空间>.svc.cluster.local`。

替换之后的结果用 `make envoy-validate` 同样的方式校验一遍再部署。

### 6.4 多实例下要重新看的数

- **`max_ejection_percent: 100`、`healthy_panic_threshold: 0`** 保留。两个副本时默认值仍然不行（10% 的 2 是 0 个）。含义变了：现在摘除的是**一个 Pod**，另一个继续接流量；两个都被摘时立刻失败，和单实例时一样。
- **并发上限是每个调用方 Pod 各算各的**：`max_requests: 128` 是“这一个 gateway Pod 对 inventory 的在途请求数”。两个 gateway 就是合计 256。入站的上限（1024）是每个被调用 Pod 的。数值先不改，量过再说。
- **DNS 刷新**：一个 Pod 被删到调用方的边车不再给它发请求，中间隔着“端点变成未就绪、CoreDNS 的缓存和记录的 TTL、Envoy 的 `dns_refresh_rate`”。集群上 CoreDNS 的配置是 `cache 30`、`kubernetes` 插件没有写 `ttl`（读过 ConfigMap）；其余几个数的默认值按记忆是 5 秒量级，**不照记忆写，在集群上量**（删一个 Pod，持续请求，看调用方最后一次把请求发给它是什么时候）。量出来的数决定 `DRAIN_DELAY`（第 8 节）。
- **被摘流的 Pod 在途的请求**：出站路由不重试（ADR-0020：同步链上不重试）。靠的是 Pod 在 `DRAIN_DELAY` 里继续正常服务，而不是出错后重试。

### 6.5 为什么不换服务网格

Istio、Linkerd 做的就是这件事的通用版：自动注入边车，端点经 xDS 实时下发（没有 DNS 的延迟），还带 mTLS。不换的理由：1d 的配置和三个坑是逐项实测过的，时限和代码对着；控制面要多占 1 GB 上下的内存，现在的余量放不下；网格会把“端点怎么传到代理、摘流要等多久”这些正要弄明白的事盖住。代价是端点靠 DNS，变化传得慢，要用 `DRAIN_DELAY` 垫；服务多了以后手写配置不可维护。记进 ADR-0020 的补充。

**主人的决定（2026-11-15）**：服务网格更贴近真实环境，要做，但分两步。2c 先用手写的边车，把端点传播、摘流要等多久这些事亲手量一遍；**2d 把服务之间的流量治理迁到 Istio**（熔断、并发上限、路由超时改写成它的资源，逐项对照 1d 的三个坑重新验证）。届时要处理的：内存（估计控制面加 10 个边车 1.5 到 2 Gi，没有实测；那时监控还没装，只能按预算算）；本机 compose 仍是手写的边车，同一个超时要在两处维护，Go 里核对时限的测试要有对应的办法。

## 7. chart 和 Pod 的规格

### 7.1 一个公共 chart

`deploy/charts/service`，每个服务一份 values（`deploy/cluster/services/<服务>.yaml`），装成六个 release。六个服务的形状相同（一个容器、一个可选的边车、两个 Service、探针、PDB），差别都是数据：镜像、环境变量、端口、副本数、要不要边车、要不要 HPA、要不要 HTTPRoute。每个服务一个 chart 会把同一份模板抄六遍。cdc 用同一个 chart，`replicas: 1`、`strategy: Recreate`、没有边车和 Service。

### 7.2 配置和密钥

- **普通配置**进 ConfigMap，用 `envFrom` 整个注入。服务的配置入口本来就是环境变量，不用改代码。
- **密码**：MySQL 的密码在 `mysql` 命名空间的 `<release>-auth` 里，Secret 不能跨命名空间引用。部署脚本把服务需要的那几个键（`app-password`、cdc 的 `cdc-password`）复制成服务命名空间里的 Secret；DSN 用 Kubernetes 的变量展开拼出来（`value: "app:$(MYSQL_ORDER_0_PASSWORD)@tcp(mysql-order-0-primary.mysql.svc:3306)/order_db_0"`），密码不进 ConfigMap 和 values。服务用 `app` 账号（2b 的 accounts 任务建的，只对本实例的业务库有权限），不用 root。
- **配置变更怎么生效**：环境变量只在进程启动时读，改了 ConfigMap 不会自己生效。Pod 模板上加一个 ConfigMap 内容的校验和注解，内容一变模板就变，`helm upgrade` 触发滚动更新。这是有意的：配置变更和发版走同一条有摘流、有就绪检查的路径。**运行中要改的东西不走这条路**：降级开关走管理端口（见 7.6 的遗留问题）。
- 要讲得清楚的：ConfigMap 挂成文件会自动更新（有延迟，`subPath` 挂的不会），注入成环境变量不会；Secret 默认只是 base64，etcd 里没有开静态加密（2b 的发现 11）。

### 7.3 探针

| 探针 | 服务容器 | 边车 |
|---|---|---|
| startup | `GET /healthz`，给足时间（迁移、建 topic；第一次部署在空库上最慢） | `GET /ready`（管理端口 9901） |
| liveness | `GET /healthz` | `GET /ready` 换成只看进程能应答 |
| readiness | `GET /readyz` | `GET /ready` |

- liveness 只回答“进程还能不能响应”，不检查任何依赖。依赖坏了重启自己没有用，还会让所有副本一起重启（`pkg/app` 的注释里已经写明就绪也不看依赖）。
- startup 探针存在的意义是让 liveness 不必为了容忍慢启动而放宽：启动期间由 startup 管，过了之后 liveness 可以用短的阈值。
- `/readyz` 在启动完成前和收到 SIGTERM 后是 503。Pod 的就绪还要求边车就绪。

### 7.4 副本数、资源、QoS

| 服务 | 副本 | 服务容器内存 requests / limits | CPU requests | 边车内存 requests / limits |
|---|---|---|---|---|
| gateway | 2（HPA 2 到 4） | 128 Mi / 512 Mi | 200m | 48 Mi / 192 Mi |
| order | 2（HPA 2 到 4） | 192 Mi / 768 Mi | 300m | 48 Mi / 192 Mi |
| inventory | 2 | 128 Mi / 512 Mi | 200m | 48 Mi / 192 Mi |
| payment | 2 | 128 Mi / 512 Mi | 100m | 48 Mi / 192 Mi |
| orderindex | 2 | 128 Mi / 512 Mi | 100m | 48 Mi / 192 Mi |
| cdc | 1 | 128 Mi / 512 Mi | 100m | — |

内存 requests 合计约 1.9 Gi（预算 3 GB）。这些数是估的：本机没有量过各服务的常驻内存，部署后用 `kubectl top` 对一遍再改。

- **QoS 是 Burstable**（requests 小于 limits，CPU 不设 limits）。和中间件的选择一致：CPU 不设上限，避免被节流造成延迟的长尾；内存设上限，泄漏时杀的是自己。节点内存紧张时业务服务先于中间件被驱逐（中间件有 PriorityClass `middleware`，服务用默认的优先级）。
- **`GOMEMLIMIT`** 设成内存 limits 的约 90%（经 Downward API 从 limits 取）。不设的话 Go 的垃圾回收不知道容器的上限，堆涨到被 OOM 杀掉之前不会加紧回收。`GOMAXPROCS`：没有 CPU limits 时它等于节点的核数，不用管；实现时用 `go doc runtime` 确认当前版本对 cgroup 的 CPU 上限的行为再写进笔记，不凭记忆。
- **排布**：同一个服务的副本用分布约束分到不同节点（和入口的 Envoy 同样的写法，滚动时多出来的那个可以落到第三个节点）。PDB `maxUnavailable: 1`。
- **连接数的账**：order 每个 Pod 对每个订单实例最多 25 个主库连接、25 个从库连接。4 个 Pod 是 100，在 `max_connections` 200 以内；HPA 的上限由这笔账定，不是随手写的。`mysql-main` 上是 inventory、payment、orderindex（两个库）、order 的冷库，实现时把每个服务的池大小加起来核一遍。

### 7.5 入口

`HTTPRoute` 挂在 2a 的 `Gateway public` 上，按主机名分：`api.oppenheimer.test` 到 `oppenheimer` 命名空间的 gateway，`e2e.oppenheimer.test` 到 `oppenheimer-e2e` 的。不改任何人的 DNS：从 MacBook 用 `curl --resolve` 或带 `Host` 头，集群里的测试 Job 用 `hostAliases`。

`TRUSTED_PROXIES`：入口的 Envoy 会在 `X-Forwarded-For` 里追加真实的客户端地址（2a 配了 `externalTrafficPolicy: Local`，它看得到）。gateway 只在请求来自受信任的代理时才信这个头。入口 Envoy 的 Pod 地址不固定，先信整个 Pod 网段；任何 Pod 都能伪造这个头的问题留给 2d 的 NetworkPolicy（只允许入口的 Envoy 访问 gateway 的 8080）。

gateway 的管理端口（降级开关）和各服务的内部端口不进任何 HTTPRoute，只能在集群里或经 `kubectl port-forward` 访问。

### 7.6 多副本下已知要处理的和留下的

| 项 | 处理 |
|---|---|
| 节点号 | 第 5 节，2c 做 |
| 按 IP、热点参数的限流乘以副本数 | 2c 不改，写明“限额是每个 gateway 副本的”；入口流量同一时刻只进一个入口 Envoy，但它会分给两个 gateway。要精确就得挪进 Redis，而 1d 的 4.7 有意把按 IP 的放在进程内（不让攻击流量压到 Redis 上）。HPA 伸缩时实际限额跟着变，记为已知的不精确 |
| 降级开关每个 Pod 各管各的 | 2c 提供 `scripts/cluster/degrade.sh <服务> <开关> on|off`，对这个服务的每个 Pod 调一遍管理端口，并打印各 Pod 的当前值。**新起来的 Pod 不知道之前被改过**这个缺口留着，记下来：要补就得让开关有一个共同的来源（ConfigMap 挂成文件再监听，或者放 Redis），阶段 4 做降级演练时再定 |
| cdc 只能一个 | `Recreate` 策略（先停旧的再起新的），不用 PDB。它停着的时候商家索引落后，不丢（位点在 Kafka 里） |
| 第一次部署时多个副本同时在空库上迁移 | 盯着那个没查到根因的偶发失败；出现时按 PROGRESS 里记的线索查（失败的是不是第一个拿到锁的） |
| Kafka 的再平衡 | 每次滚动发布，每个消费组都会再平衡。量停顿有多长，和静态成员一起评估（第 12 节） |

## 8. 启停和滚动发布

### 8.1 一个 Pod 被删时发生什么

API 把 Pod 标成 Terminating 之后，两件事**同时**开始，互相不等：

1. kubelet 给容器发 SIGTERM。
2. 端点控制器把这个 Pod 从 EndpointSlice 里标成不可用，然后各处才陆续知道：入口的 Envoy（经 Envoy Gateway 的 xDS）、调用方的边车（经 DNS）。

所以收到 SIGTERM 的那一刻，别人还在给它发请求。`pkg/app` 已有的流程正是为这个写的：SIGTERM → `/readyz` 变 503、gRPC 健康检查变不可用、**继续正常服务 `DRAIN_DELAY`** → 停止接新请求、处理完在途的（最多 20 秒）→ 退出。之后 kubelet 才停边车。

- `DRAIN_DELAY` 取“6.4 量出来的传播时间”加余量，起点 15 秒。
- `terminationGracePeriodSeconds` 要大于 `DRAIN_DELAY` 加停止时限（20 秒）加边车自己退出的时间，起点 45 秒。超过它 kubelet 直接 SIGKILL。
- **不用 `preStop` 里 sleep 的常见写法**：它做的是同一件事（推迟 SIGTERM），但进程在这段时间里不知道自己要走了，就绪探针还是通过的。这里进程自己知道，日志里看得到摘流开始和结束。
- 消费者：停止时提交位点、主动退出消费组，分区马上交给别的副本，不用等会话过期。

### 8.2 滚动发布

Deployment 用 `maxSurge: 1`、`maxUnavailable: 0`：先起一个新的，就绪了再停一个旧的，任何时刻就绪的副本数不低于期望值。`minReadySeconds` 给几秒，避免“刚就绪就崩”的 Pod 被当成可用。

新旧版本会同时在线，这要求：proto 和消息格式向后兼容（现在的约定）；数据库迁移必须是新旧代码都能跑的（先加列、后用列）。2c 不涉及表结构变更，只把这条写进笔记。

### 8.3 验证

- **滚动发布零报错**（验收项）：`load run` 以固定速率持续下单和查询，中途 `helm upgrade` 换一个新 tag 的镜像（内容相同），六个服务都滚一遍。统计非预期的响应（除 2xx 和业务上的 4xx 之外的一切）。目标是 0；不是 0 就按是哪一跳、什么错误码去查。
- **删 Pod**（验收项）：同样的负载下逐个 `kubectl delete pod`，每个服务一次。再做一次 `kubectl delete pod --grace-period=0 --force` 和一次直接在节点上杀进程：这两种没有摘流，预期有少量失败，记下多少、持续多久、靠什么恢复（边车的摘除、就绪探针）。
- `DRAIN_DELAY=0` 再滚一遍作对照，看失败是不是如预期出现。没有对照，“零报错”说明不了 `DRAIN_DELAY` 起了作用。

## 9. HPA

- 按 CPU：gateway 和 order，目标是 CPU requests 的 70%，2 到 4 个副本。扩容用默认的行为，缩容的稳定窗口留 5 分钟的默认值。压测把它推到扩容，看新 Pod 多久开始接流量（这里又用到 6.2：没有请求级的均衡，新 Pod 接不到流量，CPU 降不下来）。
- 要讲得清楚的：HPA 的算式（期望副本数 = 当前副本数 × 当前值 ÷ 目标值）；利用率是对 **requests** 算的，所以 requests 写错 HPA 就失真；指标从 metrics-server 来，有十几秒到一分钟的滞后；为什么 CPU 对 gateway 是个还行的信号、对卡在下游的服务不是（order 落库慢的时候 CPU 是低的）。
- **按 QPS 的 HPA 挪到阶段 3a**（待确认项 7）：需要 Prometheus 加 prometheus-adapter（或 KEDA）才有自定义指标的 API。为它在 2c 单独装一套、3a 再重装没有意义。阶段计划里把这一条拆成两条。
- payment、inventory、orderindex 不做 HPA：它们的吞吐由消费的分区数和下游的数据库决定，加副本不一定加吞吐。

## 10. 测试上集群

### 10.1 端到端测试

- 命名空间 `oppenheimer-e2e`：同一个 chart、同样的镜像，values 叠一层 e2e 的覆盖（缩短的时限、`DB_PREFIX=e2e_` 对应的库名、每次运行不同的 `KAFKA_PREFIX` 和 `REDIS_PREFIX`、按 IP 的限流关掉、OTLP 指向测试 Job）。gateway 和 order 各 2 个副本，让用例真的跨副本；其余 1 个。和长期运行的那一套共用中间件，靠前缀隔离，和本机的做法一样。跑完整个命名空间删掉。
- 测试是一个 Job：镜像里是 `go test -c -race` 编出来的测试二进制加 `archiver`。它在集群里，所以能直接连 MySQL 的 Service；经 `hostAliases` 把 `e2e.oppenheimer.test` 指到入口的地址（192.168.50.51），**流量从入口进**，不是直接连 gateway 的 Service。OTLP 接收端在 Job 的 Pod 里，前面放一个 Service 让各服务找到它。
- 建库：Job 开始前由脚本用 root 在各主库上建 `e2e_*` 库并授权给 `app` 和 `cdc`（对应本机 `e2e.sh` 的开头）。测试自己需要的 root 连接（停从库的复制线程、读 GTID）用 root 的密码，只进这个命名空间的 Secret。
- **主从延迟的用例会真的停订单从库的复制线程**，这个从库是和长期运行的那一套共用的：停的那几秒里 `oppenheimer` 命名空间的展示读会退回主库、cdc 会落后。可以接受（这本来就是那套降级逻辑要应付的），但跑 e2e 时不要同时做压测和对账。
- `scripts/cluster/e2e.sh`，`make cluster-e2e`。结果看 Job 的退出状态和日志；失败时把各服务的日志收下来。

没有选“MacBook 上跑加端口转发”：要转发 5 个 MySQL 和入口，`port-forward` 在连接多的时候不稳，各服务的链路还要能连回 MacBook（地址是 DHCP 的）。

### 10.2 一致性测试

`load run` 加 `load verify` 加 `recon`，同样作为 Job 在 `oppenheimer-e2e`（或一个专门的 `oppenheimer-load` 配置）里跑。chaos 的“随机杀进程”在集群上对应的是删 Pod，脚本要重写（`kubectl delete pod` 代替 `kill -9`）；2c 做一个最小的版本（压测期间每隔几秒随机删一个业务 Pod，静止后 `verify` 和 `recon` 都通过），完整的故障实验是阶段 4。

### 10.3 吞吐对比（计划里从 1b 挂过来的一条）

在集群上重做 1a 和 1b 的下单吞吐对比。要先想清楚的：

- **1a 的二进制**要从 1a 的提交构建一个镜像（那时没有 Kafka、Redis、分片），它的库表结构也不同，要单独的库。先确认 `scripts/bench.sh` 的 `BENCH_ARCH=1a` 那条路径需要什么，再决定是做成集群上的一套临时部署还是简化。
- **压测机**：发压的进程和被测系统在同一个集群里会抢 CPU。Mac mini 是阶段 3 才用的，有线又只有百兆。2c 的做法是把压测 Job 钉在一个节点上、给足 CPU requests，同时记下各节点的 CPU 使用率；结论写成“在这个条件下”的，阶段 3 用 Mac mini 复测时再对一次。
- 这一条排在最后做，前面的都不依赖它。

## 11. 部署的入口

`scripts/cluster/services.sh`，和 `middleware.sh` 同样的写法：

```
services.sh images            构建并推送全部镜像（或指定服务）
services.sh deploy [服务...]   复制 Secret、渲染边车配置、helm upgrade --install、等滚动完成、打印各 Pod 的状态
services.sh status
services.sh e2e-up | e2e-down
```

`make images`、`make deploy`（两步连做）、`make deploy-status`、`make cluster-e2e`。验收项“从空虚拟机到整套系统运行”就是 `make cluster-up && make middleware-up && make deploy`。

Argo CD 留在 2d：它解决的是“集群的状态以 git 为准、有人手工改了能发现”，而现在的状态本来就全在 git 里、由一个人从一台机器上部署。

## 12. Kafka 静态成员的评估

计划里的一条：用 `group.instance.id`（取 Pod 名）让重启的实例直接接回原来的分区。先把预期写下来，再量：

- **它帮得上的情形**：同一个成员在会话超时之内回来。容器崩溃后在**同一个 Pod** 里重启（Pod 名不变）是这种情形；1b 里“`kill -9` 之后新实例要等旧成员的会话过期（10 秒）才分到分区”就是它能解决的。
- **帮不上的情形**：Deployment 的滚动发布。新 Pod 的名字是新的，是另一个成员；而且静态成员退出时默认**不**主动离组（这正是它的设计：假设你马上回来），旧 Pod 的分区要等会话超时才交出去，比现在的主动离组更慢。franz-go 在设了实例号之后退出时的行为，实现前读它的文档和源码确认，不凭记忆。
- 所以评估的问题是：滚动发布（每次都发生、可以主动离组）和崩溃重启（少见、离不了组）哪个更该优化。先量现状：滚动一个消费者服务时，分区停顿多久（消费延迟的指标、或者对一个持续生产的 topic 看位点）。再决定要不要做、怎么做（比如只在 StatefulSet 形态下才有意义）。结论记进第 13 节和 ADR-0007 的补充。

## 13. 实现顺序和分工

| 步 | 内容 | 谁做 |
|---|---|---|
| 一 | Dockerfile、`.dockerignore`、`images.sh`；六个镜像推到仓库；版本登记 | Sonnet 写，主会话验收并执行 |
| 二 | 公共 chart、六份 values、Secret 的复制、边车配置的渲染、`services.sh deploy`；**每个服务先 1 个副本**跑通，入口能下单 | chart 和脚本派 Sonnet；集群上的执行、第一次部署的排查由主会话做 |
| 三 | 节点号的租约（`pkg/idgen` 加租约、order 和 payment 的迁移与装配、测试）；ADR-0028 | Fable 写，测试的铺开派 Opus |
| 四 | 副本数加到设计值；量 DNS 传播时间，定 `DRAIN_DELAY`；探针和启停；gRPC 负载均衡的对照实验；滚动发布和删 Pod 的验证 | 主会话；负载脚本的改动派 Sonnet |
| 五 | e2e 上集群（测试镜像、Job、建库脚本、e2e 的 values）；最小的删 Pod 一致性测试 | Opus（要读懂 e2e 的依赖）；主会话执行 |
| 六 | HPA（gateway、order），压到扩容 | Sonnet 写清单，主会话执行 |
| 七 | 静态成员的评估；吞吐对比 | 主会话量；需要改 `kafkax` 时由 Fable 写 |
| 八 | `make deploy` 收尾；从空集群重建一遍的验收；MySQL 恢复耗时在灌过数据后重量（2b 留下的）；切换手册里“服务的连接池自己恢复”和“CDC 换源”两步的验证；文档、笔记 | 主会话执行；笔记派 Sonnet |

第三步和第二步互不相干（一个是 Go 代码，一个是清单），可以并行；第三步没做完之前 order 和 payment 只能跑 1 个副本，第二步本来就是这样。集群上的执行都由主会话串行做。

### 实现中的发现和量到的数

**第一步（镜像，2026-11-15，在第 2 节确认之前先做的）。** `deploy/docker/Dockerfile`、`.dockerignore`、`scripts/cluster/images.sh`（`build`、`push`、`all`、`tag`）、`make images`（只做部分：`IMAGE_SERVICES="order gateway"`）。Sonnet 写，主会话读过全部文件并验证。

- 版本：`golang:1.27.4`；`gcr.io/distroless/static-debian13:nonroot`，按多架构清单的摘要固定（已登记到 `versions.md`）。
- 七个镜像（六个服务加 `recon`）：4 到 18 MB（本机镜像库报的大小），都是 amd64、用户 nonroot。全部构建并推送 2 分 20 秒；改一行代码后重新构建一个服务约 3 秒，依赖下载那一层走缓存。
- 验证：order 的镜像在本机能启动到“连不上数据库”；`recon` 的镜像在 k3s-agent-1 上从集群仓库拉下来并运行（打印用法后退出）；order 的镜像里有 `/app/order` 和 `/app/archiver`。
- **入口是一个符号链接**：exec 形式的 `ENTRYPOINT` 不能引用构建参数，运行镜像里又没有 shell，所以构建阶段建 `/app/entrypoint -> <服务>`。归档任务的 Pod 用 `command: ["/app/archiver"]` 覆盖。
- 比设计多的一项：`-ldflags="-s -w"`（去掉符号表和调试信息）。panic 的堆栈不受影响；阶段 4 用 pprof 时函数名来自另一张表（pclntab），也不受影响，到时候实际看一眼再确认。
- 留下的：工作区不干净时每次构建一个新 tag，仓库会越积越多，没有清理；基础镜像按摘要固定之后不会自己拿到安全更新，要人去换；任何一个模块加依赖，七个镜像的依赖层都失效（有缓存挂载垫着）。

**第二步（chart 和第一次部署，2026-11-15）。** 公共 chart `deploy/charts/service`、六份 values（`deploy/cluster/services/`）、`scripts/cluster/services.sh`（`deploy`、`status`、`render`、`envoy-validate`）、`make deploy`、`make deploy-status`。Sonnet 写，主会话读过模板和 values 后执行。

- **六个服务第一次部署就全部就绪**，从开始到 gateway 就绪约 100 秒。边车没有出现子 Agent 担心的“等 DNS 等很久”：按依赖顺序部署，下游的 headless Service 先存在。
- 从入口（`192.168.50.51`，`Host: api.oppenheimer.test`）走通了整条链路：查库存、下单（202）、落库（`pending_payment`，金额对）、支付、订单变 `paid`、库存 100 变 98 且 `sold` 2、商家索引里查得到。也就是 gateway、order、Kafka、inventory、payment、cdc、orderindex 和两套 Redis、五个 MySQL 实例都连对了。
- 示例 SKU 是手工在 `mysql-main-0` 上插的（1001 到 1003、2001，和本机 `make seed` 一样），还没有做成脚本。
- 和设计不同的地方（都是子 Agent 读代码后定的，核对过）：gateway 没有 headless Service（它的边车没有入站）；gateway 的探针打 8080（`/healthz`、`/readyz` 在对外的端口上），管理端口保持只听 127.0.0.1；边车的存活探针用 `/server_info` 而不是 `/ready`（后者在排空时是 503）；边车和服务容器都以数字 uid 运行（Envoy 的镜像默认是 root）；服务容器根文件系统只读，另挂一个 `/tmp`；边车 CPU requests 20m（设计的表里漏了）。
- **要记住的两点**：各服务的 `/degrade` 在各自的 HTTP 端口上，没有认证，集群里任何 Pod 都能调；Envoy 的管理端口 9901 同样对集群可见。都留给 2d 的 NetworkPolicy。镜像里没有 shell，`degrade.sh` 只能走 `kubectl port-forward`，不能 `kubectl exec`。

**第三步（节点号的租约）的集群验证（2026-11-15）。** 代码和测试见 5.3。order、payment 改成 2 个副本后重新部署：

- 四个 Pod 各租到不同的号（order 2 和 3，payment 2 和 3）。之前单副本的 Pod 用的是 0，滚动时多开的那个短命的 Pod 用过 1：**滚动发布期间新旧 Pod 各占一个号**，这正是 StatefulSet 做不到的。
- 旧 Pod 退出后表里它那一行 `owner` 为空，`last_ms` 是它实际发过的最后一个号的毫秒时间（和那一单的订单号对得上），不是登记的上界。
- 没有在集群上验证的：租约所在的库不可用时受理被拒绝再恢复（本机只有单元测试）；进程被 `kill -9` 后那个号 30 秒后才能被租走。

**第四步（多副本、摘流、滚动发布、gRPC 负载均衡，2026-11-15）。** 除 cdc 外都是 2 个副本。负载用新写的 `scripts/cluster/probe.py`：20 个 worker 从 MacBook 经入口持续发查库存、下单、查订单三种请求（约 90 个/秒），记下每一个不该出现的响应和时刻。基线 20 秒 0 失败。

*端点传播要多久（定 `DRAIN_DELAY` 的依据）。* 带着负载删一个 Pod，同时每 0.2 秒读两个 gateway 边车的 `/clusters`，看它们最后一次把请求发给这个 Pod 是什么时候。5 次（inventory 3 次、order 2 次），10 个样本：

| | 最短 | 最长 |
|---|---|---|
| 删除之后调用方的边车还在给它发请求 | 1.7 秒 | 9.3 秒 |
| 它从边车的上游列表里消失 | 2.0 秒 | 9.5 秒 |

样本在 2 到 9.5 秒之间散开，和“headless 记录的 TTL 加 Envoy 的重新解析间隔，各约 5 秒，相位随机”的预期相符（两个间隔本身没有单独去量）。**`DRAIN_DELAY` 定为 15 秒**：最长的 9.5 秒加 5 秒余量；`terminationGracePeriodSeconds` 45 秒不变。这 5 次删除期间 2 万多个请求 0 失败。

*对照：`DRAIN_DELAY=0`。* 只把 inventory 改成 0，删两次 Pod：每次 15、16 个查库存的请求失败（`503 UNAVAILABLE`，`upstream connect error ... connection timeout`），都集中在约 1 秒之内。之后没有再失败，不是因为调用方知道它没了（那要等几秒），而是边车连续 5 次失败后把它摘除了。两层各管一段：`DRAIN_DELAY` 让正常的退出一个请求都不丢，摘除给不正常的退出兜底。

*滚动发布零报错（验收项）。* 带着负载 `make deploy` 换一个新 tag，六个服务全部滚一遍（约 1 分 46 秒）。做了三次：

- 第一次 49 个失败，全部是查库存，全部落在 inventory 滚动的那 16 秒里。原因是上面的对照实验留下的：被替换掉的旧 Pod 还带着 `DRAIN_DELAY=0`。其余五个服务（包括 gateway 自己）这一次是 0 失败。**教训**：对照实验改过的配置要先恢复并等它生效，再做下一个实验；“恢复”和“下一个实验”合成一步时，被换掉的是带着旧配置的那一批。
- 第二次（干净的状态）15000 个请求 0 失败。第三次（改资源 requests 引起的滚动）13500 个请求 0 失败。

没有覆盖到的：支付的同步调用（探针不发 `pay`）；只有约 90 请求/秒，不是压测；`kubectl delete pod --grace-period=0 --force` 和在节点上直接杀进程两种没有做。

*gRPC 负载均衡的对照。* 读两个 inventory Pod 边车的入站计数，各压 30 秒：

| gateway 怎么连 inventory | 两个 inventory Pod 各收到 |
|---|---|
| 经自己的边车（现在的做法） | 770 和 770 |
| 直连一个 ClusterIP Service（临时建的，实验后已删） | 1380 和 160 |

直连时那 160 是 order 经它自己的边车发来的一半，gateway 的约 1200 个请求**全部**去了同一个 Pod：两个 gateway 进程各建了一条长连接，kube-proxy 按连接选后端，两条恰好选了同一个。接着删掉那个满载的 Pod：连接断开时 20 个请求失败（直连没有“下游要走了”这个信号可用，`remote connection failure`），重连之后两条连接这次恰好各落在一个 Pod 上，分布变成约一半一半。也就是说直连时的分布是每次建连接时掷出来的，而且只在重连时才重掷。经边车时是按请求轮流，两边完全相等。

*资源。* 约 90 请求/秒时每个容器的实际用量：order 43 到 46 Mi，inventory 27 到 31 Mi，gateway 8 到 26 Mi，payment、orderindex、cdc 8 到 12 Mi，每个 Envoy 边车 15 到 18 Mi；CPU 都在 40m 以下。原来估的 requests（128 到 192 Mi，边车 48 Mi）偏高，改成了服务 64 Mi（order 96 Mi）、边车 32 Mi，limits 不变。这是轻载下的数，阶段 3 压测时再校。11 个 Pod 的内存 requests 合计从约 1.9 Gi 降到约 1.1 Gi。

**k3s-server 的内存用到了 86%**（`kubectl top`，3927 Mi；它一共 6 GB，可分配约 4.4 Gi）。副本数加到 2 之后有五个业务 Pod 被分布约束放到了它上面，和 etcd、Kafka 的一个 broker、Redis 的两个实例在一起。最后一次滚动之后业务 Pod 恰好都在两个 agent 上（6 个和 5 个），但那是调度的结果，不是约束保证的。**主人的决定（2026-11-15）：避开**（加内存太贵）。chart 加了 `avoidControlPlane`（默认开，节点亲和要求没有 `node-role.kubernetes.io/control-plane` 标签），同一服务的副本分散从硬约束改成软的（`ScheduleAnyway`）：能放的只剩两个工作节点，滚动时多开的那个 Pod 必然和某个旧 Pod 同节点，硬约束会让滚动卡住。改完带着负载滚了一遍，13600 个请求 0 失败，11 个业务 Pod 都在两个 agent 上。两件事要如实记下：

- **软约束的代价当场就出现了**：这次滚动之后 inventory 的两个副本都在 k3s-agent-1 上，payment 也是。滚动是“先多开一个、再删一个旧的”，调度器每次只按当时的分布打分，删掉哪个旧 Pod 不由它定，结果可以是两个新 Pod 同节点。agent-1 一挂，这两个服务就没有副本了，要等重建。没有处理。可选的做法：装 descheduler 定期纠正；这类服务改成 `maxSurge: 0`、`maxUnavailable: 1`（原地换，不会挤到一起，但滚动时少一个副本的容量）；或者 3 个副本。阶段 4 做关节点的实验之前要定。
- **server 节点的内存并没有因此松下来**：业务 Pod 走了之后它仍然是 82%（3712 Mi）。所以 86% 的主要来源不是业务 Pod，而是本来就在上面的 etcd、k3s、Kafka 的一个 broker、两个 Redis。避开只是不再往上加。阶段 3 装监控时不能指望这个节点还有余量，DaemonSet 类的组件（node-exporter、日志采集）在它上面的 requests 要算进去。

**第六步（HPA，2026-11-15）。** chart 加了 `templates/hpa.yaml` 和 `autoscaling` 一组 values（默认关），gateway 和 order 打开：2 到 4 个副本，目标是 CPU requests 的 70%，`behavior` 用默认值。主会话写的（三十来行，没有派出去）。

- **开了 HPA 之后 Deployment 不再写 `replicas`**，否则每次 `helm upgrade` 都把副本数改回 values 里的数。**去掉这个字段的那一次部署有一个瞬态**：字段从清单里消失，API server 给它补默认值 1，Deployment 立刻缩到 1 个（删掉一个 Pod），HPA 在同一秒发现低于 `minReplicas` 又拉回 2 个（事件里是 `Current number of replicas below Spec.MinReplicas`）。order 和 gateway 各有约 5 秒只有 1 个就绪副本。被删的 Pod 走的是正常的摘流，带着负载做的，15400 个请求 0 失败；但如果当时一个副本扛不住流量，这就是一次事故。避开的办法是先建 HPA、等它接管，再从清单里去掉 `replicas`（两次发布）。
- **利用率是按整个 Pod 算的**：服务容器加边车的用量之和除以两者 requests 之和（gateway 是 200m 加 20m）。压测时边车用了约 60m，是它自己 requests 的三倍，都摊进了这个比例。只想看服务容器要用 `ContainerResource` 类型的指标，没有换：边车的 CPU 也是随流量涨的，算进去不算错。
- **压到扩容。** `probe.py --workers 120 --rate 3`，从 MacBook 经入口，实际约 810 请求/秒，持续 200 秒：

  | 时刻（从开始发压算起） | gateway | order |
  |---|---|---|
  | 0 秒 | 2 个，18% | 2 个，17% |
  | 约 55 秒 | 78%，决定扩到 3；5 秒后第 3 个就绪 | 23% |
  | 约 100 秒 | 79%，决定扩到 4；5 秒后就绪 | 79%，决定扩到 3；5 秒后就绪 |
  | 约 150 秒 | 4 个，79% | 90%，决定扩到 4；5 秒后就绪 |
  | 结束时 | 4 个，79% | 4 个，73% 到 87% |

  - 从开始发压到第一次扩容约 55 秒，**几乎全是指标的滞后**：HPA 看到的利用率在负载已经到位之后还爬了近一分钟（18%、23%、58%、78%）。Pod 本身从创建到就绪只要约 5 秒（镜像已经在节点上）。
  - 它是分两步扩到 4 的，而不是一步：按最终的数倒推，这个负载在 2 个副本上的真实利用率约 158%，一步就该要 5 个；但每次做决定时看到的都是还没爬完的数（78% 只够要 3 个）。扩到 4 之后 gateway 仍是 79%，高于目标，被 `maxReplicas` 挡住了。
  - **新 Pod 确实接到了流量**：结束时 4 个 gateway 容器各 114m 到 122m，4 个 order 容器各 165m 到 178m。gateway 的流量是入口的 Envoy 分的，order 的是 gateway 的边车分的，都是按请求均衡。这就是 6.2 说的：换成直连 ClusterIP，老连接不会挪到新 Pod 上，扩了也白扩。
  - order 是被“查订单”的读流量推上去的（这个负载里 create 只有约 90 个/秒，其中三分之一被限流）。order 的 CPU 涨得比 gateway 晚约 45 秒，原因没有查（可能是 gateway 扩容之前自己是瓶颈，放过去的请求少）。
  - 这次负载里 429 很多（查订单 26406 个、下单 5700 个，都是按用户的限额），所以它只是“把 CPU 推上去”的负载，不是吞吐的测量。
- **缩容**：停止发压（12:07:22）之后 5 分 07 秒才开始缩（12:12:29），正是默认的 5 分钟稳定窗口；之后 order 在 15 秒里、gateway 在 15 秒里各从 4 回到 2（每 15 秒一步）。缩容期间挂着约 90 请求/秒的探针，28860 个请求 0 失败。
- **缩容时看到的一件事**：order 的两个多出来的 Pod 相隔 15 秒被删。第一个退出时交出的分区（`order.submit` 的 3、`payment.events` 的 2）被分给了**第二个已经在摘流、15 秒后也要退出的 Pod**，于是这两个分区挪了两次（各停 2.4 秒和 1.3 秒）。在摘流的 Pod 仍然是消费组里的正常成员，协调者不知道它要走。没有处理：代价只是多一次一两秒的停顿；要避免得让进程在收到 SIGTERM 时先离组，那样摘流的 15 秒里它就不消费了。
- **扩容时出现了 14 个失败的请求，没有修。** 都是查订单返回 `503 STOCK_GATE_UNAVAILABLE`，落在 order 的第 3 个 Pod 就绪之后的 20 秒里（前 5 个在它就绪的同一秒）；第 4 个 Pod 就绪时没有出现。推断（只有时间上的吻合，没有直接证据）：新 Pod 一就绪就按轮询拿到四分之一到三分之一的流量（约 200 请求/秒），它到闸门 Redis 的连接池是空的，建连接加上读闸门的 300 毫秒时限，一批请求超时后按降级答复。这和 1b 里“冷连接池遇上突发流量”是同一类问题。可选的修法：就绪之前把连接池预热到一个下限（go-redis 的 `MinIdleConns`）；或者在调用方边车的集群上开 Envoy 的慢启动（新端点的权重在一个窗口里从低升到满）。滚动发布没有暴露它是因为那时只有约 90 请求/秒。阶段 3 压测之前要处理。
- 压测期间 order 的新 Pod 打过 `replica lags behind, reading primary`（订单库 0 的从库落后超过 1 秒，读退回主库）：读己之写的降级在集群上第一次被真实的负载触发，行为符合设计，落后了多久没有量。

**第七步的前一半（Kafka 静态成员的评估，2026-11-15）。结论：不做。** 先量了现状。带着约 90 请求/秒的负载 `kubectl rollout restart deploy/payment`（2 个副本，`order.events` 4 个分区，每个成员 2 个），同时把新旧四个 Pod 的 `kafka partitions revoked`、`assigned` 两种日志按时间排在一起：

| 时刻 | 发生了什么 | 这个分区没有人消费的时间 |
|---|---|---|
| 12:09:13.66 | 新 Pod 加入，旧 Pod 交出分区 2；12:09:14.17 新 Pod 拿到 | 0.5 秒 |
| 12:09:24.67 | 第二个新 Pod 加入，另一个旧 Pod 交出分区 3 | 0.5 秒 |
| 12:09:33.03 | 第一个旧 Pod 摘流 15 秒后退出，交出分区 1；12:09:35.69 被接走 | 2.7 秒 |
| 12:09:43.04 | 第二个旧 Pod 退出，交出分区 0；12:09:46.20 被接走 | 3.2 秒 |

- 每个分区在一次滚动里只挪一次，停 0.5 到 3.2 秒；没有任何一个分区等会话超时（10 秒）。期间 9500 个请求 0 失败。
- 两种停顿的来源不同。新成员加入时是增量再平衡（franz-go 默认的 cooperative-sticky）：只有要挪的那个分区被收回、下一轮分出去，其余分区一直在消费，所以只有 0.5 秒。旧成员退出时它主动发了离组请求，协调者马上开始再平衡，但**剩下的成员要到下一次心跳才知道**；心跳间隔是会话超时的三分之一，即 3.3 秒，所以是 0 到 3.3 秒之间的一个数（量到 2.7 和 3.2）。想再压低，调的是心跳间隔，不是静态成员。
- 旧 Pod 在 `DRAIN_DELAY` 的 15 秒里一直在消费，分区是进程真正退出时才交出去的。
- **静态成员在这里帮倒忙**。读了 franz-go v1.22.3 的 `InstanceID` 的文档和 `consumer_group.go` 里的 `leave`：设了实例号之后，关闭时**不发离组请求**（只有一种例外，增量再平衡里刚交出分区还没重新加入就关闭）。Deployment 滚动出来的新 Pod 名字是新的，对 Kafka 来说是另一个成员；旧 Pod 不离组，它的分区要等会话超时才会被分出去，而文档还建议用静态成员时把会话超时调大到够一次重启。也就是把现在的 3 秒变成 10 秒以上。
- 它帮得上的只有“同一个名字在会话超时之内回来”：容器在同一个 Pod 里崩溃重启（现在这种情况要等 10 秒会话过期，1b 的 chaos 里见过），或者 StatefulSet。前者少见，而且靠它省下的几秒要用每次发布多停几秒来换；后者我们不用（节点号一节讲过为什么）。所以不做，`kafkax` 不改。阶段 4 做“杀进程”的实验时，这 10 秒是预期之内的数。

**第五步的前一半（e2e 上集群，2026-11-15）。** `make cluster-e2e`（`scripts/cluster/e2e.sh`、`e2e-lib.sh`，`services.sh` 的 `e2e-prepare`、`e2e-up`、`e2e-down`、`e2e-render`）；测试镜像 `deploy/docker/e2e.Dockerfile`；e2e 的 values 在 `deploy/cluster/services/e2e/`，Job 和 OTLP 的 Service 在 `deploy/cluster/e2e/`。Opus 写，主会话读过 diff 后在集群上执行。**第一次执行就通过**：先跑两个用例（保留现场），再跑全量，19 个用例全部通过，从构建镜像到清理完一共 203 秒（验收项“端到端测试对着集群入口全部通过”的这一半）。

- **测试镜像**：`-race` 要 cgo，二进制是动态链接的，放不进 `static` 基础镜像。做法是构建阶段仍在原生的 arm64 上，装 Debian 的交叉 gcc 编 amd64；运行镜像换成带 glibc 的 `distroless/base-debian13`（按摘要固定，已登记）。第一次构建 39 秒，镜像 29.7 MB。
- **和设计不同的三处**（都是子 Agent 读代码后提出的，核对过）：
  - **Job 先于服务启动**，设计写的是反过来。OTLP 接收端在测试进程里，服务先起而接收端不在的话，导出器按指数退避重连（最长 120 秒），部署要几分钟，测试开始时 span 可能发不出来。于是测试的 `TestMain` 加了可选的 `E2E_WAIT_READY`：先起接收端，再经入口轮询 `/readyz` 连续 3 次成功才开始。这是 `test/e2e` 唯一的改动，本机不设这个变量。
  - **cdc 的 server id 必须和长期运行的那一套不同**（e2e 用 1012、1022）。两套 cdc 读的是同一个从库，同一个 server id 的第二个连接会把第一个踢掉，两边轮流断线。设计漏了。
  - root 密码的用处比设计写的多（测试直接读写各个 e2e 库、跑 `archiver`），仍然只进 `oppenheimer-e2e` 这一个命名空间。
- **“从入口进”要打个折扣**：Job 在集群里访问 LoadBalancer 的地址时，kube-proxy 在本节点上直接转给入口 Envoy 的 Pod，不经过 MetalLB 的 ARP 应答。测到的是入口 Envoy 加 HTTPRoute 加后面的全部，不是从集群外进来的那一段（那一段由 `probe.py` 从 MacBook 覆盖）。
- **隔离**逐项核对过：库名、topic 和消费组、cdc 的位点（在带前缀的 topic 里）、节点号的租约表和迁移锁（跟着库）、Redis 的键（含限流）都分开了。**隔离不了的**：共用从库的复制线程会被停几秒；两套写进同一个 `gtid_executed`；MySQL 的连接数（每个订单主库上长期那套最多 4×25，加 e2e 的 2×25）；`redis-cache` 是 LRU，e2e 的键会挤掉一些缓存；节点的内存。清理按 `e2e-`、`e2e_` 的前缀删，不分运行号，所以同一时刻只能有一次 e2e 在跑。
- **`gtid` 读模式在集群上第一次跑**（长期那一套是默认的 `primary`），用 `app` 账号，三个主从延迟的用例都过了。
- 留下的：cdc 的源写死的是 `mysql-order-N-1` 这个 Pod，主从延迟的用例停的是 `-replica` Service 选中的 Pod，现在是同一个，主从切换之后两处要一起改；Job 拉不到镜像时脚本要等到 Job 的时限（40 分钟）才失败；限流用例对时间敏感，这次过了，没有量余量；冷缓存下的镜像构建耗时没有量；没有进 CI（CI 里没有集群）。

**第五步的后一半（删 Pod 的一致性测试，2026-11-15）。** `make cluster-chaos`（`scripts/cluster/chaos.sh`，清单在 `deploy/cluster/chaos/`，values 的第三层在 `deploy/cluster/services/chaos/`，镜像 `deploy/docker/chaos.Dockerfile` 里是 `load` 和 `recon`）。Opus 写，主会话读过后在集群上执行。和 e2e 共用命名空间 `oppenheimer-e2e`、`e2e_*` 库和清理，靠命名空间上的标签互斥（正有一次在跑就拒绝，退出码 3）。发压的 Pod 常驻，各阶段用 `kubectl exec` 启动；压测期间每 5 秒随机删一个业务 Pod（名单是 gateway、order、inventory、payment，各 2 个副本），结束后等 Deployment 恢复，再 `wait-quiet`、`recon`（`payment,stock,replica,index` 四对）、`verify`（22 项里除归档外都查，**商家索引那一项本机是跳过的，这里是真查**）。判定标准是数据一致，不是零报错；另外要求没有意外的容器重启。

| 运行 | 删了几个 Pod | 受理的订单 | 客户端看到的 5xx | verify、recon | 结果 |
|---|---|---|---|---|---|
| 60 秒，正常的优雅期 | 11 | 372 | 0 | 通过，无差异 | 数据通过；脚本判失败（见下） |
| 120 秒，正常的优雅期 | 22 | 342 | 0 | 通过，无差异 | 数据通过；一个 inventory 容器启动时重启过一次 |
| 120 秒，正常的优雅期 | 23 | 392 | 0 | 通过，无差异 | **PASS** |
| 120 秒，`CHAOS_FORCE=1`（宽限期 0，相当于 `kill -9`） | 22 | 452 | 29（下单 15、支付 14） | 通过，无差异 | **PASS** |

- 四次的数据都对得上：受理的订单全部在库里，没有超卖（三个 SKU 各 60 件都恰好卖完），没有进死信的消息，闸门没有做过修正，商家索引和订单表一致，主从一致。这是验收项“一致性测试对着集群入口通过”和“删除任意一个业务 Pod，服务自动恢复且请求基本无感知”。
- **正常删除时客户端一个 5xx 都没有看到，强制删除时有 29 个**：又一次对照，说明零报错是摘流（`DRAIN_DELAY`）换来的；强删时在途的请求被重置，靠客户端带幂等键重试，数据仍然一致。强删那一次落库延迟的 p99 从约 0.7 秒变成 4.7 秒（消费者没有离组，分区要等会话超时）。
- **这个测试查出一个真缺陷，已修（`pkg/kafkax`，Fable 改）**：第一次运行里 order 和 orderindex 各有一个容器在启动时退出、被 kubelet 重启，日志是 `kafkax: dlq topic ... UNKNOWN_TOPIC_OR_PARTITION`。原因：`EnsureTopics` 建完 topic 之后等的是“它问的那个 broker 看得见”，而消费者是另一个客户端，启动时查死信 topic 可能问到另一个元数据还没更新的 broker。本机和 CI 都只有一个 broker，永远不会出现；集群上是三个。修法是 `checkDLQTopics` 查不到时最多等 10 秒再下结论。这和 2a 里“两个 DNS 上游一好一坏”是同一个教训：有多个副本时，一次检查通过只说明被问到的那一个是好的。之前的 `make deploy` 和 `make cluster-e2e` 很可能也发生过，只是没有人看重启次数（e2e 的脚本不查）。
- **一次没有解释的重启**：第二次运行里一个 inventory 容器在启动阶段重启过一次，那时用的已经是修过的镜像。脚本收上一个容器日志的写法有 bug（`--all-containers --previous` 在边车没重启过时整条命令报错），日志没有留下；写法已改，之后两次运行没有再出现。再出现时看 `tmp/cluster-chaos-logs/<运行号>/<Pod>.previous.log`。
- 第一次运行判失败的另一个原因是脚本自己的 bug（取受理数时匹配到两行），已改。
- 和本机 `make chaos` 的差别：每个服务 2 个副本（不是每次删除都让那个服务整个不可用）；默认是正常的优雅期；Pod 由 Deployment 重建；流量经入口和边车；多查商家索引和主从两对。
- 盲区（如实记下）：入口的 Envoy、cdc、MySQL、Kafka、Redis 不在名单里（它们和长期运行的那一套共用）；删 Pod 不经过 PDB（只有驱逐才受它约束）；负载只有十几单每秒，库存 60 件很快卖完，之后主要是被拒绝的请求；被强删的 inventory Pod 的最后几行日志可能收不到，会让 verify 第 19 项漏看一次闸门修正；脚本被 `kill -9` 时命名空间留着 `running` 标签，要手工 `services.sh e2e-down`。完整的故障实验是阶段 4。
- 为了不把密码放在命令行上，`test/load` 的 `verify` 和 `wait-quiet` 多了一个从环境变量读 DSN 的退路（`LOAD_*_DSN`），本机不设，行为不变。

**第七步的后一半（吞吐对比，2026-11-15）。结论：在这个集群、这个条件下，没有数量级的提升；持续过载时 1b 的受理吞吐还略低于 1a。** `make cluster-bench`（`scripts/cluster/bench.sh`，1a 的清单 `deploy/cluster/bench/1a.yaml`、镜像 `deploy/docker/bench1a.Dockerfile` 从提交 `732f9ab` 的 worktree 构建，1b 复用 e2e 那一套加 `deploy/cluster/services/bench/` 一层 values）。Opus 写，主会话执行。原始输出在 `tmp/cluster-bench/1794749534/`。

条件：发压的 Pod 在 k3s-agent-1 上（requests 2 核），经入口的 Envoy；`fire` 模式（闭环，提交后不等落库，和本机那张对比表同一种）；每档并发 2 轮、每轮 60 秒；10 个 SKU，库存足够不会卖完。1a 每个服务 1 个副本、三个库都在 `mysql-main` 上；1b 是现在的代码（含 1c、1d、2c 的全部改动），gateway、order、inventory、payment、orderindex 各 2 个副本，订单分在两个主从实例上，限流关闭，背压保持默认（`ADMIT_MAX_QUEUE` 10000）。

| 并发 | 1a 下单（201，已落库）/秒 | 1b 受理（202）/秒 | 1b 落库/秒 | 1b 被背压拒绝的请求（429） | 1b 排空 |
|---|---|---|---|---|---|
| 50 | 387、380 | 364、369 | 198、198 | 约 1.0 万 | 约 51 秒 |
| 200 | 402、403 | 368、362 | 198、197 | 约 5.6 万 | 约 52 秒 |
| 800 | 402、401 | 352、348 | 189、188 | 约 24 万 | 约 52 秒 |

怎么读这张表：

- **1b 的“受理吞吐”量到的是背压的上限，不是闸门能受理多快。** 约 365 单/秒恰好等于落库速度（198）加上一轮里队列能多攒的量（10000 ÷ 60 秒 ≈ 167）。队列一满，多出来的请求全部 429。这是 ADR-0011 有意的行为：受理可以比落库快，但只能快出一个队列的量；持续过载时受理速度被压回落库速度。所以“受理吞吐相对 1a 有数量级提升”这句话只在**突发**（一次冲击不超过队列长度）时成立，在持续 60 秒的压测里看不到。关掉背压的对照见下面。
- **1b 的落库吞吐只有 1a 的一半**（198 对 400），而且三档并发下都是平的。1b 每单要做的事比 1a 多（闸门、Kafka 一去一回、outbox、落库时再调 inventory 预占、分片库的主从复制），这在本机也是如此，但本机量到的是 686 到 870 单/秒。集群上低这么多的原因**没有查**；能看到的线索：这几轮里最忙的 MySQL 是订单库 0 的**从库**（`mysql-order-0-1`，约 1 核，像是单线程的回放），业务 Pod 合计只用了 2.5 到 5 核，节点最高 48% 到 82%。先怀疑 MySQL 的提交刷盘在虚拟机的盘上比本机慢，以及落库并发度（`SUBMIT_CONCURRENCY` 8）乘以两个副本之后卡在哪里。这是阶段 3 压测要回答的第一个问题。
- **1a 也是平的**：并发从 50 加到 800，吞吐停在约 400 单/秒，CPU 很闲（四个服务合计不到 1 核，MySQL 约 0.8 核）。瓶颈不在 CPU，符合 1d 里量到的“热门 SKU 的行锁加每次提交的刷盘”。
- **发压端和入口都不是瓶颈**：发压的 Pod 最多用 0.77 核（给了 2 核），最忙的入口 Envoy 最多 0.84 核。宿主机的有线网卡每轮只走了约 1.3 MB，流量确实留在宿主机内部，百兆网线不影响这个测量。

*对照：关掉 1b 的背压*（`BENCH_1B_ADMIT_MAX_QUEUE=0`）。只跑了 1b，每档 1 轮（原始输出在 `tmp/cluster-bench/` 下最后一个目录）：

| 并发 | 1b 受理/秒 | 受理延迟 p50、p99 | 落库/秒 | 排空 | 相对 1a（约 400） |
|---|---|---|---|---|---|
| 200 | 1937 | 70、124 毫秒 | 201 | 518 秒 | 4.8 倍 |
| 800 | 2503 | 179、430 毫秒 | 177 | 792 秒 | 6.2 倍 |

没有背压时闸门每秒能受理约 2000 到 2500 单，是 1a 的 5 到 6 倍，仍然不到一个数量级；并发 800 那一档节点 CPU 到了 88.6%、有 133 个错误，已经接近这三台虚拟机的上限，所以这个数受集群大小限制，不是闸门本身的上限。代价一目了然：一分钟受理的订单要 9 到 13 分钟才落完库，排队时间远超过支付时限，这正是背压存在的理由。落库速度不受影响（约 200 单/秒），所以**现在整个系统的短板是落库，不是受理**。

这个对比在方法上不对等的地方（如实记下，结论只在这些条件下成立）：1a 是 1 个副本、1b 是 2 个加边车；1a 的库在一个实例上、1b 分在两个主从实例上；“1b”其实是现在的代码；两边都多了入口这一跳，本机没有；数据库不是每轮清空的；`fire` 模式下 1b 每单还多一次查询；和长期运行的那一套共用中间件（空闲但开着）；三台虚拟机在同一台宿主机上，互相抢 CPU 在节点的数字里看不出来。脚本里有把 1b 压成单副本（`BENCH_1B_REPLICAS=1`）、让 1a 的订单库也单独一个实例（`BENCH_1A_ORDER_MYSQL=order-0`）、绕过入口（`BENCH_VIA=service`）的开关，都没有跑。

顺带发现：**本机的 `scripts/bench.sh` 在 `BENCH_ARCH=1a` 下从分片那次提交（`f085541`）起就是坏的**（给 1a 传的是它不认识的 `MYSQL_DSNS`，建的和统计的是分片后的表名），笔记里 1a 的数是更早的版本量的。没有修。

**第八步里做了的（2026-11-15）。**

- `scripts/cluster/seed.sh`（`make deploy-seed`）：示例 SKU 1001 到 1003、2001，加探针用的 3001。在 `mysql-main` 的主库 Pod 里用 root 执行，可重复（已有的行不动库存）。
- `scripts/cluster/degrade.sh`（`make deploy-degrade ARGS="..."`）：对一个服务的每个 Pod 经 `kubectl port-forward` 调 `/degrade`，列出或修改开关。验证过：把两个 gateway Pod 的 `stock_query` 设成 `off`，查库存立刻全部 503，改回 `auto` 全部 200；写错开关名时逐个 Pod 报错并以非零退出。新起来的 Pod 不知道被改过，这个缺口照旧（7.6）。
- **MySQL 恢复耗时重量了，但数据量仍然很小，这一条不算做完。** 现在三个实例的备份各约 1.2 到 1.4 MB（订单库每个约 1.5 万单、1 万行 outbox），恢复各 4 到 5 秒，和 2b 用 1000 行记号数据时量到的没有可区分的差别：这个量级下耗时全是起 Pod 和建库的固定开销。要得到有意义的数得灌到百万行以上，留给阶段 3 压测之后。
- **恢复演练的比对在服务跑着的时候必然报不一致，没有修。** `mysql-order-0` 和 `mysql-main` 的演练各报了不一致：`idgen_worker`（两个库都有）和 `order_db_0.outbox`。原因不是备份有问题，而是“备份之后没有写入”这个前提在服务运行时永远不成立：节点号的租约每隔几秒续一次，outbox 的清理任务在删已发布的行。`mysql-order-1` 通过是因为租约表不在它上面、那一刻清理任务没有动。脚本头部早就写了“有流量时要按备份时刻的 GTID 去比”，现在知道了没有流量也一样。要做对：把一个从库用 `START REPLICA UNTIL SQL_AFTER_GTIDS` 停在备份记录的 GTID 上再比，或者演练时把服务缩到 0。阶段 4 做主库故障之前要补，那时要靠它确认恢复出来的数据。

- **切换手册里留给 2c 的两步验证了一步半**（细节写回了 [mysql-switchover.md](../infra/mysql-switchover.md)）：带着探针对 `mysql-order-1` 做计划内切换，来回两趟。写入中断 12 到 14 秒，期间只有落在这个分片上的下单被拒绝（`503 ORDER_LANDING_UNAVAILABLE`，70 和 76 个），其余请求 0 失败；服务没有重启，自己恢复；订单没有丢。cdc 的源是写死的 Pod，切换后读的是同一个实例（它变成了主库），商家索引照常跟上，所以计划内切换不涉及“换源”；真正让 cdc 改读另一个实例没有验证，留给阶段 4。现在主库都回到了序号 0。
- 2c 的面试笔记已写：`docs/notes/2c-services.md`（Sonnet 写，613 行，逐条覆盖阶段计划里 2c 的考点；标了“通用知识”的段落面试前要对照官方文档核对）。它写的时候第五步后一半、第八步的结果还没有，笔记里没有删 Pod 测试的四次运行、切换验证和恢复演练的发现，下次补。

*顺带看到的。* 把副本数从 1 改成 2 会触发一次滚动，而不只是多起一个 Pod：分布约束只在副本数大于 1 时才渲染，Pod 模板因此变了。探针的延迟里偶尔有一个约 1 秒的查库存（基线里就有，没有查）。

## 14. 不在 2c 做的

- 按 QPS 的 HPA（阶段 3a）；中间件和服务的指标、日志、链路的后端（阶段 3a）。部署时 `OTEL_EXPORTER_OTLP_ENDPOINT` 不设，服务不导出；e2e 的那一套导出到测试 Job。
- NetworkPolicy、证书、Argo CD、灰度发布（2d）。
- 服务网格。
- 降级开关的共同来源（阶段 4 再定）。
- 订单库从 2 个扩到 4 个在集群上重做。
- 归档任务做成 CronJob：order 的镜像里带着 `archiver`，要跑的时候手工起一个 Job；定时的等有了数据和监控再加。
- 主库故障的切换、节点故障的实验（阶段 4）。
