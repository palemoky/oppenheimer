# MySQL 主从切换手册（集群）

适用于集群上一主一从的实例（`mysql-order-0`、`mysql-order-1`）。`mysql-main` 是单实例，没有可切换的对象。
设计背景：设计文档 [2b-middleware.md](../design/2b-middleware.md) 的 3.3 和第 10 节的发现 2，决定见 ADR-0027。

**状态**：第一节（计划内切换）2026-11-15 在 `mysql-order-1` 上演练过两趟，结果在设计文档 2b 第 10 节的发现 15；第二节（主库故障）还没有演练，放在阶段 4。

## 先弄清楚三件事

切换是人工的，没有自动故障切换。每一步为什么是这个顺序，靠下面三个事实：

1. **“谁是主库”有两处，要人让它们一致。** 一处是实例自己的状态（可写、没配复制的是主库；配了复制、`super_read_only` 的是从库），一处是 values 里的 `primaryOrdinal`（决定 `<release>-primary`、`<release>-replica` 两个 Service 指向哪个 Pod）。`helm upgrade` 只改 Service，不碰实例；改实例的语句也不会动 Service。`scripts/cluster/middleware.sh mysql` 结尾的核对就是比这两处，不一致时打印 `ANOMALY` 并以非零退出。
2. **Service 只管新建的连接。** 已经建立的连接是连到某个 Pod 的地址上的，Service 改了指向它也不会断。服务的连接池最长会把一个连接留 30 分钟（`pkg/mysqlx` 的 `ConnMaxLifetime`）。所以“改 Service”不能当成“把写入挪走”的手段：必须先让旧主库拒绝写入，再改 Service。
3. **复制是异步的。** 主库答复“提交成功”时，这个事务不一定已经到了从库。计划内切换可以等从库追平，不丢数据；主库突然没了的时候，从库上没有的那些事务就丢了，除非旧主库的盘还能读出来。

就绪探针（`files/probe.sh`）也按状态判断：只读又没配复制的实例**不就绪**。计划内切换里旧主库设成只读之后，`-primary` Service 立刻没有端点，新的连接会失败而不是写到错的地方。整个流程里任何一步停下来，结果都是“写不进去”，不是“写到两个地方”。

下面用 `R` 表示实例名（如 `mysql-order-0`），`OLD` 是当前主库的序号，`NEW` 是要提升的从库的序号。命令都带着：

```sh
export KUBECONFIG=~/.kube/oppenheimer.yaml
R=mysql-order-0 OLD=0 NEW=1
# 在某个 Pod 里以 root 执行语句。密码在 Pod 的环境变量里，不出现在本机的命令行上。
q() { kubectl -n mysql exec -i "$R-$1" -c mysql -- bash -c '. /scripts/lib.sh; rsql 127.0.0.1 -N -B'; }
```

## 一、计划内切换（主库是好的）

用于腾空一个节点、演练。写入中断的时间是第 2 步到第 6 步。演练时把这几步连成脚本执行，中断约 5 秒，大部分花在第 6 步的 `helm upgrade` 上；手工一步步敲会更久，所以先把命令都准备好再开始第 2 步。

**1. 确认起点是干净的。**

```sh
scripts/cluster/middleware.sh mysql      # 结尾不能有 ANOMALY
echo "SHOW REPLICA STATUS" | kubectl -n mysql exec -i "$R-$NEW" -c mysql -- bash -c '. /scripts/lib.sh; rsql 127.0.0.1 --vertical' | grep -E 'Running|Seconds_Behind|Last_.*Error:'
```

两个线程都是 `Yes`，延迟接近 0。从库落后很多时先不要切：第 3 步要等它追平，等多久写入就停多久。

**2. 旧主库停止接受写入。** 这一步之后写入开始失败（错误 1290）。

```sh
echo "SET PERSIST super_read_only = ON" | q $OLD
```

- 用 `PERSIST` 而不是 `GLOBAL`：旧主库的 Pod 在流程中途重启的话，它起来仍然是只读的，不会变回一个可写的旧主库。
- 这条语句会等正在提交的事务结束再生效；之后已有连接上的写入也会被拒绝（这正是第 2 个事实要的）。有长事务时它会一直等，先查 `information_schema.innodb_trx`。

**3. 等从库追平，并确认。** 取旧主库的 `gtid_executed`，在从库上等它全部执行完：

```sh
G="$(echo "SELECT REPLACE(@@global.gtid_executed, '\n', '')" | q $OLD)"
echo "SELECT WAIT_FOR_EXECUTED_GTID_SET('$G', 60)" | q $NEW     # 返回 0 是追平了，1 是超时
```

返回 1 就停在这里查原因，不要往下做。要回退的话在旧主库上 `SET PERSIST super_read_only = OFF; SET PERSIST read_only = OFF;`，一切回到起点。

**4. 提升从库。** 去掉它的复制配置，放开写入：

```sh
q $NEW <<'SQL'
STOP REPLICA;
RESET REPLICA ALL;
SET PERSIST super_read_only = OFF;
SET PERSIST read_only = OFF;
SQL
```

从这一刻起它按状态算是主库，但 `-primary` 还指着旧主库（没有端点），所以还没有流量。

**5. 旧主库改成新主库的从库。** 复制账号的密码取自旧主库 Pod 自己的环境变量：

```sh
kubectl -n mysql exec -i "$R-$OLD" -c mysql -- env NEW="$NEW" bash -c '
  . /scripts/lib.sh
  rsql 127.0.0.1 <<SQL
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST = "$(peer_host "$NEW")", SOURCE_PORT = 3306,
  SOURCE_USER = "repl", SOURCE_PASSWORD = "$REPL_PASSWORD",
  SOURCE_AUTO_POSITION = 1, GET_SOURCE_PUBLIC_KEY = 1,
  SOURCE_CONNECT_RETRY = 10, SOURCE_RETRY_COUNT = 1000000;
START REPLICA;
SQL'
```

这一步放在改 Service 之前：改了之后 `-replica` 就指向旧主库，它得已经在复制，读从库的请求才不会落到一个停在旧状态的实例上（只读又没配复制的实例不就绪，那样 `-replica` 会没有端点）。

参数和 `files/init-replica.sh` 里新建从库时用的一样。`SOURCE_AUTO_POSITION = 1` 让它按自己的 `gtid_executed` 去要缺的事务：第 3 步确认过两边一致，所以这里什么都不用补。

**6. 改 Service 的指向。** 把 `deploy/cluster/mysql/$R.yaml` 里的 `primaryOrdinal` 改成 `NEW`，然后：

```sh
scripts/cluster/middleware.sh mysql
```

它做 `helm upgrade`（只有两个 Service 的 selector 变了，Pod 不重启；前提是这次没有夹带 chart 的其他改动），再核对。结尾应当是：新主库 `role=primary`，旧主库 `role=replica`、两个线程在跑，没有 `ANOMALY`。写入从这里恢复。

**7. 把留在旧主库上的连接赶走。** 服务原来连着旧主库的连接还在，在上面写会一直报 1290，直到连接池把它们换掉。在旧主库上断开业务账号的连接，连接池重连时经 Service 到新主库：

```sh
echo "SELECT CONCAT('KILL ', id, ';') FROM information_schema.processlist WHERE user = 'app'" | q $OLD | q $OLD
```

（业务服务上集群是 2c 的事，那时要验证服务碰到 1290 和断连之后自己恢复。）

**8. 提交 values 的改动。** 主从关系记在 git 里；不提交的话，下一次有人照着仓库部署，Service 会指回旧主库（那时它是只读的从库，写入全部失败，核对会报 `ANOMALY`）。

### 切换之后要知道的

- **CDC 读的是“从库”的 binlog。** 切换之后从库换了一个实例。CDC 按 GTID 续读在原理上可以换源，但行版本号的正确性依赖 CDC 的源（ADR-0014 的后果一节），集群上还没有验证过，2c 接 CDC 时要做。
- **节点的分布变了。** 两个分片的主库原来分在两个 agent 上（一个 agent 挂了只丢一个分片的主库）；切过一个之后两个主库在同一个 agent 上。演练完切回去。

## 二、主库故障（主库的 Pod 或它所在的节点起不来）

**没有演练过。** 这里是按上面三个事实推出来的步骤，阶段 4 做故障实验时照着走一遍并修正。

先判断要不要切。主库的 Pod 被删、进程崩溃，它自己十几秒就回来了，数据在本地卷上，**什么都不用做**；切换反而会丢数据。只有它短时间内回不来（节点坏了、卷坏了）才切。

**1. 确认旧主库真的不在了，并且在你处理完之前不会自己回来。** 这是最要紧的一步：旧主库的 `super_read_only` 是关的，它一回来就是一个可写的实例，而且就绪探针会让它通过。

- 节点失联：`kubectl cordon` 那个节点，Pod 回不来。
- 能连上旧主库的话走第一节，不走这里。

**2. 让从库把已经收到的事务应用完，再看它有多少。** 从库的 IO 线程已经断了，但它收到的事务不一定都应用了：收到的在中继日志里（`Retrieved_Gtid_Set`），应用了的才进 `gtid_executed`。这时提升会把“已经到了从库、只是还没应用”的事务也丢掉。等到前者被后者包含：

```sh
q $NEW <<'SQL'
SELECT GTID_SUBSET(RECEIVED_TRANSACTION_SET, @@global.gtid_executed) AS applied_all,
       (SELECT SERVICE_STATE FROM performance_schema.replication_applier_status) AS sql_thread
FROM performance_schema.replication_connection_status;
SELECT REPLACE(@@global.gtid_executed, '\n', '');
SQL
```

`applied_all` 是 1 才往下做；是 0 就等（SQL 线程要是 `ON` 的；它停了先看 `Last_SQL_Error`）。不要在这之前执行 `STOP REPLICA`。

记下这个 `gtid_executed`。主库上提交过、这里没有的事务，从这一刻起就是“可能丢了”的：服务收到过成功的答复，数据却不在新主库上。对订单库来说是“已受理并落库的订单不见了”，之后要靠对账（`recon`）和上游的重试找回来，这是异步复制的代价（要不丢就得半同步，见 ADR-0027 放弃了什么）。

**3. 提升从库**（同第一节第 4 步），**改 `primaryOrdinal` 并部署**（同第 6 步）。核对会因为旧主库连不上而报异常，这时是预期的。

**4. 旧主库回来之前，决定它以什么身份回来。** 两种：

- 它的盘还在、能起来：起来之后**立刻**设只读（`SET PERSIST super_read_only = ON`），然后比较两边的 GTID：

  ```sql
  -- 在旧主库上执行，参数是第 2 步记下的新主库的 gtid_executed 加上它之后新写入的（直接取新主库当前的值）
  SELECT GTID_SUBTRACT(@@global.gtid_executed, '<新主库的 gtid_executed>');
  ```

  结果为空：旧主库没有多出来的事务，按第一节第 5 步把它接成从库。结果不为空：那些就是没复制出去的事务。先把它们从旧主库的 binlog 里导出来留证（`mysqlbinlog --include-gtids`），再决定补写还是放弃；旧主库本身不能直接接成从库（两边已经分叉），要重新 CLONE。
- 不要它的数据了（或者盘坏了）：删掉它的 PVC（`data-$R-$OLD`）再让 Pod 重建，init 容器看到空的数据目录会从新主库 CLONE 全量并配好复制。**删 PVC 是破坏性操作，先和主人确认**；有分叉的事务时先按上一条留证。

从“旧主库回来”到“设成只读”之间有一个窗口：`-replica` Service 这时指向它，读从库的请求会读到旧数据。第 1 步的 cordon 就是为了让这个时刻由人来定。

**5. 核对**：`scripts/cluster/middleware.sh mysql` 没有 `ANOMALY`，提交 values。

## 出了岔子怎么看

| 现象 | 多半是 |
|---|---|
| 核对报 `WRITABLE but not the primary` | 旧主库回来了、还可写。先设只读，再按第二节第 4 步处理 |
| 核对报 `neither primary nor replicating` | 切换做了一半：设了只读、还没配复制，或者提升了一半。对照第一节看停在哪一步 |
| 核对报 `replicating from ... expected ...` | 改了 values 但没改复制源，或者反过来 |
| `-primary` 没有端点 | 它指向的实例不就绪：只读又没复制，或者 Pod 不在 |
| 建库建账号的 hook 失败，日志里是 `more than one writable instance` | 有两个可写的实例。hook 拒绝往任何一个里写，这是有意的 |
