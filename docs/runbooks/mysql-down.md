# MySQL 连不上

覆盖的告警：`MySQLDown`（critical）

## 这条告警在说什么

`mysql_up == 0`，持续 2 分钟（`for: 2m`，30 秒求值一次）。`mysql_up` 是 mysqld_exporter 自己报的“现在能不能连上数据库”。不看 `up`：exporter 活着而数据库死了时 `up` 是 1（规则文件开头的说明）。exporter 自己不在了（序列消失）不归这条管，归自带的 `TargetDown`。

实例是 Pod 名：`mysql-order-0-0`、`mysql-order-0-1`、`mysql-order-1-0`、`mysql-order-1-1`、`mysql-main-0`（`cluster` 标签是 `mysql-order-0` 之类）。阈值和出处：[3a 第 5 步（exporter 和大盘）](../design/3a-observability.md)、[设计 4.4](../design/3c-slo-alerting.md)。

抑制：`MySQLDown` 在响时，同一实例的 `MySQLReplica*`、`MySQLConnectionsHigh`，和同一套里的 `MySQLReplicaIOThreadStopped` 不再通知（它们的指标这时也没了，是同一件事的结果）。

## 影响

- 主库连不上：落在这套上的写入失败。订单库连不上时，受理端的落库健康检查会拒绝新订单（ADR-0021、[ADR-0027](../adr/0027-mysql-statefulset-not-operator.md) 的后果一节），用户看到下单失败。没有自动切换。
- 从库连不上：读从库的接口退回主库，CDC 停住，备份失败（备份在从库上做）。
- `mysql-main`（inventory、payment、商家索引、冷库）没有从库，连不上就是整块不可用。
- 数据风险：进程崩溃或 Pod 被删，数据在本地卷上，不丢。主库的盘或节点坏了才有“从库缺事务”的问题（[切换手册](../infra/mysql-switchover.md)第二节）。
- 2 分钟的 `for` 之后立刻处理，critical 会推到 Telegram。

## 先看什么

1. 是哪个实例，是不是只有 exporter 看不到：

   ```
   scripts/cluster/monitoring.sh promql 'mysql_up == 0'
   scripts/cluster/monitoring.sh promql 'up{job="mysql"} == 0'
   ```
2. Pod 的状态、重启次数、退出原因：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n mysql get pods -o wide
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n mysql describe pod mysql-order-0-1 | rg -A8 'Last State|Events'
   ```
3. mysqld 自己的日志（上一次崩溃用 `--previous`）：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n mysql logs mysql-order-0-1 -c mysql --tail=100
   ```
   Loki：`{k8s_namespace_name="mysql", k8s_pod_name="mysql-order-0-1"}`。
4. 集群上的主从核对（只读，看有没有 `ANOMALY`）：`scripts/cluster/middleware.sh mysql`。
5. exporter 是不是自己的问题（别的实例的 `mysql_up` 都是 1 而只有一个是 0，多半是实例本身）：
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring logs deploy/mysqld-exporter --tail=50`。

## 常见的原因和各自的处理

1. **崩溃恢复还没完。** 启动探针最长给 10 分钟（失败阈值 60、周期 10 秒，[2b 笔记 6.6](../notes/2b-middleware.md)），断电或被杀之后的第一次启动要做崩溃恢复，这段时间连不上。看日志里是否在 `InnoDB: ... recovery`，等就是了；超过 10 分钟被探针杀掉才是问题。
2. **节点非正常关机（宿主机断电、虚拟机被强制关闭）。** 主库靠自己的落盘恢复。订单库的从库提交时不逐次刷盘（[ADR-0032](../adr/0032-replica-relaxed-durability.md)），起来之后**不管复制看着是否正常，一律重建**，步骤在[切换手册](../infra/mysql-switchover.md)第三节（删 PVC 是破坏性操作，先问主人）。
3. **数据目录是半初始化的、循环崩溃。** 2b 里 AppArmor 拦住了 `mysqld` 的信号，启动探针超时杀掉容器，留下半初始化的数据目录，之后每次都报“数据文件损坏”（[2b 笔记第 8 节](../notes/2b-middleware.md)）。节点配置在 `scripts/cluster/vm.sh` 的 cloud-init 里，新建节点后如果又出现，先对这一条。
4. **主库的 Pod 或节点短时间回不来。** 要不要切，看[切换手册](../infra/mysql-switchover.md)第二节：Pod 被删、进程崩溃十几秒就回来，切换反而丢数据；节点、卷坏了才切。没有演练过，放在阶段 4。
5. **OOMKilled**（推断，没有遇到过）。上限是 1 Gi（`mysql-main` 1.5 Gi），2b 记录里写着“现在没有结论”。`describe` 里 `Last State` 是 `OOMKilled` 就是它。
6. **只有 exporter 连不上、数据库其实是好的**（推断，没有遇到过）。比如监控账号 `exporter` 被改了或密码和 Secret `mysql-exporter-auth` 不一致；应用侧的探针和写入都正常可以佐证。

## 怎么确认恢复了

- 告警自己恢复的条件：`mysql_up` 回到 1，告警在下一个求值周期消失。
- 确认：`scripts/cluster/monitoring.sh promql 'mysql_up'` 全是 1；`scripts/cluster/middleware.sh mysql` 没有 `ANOMALY`，从库两个复制线程都在跑（见 [mysql-replication.md](mysql-replication.md)）。
- 主库出过事（重建、切换、从库缺事务）的，最后跑一次对账，差异为 0 才算完：

  ```
  KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%H%M)
  ```

## 怎么人为触发它

设计 5 节第 2 层没有给这一条专门的办法。建议（待定，没有验证过）：在**不影响业务的实例**上把 Pod 缩到 0，让 exporter 连不上：`kubectl -n mysql scale statefulset/mysql-main --replicas=0` 这类会影响业务，故障注入时选业务影响最小的时机。更温和的做法是临时把 `for` 和表达式改成 `mysql_up == 1` 验证“路是通的”，响了再改回来，并写明这只验证了路由。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
