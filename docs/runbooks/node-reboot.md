# 节点重启过

覆盖的告警：`NodeRebooted`（warning）

## 这条告警在说什么

某个节点（node-exporter 的 `instance`，形如 `192.168.50.41:9100`）的启动时间 `node_boot_time_seconds` 和 30 分钟前比变了，差超过 60 秒（容忍时钟抖动），持续 1 分钟。表达式的值是新的启动时间，告警的 summary 把它显示成时刻。节点重启之后的约 30 分钟里一直响，之后自己恢复。三个节点是同一台 Ubuntu 宿主机上的 KVM 虚拟机，宿主机重启时三个一起响，Alertmanager 按 `alertname` 分组，只发一条通知。

它存在的原因是 [ADR-0032](../adr/0032-replica-relaxed-durability.md)：订单库的两个从库提交时不逐次刷盘（`innodb_flush_log_at_trx_commit=2`、`sync_binlog=0`）。节点非正常关机（掉电、内核崩溃、虚拟机被强制关闭）之后，从库的数据和它自己的 binlog 可能对不上，其中一种不报错：复制照常在跑，数据却少了几个事务。所以节点重启要有人知道，并判断是正常关机还是非正常关机。设计里的出处见 [设计 3c 的 4.5](../design/3c-slo-alerting.md)。

## 影响

重启期间节点上的 Pod 不可用，用户可能看到 503（见 [ingress-gateway-5xx.md](ingress-gateway-5xx.md)）；重启完成后集群会自己恢复（2026-11-20 宿主机重启那次就是自己恢复的）。**数据风险在从库**：非正常关机之后，读从库的查询、CDC、备份可能拿到少了事务的数据，要从主库重建从库，在重建完成之前不要信任它。主库每次提交都落盘，不受影响。

正常关机（先 drain，或者宿主机的 `libvirt-guests` 正常关掉虚拟机）：不用重建，只要核对。非正常关机：要重建从库，这是破坏性操作，先问主人。

## 先看什么

1. 是哪几个节点，是不是同时（宿主机重启）：

   ```sh
   scripts/cluster/monitoring.sh promql 'node_boot_time_seconds'
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl get nodes
   ```

2. **正常关机还是非正常关机**，这是最关键的一步。到节点上看上一次启动是怎么结束的（节点 IP：server 192.168.50.40，agent-1 .41，agent-2 .42，用户 `ubuntu`）：

   ```sh
   ssh ubuntu@192.168.50.41 'last -x shutdown reboot | head -5; journalctl --list-boots | tail -3; journalctl -b -1 -n 30 --no-pager'
   ```

   上一次启动的日志末尾有 `Reached target Shutdown`、`systemd-shutdown` 就是正常关机；日志突然截断、没有关机的记录就是非正常（掉电、被强制关掉）。

3. MySQL 主从状态，有没有 `ANOMALY`，从库的落盘方式和线程：

   ```sh
   scripts/cluster/middleware.sh mysql
   ```

4. 集群整体回来了没有，Kafka 的 operator 有没有调和成功（见常见原因 2）：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl get pods -A | grep -v Running | grep -v Completed
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get kafka
   scripts/cluster/loadtest.sh preflight
   ```

5. 其他告警：重启期间 `TargetDown`、复制、Redis 的告警可能同时响过，看 `scripts/cluster/monitoring.sh alerts`。

## 常见的原因和各自的处理

1. **宿主机重启（三台一起）。** 记录：`docs/PROGRESS.md` 里“宿主机重启之后的集群”（2026-11-20）：三台虚拟机是正常关机的，所以从库不用重建；自启后集群自己恢复，`preflight` 通过，Redis 和 MySQL 的复制正常；闸门 Redis 的主库回到了 `redis-gate-0`。k3s-server 关机用了 115 秒，`libvirt-guests` 的时限是 120 秒，差 5 秒就会被强制关掉，所以 `SHUTDOWN_TIMEOUT` 调到了 300 秒（宿主机上直接改了，同样的改法写进了 `host-setup.sh`，没有重跑整个脚本）。处理：核对第 2、3 步，确认没有被强制关掉。
2. **重启后 Strimzi 的 operator 调和失败。** 记录：[round-8 的 6.3](../notes/3b-rounds/round-8-cpu-requests.md)：宿主机重启后 3 分钟起，`Kafka` 资源 `NotReady` 两天没人发现（operator 启动时赶在集群 DNS 就绪之前探测集群域名）。已经把 `KUBERNETES_SERVICE_DNS_DOMAIN=cluster.local` 写死，但要在重启后看一眼 `Kafka` 资源是不是 `Ready`：这一项还没有告警（设计 8 里记着“还欠着”）。
3. **非正常关机（掉电、虚拟机被强制关闭）。** 没有真的遇到过。处理按 [切换手册第三节](../infra/mysql-switchover.md)：先确认主库是好的，重建从库（删 PVC 再删 Pod，init 容器从主库 CLONE），再核对、看 CDC、不信任出事后到重建前做的备份。**删 PVC 是破坏性操作，先和主人确认。** 切换手册第三节注明没有演练过。
4. **k3s 或内核自己崩溃重启。** 推断，没有遇到过。同上，按非正常关机处理，除非日志证明是正常关机。
5. **节点被人手工重启。** 推断，没有遇到过。先 drain 再重启是正常关机，不用重建。

## 怎么确认恢复了

告警在重启后约 30 分钟自己恢复，这只说明时间过去了，不说明从库是好的。真正的确认：

- `scripts/cluster/middleware.sh mysql` 没有 `ANOMALY`，从库的两个复制线程在跑，从库的 GTID 和主库一致；
- `scripts/cluster/loadtest.sh preflight` 通过；
- 如果是非正常关机，重建完成后再跑一次上面两项。

## 怎么人为触发它

设计 3c 第 5 节没有单列这一条。**待定**。真的重启一个 agent 虚拟机是最直接的办法，但那是节点级的故障注入：要先登记到 `docs/plan/phase-3-observability.md`，且重启虚拟机会让上面的业务和中间件短暂不可用，必须正常关机（`sudo shutdown -r now`，不要强制），**先问主人**。

不方便真做时：临时把规则里的 `> 60` 改成 `> -1`（`abs()` 恒不小于 0，条件恒成立），`for` 1 分钟后响，说明这只验证了“路是通的”，改完要改回。另外规则的单元测试已经覆盖了表达式本身（`make rules-test`）。

## 历史

（还没有真的响过）
