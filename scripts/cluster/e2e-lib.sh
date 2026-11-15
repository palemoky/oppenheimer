# shellcheck shell=bash
# 集群上端到端测试（设计文档 2c 的 10.1）对共用中间件的操作：建库授权、恢复从库复制、清理 e2e 留下的东西。
# 被 scripts/cluster/e2e.sh 和 services.sh 的 e2e-down 引用，不单独运行。调用方已经 source 过 cluster.env、
# 设好了 KUBECONFIG。
#
# e2e 那一套和长期运行的那一套共用 MySQL、Kafka、Redis，靠名字隔离（和本机的做法一样）：
#   MySQL  e2e_ 前缀的库，建在三个实例的主库上（从库靠复制）
#   Kafka  e2e-<运行号>. 前缀的 topic 和消费者组
#   Redis  e2e-<运行号>: 前缀的键（闸门实例和缓存集群都有）
# 这里的清理按 e2e- / e2e_ 的前缀删，不区分运行号：同一时刻只该有一次 e2e 在跑。
#
# 在 macOS 自带的 bash 3.2 上运行。密码不进命令行：SQL 在 mysql 的 Pod 里用 root 执行，
# 密码取自那个容器自己的环境变量（chart 注入的 MYSQL_ROOT_PASSWORD），SQL 经标准输入传。

E2E_MYSQL_NS="mysql"
# 共享实例上的 e2e 库（本机 scripts/e2e.sh 在单实例 mysql 上建的那五个）。
E2E_MAIN_DBS="e2e_inventory_db e2e_payment_db e2e_merchant_index_db_0 e2e_merchant_index_db_1 e2e_order_archive_db"
# 订单分片：release 序号 i 上是 e2e_order_db_i。
E2E_ORDER_SHARDS="0 1"

# 某个 release 当前作为主库或从库的 Pod：以 -primary、-replica 两个 Service 实际选中的 Pod 名为准
# （和 middleware.sh 的 mysql_check 同样的办法），不假设序号。
e2e_mysql_pod() { # release primary|replica
  local pod
  pod="$(kubectl -n "$E2E_MYSQL_NS" get service "$1-$2" -o jsonpath='{.spec.selector.statefulset\.kubernetes\.io/pod-name}')"
  if [[ -z "$pod" ]]; then
    echo "service $E2E_MYSQL_NS/$1-$2 has no pod-name selector" >&2
    return 1
  fi
  echo "$pod"
}

# 在某个 Pod 里用 root 执行标准输入里的 SQL。额外的参数原样交给 mysql 客户端（比如 -N -B）。
# 连 127.0.0.1：就是这个 Pod 里的实例，不经过 Service。
e2e_root_sql() { # pod [mysql 参数...]
  local pod="$1"
  shift
  # shellcheck disable=SC2016 # $MYSQL_ROOT_PASSWORD 和 "$@" 要在容器里展开
  kubectl -n "$E2E_MYSQL_NS" exec -i "$pod" -c mysql -- \
    bash -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql --no-defaults -uroot -h127.0.0.1 -P3306 --connect-timeout=5 "$@"' mysql "$@"
}

# GRANT 的库名里下划线是通配符，要转义成 \_（同 deploy/charts/mysql/files/accounts.sh）。
e2e_grant_name() { echo "${1//_/\\_}"; }

# 每次从干净的库开始（对应本机 scripts/e2e.sh 的开头），并给服务用的账号授权：
#   app  对 e2e 库的全部权限，和 2b 的 accounts.sh 给 app 的业务库权限一样（迁移要建表、改表）
#   cdc  订单分片上 e2e_order_db_N 的 SELECT（全量阶段分批读表）；REPLICATION SLAVE、REPLICATION CLIENT
#        是全局的，accounts.sh 已经给过
# GRANT 写在 CREATE 前面：库级授权不要求库已存在，这样从库上看到库的时候授权一定已经复制到了。
# 授权在 DROP DATABASE 之后仍然留在 mysql.db 里（MySQL 不随库删除授权），下次重跑 GRANT 没有副作用。
# 都在主库上做：从库只读，DDL 和授权靠复制到达（在从库上做会造成 GTID 分叉）。
e2e_create_dbs() {
  local pod sql db i
  pod="$(e2e_mysql_pod mysql-main primary)"
  sql=""
  for db in $E2E_MAIN_DBS; do
    sql="$sql
GRANT ALL PRIVILEGES ON \`$(e2e_grant_name "$db")\`.* TO 'app'@'%';
DROP DATABASE IF EXISTS \`$db\`;
CREATE DATABASE \`$db\`;"
  done
  printf '%s\n' "$sql" | e2e_root_sql "$pod"
  echo "    mysql-main ($pod): $E2E_MAIN_DBS"
  for i in $E2E_ORDER_SHARDS; do
    db="e2e_order_db_$i"
    pod="$(e2e_mysql_pod "mysql-order-$i" primary)"
    printf '%s\n' "GRANT ALL PRIVILEGES ON \`$(e2e_grant_name "$db")\`.* TO 'app'@'%';
GRANT SELECT ON \`$(e2e_grant_name "$db")\`.* TO 'cdc'@'%';
DROP DATABASE IF EXISTS \`$db\`;
CREATE DATABASE \`$db\`;" | e2e_root_sql "$pod"
    echo "    mysql-order-$i ($pod): $db"
  done
}

# 等订单库复制到从库，否则 order 启动时连从库会报库不存在、cdc 找不到要捕获的库。有上限，超时就失败。
e2e_wait_replicated() {
  local i db pod n
  for i in $E2E_ORDER_SHARDS; do
    db="e2e_order_db_$i"
    pod="$(e2e_mysql_pod "mysql-order-$i" replica)"
    n=0
    for _ in $(seq 1 60); do
      n="$(printf '%s\n' "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='$db';" |
        e2e_root_sql "$pod" -N -B 2>/dev/null)" || n=0
      [[ "$n" == 1 ]] && break
      sleep 1
    done
    if [[ "$n" != 1 ]]; then
      echo "$db did not replicate to $pod within 60s (is replication running? scripts/cluster/middleware.sh mysql)" >&2
      return 1
    fi
    echo "    $db replicated to $pod"
  done
}

# 无条件把两个订单从库的复制拉起来（同本机 scripts/e2e.sh 的 trap）。主从延迟的用例会停从库的 SQL 线程，
# 用例自己用 t.Cleanup 恢复，但 Job 被杀、超时、测试 panic 时 Cleanup 可能没机会跑；
# 这两个从库是和长期运行的那一套共用的，停着不管它们的展示读一直退回主库、cdc 一直落后。
# 已在运行时 START REPLICA 只产生警告。失败只报出来，不盖掉调用方的退出码。
e2e_start_replicas() {
  local i pod
  for i in $E2E_ORDER_SHARDS; do
    if pod="$(e2e_mysql_pod "mysql-order-$i" replica)" &&
      printf 'START REPLICA;\n' | e2e_root_sql "$pod" >/dev/null 2>&1; then
      echo "    START REPLICA on $pod"
    else
      echo "    WARNING: START REPLICA on the replica of mysql-order-$i failed; check it by hand (scripts/cluster/middleware.sh mysql)" >&2
    fi
  done
}

e2e_drop_dbs() {
  local pod sql db i
  pod="$(e2e_mysql_pod mysql-main primary)"
  sql=""
  for db in $E2E_MAIN_DBS; do
    sql="$sql
DROP DATABASE IF EXISTS \`$db\`;"
  done
  printf '%s\n' "$sql" | e2e_root_sql "$pod"
  for i in $E2E_ORDER_SHARDS; do
    pod="$(e2e_mysql_pod "mysql-order-$i" primary)"
    printf '%s\n' "DROP DATABASE IF EXISTS \`e2e_order_db_$i\`;" | e2e_root_sql "$pod"
  done
  echo "    dropped e2e_* databases"
}

# Kafka：删 e2e- 开头的 topic 和消费者组。在一个 broker 的 Pod 里用自带的命令行工具，连本 Pod 的明文监听。
# 命令行工具是一个新的 JVM，跑在 broker 的容器里，会继承 Strimzi 给 broker 设的 KAFKA_HEAP_OPTS（按 broker 的内存定的），
# 所以用 env 显式给一个小堆，不和 broker 抢容器的内存上限。
# 消费者组要等成员都离开（服务的 Pod 删掉之后）才能删；删不掉的空组也只是占 __consumer_offsets 里几条记录，
# 到 offsets.retention.minutes（默认 7 天）会自己过期，所以失败只警告。
e2e_clean_kafka() {
  local pod groups g args
  pod="$(kubectl -n kafka get pods -l strimzi.io/cluster=kafka,strimzi.io/broker-role=true -o jsonpath='{.items[0].metadata.name}')"
  if [[ -z "$pod" ]]; then
    echo "    WARNING: no kafka broker pod found; e2e topics not deleted" >&2
    return 0
  fi
  kubectl -n kafka exec "$pod" -- env KAFKA_HEAP_OPTS=-Xmx128m /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --if-exists --topic 'e2e-.*' >/dev/null
  echo "    deleted topics e2e-.* (via $pod)"
  groups="$(kubectl -n kafka exec "$pod" -- env KAFKA_HEAP_OPTS=-Xmx128m /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list 2>/dev/null)" || groups=""
  args=()
  for g in $groups; do
    case "$g" in
    e2e-*) args+=(--group "$g") ;;
    esac
  done
  if [[ ${#args[@]} -eq 0 ]]; then
    return 0
  fi
  if kubectl -n kafka exec "$pod" -- env KAFKA_HEAP_OPTS=-Xmx128m /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --delete "${args[@]}" >/dev/null 2>&1; then
    echo "    deleted $((${#args[@]} / 2)) consumer groups e2e-*"
  else
    echo "    WARNING: some e2e-* consumer groups were not deleted (still have members?); they expire on their own" >&2
  fi
}

# Redis：在每个主节点上扫 e2e- 开头的键并 UNLINK。闸门实例是一主一从，缓存是 Cluster（每个主节点只有自己那部分槽的键）；
# 两者都是“在主节点上执行、从节点跳过”。每个键一条 UNLINK，经一个连接流水线发出：Cluster 下多个键的 DEL 跨槽会报错。
# 在 Redis 的 Pod 里用 sh 执行（官方镜像有 sh），不把键列表拉回 MacBook。键名是服务自己生成的，不含空白和引号。
e2e_clean_redis() {
  local sts n i
  for sts in redis-gate redis-cache; do
    n="$(kubectl -n redis get statefulset "$sts" -o jsonpath='{.spec.replicas}' 2>/dev/null)" || n=""
    if [[ -z "$n" ]]; then
      echo "    WARNING: statefulset redis/$sts not found; its e2e keys not deleted" >&2
      continue
    fi
    for ((i = 0; i < n; i++)); do
      # shellcheck disable=SC2016 # 整段在容器里执行
      kubectl -n redis exec "$sts-$i" -c redis -- sh -c '
        [ "$(redis-cli role | head -n 1)" = master ] || exit 0
        c="$(redis-cli --scan --pattern "e2e-*" | wc -l)"
        redis-cli --scan --pattern "e2e-*" | while IFS= read -r k; do printf "UNLINK \"%s\"\n" "$k"; done | redis-cli >/dev/null
        echo "    $HOSTNAME: unlinked $c keys e2e-*"' || echo "    WARNING: cleaning e2e keys on redis/$sts-$i failed" >&2
    done
  done
}
