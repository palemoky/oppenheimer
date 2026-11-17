#!/usr/bin/env bash
# 在 MacBook 上给集群装可观测的后端（设计文档 3a、ADR-0030）。每个子命令都可以重复运行。
#
#   monitoring.sh kps               kube-prometheus-stack：Prometheus、Alertmanager、Grafana、node-exporter、kube-state-metrics
#   monitoring.sh loki              Loki（单体模式，日志）
#   monitoring.sh tempo             Tempo（单体模式，链路；带 metrics-generator），并 apply Grafana 的数据源
#   monitoring.sh collector         OpenTelemetry Collector：gateway（Deployment，收 OTLP 转发给后端）和 agent（DaemonSet，读容器日志）
#   monitoring.sh exporters         MySQL、Redis 的 exporter（多目标模式，各一个 Deployment）、MySQL 的监控账号、Kafka 的 PodMonitor
#   monitoring.sh datasources       只 apply Grafana 的 Loki 和 Tempo 数据源（tempo 已经包含这一步）
#   monitoring.sh up                按顺序装全部：kps、loki、tempo、collector、exporters
#   monitoring.sh status            只读：Pod 在哪个节点、卷、Helm release、Prometheus 的抓取目标里不健康的
#   monitoring.sh password          打印 Grafana 的管理员密码
#   monitoring.sh forward <名字>    端口转发到本机：prometheus（9090）、alertmanager（9093）。Ctrl-C 结束
#   monitoring.sh promql '<查询>'   对集群里的 Prometheus 执行一次即时查询，打印 JSON（脚本和排查用）
#
# 用 $KUBECONFIG_OUT 指向的 kubeconfig，不看当前的 context：这台机器上还有 OrbStack 的集群。
# 版本来自 cluster.env。在 macOS 自带的 bash 3.2 上运行。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
ROOT="$(cd "$HERE/../.." && pwd)"
DEPLOY="$ROOT/deploy/cluster/monitoring"

export KUBECONFIG="$KUBECONFIG_OUT"
if [[ ! -f "$KUBECONFIG" ]]; then
  echo "$KUBECONFIG not found; run scripts/cluster/k3s.sh kubeconfig first" >&2
  exit 1
fi

NS="monitoring"
KPS_RELEASE="kps" # 资源名的前缀：kps-grafana、prometheus-kps-prometheus-0 ……

server_ip() {
  local spec
  for spec in "${NODES[@]}"; do
    if [[ "${spec%%:*}" == "k3s-server" ]]; then
      echo "$spec" | cut -d: -f2
      return 0
    fi
  done
  echo "k3s-server not found in cluster.env" >&2
  return 1
}

render() {
  sed -e "s#__SERVER_IP__#$(server_ip)#g" \
    -e "s#__PROMETHEUS_TAG__#$PROMETHEUS_VERSION#g" \
    -e "s#__GRAFANA_HOST__#$GRAFANA_HOST#g" \
    -e "s#__MYSQLD_EXPORTER_TAG__#$MYSQLD_EXPORTER_VERSION#g" \
    -e "s#__REDIS_EXPORTER_TAG__#$REDIS_EXPORTER_VERSION#g" \
    "$1"
}

# 经代理拉 chart 可能断：重试三次。
helm_retry() {
  local i
  for i in 1 2 3; do
    if "$@"; then return 0; fi
    echo "    helm failed (attempt $i), retrying in 10 s" >&2
    sleep 10
  done
  return 1
}

ensure_ns() {
  kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS" >/dev/null
}

# Grafana 的管理员密码：第一次随机生成，之后不动（重跑不会换密码）。
ensure_grafana_secret() {
  if kubectl -n "$NS" get secret grafana-admin >/dev/null 2>&1; then
    return 0
  fi
  local pw
  # 不用 tr </dev/urandom | head：head 拿够就退出，tr 收到 SIGPIPE，在 pipefail 下整个脚本无声地退出。
  pw="$(openssl rand -hex 16)"
  kubectl -n "$NS" create secret generic grafana-admin \
    --from-literal=admin-user=admin --from-literal=admin-password="$pw" >/dev/null
  echo "==> generated the Grafana admin password (monitoring.sh password prints it)"
}

cmd_kps() {
  echo "==> kube-prometheus-stack $KPS_VERSION (Prometheus $PROMETHEUS_VERSION)"
  ensure_ns
  ensure_grafana_secret
  local values
  values="$(mktemp)"
  # shellcheck disable=SC2064 # 要的就是现在展开
  trap "rm -f '$values'" EXIT
  render "$DEPLOY/kube-prometheus-stack.yaml" >"$values"
  # CRD 很大，放进 Helm 的 release 记录会超过 Secret 的上限，chart 自己用一个 Job 来装和升级。
  helm_retry helm upgrade --install "$KPS_RELEASE" kube-prometheus-stack \
    --repo https://prometheus-community.github.io/helm-charts \
    --version "$KPS_VERSION" --namespace "$NS" \
    -f "$values" --wait --timeout 10m >/dev/null
  # Prometheus 和 Alertmanager 是 operator 根据自定义资源建的 StatefulSet，helm --wait 不等它们。
  kubectl -n "$NS" wait --for=condition=Available "prometheus/$KPS_RELEASE-prometheus" --timeout=5m
  kubectl -n "$NS" wait --for=condition=Available "alertmanager/$KPS_RELEASE-alertmanager" --timeout=5m
  render "$DEPLOY/grafana-route.yaml" | kubectl apply -f - >/dev/null
  echo "    Grafana: curl -H 'Host: $GRAFANA_HOST' http://$LB_GATEWAY_IP/  (browser: add '$LB_GATEWAY_IP $GRAFANA_HOST' to /etc/hosts)"
}

# Loki 和 Tempo 的 chart 在 grafana-community 的仓库（版本和来由见 cluster.env）。values 里没有占位符，直接用。
# 镜像 tag 用 cluster.env 里的版本覆盖：chart 升级时默认的应用版本可能前移，版本以 cluster.env 为准。
cmd_loki() {
  echo "==> Loki $LOKI_VERSION (chart $LOKI_CHART_VERSION)"
  ensure_ns
  helm_retry helm upgrade --install loki loki \
    --repo "$GRAFANA_COMMUNITY_REPO" \
    --version "$LOKI_CHART_VERSION" --namespace "$NS" \
    -f "$DEPLOY/loki.yaml" --set "loki.image.tag=$LOKI_VERSION" \
    --wait --timeout 10m >/dev/null
  echo "    OTLP: http://loki.$NS:3100/otlp  (Collector 的 otlphttp 导出器 endpoint 写 http://loki.$NS:3100/otlp)"
}

OTELCOL_REPO="https://open-telemetry.github.io/opentelemetry-helm-charts"

# 先装 gateway 再装 agent：agent 把日志发给 gateway，gateway 先就绪，agent 起来时就不会一开始就发送失败。
# 镜像 tag 同样用 cluster.env 的版本覆盖（chart 默认的应用版本落后）；镜像仓库在 values 里。
cmd_collector() {
  echo "==> OpenTelemetry Collector $OTELCOL_VERSION (chart $OTELCOL_CHART_VERSION)"
  ensure_ns
  local rel
  for rel in gateway agent; do
    helm_retry helm upgrade --install "otel-$rel" opentelemetry-collector \
      --repo "$OTELCOL_REPO" \
      --version "$OTELCOL_CHART_VERSION" --namespace "$NS" \
      -f "$DEPLOY/otel-$rel.yaml" --set "image.tag=$OTELCOL_VERSION" \
      --wait --timeout 10m >/dev/null
  done
  echo "    OTLP: otel-gateway.$NS:4317 (gRPC)、otel-gateway.$NS:4318 (HTTP)；agent 在每个节点上读容器日志"
}

# MySQL、Redis 的 exporter（设计文档 3a、ADR-0030 第 8 项）。三步：
#   1. Secret mysql-exporter-auth：监控账号的密码，第一次随机生成，之后不动
#   2. 在每个 MySQL 实例的主库上建（或对齐）监控账号 exporter；账号随复制到从库，不重启任何 MySQL Pod
#   3. apply 两个 exporter 的清单和 Kafka 的 PodMonitor，等 exporter 就绪
# Kafka 一侧（broker 的 JMX exporter、Kafka Exporter）在 deploy/cluster/kafka/kafka.yaml，
# 由 middleware.sh kafka 提交（那一步会滚动重启 broker）；它不放在这里，因为 Kafka 的资源由 Strimzi 的 Operator 管，
# 和 Kafka 的其余配置在同一个文件、同一次提交里才不会漂移。PodMonitor 在这里提交，因为它依赖 kps 装的 CRD。
# 依赖：kps 已装（ScrapeConfig、PodMonitor 的 CRD），MySQL 已装（middleware.sh mysql）。
MYSQL_NS="mysql"
MYSQL_INSTANCES=(mysql-order-0 mysql-order-1 mysql-main) # 要和 middleware.sh 的 MYSQL_RELEASES、mysqld-exporter.yaml 的目标一致

ensure_mysql_exporter_secret() {
  if kubectl -n "$NS" get secret mysql-exporter-auth >/dev/null 2>&1; then
    echo "    secret mysql-exporter-auth exists, keeping it"
    return 0
  fi
  echo "    creating secret mysql-exporter-auth with a random password"
  # 密码不打印，不出现在命令行参数里：清单经标准输入交给 kubectl，printf 是 shell 的内建命令。
  # 用 create 不用 apply：client-side apply 会把整份清单（含密码）存进 last-applied-configuration 注解。
  printf 'apiVersion: v1\nkind: Secret\ntype: Opaque\nmetadata:\n  name: mysql-exporter-auth\n  namespace: %s\nstringData:\n  password: "%s"\n' \
    "$NS" "$(openssl rand -hex 16)" | kubectl create -f - >/dev/null
}

# 在一个实例的主库上建监控账号。可以重复执行：CREATE USER IF NOT EXISTS 加 ALTER USER，密码每次对齐到 Secret
# （Secret 是唯一来源，重建 Secret 后重跑一次就一致了）。
# 主库以 <实例>-primary Service 指向的 Pod 为准；从库是只读的，账号靠复制过去，所以不能直接在从库上建。
# 权限只给 REPLICATION CLIENT（SHOW REPLICA STATUS 用，也是唯一一个采集器要的权限）：
#   全局状态、全局变量走 SHOW GLOBAL STATUS/VARIABLES，不需要权限；在 MySQL 9.7.4 上只授这一个，三个采集器都成功（实测）。
#   官方文档建议的 PROCESS、SELECT 是给 processlist、information_schema 这类采集器的，我们不开，就不给；以后开了再加。
# MAX_USER_CONNECTIONS 3：exporter 每个目标最多一个连接，留一点余量；账号被滥用时也占不满 max_connections。
# 语句和密码经标准输入传给 Pod 里的 mysql 客户端，root 密码用 Pod 自己的环境变量，都不出现在任何命令行参数里。
mysql_exporter_account() { # <实例>
  local rel="$1" primary
  primary="$(kubectl -n "$MYSQL_NS" get service "$rel-primary" -o jsonpath='{.spec.selector.statefulset\.kubernetes\.io/pod-name}')"
  if [[ -z "$primary" ]]; then
    echo "    $rel-primary has no pod-name selector; is $rel installed?" >&2
    return 1
  fi
  echo "    exporter account on $primary"
  local pw
  pw="$(kubectl -n "$NS" get secret mysql-exporter-auth -o jsonpath='{.data.password}' | base64 -d)"
  # shellcheck disable=SC2016 # $MYSQL_ROOT_PASSWORD 要在 Pod 里展开
  kubectl -n "$MYSQL_NS" exec -i "$primary" -c mysql -- \
    bash -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql --no-defaults -h127.0.0.1 -uroot --connect-timeout=5' <<SQL
CREATE USER IF NOT EXISTS 'exporter'@'%' IDENTIFIED BY '$pw' WITH MAX_USER_CONNECTIONS 3;
ALTER USER 'exporter'@'%' IDENTIFIED BY '$pw' WITH MAX_USER_CONNECTIONS 3;
GRANT REPLICATION CLIENT ON *.* TO 'exporter'@'%';
SQL
}

cmd_exporters() {
  echo "==> exporters: mysqld_exporter $MYSQLD_EXPORTER_VERSION, redis_exporter $REDIS_EXPORTER_VERSION"
  ensure_ns
  ensure_mysql_exporter_secret
  local rel
  for rel in "${MYSQL_INSTANCES[@]}"; do
    mysql_exporter_account "$rel"
  done
  local f
  for f in mysqld-exporter redis-exporter kafka-podmonitor; do
    render "$DEPLOY/$f.yaml" | kubectl apply -f - >/dev/null
  done
  kubectl -n "$NS" rollout status deployment/mysqld-exporter --timeout=3m
  kubectl -n "$NS" rollout status deployment/redis-exporter --timeout=3m
  echo "    targets show up in Prometheus within one scrape interval (30 s); check: $0 status"
  echo "    Kafka: broker 和 Kafka Exporter 的指标要 middleware.sh kafka 提交 metricsConfig 之后才有（会滚动重启 broker）"
}

cmd_tempo() {
  echo "==> Tempo $TEMPO_VERSION (chart $TEMPO_CHART_VERSION)"
  ensure_ns
  helm_retry helm upgrade --install tempo tempo \
    --repo "$GRAFANA_COMMUNITY_REPO" \
    --version "$TEMPO_CHART_VERSION" --namespace "$NS" \
    -f "$DEPLOY/tempo.yaml" --set "tempo.tag=$TEMPO_VERSION" \
    --wait --timeout 10m >/dev/null
  echo "    OTLP: tempo.$NS:4317 (gRPC)、tempo.$NS:4318 (HTTP)"
  cmd_datasources
}

# 数据源放在 ConfigMap 里，Grafana 的 sidecar 自动加载；Loki 和 Tempo 还没装也可以先 apply。
cmd_datasources() {
  ensure_ns
  kubectl apply -f "$DEPLOY/datasources.yaml" >/dev/null
  echo "    Grafana datasources (loki, tempo) applied"
}

cmd_status() {
  kubectl -n "$NS" get pods -o wide
  kubectl -n "$NS" get pvc
  helm -n "$NS" list
  echo "==> scrape targets that are not up:"
  cmd_promql 'up == 0' | python3 -c '
import json, sys
r = json.load(sys.stdin)["data"]["result"]
for s in r:
    m = s["metric"]
    print("   ", m.get("job"), m.get("instance"))
print("    (none)" if not r else "    %d down" % len(r))'
  # exporter 抓取本身成功（up 1）不代表数据库是好的：连不上时 exporter 照样返回 200，只是 mysql_up、redis_up 是 0。
  echo "==> databases the exporters cannot reach (mysql_up / redis_up == 0):"
  cmd_promql '{__name__=~"mysql_up|redis_up"} == 0' | python3 -c '
import json, sys
r = json.load(sys.stdin)["data"]["result"]
for s in r:
    m = s["metric"]
    print("   ", m.get("job"), m.get("instance"))
print("    (none)" if not r else "    %d unreachable" % len(r))'
}

cmd_password() {
  kubectl -n "$NS" get secret grafana-admin -o jsonpath='{.data.admin-password}' | base64 -d
  echo
}

cmd_forward() {
  case "${1:-}" in
  prometheus) kubectl -n "$NS" port-forward "svc/$KPS_RELEASE-prometheus" 9090:9090 ;;
  alertmanager) kubectl -n "$NS" port-forward "svc/$KPS_RELEASE-alertmanager" 9093:9093 ;;
  *)
    echo "usage: $0 forward prometheus|alertmanager" >&2
    return 2
    ;;
  esac
}

# 经 apiserver 的代理访问 Prometheus，不用另开端口转发。
cmd_promql() {
  local q="${1:?usage: monitoring.sh promql '<query>'}"
  kubectl get --raw \
    "/api/v1/namespaces/$NS/services/http:$KPS_RELEASE-prometheus:9090/proxy/api/v1/query?query=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1]))' "$q")"
}

cmd="${1:-}"
shift || true
case "$cmd" in
kps) cmd_kps ;;
loki) cmd_loki ;;
tempo) cmd_tempo ;;
collector) cmd_collector ;;
datasources) cmd_datasources ;;
exporters) cmd_exporters ;;
up)
  cmd_kps
  cmd_loki
  cmd_tempo
  cmd_collector
  cmd_exporters
  ;;
status) cmd_status ;;
password) cmd_password ;;
forward) cmd_forward "$@" ;;
promql) cmd_promql "$@" ;;
*)
  echo "usage: $0 kps | loki | tempo | collector | exporters | datasources | up | status | password | forward <name> | promql '<query>'" >&2
  exit 2
  ;;
esac
