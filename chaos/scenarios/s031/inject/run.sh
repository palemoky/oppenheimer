#!/usr/bin/env bash
# Kafka 上给客户端号 order 设一条拉取配额（consumer_byte_rate，每个 broker 每秒 2048 字节）：
# broker 对这个客户端号的拉取请求限速，生产不受影响。配额存在集群的元数据里，要明确地删；另起一个后台任务到时自己删。
# 注意客户端号不分命名空间：e2e 那一套的 order 也叫 order，两套会一起被限。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
# shellcheck source=scripts/cluster/chaos-lib.sh
source "$ROOT/scripts/cluster/chaos-lib.sh"
cx_init 900
KNS=kafka
RATE="${CX_FETCH_BYTES:-2048}"

kpod() { kubectl -n "$KNS" get pods -l strimzi.io/broker-role=true -o jsonpath='{.items[0].metadata.name}'; }
quota() { # pod add|del
  local cfg=(--add-config "consumer_byte_rate=$RATE")
  [[ "$2" == del ]] && cfg=(--delete-config consumer_byte_rate)
  kubectl -n "$KNS" exec "$1" -c kafka -- env KAFKA_HEAP_OPTS=-Xmx128m /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server localhost:9092 --alter --entity-type clients --entity-name order "${cfg[@]}"
}

case "${1:-}" in
inject)
  pod="$(kpod)"
  [[ -n "$pod" ]] || cx_die "no kafka broker pod found"
  cx_remember pod "$pod"
  cx_bg "$KNS" "$pod" kafka undo <<'EOS'
while [ ! -e "$STOP" ] && [ "$(date +%s)" -lt "$END" ]; do sleep 5; done
[ -e "$STOP" ] && exit 0
KAFKA_HEAP_OPTS=-Xmx128m /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type clients --entity-name order --delete-config consumer_byte_rate
EOS
  quota "$pod" add
  ;;
recover)
  pod="$(cx_recall pod)"
  [[ -n "$pod" ]] || pod="$(kpod)"
  cx_stop "$KNS" "$pod" kafka
  # 配额已经不在时（到时自己删过了）删除会报错，不算失败；删完再查一次确认没有了。
  quota "$pod" del || true
  left="$(kubectl -n "$KNS" exec "$pod" -c kafka -- env KAFKA_HEAP_OPTS=-Xmx128m /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server localhost:9092 --describe --entity-type clients --entity-name order)"
  if grep -q consumer_byte_rate <<<"$left"; then
    echo "quota is still there: $left" >&2
    exit 1
  fi
  cx_clean "$KNS" "$pod" kafka 6
  ;;
*)
  echo "usage: run.sh inject|recover" >&2
  exit 2
  ;;
esac
