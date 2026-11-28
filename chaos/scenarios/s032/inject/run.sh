#!/usr/bin/env bash
# 一个 broker 的 Pod 里循环起命令行的消费者，用的是 orderindex 的消费者组名：进组约 15 秒（分到一部分分区，拉消息但不提交位点），
# 被 timeout 结束后正常退组，停 10 秒，再来。组每 25 秒左右重平衡两次。不提交位点，所以不丢消息：分区还回去之后从原来的位点接着消费。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
# shellcheck source=scripts/cluster/chaos-lib.sh
source "$ROOT/scripts/cluster/chaos-lib.sh"
cx_init 900
KNS=kafka

kpod() { kubectl -n "$KNS" get pods -l strimzi.io/broker-role=true -o jsonpath='{.items[0].metadata.name}'; }

case "${1:-}" in
inject)
  pod="$(kpod)"
  [[ -n "$pod" ]] || cx_die "no kafka broker pod found"
  cx_remember pod "$pod"
  prefix="$(cx_app_env orderindex KAFKA_PREFIX)"
  cx_bg "$KNS" "$pod" kafka tail "TOPIC=${prefix}cdc.order.orders" "GROUP=${prefix}orderindex.cdc-orders" <<'EOS'
while [ ! -e "$STOP" ] && [ "$(date +%s)" -lt "$END" ]; do
  KAFKA_HEAP_OPTS=-Xmx128m timeout 15 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic "$TOPIC" --group "$GROUP" \
    --consumer-property enable.auto.commit=false \
    --consumer-property client.id=report-export \
    --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor \
    >/dev/null 2>"$TAG.tail.last"
  t=10
  while [ "$t" -gt 0 ] && [ ! -e "$STOP" ]; do sleep 2; t=$((t - 2)); done
done
EOS
  cx_bg_check "$KNS" "$pod" kafka tail
  ;;
recover)
  pod="$(cx_recall pod)"
  [[ -n "$pod" ]] || pod="$(kpod)"
  cx_stop "$KNS" "$pod" kafka
  # 正在组里的那一个最多再待 15 秒，等它退组之后再删文件。
  cx_clean "$KNS" "$pod" kafka 18
  ;;
*)
  echo "usage: run.sh inject|recover" >&2
  exit 2
  ;;
esac
