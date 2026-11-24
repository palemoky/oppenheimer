#!/usr/bin/env bash
# 阶段 4 每次故障实验收尾用的“对账差异为 0”（make recon-check FROM=<RFC3339> TO=<RFC3339>）。两件事，缺一不可：
#
#   1. 读三个常驻核对的指标（scripts/cluster/monitoring.sh promql）：
#        没有不一致：钱（recon_totals_mismatch）和库存（recon_stock_mismatch_skus）从 FROM 到现在的最大值是 0，
#                    变更核对（recon_changes_checked_total）里 mismatch 在这段时间没有增加；
#        核对到了 TO：三样各自“核对到了哪里”都越过 TO。
#      “差异是 0”和“还没核对到”分开报：还没核对到时提示等一会儿再跑，不当成通过（核对没跑时指标也是 0 或者没有）。
#   2. 对 [FROM, TO) 跑一次逐行的对账（recon 的 -from、-to）：从 recon-hourly 的模板起一个一次性的 Job，只改参数。
#
# 用法：
#   scripts/cluster/recon-check.sh --from 2026-11-24T10:00:00Z --to 2026-11-24T10:20:00Z [--dry-run]
#   --dry-run  只打印要执行的查询和命令，不连集群、不起 Job
#   --metrics-only  只做第 1 件（不起 Job）
#
# 退出码：0 通过；1 有差异（指标不是 0，或者逐行对账退出码 1）；2 用法不对或者查询、Job 出了错（没有结论）；
#         3 没有差异，但还没有核对到 TO，等一会儿再跑。有差异比没核对到优先。
#
# 几处取舍：
#   - 指标的窗口取 [FROM, 现在]，不是 [FROM, TO]：一条在 TO 之前提交的变更，核对发生在 TO 之后（延迟 10 秒起），
#     计数器是在核对的那一刻增加的，窗口停在 TO 会漏掉。代价是 TO 之后别的原因造成的不一致也会算进来。
#   - 钱的核对每一轮看“最近 30 分钟，去掉最新的一分钟”，所以要有一轮在 TO 之后至少 2 分钟开始并且成功，才算覆盖到了 TO。
#   - 库存的“已售”那条等式只有完整的一轮（每小时一次）才核对，所以覆盖要等到 TO 之后有一次完整的一轮成功，最长一小时。
#   - 变更核对的 verified_through 只在有新的变更时往前走；TO 之后订单库没有写入的话它永远不会越过 TO（探针一直在写，平时不会）。
#   - 起的 Job 去掉了 ownerReferences，不属于 CronJob：失败了不会触发 ReconFoundDifferences（没有验证过这一点）。
#
# 用 $KUBECONFIG_OUT 指向的 kubeconfig（和 monitoring.sh 一样）。在 macOS 自带的 bash 3.2 上运行。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"

NS="${APP_NAMESPACE:-oppenheimer}"
SEL="k8s_namespace_name=\"$NS\""
STEP=15s            # 子查询的步长
CHECK_MARGIN=120    # 钱的核对要在 TO 之后多少秒开始的一轮才算覆盖到 TO（去掉最新一分钟，再留一轮）
JOB_TIMEOUT="${RECON_CHECK_TIMEOUT:-35m}" # 等逐行对账的 Job（模板的 activeDeadlineSeconds 是 30 分钟，重试最多再加一点）

FROM="" TO="" DRY=0 ROWS=1
while [[ $# -gt 0 ]]; do
  case "$1" in
  --from) FROM="${2:-}"; shift 2 ;;
  --to) TO="${2:-}"; shift 2 ;;
  --dry-run) DRY=1; shift ;;
  --metrics-only) ROWS=0; shift ;;
  -h | --help) sed -n '2,31p' "${BASH_SOURCE[0]}"; exit 0 ;;
  *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
if [[ -z "$FROM" || -z "$TO" ]]; then
  echo "usage: recon-check.sh --from <RFC3339> --to <RFC3339> [--dry-run] [--metrics-only]  (make recon-check FROM=... TO=...)" >&2
  exit 2
fi

# RFC 3339 转 Unix 秒。
epoch() {
  python3 -I -c '
import sys
from datetime import datetime
try:
    print(int(datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00")).timestamp()))
except ValueError as e:
    sys.exit("not an RFC 3339 time: %s (%s)" % (sys.argv[1], e))
' "$1"
}
FROM_E="$(epoch "$FROM")" || exit 2
TO_E="$(epoch "$TO")" || exit 2
NOW_E="$(date +%s)"
if [[ "$FROM_E" -ge "$TO_E" ]]; then
  echo "--from must be before --to" >&2
  exit 2
fi
if [[ "$TO_E" -ge "$NOW_E" ]]; then
  echo "--to is not in the past: nothing after it has been checked yet" >&2
  exit 2
fi
W=$((NOW_E - FROM_E))           # 指标的窗口：从 FROM 到现在
SINCE_TO=$((NOW_E - TO_E))      # TO 之后过了多久
SINCE_CHECKED=$((SINCE_TO - CHECK_MARGIN))

# 即时查询，打印第一个结果的值；没有结果打印 none。dry-run 只打印查询。
promql() { # 说明 表达式
  if [[ "$DRY" -eq 1 ]]; then
    echo "  [dry-run] $1:" >&2
    echo "      scripts/cluster/monitoring.sh promql '$2'" >&2
    echo "none"
    return 0
  fi
  local out
  out="$("$HERE/monitoring.sh" promql "$2")" || {
    echo "query failed ($1): $2" >&2
    return 1
  }
  printf '%s' "$out" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
if d.get("status") != "success":
    sys.exit("prometheus: %s" % d)
r = d["data"]["result"]
print(r[0]["value"][1] if r else "none")
'
}

# 值比较（可能是浮点）：gt0 v / ge v n。none 不算。
is_pos() { python3 -I -c 'import sys; v=sys.argv[1]; sys.exit(0 if v!="none" and float(v)>0 else 1)' "$1"; }
is_ge() { python3 -I -c 'import sys; v=sys.argv[1]; sys.exit(0 if v!="none" and float(v)>=float(sys.argv[2]) else 1)' "$1" "$2"; }

DIFF=0 PENDING=0 ERR=0
say() { printf '%s\n' "$*"; }

echo "==> 1/2 常驻核对的指标（窗口 $FROM 到现在，要求核对越过 ${TO}）"

# ---- 没有不一致 ----
v="$(promql "钱：笔数和金额的差的最大值" "max_over_time(max(recon_totals_mismatch{$SEL})[${W}s:$STEP])")" || ERR=1
if [[ "$DRY" -eq 1 ]]; then :
elif [[ "$v" == none ]]; then say "  钱      没有数据：recon_totals_mismatch 在这段时间没有序列，核对没在跑，不能算 0"; PENDING=1
elif is_pos "$v"; then say "  钱      有差异：recon_totals_mismatch 最大 ${v}（见 docs/runbooks/payment-totals.md）"; DIFF=1
else say "  钱      差异是 0"; fi

v="$(promql "库存：不一致的 SKU 数的最大值" "max_over_time(max(recon_stock_mismatch_skus{$SEL})[${W}s:$STEP])")" || ERR=1
if [[ "$DRY" -eq 1 ]]; then :
elif [[ "$v" == none ]]; then say "  库存    没有数据：recon_stock_mismatch_skus 在这段时间没有序列，核对没在跑，不能算 0"; PENDING=1
elif is_pos "$v"; then say "  库存    有差异：不一致的 SKU 最大 ${v}（见 docs/runbooks/recon-stock.md）"; DIFF=1
else say "  库存    差异是 0"; fi

# 计数器：increase 加上“窗口开头没有、之后出现”的序列的值（第一次出现时 increase 是 0，规则里也是这样补的）。
M="recon_changes_checked_total{$SEL,result=\"mismatch\"}"
v="$(promql "变更：mismatch 的增加" "(sum(increase(${M}[${W}s])) or vector(0)) + (sum(${M} unless ${M} offset ${W}s) or vector(0))")" || ERR=1
if [[ "$DRY" -eq 1 ]]; then :
elif is_pos "$v"; then say "  变更    有差异：mismatch 增加了 ${v}（见 docs/runbooks/recon-changes.md）"; DIFF=1
else say "  变更    mismatch 没有增加"; fi

# ---- 核对到了 TO ----
if [[ "$SINCE_CHECKED" -lt 60 ]]; then
  # 窗口太短，increase 量不准；也一定还没有核对到。
  say "  钱、库存  距 TO 才 ${SINCE_TO} 秒，核对还没有越过 TO（要 TO 之后 ${CHECK_MARGIN} 秒以上）"
  PENDING=1
else
  R="${SINCE_CHECKED}s"
  v="$(promql "钱：TO 之后开始的成功的一轮" "sum(increase(recon_totals_runs_total{$SEL,outcome=\"ok\"}[$R]))")" || ERR=1
  if [[ "$DRY" -eq 1 ]]; then :
  elif is_pos "$v"; then say "  钱      TO 之后有成功的一轮（$v 轮）"
  else say "  钱      还没核对到 TO：TO 之后没有成功的一轮"; PENDING=1; fi

  v="$(promql "库存：TO 之后成功的快的一轮" "sum(increase(recon_stock_runs_total{$SEL,kind=\"quick\",outcome=\"ok\"}[$R]))")" || ERR=1
  if [[ "$DRY" -eq 1 ]]; then :
  elif is_pos "$v"; then say "  库存    预占：TO 之后有成功的一轮（$v 轮）"
  else say "  库存    预占还没核对到 TO：TO 之后没有成功的快的一轮"; PENDING=1; fi

  v="$(promql "库存：TO 之后成功的完整的一轮" "sum(increase(recon_stock_runs_total{$SEL,kind=\"full\",outcome=\"ok\"}[$R]))")" || ERR=1
  if [[ "$DRY" -eq 1 ]]; then :
  elif is_pos "$v"; then say "  库存    已售：TO 之后有成功的完整的一轮"
  else say "  库存    已售还没核对到 TO：完整的一轮每小时一次，最长再等 1 小时"; PENDING=1; fi
fi

v="$(promql "变更：核对到的提交时刻（Unix 秒）" "max(recon_changes_verified_through{$SEL})")" || ERR=1
if [[ "$DRY" -eq 1 ]]; then :
elif [[ "$v" == none ]]; then say "  变更    没有 recon_changes_verified_through：核对没起来，或者还没有核对过变更"; PENDING=1
elif is_ge "$v" "$TO_E"; then say "  变更    核对到了 ${v}，越过 TO（${TO_E}）"
else say "  变更    还没核对到 TO：核对到 ${v}，TO 是 ${TO_E}（订单库没有新的写入时它不会往前走）"; PENDING=1; fi

# ---- 逐行对账 ----
if [[ "$ROWS" -eq 1 ]]; then
  echo "==> 2/2 逐行对账 [$FROM, $TO)"
  JOB="recon-check-$NOW_E"
  # 从 recon-hourly 的模板起：同样的镜像、库、资源和安全上下文，只换参数（去掉 -since，加 -from、-to；-to 替代 -settle），
  # 去掉 ownerReferences（不属于 CronJob）。-recheck 30s 和 -timeout 沿用模板的取值。
  JOB_CMD="kubectl -n $NS create job $JOB --from=cronjob/recon-hourly --dry-run=client -o json | <改参数、去掉 ownerReferences> | kubectl -n $NS create -f -"
  if [[ "$DRY" -eq 1 ]]; then
    echo "  [dry-run] $JOB_CMD" >&2
    echo "      args: -pairs=payment,stock,replica,index -from=$FROM -to=$TO -recheck=30s -timeout=25m" >&2
    echo "  [dry-run] kubectl -n $NS wait --for=condition=complete|failed job/$JOB --timeout=${JOB_TIMEOUT}，再读 Failed 的 reason（PodFailurePolicy 是有差异）" >&2
    echo "  [dry-run] kubectl -n $NS logs job/$JOB" >&2
  else
    export KUBECONFIG="$KUBECONFIG_OUT"
    if kubectl -n "$NS" create job "$JOB" --from=cronjob/recon-hourly --dry-run=client -o json |
      python3 -I -c '
import json, sys
j = json.load(sys.stdin)
j["metadata"].pop("ownerReferences", None)
c = j["spec"]["template"]["spec"]["containers"][0]
c["args"] = ["-pairs=payment,stock,replica,index", "-from=" + sys.argv[1], "-to=" + sys.argv[2], "-recheck=30s", "-timeout=25m"]
json.dump(j, sys.stdout)
' "$FROM" "$TO" | kubectl -n "$NS" create -f - >/dev/null; then
      echo "  job $NS/$JOB started, waiting up to $JOB_TIMEOUT"
      # 完成和失败各等一次，谁先到谁结束；另一个等待会超时，丢掉它的输出。
      kubectl -n "$NS" wait --for=condition=complete "job/$JOB" --timeout="$JOB_TIMEOUT" >/dev/null 2>&1 &
      pc=$!
      kubectl -n "$NS" wait --for=condition=failed "job/$JOB" --timeout="$JOB_TIMEOUT" >/dev/null 2>&1 &
      pf=$!
      while kill -0 "$pc" 2>/dev/null && kill -0 "$pf" 2>/dev/null; do sleep 3; done
      kill "$pc" "$pf" 2>/dev/null || true
      wait "$pc" "$pf" 2>/dev/null || true
      reason="$(kubectl -n "$NS" get job "$JOB" -o jsonpath='{.status.conditions[?(@.type=="Failed")].reason}' 2>/dev/null || true)"
      done_ok="$(kubectl -n "$NS" get job "$JOB" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || true)"
      kubectl -n "$NS" logs "job/$JOB" --tail=40 2>&1 | sed 's/^/    | /' || true
      if [[ "$done_ok" == True ]]; then
        say "  逐行对账 差异是 0（job/${JOB}，退出码 0）"
      elif [[ "$reason" == PodFailurePolicy ]]; then
        say "  逐行对账 有差异（job/${JOB}，退出码 1）：kubectl -n $NS logs job/$JOB"; DIFF=1
      else
        say "  逐行对账 没有结论（job/${JOB}，Failed 的原因 '${reason:-未知，可能还在跑}'）：kubectl -n $NS describe job/$JOB"; ERR=1
      fi
    else
      say "  逐行对账 起不来 Job（recon-hourly 不存在？先 make recon-cron）"; ERR=1
    fi
  fi
fi

echo "==> 结果"
if [[ "$DRY" -eq 1 ]]; then
  echo "  dry-run：没有连集群，没有结论。"
  [[ "$ERR" -eq 0 ]] || exit 2
  exit 0
fi
if [[ "$DIFF" -eq 1 ]]; then
  echo "  不通过：有差异（上面标了有差异的几项）。"
  exit 1
elif [[ "$ERR" -eq 1 ]]; then
  echo "  不通过：有一步出了错，没有结论（上面的错误信息）。"
  exit 2
elif [[ "$PENDING" -eq 1 ]]; then
  echo "  还不能说差异是 0：有核对还没有越过 TO（上面标了还没核对到或者没有数据的几项）。等一会儿再跑同样的命令。"
  exit 3
fi
echo "  通过：三样核对到 $TO 之后都没有不一致，逐行对账 [$FROM, $TO) 差异是 0。"
