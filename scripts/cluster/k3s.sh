#!/usr/bin/env bash
# 在 MacBook 上经 ssh 给 cluster.env 里的节点装 k3s（1 个 server 加若干 agent）。
#
#   k3s.sh prepare              虚拟机刚建好或重建后运行：更新本机 known_hosts 里的主机密钥，等 cloud-init 做完
#   k3s.sh install              装 server，再逐个装 agent，最后等所有节点 Ready；可重复运行
#   k3s.sh kubeconfig           取回 kubeconfig 存到 $KUBECONFIG_OUT（server 地址和 context 名已改好）
#   k3s.sh status               各节点的 k3s 版本、服务状态，以及集群里的节点和 Pod
#   k3s.sh uninstall <节点名...> 在指定节点上卸载 k3s，必须写明节点名
#   k3s.sh etcd-backup install  在 server 上装“把 etcd 快照复制到 NAS”的定时器（设计文档 2b 的 7.2）；可重复运行
#   k3s.sh etcd-backup run      立刻拍一份快照并复制到 NAS
#   k3s.sh etcd-backup list     列出本地和 NAS 上的快照
#
# 配置模板在 deploy/cluster/k3s/，渲染结果经 ssh 的标准输入送到节点，不在本机落盘：
# agent 的 token 因此不会出现在输出、进程参数或本机文件里（只在 awk 进程的环境变量里短暂存在）。
# 幂等：配置没变且版本相同时不重启任何服务；配置变了才重启。
# 额外的 ssh 选项放在 NODE_SSH_OPTS。脚本要在 macOS 自带的 bash 3.2 上跑，不用 bash 4 的特性。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
TPL_DIR="$HERE/../../deploy/cluster/k3s"

SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10)
if [[ -n "${NODE_SSH_OPTS:-}" ]]; then
  read -r -a EXTRA_SSH_OPTS <<<"$NODE_SSH_OPTS"
  SSH_OPTS+=("${EXTRA_SSH_OPTS[@]}")
fi

usage() {
  cat >&2 <<USAGE
usage: $0 <command>
  prepare                更新 known_hosts 里节点的主机密钥，等 cloud-init 做完
  install                装 server，再装 agent，等所有节点 Ready
  kubeconfig             取回 kubeconfig 到 \$KUBECONFIG_OUT
  status                 查看各节点 k3s 状态
  uninstall <node...>    卸载指定节点上的 k3s
  etcd-backup install|run|list   etcd 快照复制到 NAS 的定时器
USAGE
  exit 2
}

# 把 "名字:地址:vCPU:内存:磁盘:MAC" 拆开。MAC 里自己带冒号，所以只切前五个。
parse_node() {
  local spec="$1"
  IFS=: read -r N_NAME N_IP N_CPU N_MEM N_DISK N_MAC <<<"$spec"
}

find_node() {
  local want="$1" spec
  for spec in "${NODES[@]}"; do
    parse_node "$spec"
    [[ "$N_NAME" == "$want" ]] && return 0
  done
  return 1
}

# 远端命令都经这一个函数发出：第一个参数是地址，其余是远端 shell 要执行的命令行。
rsh() {
  local ip="$1"
  shift
  ssh "${SSH_OPTS[@]}" "$VM_USER@$ip" "$@"
}

SERVER_IP=""
resolve_server() {
  local spec
  for spec in "${NODES[@]}"; do
    parse_node "$spec"
    if [[ "$N_NAME" == "$K3S_SERVER_NODE" ]]; then
      SERVER_IP="$N_IP"
      return 0
    fi
  done
  echo "K3S_SERVER_NODE=$K3S_SERVER_NODE is not in NODES" >&2
  exit 1
}

# 渲染模板：render <模板文件> 之后的参数是 占位符=值 对。值经环境变量传给 awk，
# 不进命令行参数（token 要走这条路），也不用 sed，免得值里有 / 或 & 出问题。
render() {
  local tpl="$1"
  shift
  local i=0 pair
  # 用 export 而不是 env VAR=值 awk：后者会把值放进 env 进程的命令行参数
  for pair in "$@"; do
    export "RENDER_K$i=${pair%%=*}" "RENDER_V$i=${pair#*=}"
    i=$((i + 1))
  done
  RENDER_N="$i" awk '
    BEGIN { n = ENVIRON["RENDER_N"] + 0 }
    {
      line = $0
      for (j = 0; j < n; j++) {
        key = ENVIRON["RENDER_K" j]; val = ENVIRON["RENDER_V" j]
        out = ""
        while ((p = index(line, key)) > 0) {
          out = out substr(line, 1, p - 1) val
          line = substr(line, p + length(key))
        }
        line = out line
      }
      print line
    }' "$tpl"
}

# 把内容（标准输入）和节点上的文件比较，有变化才覆盖。变了返回 0，没变返回 1。
# cmp 在文件不存在时也非零，按“变了”处理。目标文件 root 才读得了，所以 cmp 也走 sudo。
sync_file() {
  local ip="$1" dest="$2" content="$3"
  if printf '%s\n' "$content" | rsh "$ip" "sudo cmp -s - $dest"; then
    return 1
  fi
  printf '%s\n' "$content" | rsh "$ip" \
    "sudo mkdir -p /etc/rancher/k3s && sudo install -m 600 -o root -g root /dev/stdin $dest"
  return 0
}

# 装一个节点。参数：名字 地址 角色（server|agent）。agent 还要环境变量 K3S_JOIN_TOKEN 里放 token。
install_node() {
  local name="$1" ip="$2" role="$3"
  local svc="k3s" cfg reg changed=0 have="" want_cfg_state="unchanged"
  [[ "$role" == "agent" ]] && svc="k3s-agent"

  if [[ "$role" == "server" ]]; then
    cfg="$(render "$TPL_DIR/server.yaml" "__NODE_IP__=$ip" "__NODE_NAME__=$name")"
  else
    cfg="$(render "$TPL_DIR/agent.yaml" \
      "__SERVER_IP__=$SERVER_IP" "__TOKEN__=$K3S_JOIN_TOKEN" "__NODE_IP__=$ip")"
  fi
  reg="$(render "$TPL_DIR/registries.yaml" "__REGISTRY_ADDR__=$REGISTRY_ADDR")"

  echo "==> $name: syncing config"
  if sync_file "$ip" /etc/rancher/k3s/config.yaml "$cfg"; then changed=1; fi
  if sync_file "$ip" /etc/rancher/k3s/registries.yaml "$reg"; then changed=1; fi
  [[ $changed -eq 1 ]] && want_cfg_state="changed"

  # 已装的版本：k3s --version 第一行形如 "k3s version v1.37.2+k3s1 (abcdef)"
  local ver_out=""
  ver_out="$(rsh "$ip" 'test -x /usr/local/bin/k3s && /usr/local/bin/k3s --version || true')"
  have="$(printf '%s\n' "$ver_out" | awk 'NR==1 { print $3 }')"

  if [[ "$have" != "$K3S_VERSION" ]]; then
    echo "==> $name: installing k3s $K3S_VERSION as $role (found: ${have:-none})"
    # 版本号在本地展开；安装脚本读 /etc/rancher/k3s/config.yaml，命令行不带其他参数
    rsh "$ip" "curl -sfL https://get.k3s.io | sudo INSTALL_K3S_VERSION='$K3S_VERSION' sh -s - $role"
  elif [[ $changed -eq 1 ]]; then
    echo "==> $name: config $want_cfg_state, restarting $svc"
    rsh "$ip" "sudo systemctl restart $svc"
  else
    echo "==> $name: unchanged"
  fi
}

wait_server_ready() {
  echo "==> waiting for the API server on $K3S_SERVER_NODE (up to 120s)"
  local i out=""
  for ((i = 0; i < 60; i++)); do
    if out="$(rsh "$SERVER_IP" 'sudo k3s kubectl get --raw /readyz' 2>/dev/null)"; then
      if [[ "$out" == "ok" ]]; then
        echo "==> API server ready"
        return 0
      fi
    fi
    sleep 2
  done
  echo "API server not ready after 120s" >&2
  return 1
}

# 重建的虚拟机主机密钥是新的，known_hosts 里还是旧的，ssh 会拒绝连接。这里的做法是首次连接时信任
# （和第一次手工 ssh 时回答 yes 一样）：密钥变了但 known_hosts 里的还能用就什么都不动；连不上才删掉
# 旧的、接受新的。局域网里的实验集群可以接受这一点；要严格的话应当经宿主机的控制台取公钥。
# 然后等 cloud-init 做完：它在第一次开机时装 nfs-common，没装完就挂 NFS 卷会失败。
cmd_prepare() {
  local spec
  for spec in "${NODES[@]}"; do
    parse_node "$spec"
    if ! rsh "$N_IP" true 2>/dev/null; then
      echo "==> $N_NAME: refreshing host key in known_hosts"
      ssh-keygen -R "$N_IP" >/dev/null 2>&1 || true
      ssh "${SSH_OPTS[@]}" -o StrictHostKeyChecking=accept-new "$VM_USER@$N_IP" true
    fi
    echo "==> $N_NAME: waiting for cloud-init"
    rsh "$N_IP" 'cloud-init status --wait >/dev/null 2>&1 || cloud-init status --long'
  done
}

cmd_install() {
  resolve_server
  local spec
  for spec in "${NODES[@]}"; do
    parse_node "$spec"
    if [[ "$N_NAME" == "$K3S_SERVER_NODE" ]]; then
      install_node "$N_NAME" "$N_IP" server
    fi
  done
  wait_server_ready

  # token 只放在变量里，不打印、不写盘
  local token
  token="$(rsh "$SERVER_IP" 'sudo cat /var/lib/rancher/k3s/server/node-token')"
  [[ -n "$token" ]] || {
    echo "empty node token from $K3S_SERVER_NODE" >&2
    exit 1
  }
  for spec in "${NODES[@]}"; do
    parse_node "$spec"
    if [[ "$N_NAME" != "$K3S_SERVER_NODE" ]]; then
      K3S_JOIN_TOKEN="$token" install_node "$N_NAME" "$N_IP" agent
    fi
  done
  token=""

  echo "==> waiting for all nodes Ready"
  rsh "$SERVER_IP" 'sudo k3s kubectl wait --for=condition=Ready node --all --timeout=180s'
  rsh "$SERVER_IP" 'sudo k3s kubectl get nodes -o wide'
}

cmd_kubeconfig() {
  resolve_server
  echo "==> fetching kubeconfig from $K3S_SERVER_NODE"
  mkdir -p "$(dirname "$KUBECONFIG_OUT")"
  local raw
  raw="$(rsh "$SERVER_IP" 'sudo cat /etc/rancher/k3s/k3s.yaml')"
  # umask 077：文件从创建起就只有自己能读。sed 只匹配整行，k3s 生成的名字全是 default：
  # "- name: default"（clusters、users 的条目）、"  name: default"（context）、
  # "cluster: default"、"user: default"（context 里的引用）、"current-context: default"
  (
    umask 077
    printf '%s\n' "$raw" | sed -E \
      -e "s#^( *)server: https://127\\.0\\.0\\.1:#\\1server: https://$SERVER_IP:#" \
      -e "s#^( *(- )?(name|cluster|user)): default\$#\\1: $KUBE_CONTEXT#" \
      -e "s#^current-context: default\$#current-context: $KUBE_CONTEXT#" \
      >"$KUBECONFIG_OUT"
  )
  chmod 600 "$KUBECONFIG_OUT"
  raw=""
  echo "==> wrote $KUBECONFIG_OUT (context $KUBE_CONTEXT)"
  kubectl --kubeconfig "$KUBECONFIG_OUT" get nodes
}

cmd_status() {
  resolve_server
  local spec svc ver active
  for spec in "${NODES[@]}"; do
    parse_node "$spec"
    svc="k3s"
    [[ "$N_NAME" != "$K3S_SERVER_NODE" ]] && svc="k3s-agent"
    ver="$(rsh "$N_IP" 'test -x /usr/local/bin/k3s && /usr/local/bin/k3s --version | head -n 1 || echo "not installed"' || echo "unreachable")"
    active="$(rsh "$N_IP" "systemctl is-active $svc || true" || echo "unreachable")"
    echo "==> $N_NAME ($N_IP): $ver; $svc: $active"
  done
  rsh "$SERVER_IP" 'sudo k3s kubectl get nodes -o wide'
  rsh "$SERVER_IP" 'sudo k3s kubectl get pods -A'
}

cmd_uninstall() {
  [[ $# -ge 1 ]] || {
    echo "uninstall needs at least one node name" >&2
    usage
  }
  local name
  # 先全部校验，免得卸了一半才发现名字写错
  for name in "$@"; do
    find_node "$name" || {
      echo "unknown node: $name" >&2
      exit 2
    }
  done
  for name in "$@"; do
    find_node "$name"
    echo "==> $N_NAME: uninstalling k3s"
    # 两个脚本哪个存在运行哪个，都没有说明没装
    rsh "$N_IP" 'if [ -x /usr/local/bin/k3s-uninstall.sh ]; then sudo /usr/local/bin/k3s-uninstall.sh;
      elif [ -x /usr/local/bin/k3s-agent-uninstall.sh ]; then sudo /usr/local/bin/k3s-agent-uninstall.sh;
      else echo "k3s is not installed"; fi'
  done
  echo "==> note: the node objects stay in the cluster; run 'kubectl delete node <name>' yourself if needed"
}

# etcd 的快照复制到 NAS（设计文档 2b 的 7.2）。快照本身由 k3s 自带的定时任务拍（默认每 12 小时、本地留 5 份，
# 没有在 config.yaml 里改），这里装的是 server 节点上的一个 systemd 定时器加一个脚本，脚本里有详细说明。
# 不放进集群里做成 CronJob：它要备份的正是集群的控制面，控制面坏了的时候它也得能跑。
ETCD_BACKUP_KEEP=14 # NAS 上保留的份数，一天两份，约一周

# 把内容（标准输入）装到节点上的 <dest>，权限 <mode>，有变化才覆盖。变了返回 0，没变返回 1。
install_file() {
  local ip="$1" dest="$2" mode="$3" content="$4"
  if printf '%s\n' "$content" | rsh "$ip" "sudo cmp -s - $dest"; then
    return 1
  fi
  printf '%s\n' "$content" | rsh "$ip" "sudo install -m $mode -o root -g root /dev/stdin $dest"
  return 0
}

cmd_etcd_backup() {
  resolve_server
  local sub="${1:-}" script changed=0
  case "$sub" in
    install)
      echo "==> $K3S_SERVER_NODE: installing etcd-snapshot-sync (NAS $NAS_IP:$NAS_EXPORT/backup/etcd, keep $ETCD_BACKUP_KEEP)"
      script="$(render "$TPL_DIR/etcd-snapshot-sync.sh" \
        "__NAS_IP__=$NAS_IP" "__NAS_EXPORT__=$NAS_EXPORT" "__KEEP__=$ETCD_BACKUP_KEEP")"
      if install_file "$SERVER_IP" /usr/local/sbin/etcd-snapshot-sync 755 "$script"; then changed=1; fi
      if install_file "$SERVER_IP" /etc/systemd/system/etcd-snapshot-sync.service 644 \
        "$(cat "$TPL_DIR/etcd-snapshot-sync.service")"; then changed=1; fi
      if install_file "$SERVER_IP" /etc/systemd/system/etcd-snapshot-sync.timer 644 \
        "$(cat "$TPL_DIR/etcd-snapshot-sync.timer")"; then changed=1; fi
      if [[ $changed -eq 1 ]]; then
        rsh "$SERVER_IP" 'sudo systemctl daemon-reload'
      else
        echo "==> $K3S_SERVER_NODE: unchanged"
      fi
      rsh "$SERVER_IP" 'sudo systemctl enable --now etcd-snapshot-sync.timer'
      rsh "$SERVER_IP" 'systemctl list-timers etcd-snapshot-sync.timer --no-pager'
      ;;
    run)
      # 按需快照的名字是 on-demand-<节点名>-<时间>。按需快照不受“本地留 5 份”的限制，NAS 上和定时的一起按份数清理。
      echo "==> $K3S_SERVER_NODE: taking an on-demand snapshot"
      rsh "$SERVER_IP" 'sudo k3s etcd-snapshot save 2>&1 | grep -v "Unknown flag"'
      echo "==> $K3S_SERVER_NODE: copying snapshots to the NAS"
      # 经 systemd 运行而不是直接调脚本：和定时触发走同一条路，输出进 journal。
      rsh "$SERVER_IP" 'sudo systemctl start etcd-snapshot-sync.service || { sudo journalctl -u etcd-snapshot-sync -n 20 --no-pager; exit 1; }'
      rsh "$SERVER_IP" 'sudo journalctl -u etcd-snapshot-sync -n 8 --no-pager -o cat'
      ;;
    list)
      echo "==> $K3S_SERVER_NODE: local snapshots"
      rsh "$SERVER_IP" 'sudo ls -l --time-style=long-iso /var/lib/rancher/k3s/server/db/snapshots'
      echo "==> NAS ($NAS_IP:$NAS_EXPORT/backup/etcd)"
      rsh "$SERVER_IP" "m=\$(sudo mktemp -d); sudo mount -t nfs -o ro,nfsvers=4.1,soft,timeo=100,retrans=2 $NAS_IP:$NAS_EXPORT/backup/etcd \$m \
        && sudo ls -l --time-style=long-iso \$m \$m/snapshots; sudo umount \$m; sudo rmdir \$m"
      ;;
    *)
      echo "usage: $0 etcd-backup install | run | list" >&2
      exit 2
      ;;
  esac
}

[[ $# -ge 1 ]] || usage
cmd="$1"
shift
case "$cmd" in
  prepare) cmd_prepare ;;
  install) cmd_install ;;
  kubeconfig) cmd_kubeconfig ;;
  status) cmd_status ;;
  uninstall) cmd_uninstall "$@" ;;
  etcd-backup) cmd_etcd_backup "$@" ;;
  *) usage ;;
esac
