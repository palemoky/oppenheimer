#!/usr/bin/env bash
# 在宿主机上创建、查看、删除 k3s 的虚拟机。以 root 在宿主机上运行，通常经 on-host.sh 调用。
#
#   vm.sh create [名字...]    创建 cluster.env 里登记的虚拟机（不给名字就是全部）；已存在的跳过
#   vm.sh list                列出虚拟机和它们的状态
#   vm.sh destroy <名字...>   关机并删除虚拟机和它的磁盘。不可恢复，必须写明名字
#
# 做法：每台虚拟机的系统盘由 Ubuntu 的 cloud image 复制而来并放大到指定大小；第一次开机时
# cloud-init 读一张小的种子盘，按里面的内容设主机名、静态地址、登录用的 ssh 公钥。
# 所以虚拟机从创建到能登录不需要人工安装系统，重建一台和新建一台是同一条命令。
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "run as root (sudo)" >&2
  exit 2
fi
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"

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

vm_exists() { virsh dominfo "$1" >/dev/null 2>&1; }

create_one() {
  local name="$N_NAME" disk="$VM_IMAGE_DIR/$N_NAME.qcow2" seed="$VM_IMAGE_DIR/$N_NAME-seed.img"
  if vm_exists "$name"; then
    echo "==> $name exists, skipped"
    return 0
  fi
  if [[ -e "$disk" ]]; then
    echo "$disk exists but the VM does not; remove it first (vm.sh destroy $name)" >&2
    return 1
  fi
  if ping -c1 -W1 "$N_IP" >/dev/null 2>&1; then
    echo "$N_IP already answers ping; refusing to create $name on top of it" >&2
    return 1
  fi
  local pubkey
  pubkey="$(cat "${SSH_PUBKEY_FILE:?SSH_PUBKEY_FILE not set (run via on-host.sh)}")"

  echo "==> $name: $N_CPU vCPU, $N_MEM MiB, $N_DISK GB, $N_IP"
  # 复制成独立的 qcow2（不用 backing file：base 镜像被动了，所有虚拟机一起坏），再把虚拟大小放大。
  # qcow2 是按需增长的，150 GB 的盘刚建好只占不到 1 GB。
  qemu-img convert -f qcow2 -O qcow2 "$VM_BASE_IMAGE" "$disk"
  qemu-img resize -q "$disk" "${N_DISK}G"

  local tmp
  tmp="$(mktemp -d)"
  cat >"$tmp/meta-data" <<EOF
instance-id: $name-$(date +%s)
local-hostname: $name
EOF
  # 节点之间互相按名字能找到：把所有节点写进 /etc/hosts。
  local hosts="" spec
  for spec in "${NODES[@]}"; do
    hosts+="      $(cut -d: -f2 <<<"$spec") $(cut -d: -f1 <<<"$spec")"$'\n'
  done
  # kernel.apparmor_restrict_unprivileged_unconfined=0：Ubuntu 26.04 默认是 1。开着的时候，k3s 的 runc 给容器
  # 切换 AppArmor 配置时只能叠加不能替换，容器进程的标签变成 cri-containerd.apparmor.d//&unconfined，而
  # containerd 的默认配置只允许向标签恰好是 cri-containerd.apparmor.d 的进程发信号，于是容器里的进程
  # 连给自己发信号都被拒绝：官方的 MySQL 镜像起不来（gosu 空转、mysqld 初始化完不退出），所有 Go 程序的
  # 抢占信号（SIGURG）被丢掉。2026-11-15 在一个节点上验证过：设成 0 之后标签恢复正常，拒绝记录消失
  # （设计文档 2b 第 10 节的发现 1）。容器仍然受 cri-containerd.apparmor.d 约束，放宽的只是这一项系统级的限制。
  # 文件在第一次开机时才写入，那时 systemd 已经加载过 sysctl，所以 runcmd 里再加载一次；之后每次开机自动生效。
  cat >"$tmp/user-data" <<EOF
#cloud-config
hostname: $name
manage_etc_hosts: false
users:
  - name: $VM_USER
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    lock_passwd: true
    ssh_authorized_keys:
      - $pubkey
ssh_pwauth: false
package_update: true
packages:
  - qemu-guest-agent
  - nfs-common
write_files:
  - path: /etc/hosts
    append: true
    content: |
$hosts
  - path: /etc/sysctl.d/90-oppenheimer-apparmor.conf
    content: |
      kernel.apparmor_restrict_unprivileged_unconfined = 0
runcmd:
  - systemctl enable --now qemu-guest-agent
  - sysctl --system
EOF
  # 按 MAC 认网卡，不依赖网卡在虚拟机里叫什么名字。
  # 不接受 IPv6 的路由通告：通告里带着一个 IPv6 的 DNS 服务器，它不经过 R2S 的代理，返回的是被污染的
  # 真实地址；systemd-resolved 轮到它时节点和 Pod 就连不上镜像源（2026-11-15 实际发生，设计文档 2a 的 3.4）。
  # 集群只用 IPv4，关掉通告之后节点没有全局的 IPv6 地址，DNS 上游只剩 R2S 的 IPv4 地址。
  cat >"$tmp/network-config" <<EOF
version: 2
ethernets:
  lan:
    match:
      macaddress: "$N_MAC"
    set-name: eth0
    dhcp6: false
    accept-ra: false
    addresses: [$N_IP/$LAN_PREFIX]
    routes:
      - to: default
        via: $LAN_GATEWAY
    nameservers:
      addresses: [$LAN_DNS]
EOF
  cloud-localds --network-config="$tmp/network-config" "$seed" "$tmp/user-data" "$tmp/meta-data"
  rm -rf "$tmp"

  # --cpu host-passthrough：把宿主机 CPU 的全部特性给虚拟机，不模拟成通用型号。
  # 只有一台宿主机、不做热迁移，没有理由屏蔽特性。
  virt-install --name "$name" \
    --vcpus "$N_CPU" --memory "$N_MEM" --cpu host-passthrough \
    --disk "path=$disk,bus=virtio,cache=none,discard=unmap" \
    --disk "path=$seed,device=cdrom" \
    --network "network=$VM_NETWORK,model=virtio,mac=$N_MAC" \
    --osinfo "detect=on,name=linux2022" \
    --import --graphics none --console pty,target_type=serial \
    --noautoconsole --autostart >/dev/null
}

wait_ssh() {
  local name="$N_NAME" ip="$N_IP" _
  printf '==> wait for ssh on %s (%s) ' "$name" "$ip"
  for _ in $(seq 90); do
    if timeout 2 bash -c "</dev/tcp/$ip/22" 2>/dev/null; then
      echo ok
      return 0
    fi
    printf .
    sleep 2
  done
  echo " timed out" >&2
  return 1
}

cmd="${1:-}"
shift || true
case "$cmd" in
create)
  names=("$@")
  if [[ ${#names[@]} -eq 0 ]]; then
    for spec in "${NODES[@]}"; do names+=("${spec%%:*}"); done
  fi
  for n in "${names[@]}"; do
    find_node "$n" || {
      echo "unknown node $n (not in cluster.env)" >&2
      exit 2
    }
    create_one
  done
  for n in "${names[@]}"; do
    find_node "$n"
    wait_ssh
  done
  virsh list --all
  ;;
list)
  virsh list --all
  ls -lh "$VM_IMAGE_DIR"/*.qcow2 2>/dev/null || true
  ;;
destroy)
  if [[ $# -eq 0 ]]; then
    echo "destroy needs explicit VM names" >&2
    exit 2
  fi
  for n in "$@"; do
    find_node "$n" || {
      echo "unknown node $n (not in cluster.env)" >&2
      exit 2
    }
    if vm_exists "$n"; then
      virsh destroy "$n" >/dev/null 2>&1 || true
      virsh undefine "$n" --nvram >/dev/null 2>&1 || virsh undefine "$n" >/dev/null
    fi
    rm -f "$VM_IMAGE_DIR/$n.qcow2" "$VM_IMAGE_DIR/$n-seed.img"
    echo "==> $n destroyed"
  done
  ;;
*)
  echo "usage: $0 create [name...] | list | destroy <name...>" >&2
  exit 2
  ;;
esac
