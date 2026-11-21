#!/usr/bin/env bash
# 准备集群的宿主机（Ubuntu 3900X）：装 KVM 和 libvirt，把有线网卡接到网桥 br0 上。
# 在宿主机上以 root 运行，可重复运行。通常从 MacBook 经 scripts/cluster/on-host.sh 调用。
#
# 为什么用网桥：虚拟机的网卡接在 br0 上，和宿主机的有线网卡在同一个二层网络里，
# 虚拟机直接拿局域网的地址，MacBook、Mac mini、NAS 不经转发就能访问节点和 MetalLB 的地址。
# libvirt 默认的 NAT 网络（virbr0）做不到这一点。Wi-Fi 网卡不能接进网桥（接入点只认关联过的
# 那一个 MAC 地址），所以宿主机必须插网线。
#
# 改网络有把自己关在外面的风险：Wi-Fi 的配置不动，留作管理通道；本脚本从 Wi-Fi 的地址连进来执行。
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "run as root (sudo)" >&2
  exit 2
fi

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"

RUN_USER="${SUDO_USER:-xinyu}"

echo "==> packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
# qemu-system-x86：虚拟机本身；libvirt-daemon-system：管理虚拟机的守护进程；virtinst：virt-install；
# cloud-image-utils：cloud-localds，生成 cloud-init 的种子盘；nfs-common：挂 NAS 的 NFS。
apt-get install -y -qq qemu-system-x86 qemu-utils libvirt-daemon-system libvirt-clients virtinst \
  cloud-image-utils nfs-common >/dev/null

for g in libvirt kvm; do
  usermod -aG "$g" "$RUN_USER"
done
systemctl enable --now libvirtd >/dev/null

# 网线被拔掉时 br0 没有载波，但它是静态地址，地址和路由都还在（标着 linkdown）。内核默认照样用
# 这些路由，而且它们比 Wi-Fi 的路由优先：发往局域网的包全进了断掉的网桥，宿主机从 Wi-Fi 也连不上。
# 打开这个开关后内核跳过 linkdown 的路由，Wi-Fi 才真的是备用通道。（2026-11-15 实际发生过。）
printf '%s\n' \
  'net.ipv4.conf.all.ignore_routes_with_linkdown = 1' \
  'net.ipv4.conf.default.ignore_routes_with_linkdown = 1' \
  >/etc/sysctl.d/90-oppenheimer-linkdown.conf
sysctl -q --system
for dev in /proc/sys/net/ipv4/conf/*/ignore_routes_with_linkdown; do
  echo 1 >"$dev"
done

# 虚拟机的帧都经过 br0。现在它们不过宿主机的 iptables，因为 br_netfilter 没有加载；一旦有东西把它
# 加载起来，桥上的帧就要过 FORWARD 链，而 Docker 把这条链的默认策略设成了 DROP：三台虚拟机同时断网。
# 这三个开关在模块加载之前不存在，所以只写 sysctl.d 不够，要用 udev 规则在模块加载的那一刻设成 0
# （libvirt 文档的做法）。k3s 在虚拟机里面加载 br_netfilter 是它自己需要的，和这里无关。
BRIDGE_NF_SYSCTLS="net.bridge.bridge-nf-call-iptables=0 net.bridge.bridge-nf-call-ip6tables=0 net.bridge.bridge-nf-call-arptables=0"
printf '%s\n' \
  "ACTION==\"add\", SUBSYSTEM==\"module\", KERNEL==\"br_netfilter\", RUN+=\"/usr/sbin/sysctl -w $BRIDGE_NF_SYSCTLS\"" \
  >/etc/udev/rules.d/99-oppenheimer-bridge-nf.rules
udevadm control --reload
if [[ -d /proc/sys/net/bridge ]]; then
  # shellcheck disable=SC2086  # 有意按空格拆成三个参数
  sysctl -q -w $BRIDGE_NF_SYSCTLS
fi

echo "==> bridge $HOST_BRIDGE on $HOST_NIC ($HOST_IP/$LAN_PREFIX)"
if [[ ! -e /sys/class/net/$HOST_NIC ]]; then
  echo "NIC $HOST_NIC not found" >&2
  exit 1
fi
# 新装的网卡还没有任何 netplan 配置，接口是关着的：这时读 carrier 会报错，不代表没插网线。
# 先把接口打开，再等自协商完成（2.5G/5G 的网卡刚起来时链路会先起落一次）。
ip link set "$HOST_NIC" up
for _ in $(seq 15); do
  [[ "$(cat "/sys/class/net/$HOST_NIC/carrier" 2>/dev/null || echo 0)" == "1" ]] && break
  sleep 1
done
if [[ "$(cat "/sys/class/net/$HOST_NIC/carrier" 2>/dev/null || echo 0)" != "1" ]]; then
  echo "NIC $HOST_NIC has no link; plug in the cable first" >&2
  exit 1
fi
NIC_MAC="$(cat "/sys/class/net/$HOST_NIC/address")"

# 文件名排在安装器写的 00-*.yaml 后面，同名的键以这里为准（网卡不再自己要地址）。
# 网桥固定用网卡的 MAC：不固定的话每次重建网桥 MAC 会变，邻居的 ARP 缓存要等过期。
# 关掉 STP：只有一个上联口，不会成环；开着的话每次起来要等 30 秒才转发。
#
# 板载网口是兜底的管理通道，和网桥在同一个网段。同一个网段有两个口时，内核回包只看目的地址：
# 从板载口进来的连接，回包会从网桥出去。网桥的上联断了（网卡改名、网线拔了但虚拟机的端口让它
# 保持有载波）时，这些回包就丢了，板载口插着线也登录不上（2026-11-17 实际发生过）。所以：
#   - 地址写成 /32：主路由表里不出现“这个网段走板载口”的路由，宿主机自己发起的流量仍然只走网桥，
#     不会因为插了板载口就去不了虚拟机；
#   - 另建一张路由表，里面这个网段和默认路由都走板载口；
#   - 一条策略路由：源地址是板载口地址的包查这张表。回包的源地址就是它，所以从哪个口进来就从哪个口回去。
# 地址是静态的，不依赖 DHCP：兜底的通道不该再依赖别的设备。安装器给它写的 dhcp4 在这里关掉。
#
# Wi-Fi 有同样的问题，用同样的策略路由解决，但地址仍然由 DHCP 给：主路由表里它那条优先级低的路由要留着，
# 网线拔掉（网桥没有载波）时宿主机自己发起的流量靠它出去。所以策略路由按 R2S 绑定的那个地址写；
# 万一 DHCP 给了别的地址，规则不命中，退回到没有这段配置时的行为。
# 这里只给安装器已经配好的 Wi-Fi 接口补几个键（netplan 按接口名合并各个文件）。接口名对不上时不能写：
# 那等于定义一个没有接入点的 Wi-Fi，netplan 会报错，整份网络配置都不生效。
NETPLAN=/etc/netplan/10-cluster-bridge.yaml
WIFI_BLOCK=""
if [[ "$(netplan get "wifis.$HOST_WIFI_NIC.access-points" 2>/dev/null)" == "null" ]]; then
  echo "    wifi $HOST_WIFI_NIC is not configured in netplan; skip its policy route" >&2
else
  WIFI_BLOCK="  wifis:
    $HOST_WIFI_NIC:
      routes:
        - to: $LAN_NET/$LAN_PREFIX
          scope: link
          table: $HOST_WIFI_TABLE
        - to: default
          via: $LAN_GATEWAY
          on-link: true
          table: $HOST_WIFI_TABLE
      routing-policy:
        - from: $HOST_WIFI_IP
          table: $HOST_WIFI_TABLE
          priority: 100"
fi
TMP="$(mktemp)"
cat >"$TMP" <<EOF
# 由 Oppenheimer 仓库的 scripts/cluster/host-setup.sh 生成，不要手工修改。
network:
  version: 2
  ethernets:
    $HOST_NIC:
      dhcp4: false
      dhcp6: false
      optional: true
    $HOST_FALLBACK_NIC:
      optional: true
      dhcp4: false
      addresses: [$HOST_FALLBACK_IP/32]
      routes:
        - to: $LAN_NET/$LAN_PREFIX
          scope: link
          table: $HOST_FALLBACK_TABLE
        - to: default
          via: $LAN_GATEWAY
          on-link: true
          table: $HOST_FALLBACK_TABLE
      routing-policy:
        - from: $HOST_FALLBACK_IP
          table: $HOST_FALLBACK_TABLE
          priority: 100
$WIFI_BLOCK
  bridges:
    $HOST_BRIDGE:
      # optional：没插网线时开机不等它。不写的话 systemd-networkd-wait-online 要等满 2 分钟才超时，
      # 这期间 ssh 拒绝普通用户登录（pam_nologin）。
      optional: true
      interfaces: [$HOST_NIC]
      macaddress: "$NIC_MAC"
      addresses: [$HOST_IP/$LAN_PREFIX]
      routes:
        - to: default
          via: $LAN_GATEWAY
          metric: 100
      nameservers:
        addresses: [$LAN_DNS]
      parameters:
        stp: false
        forward-delay: 0
      dhcp4: false
EOF
if [[ -f $NETPLAN ]] && cmp -s "$TMP" "$NETPLAN"; then
  rm -f "$TMP"
  echo "    netplan unchanged"
else
  # 新配置通不过检查时把旧文件放回去：留一份坏的配置在那里，下次重启网络就起不来了。
  OLD="$(mktemp)"
  [[ -f $NETPLAN ]] && cp -p "$NETPLAN" "$OLD"
  install -m 600 "$TMP" "$NETPLAN"
  rm -f "$TMP"
  if ! netplan generate; then
    if [[ -s $OLD ]]; then install -m 600 "$OLD" "$NETPLAN"; else rm -f "$NETPLAN"; fi
    rm -f "$OLD"
    echo "netplan rejected the new config; restored the previous one" >&2
    exit 1
  fi
  rm -f "$OLD"
  netplan apply
  echo "    netplan applied"
fi

echo "==> wait for $HOST_BRIDGE"
for _ in $(seq 30); do
  if ip -4 addr show "$HOST_BRIDGE" 2>/dev/null | grep -q "$HOST_IP/" &&
    ping -c1 -W1 -I "$HOST_BRIDGE" "$LAN_GATEWAY" >/dev/null 2>&1; then
    ok=1
    break
  fi
  sleep 1
done
if [[ "${ok:-0}" != 1 ]]; then
  echo "$HOST_BRIDGE did not come up with $HOST_IP or cannot reach $LAN_GATEWAY" >&2
  ip -br addr >&2
  exit 1
fi

# libvirt 里登记一个指向 br0 的网络，建虚拟机时写 --network network=host-bridge。
# 默认的 NAT 网络（default）用不上，停掉并取消自启，免得多一个 virbr0 和一套 iptables 规则。
if ! virsh net-info host-bridge >/dev/null 2>&1; then
  XML="$(mktemp)"
  cat >"$XML" <<EOF
<network>
  <name>host-bridge</name>
  <forward mode="bridge"/>
  <bridge name="$HOST_BRIDGE"/>
</network>
EOF
  virsh net-define "$XML" >/dev/null
  rm -f "$XML"
fi
virsh net-autostart host-bridge >/dev/null
# 不写成 virsh ... | grep -q：grep 匹配到就退出，virsh 收到 SIGPIPE，在 pipefail 下整条管道算失败。
net_active() {
  local info
  info="$(virsh net-info "$1" 2>/dev/null)" || return 1
  [[ "$info" =~ Active:[[:space:]]+yes ]]
}
net_active host-bridge || virsh net-start host-bridge >/dev/null
if virsh net-info default >/dev/null 2>&1; then
  virsh net-autostart default --disable >/dev/null 2>&1 || true
  if net_active default; then
    virsh net-destroy default >/dev/null
  fi
fi

# 宿主机关机时 libvirt-guests 让虚拟机自己关机，等不到就强制断电。这个时限是所有虚拟机合起来的
# （它们并行关），Ubuntu 的包默认 120 秒；2026-11-20 那次重启 k3s-server 用了 115 秒，只差 5 秒。
# 被断电的后果：放松了刷盘的 MySQL 从库要重建（ADR-0032），etcd 可能要从快照恢复。
# 不要重启这个服务让配置生效：它的 stop 就是把虚拟机全部关掉。文件在 stop 的时候才读，改了就算数。
echo "==> libvirt-guests shutdown timeout"
sed -i -E 's/^#?SHUTDOWN_TIMEOUT=.*/SHUTDOWN_TIMEOUT=300/' /etc/default/libvirt-guests
grep -q '^SHUTDOWN_TIMEOUT=300$' /etc/default/libvirt-guests ||
  echo 'SHUTDOWN_TIMEOUT=300' >>/etc/default/libvirt-guests

echo "==> done"
ip -br addr show "$HOST_BRIDGE"
virsh net-list --all
[[ -r /dev/kvm ]] && echo "kvm: ok"
