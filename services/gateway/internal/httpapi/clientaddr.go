package httpapi

import (
	"fmt"
	"net/http"
	"net/netip"
	"slices"
	"strings"
)

// 客户端地址从哪来（设计文档 1d 4.9）。
//
// 网关直接面向客户端时，连接的远端地址就是客户端。前面有反向代理时，远端地址是代理的，真实地址在
// X-Forwarded-For 里；但这个头客户端可以随便写，只有请求来自受信任的代理时才能信。
//
// 不用 gin 的 c.ClientIP()：它默认信任所有代理，任何人写一个 X-Forwarded-For 就能换一个地址，
// 按 IP 的限流形同虚设，还会把进程内的桶表撑满。这里的规则自己实现、自己测。

const forwardedForHeader = "X-Forwarded-For"

// ParseTrustedProxies 解析逗号分隔的 CIDR 或单个 IP（TRUSTED_PROXIES）。空字符串表示不信任任何代理。
// 单个 IP 当作只含它自己的前缀。IPv4 映射的 IPv6（::ffff:1.2.3.4）按 IPv4 处理，和 clientAddr 的比较方式一致。
func ParseTrustedProxies(s string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for item := range strings.SplitSeq(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if strings.Contains(item, "/") {
			p, err := netip.ParsePrefix(item)
			if err != nil {
				return nil, fmt.Errorf("trusted proxy %q: %w", item, err)
			}
			if p.Addr().Is4In6() {
				// 被比较的地址都先 Unmap 成了 IPv4，映射形式的前缀永远匹配不上，写错了不如直接报出来。
				return nil, fmt.Errorf("trusted proxy %q: write IPv4-mapped prefixes in IPv4 form", item)
			}
			out = append(out, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(item)
		if err != nil {
			return nil, fmt.Errorf("trusted proxy %q: %w", item, err)
		}
		a = a.Unmap().WithZone("")
		out = append(out, netip.PrefixFrom(a, a.BitLen()))
	}
	return out, nil
}

// clientKey 返回按 IP 限流用的键。
func clientKey(r *http.Request, trusted []netip.Prefix) string {
	a, ok := clientAddr(r.RemoteAddr, r.Header.Values(forwardedForHeader), trusted)
	if !ok {
		// 远端地址解析不了只会出现在非 TCP 的监听上（网关只监听 TCP）。用原样的字符串当键：
		// 仍然受限，而不是因为认不出来就不限。
		return r.RemoteAddr
	}
	return ipKey(a)
}

// clientAddr 按受信任的代理列表确定客户端地址。
//
//   - 远端地址不是受信任的代理：客户端就是远端地址，X-Forwarded-For 一律忽略（它是客户端自己写的）。
//   - 远端地址是受信任的代理：从 X-Forwarded-For 的最右边往左找，第一个不是受信任代理的地址就是客户端。
//     每一级代理把它看到的对端地址追加在最右边，所以从右往左，只要还是受信任的代理，它写的下一个地址就可信；
//     第一个不受信任的地址是“受信任的链条之外”的那一跳，它左边的内容都可能是伪造的，不能再往左看。
//     （取最左边的地址是常见的错误：那正是客户端可以随便写的位置。）
//   - 一路都是受信任的代理、头不存在、或者在找到之前遇到解析不了的值：用远端地址。解析不了说明链条上
//     有不按规矩写头的一环，它左边的内容不可信，退回到唯一确定的远端地址。
//
// 多个 X-Forwarded-For 头按出现的顺序拼起来，等价于一个逗号连接的头（RFC 9110 5.3）。
// 返回的地址去掉了 IPv6 的 zone，IPv4 映射的 IPv6 还原成 IPv4，同一个客户端只有一种写法。
// ok 为 false 表示远端地址本身解析不了。
func clientAddr(remoteAddr string, forwardedFor []string, trusted []netip.Prefix) (addr netip.Addr, ok bool) {
	remote, ok := parseHostAddr(remoteAddr)
	if !ok {
		return netip.Addr{}, false
	}
	if !isTrusted(remote, trusted) {
		return remote, true
	}
	// 从最后一个头的最右边开始往左走。逐段切而不是 strings.Split，避免每个请求分配一个切片。
	for _, rest := range slices.Backward(forwardedFor) {
		for rest != "" {
			var entry string
			if j := strings.LastIndexByte(rest, ','); j >= 0 {
				entry, rest = rest[j+1:], rest[:j]
			} else {
				entry, rest = rest, ""
			}
			a, ok := parseHostAddr(strings.TrimSpace(entry))
			if !ok {
				return remote, true
			}
			if !isTrusted(a, trusted) {
				return a, true
			}
		}
	}
	return remote, true
}

// parseHostAddr 解析 "地址" 或 "地址:端口"（IPv6 带端口时写成 "[地址]:端口"，也接受不带端口的 "[地址]"）。
// 结果去掉 zone、IPv4 映射的 IPv6 还原成 IPv4。
func parseHostAddr(s string) (netip.Addr, bool) {
	var a netip.Addr
	if ap, err := netip.ParseAddrPort(s); err == nil {
		a = ap.Addr()
	} else {
		if len(s) > 2 && s[0] == '[' && s[len(s)-1] == ']' {
			s = s[1 : len(s)-1]
		}
		if a, err = netip.ParseAddr(s); err != nil {
			return netip.Addr{}, false
		}
	}
	return a.Unmap().WithZone(""), true
}

func isTrusted(a netip.Addr, trusted []netip.Prefix) bool {
	return slices.ContainsFunc(trusted, func(p netip.Prefix) bool { return p.Contains(a) })
}

// ipKey 把客户端地址变成限流的键：IPv4 按单个地址，IPv6 按所在的 /64。
//
// IPv6 取 /64：运营商通常给一个用户（一条宽带、一部手机）分配一整个 /64，主机在里面自己选地址，
// 还会定期换临时地址（RFC 8981）。按单个地址限，同一个用户换个地址就是一个新的满桶，等于没限，
// 还会把进程内的桶表撑满。/64 是 IPv6 里“一个用户”最接近的单位。代价是同一个 /64 后面的多台机器
// （比如一个家庭）共用一个桶，按 IP 的限额本来就给得宽，可以接受。
func ipKey(a netip.Addr) string {
	if a.Is4() {
		return a.String()
	}
	p, err := a.Prefix(64)
	if err != nil {
		// a 是 IPv6（BitLen 128），64 一定在范围内；走到这里是代码缺陷，退回按单个地址。
		return a.String()
	}
	return p.String()
}
