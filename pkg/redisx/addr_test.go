package redisx

import (
	"slices"
	"testing"
)

func TestParseAddr(t *testing.T) {
	for _, tc := range []struct {
		in     string
		mode   Mode
		master string
		addrs  []string
	}{
		{"127.0.0.1:6379", Single, "", []string{"127.0.0.1:6379"}},
		{"redis-gate.redis.svc:6379", Single, "", []string{"redis-gate.redis.svc:6379"}},
		{"sentinel://gate@s0:26379,s1:26379,s2:26379", Sentinel, "gate", []string{"s0:26379", "s1:26379", "s2:26379"}},
		{"sentinel://gate@s0:26379", Sentinel, "gate", []string{"s0:26379"}},
		// 只给一个种子节点的 Cluster 仍然是 Cluster：模式看前缀，不看地址的个数。
		{"cluster://c0:6379", Cluster, "", []string{"c0:6379"}},
		{"cluster://c0:6379, c1:6379,", Cluster, "", []string{"c0:6379", "c1:6379"}},
	} {
		got, err := ParseAddr(tc.in)
		if err != nil {
			t.Errorf("%q: %v", tc.in, err)
			continue
		}
		if got.Mode != tc.mode || got.Master != tc.master || !slices.Equal(got.Addrs, tc.addrs) {
			t.Errorf("%q: got %+v", tc.in, got)
		}
	}
	for _, in := range []string{
		"",
		"a:1,b:2",          // 几个地址却没写模式：不猜
		"sentinel://a:1",   // 没有主库名
		"sentinel://@a:1",  // 主库名为空
		"sentinel://gate@", // 没有地址
		"cluster://",
		"redis://a:1", // 不认识的前缀
	} {
		if got, err := ParseAddr(in); err == nil {
			t.Errorf("%q: want an error, got %+v", in, got)
		}
	}
}

// 写错的地址在创建客户端时就报错，不等到第一条命令。
func TestNewRejectsBadAddr(t *testing.T) {
	if _, err := New(Config{Addr: "cluster://"}); err == nil {
		t.Fatal("want an error")
	}
}
