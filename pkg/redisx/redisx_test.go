package redisx

import (
	"os"
	"testing"
)

// Open 返回时连接池已经建满（默认 MinIdleConns 等于 PoolSize）。
func TestOpenWarmsThePool(t *testing.T) {
	addr := os.Getenv("OPPENHEIMER_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("OPPENHEIMER_TEST_REDIS_ADDR is not set in CI")
		}
		t.Skip("OPPENHEIMER_TEST_REDIS_ADDR is not set")
	}
	for _, tc := range []struct {
		cfg      Config
		wantIdle uint32
	}{
		{Config{Addr: addr}, DefaultPoolSize},
		{Config{Addr: addr, PoolSize: 4}, 4},
		{Config{Addr: addr, PoolSize: 4, MinIdleConns: 2}, 2},
		{Config{Addr: addr, PoolSize: 4, MinIdleConns: 9}, 4},
	} {
		client, err := Open(t.Context(), tc.cfg)
		if err != nil {
			t.Fatal(err)
		}
		st := client.PoolStats()
		if st.IdleConns < tc.wantIdle || st.TotalConns > uint32(max(tc.cfg.PoolSize, DefaultPoolSize)) {
			t.Errorf("%+v: idle %d total %d, want at least %d idle", tc.cfg, st.IdleConns, st.TotalConns, tc.wantIdle)
		}
		_ = client.Close()
	}
}
