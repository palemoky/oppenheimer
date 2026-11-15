package main

import (
	"flag"
	"io"
	"testing"
)

// 命令行上给了的优先；没给的用环境变量；变量为空时保留默认值；没注册的参数（archive-dsn 只有 verify 有）跳过。
func TestApplyEnv(t *testing.T) {
	t.Setenv("LOAD_INVENTORY_DSN", "env-inventory")
	t.Setenv("LOAD_ORDER_DSNS", "env-order-0,env-order-1")
	t.Setenv("LOAD_PAYMENT_DSN", "")
	t.Setenv("LOAD_ARCHIVE_DSN", "env-archive")

	var sf sourceFlags
	fs := flag.NewFlagSet("wait-quiet", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	sf.register(fs)
	if ok, code := parseSourceFlags(fs, []string{"-order-dsns", "flag-order"}); !ok {
		t.Fatalf("parse failed, code %d", code)
	}
	if sf.inventoryDSN != "env-inventory" {
		t.Errorf("inventory dsn = %q, want the env value", sf.inventoryDSN)
	}
	if sf.orderDSNs != "flag-order" {
		t.Errorf("order dsns = %q, want the flag value", sf.orderDSNs)
	}
	if want := fs.Lookup("payment-dsn").DefValue; sf.paymentDSN != want {
		t.Errorf("payment dsn = %q, want the default %q", sf.paymentDSN, want)
	}
	if sf.archiveDSN != "" {
		t.Errorf("archive dsn = %q, want empty (flag not registered)", sf.archiveDSN)
	}
}
