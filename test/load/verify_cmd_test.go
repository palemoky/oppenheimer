package main

import (
	"flag"
	"io"
	"slices"
	"strings"
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

func TestParseScopeSKUs(t *testing.T) {
	t.Parallel()
	initial := map[int64]int64{7001: 10, 7002: 20}
	tests := []struct {
		name, in string
		want     []int64
		wantErr  string
	}{
		{name: "same skus, any order", in: "7002, 7001", want: []int64{7001, 7002}},
		{name: "extra sku", in: "7001,7002,7003", wantErr: "only in -scope-skus [7003]"},
		{name: "missing sku", in: "7001", wantErr: "only in -initial [7002]"},
		{name: "duplicate", in: "7001,7001,7002", wantErr: "listed twice"},
		{name: "not a number", in: "7001,x", wantErr: "invalid id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseScopeSKUs(tt.in, initial)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || !slices.Equal(got, tt.want) {
				t.Fatalf("got %v %v, want %v", got, err, tt.want)
			}
		})
	}
}

// -ledger 和 -orders-jsonl 只能给一个；-scope-skus 与 -initial 不一致时是用法错误。都在连库之前返回 2。
func TestVerifyCmdUsageErrors(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{
		{"-initial", "7001=10", "-ledger", "a.jsonl", "-orders-jsonl", "b.jsonl"},
		{"-initial", "7001=10", "-scope-skus", "7002"},
	} {
		var out strings.Builder
		if code := verifyCmd(args, io.Discard, &out); code != 2 {
			t.Errorf("verify %v = %d, want 2 (stderr %q)", args, code, out.String())
		}
	}
}
