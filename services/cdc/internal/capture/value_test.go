package capture

import (
	"bytes"
	"regexp"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"google.golang.org/protobuf/proto"
)

func TestBinlogValueMatchesSQLValue(t *testing.T) {
	hash := append(bytes.Repeat([]byte{0xab}, 30), 0, 0)
	cases := []struct {
		name     string
		binlog   any
		binary   bool
		padTo    int
		sqlType  string
		sqlBytes []byte
	}{
		{"tinyint", int8(-3), false, 0, "TINYINT", []byte("-3")},
		{"bigint", int64(1) << 60, false, 0, "BIGINT", []byte("1152921504606846976")},
		{"unsigned int", uint32(4000000000), false, 0, "UNSIGNED INT", []byte("4000000000")},
		{"float", float32(0.1), false, 0, "FLOAT", []byte("0.1")},
		{"double", 0.1, false, 0, "DOUBLE", []byte("0.1")},
		{"char", "CNY", false, 0, "CHAR", []byte("CNY")},
		{"datetime", "2026-06-01 12:00:00.123456", false, 0, "DATETIME", []byte("2026-06-01 12:00:00.123456")},
		{"decimal", "12.30", false, 0, "DECIMAL", []byte("12.30")},
		// binlog 去掉了 BINARY 尾部的 0x00，SELECT 返回补满的值。
		{"binary trailing zeros", string(hash[:30]), true, 32, "BINARY", hash},
		{"varbinary", "\x00\x01", true, 0, "VARBINARY", []byte{0, 1}},
		{"blob", []byte{1, 2, 3}, true, 0, "BLOB", []byte{1, 2, 3}},
		{"text", []byte("hello"), false, 0, "TEXT", []byte("hello")},
		{"null", nil, false, 0, "VARCHAR", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := binlogValue(c.binlog, c.binary, c.padTo)
			if err != nil {
				t.Fatal(err)
			}
			want, err := sqlValue(c.sqlType, c.sqlBytes)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(got, want) {
				t.Errorf("binlog %v, sql %v", got, want)
			}
		})
	}
}

func TestFixedStringLength(t *testing.T) {
	// BINARY(32) 的元数据：real_type=MYSQL_TYPE_STRING(0xfe) 在高字节，长度 32 在低字节。
	if got := fixedStringLength(0xfe20); got != 32 {
		t.Errorf("BINARY(32) length = %d, want 32", got)
	}
	// CHAR(255) utf8mb4 = 1020 字节，长度的高两位编码在 real_type 里。
	if got := fixedStringLength(uint16(0xfe^0x30)<<8 | 0xfc); got != 1020 {
		t.Errorf("CHAR(255) utf8mb4 length = %d, want 1020", got)
	}
}

func TestColumnsOfRequiresColumnNames(t *testing.T) {
	tm := &replication.TableMapEvent{
		Schema:      []byte("db"),
		Table:       []byte("orders_0"),
		ColumnCount: 2,
		ColumnType:  []byte{mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_LONGLONG},
		ColumnMeta:  []uint16{0, 0},
	}
	_, err := columnsOf(tm)
	if err == nil || !strings.Contains(err.Error(), "binlog_row_metadata") {
		t.Fatalf("columnsOf without names = %v, want error mentioning binlog_row_metadata", err)
	}
	tm.ColumnName = [][]byte{[]byte("order_id"), []byte("version")}
	cols, err := columnsOf(tm)
	if err != nil {
		t.Fatal(err)
	}
	if cols.names[1] != "version" {
		t.Errorf("names = %v", cols.names)
	}
}

func TestRuleMatch(t *testing.T) {
	r := Rule{Pattern: regexp.MustCompile(`^orders_(\d+)$`), Table: "orders", Topic: "t", KeyColumn: "order_id"}
	if err := r.validate(); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]int32{"orders_0": 0, "orders_7": 7} {
		if slot, ok := r.match(name); !ok || slot != want {
			t.Errorf("match(%q) = %d, %v", name, slot, ok)
		}
	}
	for _, name := range []string{"orders", "orders_x", "order_items_0", "orders_1_bak", "xorders_1"} {
		if _, ok := r.match(name); ok {
			t.Errorf("match(%q) = true, want false", name)
		}
	}
	if err := (Rule{Pattern: regexp.MustCompile(`^orders_\d+$`), Table: "o", Topic: "t", KeyColumn: "k"}).validate(); err == nil {
		t.Error("pattern without capture group should be rejected")
	}
}

func TestLagStats(t *testing.T) {
	var s lagStats
	s.observe(1000)
	s.observe(3000)
	s.observe(-5)
	if s.count != 3 || s.max != 3000 || s.sum != 4000 {
		t.Errorf("stats = %+v", s)
	}
}
