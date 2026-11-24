package service

import (
	"strings"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/testdb"
)

// 入库总量的守恒由数据库保证（迁移 00006，设计文档 3e 的 4.2）：三个数之和必须等于 stocked。
func TestStockedTotalIsEnforced(t *testing.T) {
	e := newCacheEnv(t, 0)
	// 不写 stocked 的插入：默认值是三个数之和。建 SKU 的脚本和别的测试都是这样插的。
	e.insert(t, 1, 10)
	testdb.MustExec(t, e.db, `INSERT INTO sku_stock (sku_id, name, unit_price, available, reserved, sold) VALUES (2, 'sku', 100, 7, 2, 1)`)

	stocked := func(sku int64) (n int64) {
		t.Helper()
		if err := e.db.QueryRowContext(t.Context(), `SELECT stocked FROM sku_stock WHERE sku_id = ?`, sku).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if got := stocked(1); got != 10 {
		t.Errorf("stocked of sku 1 = %d, want 10", got)
	}
	if got := stocked(2); got != 10 {
		t.Errorf("stocked of sku 2 = %d, want 10", got)
	}

	// 只改一个数：总量变了而 stocked 没变，被拒绝。手工改库、应用增量的代码写错，都会撞在这里。
	for _, stmt := range []string{
		`UPDATE sku_stock SET available = available - 1 WHERE sku_id = 1`,
		`UPDATE sku_stock SET sold = sold + 1 WHERE sku_id = 2`,
		`UPDATE sku_stock SET stocked = stocked + 5 WHERE sku_id = 1`,
		`INSERT INTO sku_stock (sku_id, name, unit_price, available, stocked) VALUES (3, 'sku', 100, 10, 11)`,
	} {
		_, err := e.db.ExecContext(t.Context(), stmt)
		if err == nil || !strings.Contains(err.Error(), "chk_sku_stock_total") {
			t.Errorf("%s: err = %v, want a chk_sku_stock_total violation", stmt, err)
		}
	}

	// 三个数之间挪动（预占、确认、释放应用到 sku_stock 时就是这样）不受影响。
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET available = available - 3, reserved = reserved + 3 WHERE sku_id = 1`)
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET reserved = reserved - 2, sold = sold + 2 WHERE sku_id = 1`)
	// 补货：available 和 stocked 一起加。
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET available = available + 5, stocked = stocked + 5 WHERE sku_id = 1`)
	if got := stocked(1); got != 15 {
		t.Errorf("stocked of sku 1 after restock = %d, want 15", got)
	}
}
