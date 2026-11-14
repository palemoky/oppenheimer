package merkle_test

import (
	"database/sql"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/merkle"
	"github.com/palemoky/oppenheimer/pkg/testdb"
)

// 数据库里算的规范字符串、行哈希、按节点聚合的摘要，和 Go 这一侧逐位相同。
// 这是“摘要在数据库里算”能成立的前提（ADR-0022）：两份实现对不上时，对账会把每一行都报成差异，
// 或者更糟，两边用同一份错的实现互相印证。
func TestSQLMatchesGo(t *testing.T) {
	db, _ := testdb.Empty(t)
	testdb.MustExec(t, db, `CREATE TABLE r (
		id BIGINT NOT NULL PRIMARY KEY, n BIGINT NULL, st TINYINT NOT NULL,
		s VARCHAR(64) NULL, bin BINARY(4) NOT NULL, cur CHAR(3) NOT NULL)`)

	type row struct {
		id  int64
		n   sql.NullInt64
		st  int64
		s   sql.NullString
		bin []byte
		cur string
	}
	rows := []row{
		{1, sql.NullInt64{Int64: 5, Valid: true}, 3, sql.NullString{String: "a|~b", Valid: true}, []byte{0, 1, 2, 0xff}, "CNY"},
		{2, sql.NullInt64{}, -1, sql.NullString{}, []byte{0, 0, 0, 0}, "USD"},
		{3, sql.NullInt64{Int64: -9223372036854775808, Valid: true}, 127, sql.NullString{String: "", Valid: true}, []byte{0x7c, 0x7e, 0x27, 0x5c}, "EUR"},
		{9223372036854775807, sql.NullInt64{Int64: 9223372036854775807, Valid: true}, 0, sql.NullString{String: "订单", Valid: true}, []byte{1, 2, 3, 4}, "JPY"},
		// 和第一行同一个叶子（shift 4），和第二行、第三行也是。
		{17, sql.NullInt64{Int64: 0, Valid: true}, 1, sql.NullString{String: "~", Valid: true}, []byte{9, 9, 9, 9}, "CNY"},
	}
	for _, r := range rows {
		testdb.MustExec(t, db, "INSERT INTO r VALUES (?, ?, ?, ?, ?, ?)", r.id, r.n, r.st, r.s, r.bin, r.cur)
	}
	cols := []merkle.Column{
		{Name: "id", Expr: "id"},
		{Name: "n", Expr: "n", Nullable: true},
		{Name: "st", Expr: "st"},
		{Name: "s", Expr: "s", Kind: merkle.Bytes, Nullable: true},
		{Name: "bin", Expr: "bin", Kind: merkle.Bytes},
		{Name: "cur", Expr: "cur", Kind: merkle.Bytes},
	}
	canonOf := func(r row) string {
		var c merkle.Canon
		c.Int(r.id)
		if r.n.Valid {
			c.Int(r.n.Int64)
		} else {
			c.Null()
		}
		c.Int(r.st)
		if r.s.Valid {
			c.Bytes([]byte(r.s.String))
		} else {
			c.Null()
		}
		return c.Bytes(r.bin).Bytes([]byte(r.cur)).String()
	}

	const shift = 4
	want, err := merkle.NewTree(shift)
	if err != nil {
		t.Fatal(err)
	}
	canon := merkle.CanonExpr(cols)
	for _, r := range rows {
		var got string
		if err := db.QueryRowContext(t.Context(), "SELECT "+canon+" FROM r WHERE id = ?", r.id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != canonOf(r) {
			t.Errorf("row %d: canon in SQL %q, in Go %q", r.id, got, canonOf(r))
		}
		want.Add(uint64(r.id), merkle.HashRow(canonOf(r))) //nolint:gosec // 测试里的键都不为负
	}

	hi, lo := merkle.SumExprs("h")
	q := "SELECT id >> ? AS node, COUNT(*), " + hi + ", " + lo +
		" FROM (SELECT id, " + merkle.HashExpr(canon) + " AS h FROM r) x GROUP BY node ORDER BY node"
	got, err := merkle.NewTree(shift)
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.QueryContext(t.Context(), q, shift)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close() //nolint:errcheck // 测试
	for res.Next() {
		var (
			node uint64
			d    merkle.Digest
		)
		if err := res.Scan(&node, &d.Count, &d.Hi, &d.Lo); err != nil {
			t.Fatal(err)
		}
		got.AddLeaf(node, d)
	}
	if err := res.Err(); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 3 {
		t.Fatalf("got %d leaves, want 3 (rows 1, 2, 3 share one)", got.Len())
	}
	for i, e := range want.Leaves() {
		if g := got.Leaves()[i]; g != e {
			t.Errorf("leaf %v: SQL %v, Go %v", e.Node, g.Digest, e.Digest)
		}
	}
	if got.Root() != want.Root() {
		t.Fatalf("root: SQL %v, Go %v", got.Root(), want.Root())
	}

	// 一个节点里的和超过 2^64 时取模仍然一致：把同样的行复制很多份。
	testdb.MustExec(t, db, "CREATE TABLE big (id BIGINT NOT NULL PRIMARY KEY)")
	testdb.MustExec(t, db, "INSERT INTO big WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 500) SELECT i FROM s")
	var sum merkle.Digest
	for i := range int64(500) {
		var c merkle.Canon
		sum = sum.Add(merkle.Of(merkle.HashRow(c.Int(i + 1).String())))
	}
	var d merkle.Digest
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*), "+hi+", "+lo+
		" FROM (SELECT "+merkle.HashExpr(merkle.CanonExpr([]merkle.Column{{Expr: "id"}}))+" AS h FROM big) x").Scan(&d.Count, &d.Hi, &d.Lo); err != nil {
		t.Fatal(err)
	}
	if d != sum {
		t.Fatalf("500 rows: SQL %v, Go %v", d, sum)
	}
}
