package changes

import (
	"errors"
	"testing"
	"time"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
)

func TestJudge(t *testing.T) {
	t.Parallel()
	const v = 5
	tests := []struct {
		name string
		d    Derived
		want Verdict
	}{
		{"派生端已经是更晚的版本", Derived{Found: true, Version: 6}, OK},
		// 更晚的版本内容当然不同，不看 Diff：它由那个版本自己的变更负责。
		{"更晚的版本不比内容", Derived{Found: true, Version: 6, Diff: "status"}, OK},
		{"同一个版本内容相同", Derived{Found: true, Version: 5}, OK},
		{"同一个版本内容不同", Derived{Found: true, Version: 5, Diff: "status"}, Mismatch},
		{"派生端的版本更早", Derived{Found: true, Version: 4}, Behind},
		// 版本更早时内容不同是正常的，不能报不一致。
		{"版本更早不比内容", Derived{Found: true, Version: 4, Diff: "status"}, Behind},
		{"派生端没有这一行", Derived{}, Behind},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Judge(v, tt.d); got != tt.want {
				t.Errorf("Judge(%d, %+v) = %d, want %d", v, tt.d, got, tt.want)
			}
		})
	}
}

func intVal(n int64) *cdcv1.Value  { return &cdcv1.Value{Kind: &cdcv1.Value_Int{Int: n}} }
func strVal(s string) *cdcv1.Value { return &cdcv1.Value{Kind: &cdcv1.Value_Str{Str: s}} }
func nullVal() *cdcv1.Value        { return &cdcv1.Value{} }

func image(orderID, version int64, merchant *cdcv1.Value) map[string]*cdcv1.Value {
	return map[string]*cdcv1.Value{
		ColOrderID: intVal(orderID), ColVersion: intVal(version), ColMerchantID: merchant, "status": intVal(2),
	}
}

func TestDecode(t *testing.T) {
	t.Parallel()
	commit := time.Date(2026, 11, 24, 3, 0, 0, 123000, time.UTC)

	t.Run("after 映像", func(t *testing.T) {
		t.Parallel()
		for _, op := range []cdcv1.Op{cdcv1.Op_OP_INSERT, cdcv1.Op_OP_UPDATE} {
			c, ok, err := Decode(&cdcv1.RowChange{
				Table: SourceTable, Slot: 3, Op: op, After: image(1001, 2, intVal(77)),
				Before: image(1001, 1, intVal(77)), CommitMicros: commit.UnixMicro(),
			})
			if err != nil || !ok {
				t.Fatalf("%s: ok %v, error %v", op, ok, err)
			}
			if c.OrderID != 1001 || c.Version != 2 || c.MerchantID != 77 || c.Slot != 3 || c.Deleted || !c.Commit.Equal(commit) {
				t.Errorf("%s: %+v", op, c)
			}
		}
	})
	t.Run("DELETE 用 before 映像", func(t *testing.T) {
		t.Parallel()
		c, ok, err := Decode(&cdcv1.RowChange{Table: SourceTable, Op: cdcv1.Op_OP_DELETE, Before: image(1001, 4, intVal(77))})
		if err != nil || !ok || !c.Deleted || c.Version != 4 {
			t.Fatalf("%+v, ok %v, error %v", c, ok, err)
		}
	})
	t.Run("全量读到的行没有提交时刻，merchant_id 可以是 NULL", func(t *testing.T) {
		t.Parallel()
		c, ok, err := Decode(&cdcv1.RowChange{Table: SourceTable, Op: cdcv1.Op_OP_READ, After: image(1001, 0, nullVal())})
		if err != nil || !ok || !c.Commit.IsZero() || c.MerchantID != 0 {
			t.Fatalf("%+v, ok %v, error %v", c, ok, err)
		}
	})
	t.Run("别的表和不认识的操作不用核对", func(t *testing.T) {
		t.Parallel()
		for _, ch := range []*cdcv1.RowChange{
			{Table: "order_items", Op: cdcv1.Op_OP_INSERT, After: image(1, 1, nullVal())},
			{Table: SourceTable, Op: cdcv1.Op_OP_UNSPECIFIED},
		} {
			if _, ok, err := Decode(ch); ok || err != nil {
				t.Errorf("%s %s: ok %v, error %v; want skipped without error", ch.GetTable(), ch.GetOp(), ok, err)
			}
		}
	})
	t.Run("映像缺列或类型不对", func(t *testing.T) {
		t.Parallel()
		noVersion := image(1, 1, nullVal())
		delete(noVersion, ColVersion)
		noMerchant := image(1, 1, nullVal())
		delete(noMerchant, ColMerchantID)
		textID := image(1, 1, nullVal())
		textID[ColOrderID] = strVal("1")
		nullVersion := image(1, 1, nullVal())
		nullVersion[ColVersion] = nullVal()
		for name, ch := range map[string]*cdcv1.RowChange{
			"没有 version":                {Table: SourceTable, Op: cdcv1.Op_OP_UPDATE, After: noVersion},
			"没有 merchant_id":            {Table: SourceTable, Op: cdcv1.Op_OP_UPDATE, After: noMerchant},
			"order_id 不是整数":             {Table: SourceTable, Op: cdcv1.Op_OP_UPDATE, After: textID},
			"version 是 NULL":            {Table: SourceTable, Op: cdcv1.Op_OP_UPDATE, After: nullVersion},
			"INSERT 没有 after":           {Table: SourceTable, Op: cdcv1.Op_OP_INSERT},
			"DELETE 只有 after 没有 before": {Table: SourceTable, Op: cdcv1.Op_OP_DELETE, After: image(1, 1, nullVal())},
		} {
			if _, ok, err := Decode(ch); ok || !errors.Is(err, ErrImage) {
				t.Errorf("%s: ok %v, error %v; want ErrImage", name, ok, err)
			}
		}
	})
}
