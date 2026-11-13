package oas_test

import (
	"encoding/json/v2"
	"testing"

	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi/oas"
)

func TestIDMarshalsAsString(t *testing.T) {
	// 超过 2^53，JavaScript 的数字无法精确表示。
	const big = 9007199254741994
	got, err := json.Marshal(oas.OrderItem{SkuId: big})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"quantity":0,"sku_id":"9007199254741994","unit_price":{"amount":0,"currency":""}}`
	if string(got) != want {
		t.Errorf("Marshal = %s, want %s", got, want)
	}
}

func TestIDUnmarshal(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    oas.ID
		wantErr bool
	}{
		{"string", `"9007199254741994"`, 9007199254741994, false},
		{"integer", `9007199254741994`, 9007199254741994, false},
		{"not a number", `"abc"`, 0, true},
		{"float", `1.5`, 0, true},
		{"overflow", `"9223372036854775808"`, 0, true},
		{"empty string", `""`, 0, true},
		{"null", `null`, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got oas.ID
			err := json.Unmarshal([]byte(tt.in), &got)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Unmarshal(%s) err = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("Unmarshal(%s) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

// 可选字段为空时不出现在输出里，而不是输出 null。
func TestOrderOmitsUnsetPayTime(t *testing.T) {
	got, err := json.Marshal(oas.Order{Items: []oas.OrderItem{}})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if _, ok := m["pay_time"]; ok {
		t.Errorf("pay_time should be omitted when unset: %s", got)
	}
}
