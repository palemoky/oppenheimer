package ledger

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	lineA  = `{"run":"r1","user_id":"549755813901","order_id":"123456789012345678","idempotency_key":"lt-r1-17-3","sku_id":"7001","quantity":1,"accepted_at":"2026-11-17T15:06:01.123Z"}`
	lineA2 = `{"run":"r1","user_id":"549755813901","order_id":"123456789012345678","idempotency_key":"lt-r1-17-3","sku_id":"7001","quantity":1,"accepted_at":"2026-11-17T15:06:00.5Z"}`
	lineB  = `{"run":"r1","user_id":"7","order_id":"99","idempotency_key":"lt-r1-1-1","sku_id":"7002","quantity":2,"accepted_at":"2026-11-17T15:06:02Z"}`
)

func TestReadOrderList(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		wantIDs []string
		wantErr string // 为空表示不应出错
	}{
		{name: "two orders, blank lines skipped", in: lineA + "\n\n" + lineB + "\n", wantIDs: []string{"123456789012345678", "99"}},
		{name: "identical duplicate", in: lineA + "\n" + lineB + "\n" + lineA, wantIDs: []string{"123456789012345678", "99"}},
		{name: "duplicate differing only in accepted_at", in: lineA + "\n" + lineA2, wantIDs: []string{"123456789012345678"}},
		{
			name: "duplicate with another sku", in: lineA + "\n" + strings.Replace(lineA, `"sku_id":"7001"`, `"sku_id":"7002"`, 1),
			wantErr: "line 2: order 123456789012345678 differs from line 1",
		},
		{
			name: "duplicate with another key", in: lineB + "\n" + lineA + "\n" + strings.Replace(lineB, "lt-r1-1-1", "lt-r1-1-2", 1),
			wantErr: "line 3: order 99 differs from line 1",
		},
		{name: "not json", in: lineA + "\n{oops", wantErr: "line 2:"},
		{name: "order id as a number", in: strings.Replace(lineB, `"order_id":"99"`, `"order_id":99`, 1), wantErr: "line 1:"},
		{name: "missing order id", in: strings.Replace(lineB, `"order_id":"99",`, "", 1), wantErr: "line 1: order_id is missing"},
		{name: "non-positive user", in: strings.Replace(lineB, `"user_id":"7"`, `"user_id":"0"`, 1), wantErr: "line 1: user_id"},
		{name: "zero quantity", in: strings.Replace(lineB, `"quantity":2`, `"quantity":0`, 1), wantErr: "line 1: quantity 0"},
		{name: "bad time", in: strings.Replace(lineB, "2026-11-17T15:06:02Z", "yesterday", 1), wantErr: "line 1: accepted_at"},
		{name: "missing run", in: strings.Replace(lineB, `"run":"r1",`, "", 1), wantErr: "line 1: run is missing"},
		{name: "empty", in: "", wantIDs: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ReadOrderList(strings.NewReader(tt.in))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.wantIDs) {
				t.Fatalf("got %d entries, want %d: %+v", len(got), len(tt.wantIDs), got)
			}
			for i, e := range got {
				if e.OrderID != tt.wantIDs[i] {
					t.Errorf("entry %d order id = %s, want %s", i, e.OrderID, tt.wantIDs[i])
				}
			}
		})
	}
}

// 每一行变成一条受理成功的 submit：Check 的第 15 条只认 Op=submit、Outcome=ok、202 和有效的订单号。
func TestReadOrderListEntry(t *testing.T) {
	t.Parallel()
	got, err := ReadOrderList(strings.NewReader(lineA + "\n" + lineA2))
	if err != nil {
		t.Fatal(err)
	}
	want := Entry{
		Time: time.Date(2026, 11, 17, 15, 6, 0, 500e6, time.UTC), // 重复的行保留最早的受理时间
		Op:   OpSubmit, OrderID: "123456789012345678", UserID: 549755813901, SKUID: 7001, Quantity: 1,
		IdempotencyKey: "lt-r1-17-3", Attempt: 1, HTTPStatus: http.StatusAccepted, OrderStatus: AcceptedStatus, Outcome: OutcomeOK,
	}
	if len(got) != 1 || !got[0].Time.Equal(want.Time) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	got[0].Time = want.Time
	if got[0] != want {
		t.Errorf("entry = %+v, want %+v", got[0], want)
	}
}
