package logx_test

import (
	"bytes"
	"encoding/json/v2"
	"log/slog"
	"testing"

	"github.com/palemoky/Oppenheimer/pkg/logx"
)

func TestNewWritesJSONWithService(t *testing.T) {
	var buf bytes.Buffer
	logger := logx.New("order", &buf, slog.LevelInfo)

	logger.Debug("dropped")
	logger.Info("created", "order_id", 42)

	var entry struct {
		Level   string `json:"level"`
		Msg     string `json:"msg"`
		Service string `json:"service"`
		OrderID int64  `json:"order_id"`
	}
	// 只应有一行：debug 被级别过滤掉了。
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry); err != nil {
		t.Fatalf("log output is not a single JSON object: %v\n%s", err, buf.String())
	}
	if entry.Level != "INFO" || entry.Msg != "created" || entry.Service != "order" || entry.OrderID != 42 {
		t.Errorf("unexpected entry: %+v", entry)
	}
}

func TestParseLevel(t *testing.T) {
	tests := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"INFO":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
		"":      slog.LevelInfo,
		"bogus": slog.LevelInfo,
	}
	for in, want := range tests {
		if got := logx.ParseLevel(in); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}
