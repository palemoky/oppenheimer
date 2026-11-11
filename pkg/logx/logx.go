// Package logx 提供各服务统一的结构化日志配置。
package logx

import (
	"io"
	"log/slog"
	"os"
)

// LevelEnv 是控制日志级别的环境变量名，取值为 debug、info、warn、error。
const LevelEnv = "LOG_LEVEL"

// New 返回输出 JSON 的 logger，每条日志都带 service 字段。
func New(service string, w io.Writer, level slog.Leveler) *slog.Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(handler).With("service", service)
}

// LevelFromEnv 读取 LOG_LEVEL，未设置或无法解析时返回 info。
func LevelFromEnv() slog.Level {
	return ParseLevel(os.Getenv(LevelEnv))
}

// ParseLevel 把文本解析为日志级别，无法解析时返回 info。
func ParseLevel(s string) slog.Level {
	var level slog.Level
	if err := level.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return level
}
