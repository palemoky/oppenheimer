package tool

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// 这个文件是几组工具共用的小件：解析参数、解析时间、裁剪成行的文本。

// Decode 把模型给的参数解析进 dst。多出来的字段是错误：模型写错了参数名时要让它知道，
// 而不是悄悄忽略、用默认值查出一个它没想要的结果。
func Decode(input jsontext.Value, dst any) error {
	if len(input) == 0 {
		input = jsontext.Value("{}")
	}
	if err := json.Unmarshal(input, dst, json.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("invalid arguments: %w", err)
	}
	return nil
}

// ParseTime 解析工具的时间参数：RFC 3339（2026-11-24T03:00:00Z），或者相对现在的写法（-30m、-2h、-90s、now）。
// 空串返回 now。模型不知道现在几点，相对写法让它不用自己算。
func ParseTime(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	switch {
	case s == "" || s == "now":
		return now, nil
	case strings.HasPrefix(s, "-"):
		d, err := time.ParseDuration(s[1:])
		if err != nil {
			return time.Time{}, fmt.Errorf("time %q: relative times look like -30m, -2h or -90s", s)
		}
		return now.Add(-d), nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("time %q: use RFC 3339 (2026-11-24T03:00:00Z) or a relative time like -30m", s)
	}
	return t, nil
}

// TrimLines 裁剪成行的文本（日志）：每行截到 maxLineBytes，连续相同的行合并成一行加次数，最多留 maxLines 行。
// keepTail 为真时超出的部分丢开头的（留最新的），否则丢末尾的。
// 返回裁好的行和因为行数上限丢掉的行数（合并之后的行数）；调用方负责在结果里写明丢了多少。
func TrimLines(lines []string, maxLines, maxLineBytes int, keepTail bool) (out []string, dropped int) {
	out = make([]string, 0, min(len(lines), maxLines))
	repeat := 0
	flush := func() {
		if repeat > 1 {
			out[len(out)-1] += fmt.Sprintf("  [repeated %d times]", repeat)
		}
	}
	prev := ""
	for i, line := range lines {
		line = cutLine(line, maxLineBytes)
		if i > 0 && line == prev {
			repeat++
			continue
		}
		flush()
		out = append(out, line)
		prev, repeat = line, 1
	}
	flush()
	if len(out) > maxLines {
		dropped = len(out) - maxLines
		if keepTail {
			out = out[dropped:]
		} else {
			out = out[:maxLines]
		}
	}
	return out, dropped
}

func cutLine(line string, maxBytes int) string {
	if len(line) <= maxBytes {
		return line
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(line[cut]) {
		cut--
	}
	return fmt.Sprintf("%s… [+%d bytes]", line[:cut], len(line)-cut)
}
