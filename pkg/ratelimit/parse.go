package ratelimit

import (
	"fmt"
	"strconv"
	"strings"
)

// Parse 解析 "速率/容量"（如 "5/10"、"0.5/3"）或 "off"（不限）。给配置用。
func Parse(s string) (Limit, error) {
	s = strings.TrimSpace(s)
	if s == "off" {
		return Limit{}, nil
	}
	rate, burst, ok := strings.Cut(s, "/")
	if !ok {
		return Limit{}, fmt.Errorf("ratelimit: %q: want rate/burst or off", s)
	}
	r, err := strconv.ParseFloat(strings.TrimSpace(rate), 64)
	if err != nil {
		return Limit{}, fmt.Errorf("ratelimit: %q: bad rate: %w", s, err)
	}
	b, err := strconv.Atoi(strings.TrimSpace(burst))
	if err != nil {
		return Limit{}, fmt.Errorf("ratelimit: %q: bad burst: %w", s, err)
	}
	l := Limit{Rate: r, Burst: b}
	if !l.Enabled() {
		return Limit{}, fmt.Errorf("ratelimit: %q: rate and burst must be positive (use off to disable)", s)
	}
	if err := l.Validate(); err != nil {
		return Limit{}, err
	}
	return l, nil
}
