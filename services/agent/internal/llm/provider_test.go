package llm

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestOptionsResolve(t *testing.T) {
	env := map[string]string{"DEEPSEEK_API_KEY": "sk-d", "ANTHROPIC_API_KEY": "sk-a"}
	getenv := func(k string) string { return env[k] }

	o := Options{Provider: ProviderDeepSeek}
	if err := o.Resolve(getenv); err != nil {
		t.Fatal(err)
	}
	if o.Model != "deepseek-flash" || o.BaseURL != "https://api.deepseek.com" || o.APIKey != "sk-d" {
		t.Errorf("deepseek = %+v", o)
	}

	// 显式给的不被默认值盖掉。
	o = Options{Provider: ProviderDeepSeek, Model: "deepseek-v4-pro", BaseURL: "http://proxy", APIKey: "sk-x"}
	if err := o.Resolve(getenv); err != nil {
		t.Fatal(err)
	}
	if o.Model != "deepseek-v4-pro" || o.BaseURL != "http://proxy" || o.APIKey != "sk-x" {
		t.Errorf("explicit = %+v", o)
	}

	// anthropic 的地址留给 SDK；没有密钥不是错。
	o = Options{Provider: ProviderAnthropic}
	if err := o.Resolve(func(string) string { return "" }); err != nil {
		t.Fatal(err)
	}
	if o.Model != "claude-opus-5-5" || o.BaseURL != "" || o.APIKey != "" {
		t.Errorf("anthropic = %+v", o)
	}

	for name, bad := range map[string]Options{
		"unknown provider":            {Provider: "gemini"},
		"openai without a model":      {Provider: ProviderOpenAI},
		"compatible without base url": {Provider: ProviderCompatible, Model: "qwen"},
	} {
		if err := bad.Resolve(getenv); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if err := (&Options{Provider: "gemini"}).Resolve(getenv); err == nil || !strings.Contains(err.Error(), "deepseek") {
		t.Errorf("unknown provider error should list the known ones: %v", err)
	}
}

func TestNewPicksTheAdapter(t *testing.T) {
	for _, name := range Providers() {
		o := Options{Provider: name, Model: "m", BaseURL: "http://localhost:1"}
		if err := o.Resolve(func(string) string { return "k" }); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		c, err := New(o)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		_, isAnthropic := c.(*Anthropic)
		if isAnthropic != (name == ProviderAnthropic) {
			t.Errorf("%s: adapter = %T", name, c)
		}
		if oa, ok := c.(*OpenAI); ok {
			wantField, wantStrict := FieldMaxTokens, false
			if name == ProviderOpenAI {
				wantField, wantStrict = FieldMaxCompletionTokens, true
			}
			if oa.cfg.MaxTokensField != wantField || oa.cfg.Strict != wantStrict || oa.cfg.APIKey != "k" {
				t.Errorf("%s: config = %+v", name, oa.cfg)
			}
		}
	}
	if _, err := New(Options{Provider: "gemini"}); err == nil {
		t.Error("unknown provider: want an error")
	}
	if !slices.Contains(Providers(), ProviderDeepSeek) || KeyEnv(ProviderDeepSeek) != "DEEPSEEK_API_KEY" {
		t.Errorf("providers = %v", Providers())
	}
}

func TestPrices(t *testing.T) {
	// 每个有默认模型的后端，默认模型都要有价：否则不带 -price 就起不来。
	for name, p := range providers {
		if p.defaultModel == "" {
			continue
		}
		if _, err := PriceOf(p.defaultModel); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	p, err := ParsePrice("1.5, 6")
	if err != nil || p != (Price{Input: 1.5, Output: 6}) {
		t.Errorf("ParsePrice = %+v, %v", p, err)
	}
	p, err = ParsePrice("2,8,2.5,0.2")
	if err != nil || p != (Price{Input: 2, Output: 8, CacheWrite: 2.5, CacheRead: 0.2}) {
		t.Errorf("ParsePrice = %+v, %v", p, err)
	}
	for _, bad := range []string{"", "1", "1,x", "1,2,3,4,5", "-1,2"} {
		if _, err := ParsePrice(bad); err == nil {
			t.Errorf("ParsePrice(%q): want an error", bad)
		}
	}
	// 一百万个未命中的输入加一百万个命中的：命中的按缓存读的价算。
	got := prices["deepseek-v4-pro"].Cost(Usage{Input: 1e6, CacheRead: 1e6, Output: 1e6})
	if want := 1.32 + 0.044 + 3.96; got < want-1e-9 || got > want+1e-9 {
		t.Errorf("cost = %v, want %v", got, want)
	}
}

func TestPeakSchedule(t *testing.T) {
	p := prices["deepseek-flash"]
	at := func(s string) time.Time {
		ts, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return ts
	}
	// 2026-11-24 是周二，2026-11-28 是周六。
	for _, tc := range []struct {
		when, until string
	}{
		{"2026-11-24T00:59:59Z", ""},
		{"2026-11-24T01:00:00Z", "2026-11-24T04:00:00Z"},
		{"2026-11-24T03:59:59Z", "2026-11-24T04:00:00Z"},
		{"2026-11-24T04:00:00Z", ""},
		{"2026-11-24T05:30:00Z", ""},
		{"2026-11-24T06:00:00Z", "2026-11-24T10:00:00Z"},
		// 时区不同不影响：北京时间 17:59 是 UTC 09:59。
		{"2026-11-24T17:59:00+08:00", "2026-11-24T10:00:00Z"},
		{"2026-11-24T10:00:00Z", ""},
		{"2026-11-28T07:00:00Z", ""},
		// 北京时间周一早上 08:30 是 UTC 周一 00:30，不是高峰；周六 09:30 是 UTC 周六 01:30，也不是。
		{"2026-11-23T08:30:00+08:00", ""},
		{"2026-11-28T09:30:00+08:00", ""},
	} {
		until, peak := p.PeakUntil(at(tc.when))
		if peak != (tc.until != "") || (peak && !until.Equal(at(tc.until))) {
			t.Errorf("%s: peak = %v until %v, want %q", tc.when, peak, until, tc.until)
		}
	}
	u := Usage{Input: 1e6, Output: 1e6, CacheRead: 1e6}
	peak, off := at("2026-11-24T07:00:00Z"), at("2026-11-24T12:00:00Z")
	if got, want := CostOf("deepseek-flash", Price{}, u, peak), 0.30+1.20+0.006; got < want-1e-9 || got > want+1e-9 {
		t.Errorf("peak cost = %v, want %v", got, want)
	}
	if got, want := CostOf("deepseek-flash", Price{}, u, off), (0.30+1.20+0.006)/2; got < want-1e-9 || got > want+1e-9 {
		t.Errorf("off-peak cost = %v, want %v", got, want)
	}
	// 不分时段的模型：两个时刻一个价，也从来不在高峰里。
	claude := prices["claude-opus-5-5"]
	if _, isPeak := claude.PeakUntil(peak); isPeak || claude.At(off) != claude {
		t.Error("a model without a schedule must not have peak hours")
	}
}
