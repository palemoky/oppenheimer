package tool

import (
	"context"
	"encoding/json/jsontext"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
)

type named string

func (n named) Def() llm.ToolDef { return llm.ToolDef{Name: string(n)} }
func (named) Call(context.Context, jsontext.Value) (Result, error) {
	return Result{}, nil
}

func TestRegistryOrderIsStable(t *testing.T) {
	r, err := NewRegistry(named("prom_query"), named("kube_get"), named("loki_query"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range r.Defs() {
		names = append(names, d.Name)
	}
	// 工具定义在缓存的前缀里：顺序不能跟着登记的顺序变。
	if got := strings.Join(names, ","); got != "kube_get,loki_query,prom_query" {
		t.Errorf("order = %s", got)
	}
	if _, ok := r.Get("kube_get"); !ok {
		t.Error("kube_get not found")
	}
	if _, err := NewRegistry(named("a"), named("a")); err == nil {
		t.Error("duplicate names must be rejected")
	}
}

func TestCap(t *testing.T) {
	if got := Cap("short"); got != "short" {
		t.Errorf("short text changed: %q", got)
	}
	// 截断点落在多字节字符中间时往前退，不能切出半个字符。
	long := strings.Repeat("a", MaxResultBytes-1) + "中文" + strings.Repeat("b", 100)
	got := Cap(long)
	if !utf8.ValidString(got) {
		t.Error("cut in the middle of a rune")
	}
	if !strings.HasPrefix(got, strings.Repeat("a", MaxResultBytes-1)+"\n[truncated: showing the first") {
		t.Errorf("tail = %q", got[MaxResultBytes-10:])
	}
	if !strings.Contains(got, "narrow the query") {
		t.Error("truncation notice missing")
	}
}
