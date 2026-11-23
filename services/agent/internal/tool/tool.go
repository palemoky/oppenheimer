// Package tool 定义只读工具的接口和注册表。具体的工具在子包里（prom、loki、kube）。
//
// 工具的约定（设计 3.3、3.4）：
//   - 只读。没有任何会改变集群或数据的动作。
//   - 裁剪在工具里做：Result.Text 是给模型看的、已经裁过的；Result.Raw 是完整的原始结果，只落盘。
//   - 裁掉了什么要写在 Text 里（总数、给了多少、怎么缩小范围），否则模型会把看到的当成全部。
//   - Text 进了对话历史之后不会再被改。
package tool

import (
	"context"
	"encoding/json/jsontext"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
)

// MaxResultBytes 是单次结果给模型的上限，各工具自己的裁剪之外的最后一道。
const MaxResultBytes = 20 << 10

// Result 是一次工具调用的结果。
type Result struct {
	// Text 给模型看。
	Text string
	// Raw 是裁剪前的原始结果（通常是后端返回的 JSON），可以为空。
	Raw []byte
}

// Tool 是一个只读工具。Call 必须可以并发调用。
type Tool interface {
	Def() llm.ToolDef
	// Call 执行一次调用。input 是模型给的参数（JSON），工具自己解析和校验。
	// 返回的错误会作为出错的结果交给模型（它可以改参数重试），所以错误信息要写给模型看：
	// 说清楚哪里不对、该怎么改，不要带内部细节。ctx 带着这次调用的时限。
	Call(ctx context.Context, input jsontext.Value) (Result, error)
}

// Registry 是一组工具。建好之后只读。
type Registry struct {
	tools map[string]Tool
	names []string
}

// NewRegistry 登记一组工具。名字重复时返回错误。
func NewRegistry(tools ...Tool) (*Registry, error) {
	r := &Registry{tools: make(map[string]Tool, len(tools))}
	for _, t := range tools {
		name := t.Def().Name
		if _, dup := r.tools[name]; dup {
			return nil, fmt.Errorf("duplicate tool %q", name)
		}
		r.tools[name] = t
		r.names = append(r.names, name)
	}
	// 顺序固定：工具定义在缓存的前缀里，顺序变了缓存就全部失效。
	slices.Sort(r.names)
	return r, nil
}

// Defs 按名字的顺序返回全部工具定义。
func (r *Registry) Defs() []llm.ToolDef {
	defs := make([]llm.ToolDef, 0, len(r.names))
	for _, name := range r.names {
		defs = append(defs, r.tools[name].Def())
	}
	return defs
}

// Get 按名字取工具。
func (r *Registry) Get(name string) (Tool, bool) {
	t, ok := r.tools[name]
	return t, ok
}

// Cap 把 text 截到 MaxResultBytes 以内（不切断 UTF-8 字符），截了就在末尾写明。
func Cap(text string) string {
	if len(text) <= MaxResultBytes {
		return text
	}
	cut := MaxResultBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	var b strings.Builder
	b.WriteString(text[:cut])
	fmt.Fprintf(&b, "\n[truncated: showing the first %d of %d bytes; narrow the query]", cut, len(text))
	return b.String()
}
