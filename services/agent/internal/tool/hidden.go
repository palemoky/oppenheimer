package tool

import (
	"slices"
	"strings"
)

// 默认对模型隐藏的部分：故障注入工具自己（设计 4a 的 3.5）。
const (
	DefaultHiddenNamespaces = "chaos-mesh"
	DefaultHiddenAPIGroups  = "chaos-mesh.org"
)

// namespaceLabels 是指标和日志里装着命名空间的标签名：kube-state-metrics 和 cAdvisor 的 namespace，
// OTLP 进来的 k8s_namespace_name，Prometheus 在抓取时标签冲突改名出来的 exported_namespace。
var namespaceLabels = []string{"namespace", "k8s_namespace_name", "exported_namespace"}

// Hidden 是集群里对模型不存在的那一部分：注入故障的工具所在的命名空间，和它的 API 组。
//
// 为什么要有：故障实验里 Agent 和人各自独立排查，Agent 的结论要拿来评测。注入工具的 Pod、日志、
// 它在目标命名空间里留下的对象的事件（PodNetworkChaos 这一类）上直接写着注入的是什么，
// 模型读到就等于看了答案。
//
// 各工具的做法是“当它不存在”，不是“拒绝”：指名去读得到的是和真的不存在一样的回答，
// 列表、查询的结果里没有它。一句“不允许读”本身就在告诉模型这里有东西。
//
// 它挡的是模型无意中读到，不是一个存心绕过的调用方：聚合掉命名空间标签的查询
// （sum(...)、LogQL 里 drop 掉标签）仍然把它算在里面。权限上也没有收（RBAC 只能加不能减，
// 见设计 4a 第 6 节“第 5 步”）。
//
// nil 的 *Hidden 什么都不隐藏。建好之后只读，可以并发使用。
type Hidden struct {
	namespaces []string
	groups     []string
}

// NewHidden 用逗号分隔的命名空间和 API 组建一个 Hidden。两个都空时返回 nil。
func NewHidden(namespaces, apiGroups string) *Hidden {
	h := &Hidden{namespaces: splitList(namespaces), groups: splitList(apiGroups)}
	if len(h.namespaces) == 0 && len(h.groups) == 0 {
		return nil
	}
	return h
}

func splitList(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// Namespace 报告命名空间 ns 是不是隐藏的。
func (h *Hidden) Namespace(ns string) bool {
	return h != nil && ns != "" && slices.Contains(h.namespaces, ns)
}

// APIVersion 报告一个对象的 apiVersion（“组/版本”，核心组只有版本）是不是属于隐藏的 API 组。
// 组的子域也算（“x.chaos-mesh.org”属于“chaos-mesh.org”）。
func (h *Hidden) APIVersion(apiVersion string) bool {
	if h == nil {
		return false
	}
	group, _, ok := strings.Cut(apiVersion, "/")
	if !ok {
		return false // 核心组
	}
	return slices.ContainsFunc(h.groups, func(g string) bool {
		return group == g || strings.HasSuffix(group, "."+g)
	})
}

// Labels 报告一组指标或日志的标签是不是指着隐藏的命名空间。
func (h *Hidden) Labels(labels map[string]string) bool {
	if h == nil {
		return false
	}
	return slices.ContainsFunc(namespaceLabels, func(name string) bool { return h.Namespace(labels[name]) })
}

// IsNamespaceLabel 报告 name 是不是装着命名空间的标签名。
func IsNamespaceLabel(name string) bool { return slices.Contains(namespaceLabels, name) }
