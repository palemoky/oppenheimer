package kube

import (
	"context"
	"encoding/json/jsontext"
	"fmt"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

func kindProp() map[string]any {
	return str("Resource kind, case-insensitive; singular, plural and short names work (pod, pods, po). Allowed: " + allowedKinds + ".")
}

func getDef() llm.ToolDef {
	return llm.ToolDef{
		Name: "kube_get",
		Description: "Get a one-line summary of Kubernetes objects, like `kubectl get -o wide`: one object by name, or a list. " +
			"Read-only; only these kinds can be read: " + allowedKinds + ". " +
			"Lists are capped at 50 items (pods that are not ready or have restarted come first) and the result states the total. " +
			"Use kube_describe for details of one object.",
		Schema: schema(map[string]any{
			"kind":      kindProp(),
			"namespace": str("Namespace. Omit to search all namespaces. Ignored for Node."),
			"name":      str("Object name. Omit to list."),
			"selector":  str("Label selector, e.g. app=order or app in (order,stock)."),
		}, "kind"),
	}
}

func describeDef() llm.ToolDef {
	return llm.ToolDef{
		Name: "kube_describe",
		Description: "Describe one Kubernetes object, like a trimmed `kubectl describe`, followed by its recent events. " +
			"For a Pod this includes the node, phase, conditions, and per container: image, ready, restart count, current state, " +
			"the last termination (reason such as OOMKilled or Error, exit code, finish time) and resource requests/limits. " +
			"Environment variable values are never shown, only their names. Only these kinds can be read: " + allowedKinds + ".",
		Schema: schema(map[string]any{
			"kind":      kindProp(),
			"namespace": str("Namespace. If omitted, the object is looked up by name in all namespaces."),
			"name":      str("Object name."),
		}, "kind", "name"),
	}
}

type getArgs struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Selector  string `json:"selector"`
}

func (k *kube) get(ctx context.Context, input jsontext.Value) (tool.Result, error) {
	var args getArgs
	if err := tool.Decode(input, &args); err != nil {
		return tool.Result{}, err
	}
	s, err := lookupKind(args.Kind)
	if err != nil {
		return tool.Result{}, err
	}
	if args.Selector != "" {
		if _, err := labels.Parse(args.Selector); err != nil {
			return tool.Result{}, fmt.Errorf("selector %q is not a valid label selector (e.g. app=order or app in (order,stock)): %w", args.Selector, err)
		}
	}
	ns := args.Namespace
	if !s.namespaced {
		ns = ""
	}
	now := k.now()

	// 名字和命名空间都有时直接 get；否则列一页，按名字过滤也交给 apiserver（字段选择器 metadata.name）。
	var items []object
	more, remaining := false, (*int64)(nil)
	if args.Name != "" && (ns != "" || !s.namespaced) && args.Selector == "" {
		o, err := k.getOne(ctx, s, ns, args.Name)
		if err != nil {
			return tool.Result{}, err
		}
		items = []object{o}
	} else {
		items, more, remaining, err = k.list(ctx, s, ns, args.Name, args.Selector)
		if err != nil {
			return tool.Result{}, err
		}
		if args.Name != "" && len(items) == 0 {
			what := where(s.kind, ns, args.Name)
			if args.Selector != "" {
				what += " matching selector " + args.Selector
			}
			return tool.Result{}, fmt.Errorf("%s not found; use kube_get without a name to list what exists", what)
		}
	}
	for _, o := range items {
		redact(o)
	}
	s.sort(items)

	var b strings.Builder
	writeListHeader(&b, s, ns, args, len(items), more, remaining)
	withNS := s.namespaced && ns == ""
	writeRows(&b, s, items[:min(len(items), maxItems)], withNS, now)
	if args.Name != "" {
		b.WriteString("Use kube_describe for details and events.\n")
	}
	return tool.Result{Text: b.String(), Raw: marshalRaw(items)}, nil
}

// list 列一页对象，按名字再筛一遍（fake clientset 不认字段选择器）。
func (k *kube) list(ctx context.Context, s *spec, ns, name, selector string) (items []object, more bool, remaining *int64, err error) {
	if k.hidden.Namespace(ns) {
		// 和一个不存在的命名空间一样：apiserver 对它返回空列表。
		return nil, false, nil, nil
	}
	opts := metav1.ListOptions{LabelSelector: selector, Limit: listPageLimit}
	if name != "" {
		opts.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
	}
	p, err := s.list(ctx, k.client, ns, opts)
	if err != nil {
		return nil, false, nil, apiError(err, where(s.kind, ns, ""))
	}
	p.items = slices.DeleteFunc(p.items, func(o object) bool {
		return (name != "" && o.GetName() != name) || k.isHidden(o)
	})
	return p.items, p.more, p.remaining, nil
}

// getOne 按名字取一个对象。隐藏的（hidden.go）和不存在的回答一样。
func (k *kube) getOne(ctx context.Context, s *spec, ns, name string) (object, error) {
	what := where(s.kind, ns, name)
	if k.hidden.Namespace(ns) {
		return nil, notFound(what)
	}
	o, err := s.get(ctx, k.client, ns, name)
	if err != nil {
		return nil, apiError(err, what)
	}
	if k.isHidden(o) {
		return nil, notFound(what)
	}
	return o, nil
}

// isHidden 报告一个对象是不是对模型隐藏的：在隐藏的命名空间里，或者是关于隐藏对象的事件。
func (k *kube) isHidden(o object) bool {
	if k.hidden.Namespace(o.GetNamespace()) {
		return true
	}
	e, ok := o.(*corev1.Event)
	return ok && k.hiddenEvent(e)
}

func writeListHeader(b *strings.Builder, s *spec, ns string, args getArgs, total int, more bool, remaining *int64) {
	b.WriteString(s.kind)
	switch {
	case !s.namespaced:
	case ns != "":
		fmt.Fprintf(b, " in namespace %q", ns)
	default:
		b.WriteString(" in all namespaces")
	}
	if args.Selector != "" {
		fmt.Fprintf(b, " matching %s", args.Selector)
	}
	if args.Namespace != "" && !s.namespaced {
		fmt.Fprintf(b, " (%s is cluster-scoped; namespace ignored)", s.kind)
	}
	fmt.Fprintf(b, ": %d", total)
	if more {
		b.WriteString("+ (only the first page was fetched")
		if remaining != nil {
			fmt.Fprintf(b, ", about %d more exist", *remaining)
		}
		b.WriteString("; narrow with namespace or selector)")
	}
	if total > maxItems {
		fmt.Fprintf(b, ", showing %d", maxItems)
		if s.kind == "Pod" {
			b.WriteString(" (not-ready and restarted pods first)")
		}
		b.WriteString("; narrow with namespace, selector or name")
	}
	b.WriteString("\n")
}

func writeRows(b *strings.Builder, s *spec, items []object, withNS bool, now time.Time) {
	if len(items) == 0 {
		return
	}
	w := tabwriter.NewWriter(b, 0, 0, 2, ' ', 0)
	header := s.header
	if withNS {
		header = slices.Insert(slices.Clone(header), 0, "NAMESPACE")
	}
	fmt.Fprintln(w, strings.Join(header, "\t"))
	for _, o := range items {
		row := s.row(o, now)
		if withNS {
			row = slices.Insert(row, 0, o.GetNamespace())
		}
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}
	_ = w.Flush() // 写进 strings.Builder 不会失败
}

type describeArgs struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

func (k *kube) describe(ctx context.Context, input jsontext.Value) (tool.Result, error) {
	var args describeArgs
	if err := tool.Decode(input, &args); err != nil {
		return tool.Result{}, err
	}
	s, err := lookupKind(args.Kind)
	if err != nil {
		return tool.Result{}, err
	}
	if args.Name == "" {
		return tool.Result{}, fmt.Errorf("name is required; use kube_get to list %s objects", s.kind)
	}
	ns := args.Namespace
	if !s.namespaced {
		ns = ""
	}
	o, err := k.resolve(ctx, s, ns, args.Name)
	if err != nil {
		return tool.Result{}, err
	}
	redact(o)
	now := k.now()

	var b strings.Builder
	writeMeta(&b, s.kind, o, now)
	s.describe(&b, o, now)

	// 附上这个对象的事件。事件读不到不影响 describe 本身，写明就行；超时了就整个返回错误。
	if s.kind != "Event" {
		b.WriteString("Events")
		evs, more, err := k.fetchEvents(ctx, o.GetNamespace(), s.kind, o.GetName())
		switch {
		case ctx.Err() != nil:
			return tool.Result{}, fmt.Errorf("reading events of %s: %w", where(s.kind, o.GetNamespace(), o.GetName()), ctx.Err())
		case err != nil:
			fmt.Fprintf(&b, ": could not be read (%v)\n", err)
		default:
			b.WriteString(":")
			if more {
				fmt.Fprintf(&b, " [only the first %d events were considered]", listPageLimit)
			}
			b.WriteString(" ")
			ms := mergeEvents(evs)
			writeEvents(&b, ms, len(evs), describeEvents, now, false)
			if len(ms) > describeEvents {
				b.WriteString("Use kube_events for more.\n")
			}
		}
	}
	return tool.Result{Text: b.String(), Raw: marshalRaw(o)}, nil
}

// resolve 找到一个对象。命名空间类的资源没给命名空间时按名字在所有命名空间里找，
// 只找到一个就用它，找到多个让模型指定。
func (k *kube) resolve(ctx context.Context, s *spec, ns, name string) (object, error) {
	if ns != "" || !s.namespaced {
		return k.getOne(ctx, s, ns, name)
	}
	items, _, _, err := k.list(ctx, s, "", name, "")
	if err != nil {
		return nil, err
	}
	switch len(items) {
	case 0:
		return nil, fmt.Errorf("%s not found in any namespace; use kube_get to list what exists", where(s.kind, "", name))
	case 1:
		return items[0], nil
	}
	nss := make([]string, len(items))
	for i, o := range items {
		nss[i] = o.GetNamespace()
	}
	slices.Sort(nss)
	return nil, fmt.Errorf("%s exists in several namespaces (%s); pass namespace", where(s.kind, "", name), strings.Join(nss, ", "))
}
