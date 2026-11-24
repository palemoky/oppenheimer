package kube

import (
	"cmp"
	"context"
	"encoding/json/jsontext"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

// defaultEventsSince 是 kube_events 不给 since 时往回看多久。
const defaultEventsSince = "-1h"

func eventsDef() llm.ToolDef {
	return llm.ToolDef{
		Name: "kube_events",
		Description: "List Kubernetes events, newest first. Repeats of the same (object, reason) are merged into one line with a total count. " +
			"Filter by namespace, by the involved object's kind and/or name, and by time. " +
			"Use this to find OOM kills, failed probes, scheduling failures, image pull errors, evictions and restarts. " +
			"At most 50 merged lines are returned; the result states the total.",
		Schema: schema(map[string]any{
			"namespace": str("Namespace. Omit for all namespaces."),
			"kind":      str("Kind of the involved object, e.g. Pod, Deployment, Node. Optional."),
			"name":      str("Name of the involved object. Optional."),
			"since":     str(`Only events last seen at or after this time: RFC 3339 or relative like -30m. Default -1h.`),
		}),
	}
}

type eventsArgs struct {
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Since     string `json:"since"`
}

func (k *kube) events(ctx context.Context, input jsontext.Value) (tool.Result, error) {
	var args eventsArgs
	if err := tool.Decode(input, &args); err != nil {
		return tool.Result{}, err
	}
	now := k.now()
	since, err := tool.ParseTime(cmp.Or(args.Since, defaultEventsSince), now)
	if err != nil {
		return tool.Result{}, fmt.Errorf("since: %w", err)
	}
	// kind 在这里只是事件的过滤条件，不会去读那个对象，所以名单外的 kind 也照原样拿来过滤；
	// 名单里的别名（po、deploy）换成正式的名字，因为事件里记的是正式的。
	kind := strings.TrimSpace(args.Kind)
	if s, err := lookupKind(kind); err == nil {
		kind = s.kind
	}
	evs, more, err := k.fetchEvents(ctx, args.Namespace, kind, args.Name)
	if err != nil {
		return tool.Result{}, err
	}
	evs = slices.DeleteFunc(evs, func(e corev1.Event) bool { return eventTime(&e).Before(since) })

	var b strings.Builder
	b.WriteString("Events")
	if args.Namespace != "" {
		fmt.Fprintf(&b, " in namespace %q", args.Namespace)
	} else {
		b.WriteString(" in all namespaces")
	}
	if kind != "" || args.Name != "" {
		fmt.Fprintf(&b, " for %s/%s", cmp.Or(kind, "*"), cmp.Or(args.Name, "*"))
	}
	fmt.Fprintf(&b, " since %s (now %s)", since.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339))
	if more {
		fmt.Fprintf(&b, "\n[only the first %d events from the API were considered; narrow with namespace, kind or name]", listPageLimit)
	}
	b.WriteString("\n")
	writeEvents(&b, mergeEvents(evs), len(evs), maxItems, now, args.Namespace == "")
	return tool.Result{Text: b.String(), Raw: marshalRaw(evs)}, nil
}

// fetchEvents 取一页事件。按对象过滤用字段选择器交给 apiserver，回来之后再按同样的条件筛一遍
// （fake clientset 不认字段选择器，真的 apiserver 认，再筛一遍对真的没有坏处）。
func (k *kube) fetchEvents(ctx context.Context, ns, kind, name string) (evs []corev1.Event, more bool, err error) {
	set := fields.Set{}
	if kind != "" {
		set["involvedObject.kind"] = kind
	}
	if name != "" {
		set["involvedObject.name"] = name
	}
	opts := metav1.ListOptions{Limit: listPageLimit}
	if len(set) > 0 {
		opts.FieldSelector = fields.SelectorFromSet(set).String()
	}
	list, err := k.client.CoreV1().Events(ns).List(ctx, opts)
	if err != nil {
		return nil, false, apiError(err, where("Event", ns, ""))
	}
	evs = slices.DeleteFunc(list.Items, func(e corev1.Event) bool {
		return (kind != "" && !strings.EqualFold(e.InvolvedObject.Kind, kind)) || (name != "" && e.InvolvedObject.Name != name)
	})
	return evs, list.Continue != "", nil
}

// eventTime 是事件最后一次出现的时间。新旧两种 API 写的字段不一样，取能找到的最晚的那个。
func eventTime(e *corev1.Event) time.Time {
	t := e.CreationTimestamp.Time
	for _, c := range []time.Time{e.FirstTimestamp.Time, e.LastTimestamp.Time, e.EventTime.Time} {
		if c.After(t) {
			t = c
		}
	}
	if e.Series != nil && e.Series.LastObservedTime.After(t) {
		t = e.Series.LastObservedTime.Time
	}
	return t
}

// eventCount 是这条事件代表的发生次数。
func eventCount(e *corev1.Event) int32 {
	switch {
	case e.Series != nil && e.Series.Count > 0:
		return e.Series.Count
	case e.Count > 0:
		return e.Count
	}
	return 1
}

// merged 是同一个对象、同一个原因的事件合并成的一行。
type merged struct {
	namespace, kind, name, reason string
	warning                       bool
	message                       string
	count                         int64
	last                          time.Time
}

// mergeEvents 按（对象、原因）合并：次数相加，时间和消息取最新的一条，有一条是 Warning 就算 Warning。
// 结果按最新的在前排。
func mergeEvents(evs []corev1.Event) []merged {
	type key struct{ ns, kind, name, reason string }
	idx := make(map[key]int)
	var out []merged
	for i := range evs {
		e := &evs[i]
		k := key{e.Namespace, e.InvolvedObject.Kind, e.InvolvedObject.Name, e.Reason}
		t := eventTime(e)
		j, ok := idx[k]
		if !ok {
			idx[k] = len(out)
			out = append(out, merged{namespace: k.ns, kind: k.kind, name: k.name, reason: k.reason})
			j = len(out) - 1
		}
		m := &out[j]
		m.count += int64(eventCount(e))
		m.warning = m.warning || e.Type == corev1.EventTypeWarning
		if !ok || t.After(m.last) {
			m.last, m.message = t, e.Message
		}
	}
	slices.SortStableFunc(out, func(a, b merged) int {
		if c := b.last.Compare(a.last); c != 0 {
			return c
		}
		return cmp.Or(strings.Compare(a.namespace, b.namespace), strings.Compare(a.name, b.name), strings.Compare(a.reason, b.reason))
	})
	return out
}

// writeEvents 写合并后的事件，最多 limit 行，并写明原始条数、合并后的条数和给了多少。
func writeEvents(b *strings.Builder, ms []merged, raw, limit int, now time.Time, withNamespace bool) {
	if len(ms) == 0 {
		b.WriteString("No events.\n")
		return
	}
	shown := ms[:min(len(ms), limit)]
	fmt.Fprintf(b, "%d events, %d after merging repeats of the same (object, reason); showing %d, newest first.\n", raw, len(ms), len(shown))
	w := tabwriter.NewWriter(b, 0, 0, 2, ' ', 0)
	cols := []string{"LAST", "TYPE", "REASON", "OBJECT", "COUNT", "MESSAGE"}
	if withNamespace {
		cols = slices.Insert(cols, 0, "NAMESPACE")
	}
	fmt.Fprintln(w, strings.Join(cols, "\t"))
	for _, m := range shown {
		typ := corev1.EventTypeNormal
		if m.warning {
			typ = corev1.EventTypeWarning
		}
		row := []string{
			m.last.UTC().Format(time.RFC3339) + " (" + age(m.last, now) + " ago)", typ, m.reason,
			m.kind + "/" + m.name, strconv.FormatInt(m.count, 10), cut(m.message, maxMessageBytes),
		}
		if withNamespace {
			row = slices.Insert(row, 0, orNone(m.namespace))
		}
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}
	_ = w.Flush() // 写进 strings.Builder 不会失败
	if len(shown) < len(ms) {
		fmt.Fprintf(b, "[%d more merged events not shown; narrow with namespace, kind, name or since]\n", len(ms)-len(shown))
	}
}
