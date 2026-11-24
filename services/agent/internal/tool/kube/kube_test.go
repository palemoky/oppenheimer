package kube

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

var now = time.Date(2026, 11, 24, 3, 0, 0, 0, time.UTC)

// secret 是夹具里环境变量的值，任何输出里都不能出现。
const secret = "mysql://root:Sup3rS3cret@mysql:3306/orders"

func ago(d time.Duration) metav1.Time { return metav1.NewTime(now.Add(-d)) }

func tools(t *testing.T, c *fake.Clientset) map[string]tool.Tool {
	t.Helper()
	m := make(map[string]tool.Tool)
	for _, tl := range New(Config{Client: c, Now: func() time.Time { return now }}) {
		m[tl.Def().Name] = tl
	}
	return m
}

func call(t *testing.T, c *fake.Clientset, name, input string) (tool.Result, error) {
	t.Helper()
	return tools(t, c)[name].Call(t.Context(), jsontext.Value(input))
}

func mustCall(t *testing.T, c *fake.Clientset, name, input string) tool.Result {
	t.Helper()
	res, err := call(t, c, name, input)
	if err != nil {
		t.Fatalf("%s %s: %v", name, input, err)
	}
	return res
}

// secretEnv 是带着秘密值的环境变量，加一个来自 Secret 的。
func secretEnv() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "DB_DSN", Value: secret},
		{Name: "DB_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			Name: "db-cred", Key: "password",
		}}},
	}
}

// oomPod 是一个被 OOM 杀过三次、正在 CrashLoopBackOff 的 Pod，带一个 Envoy 边车。
func oomPod() *corev1.Pod {
	return &corev1.Pod{
		Name: "order-7d9f-abc", Namespace: "shop", CreationTimestamp: ago(3 * time.Hour),
		Labels: map[string]string{"app": "order"},
		Annotations: map[string]string{
			"short":     "v",
			"long":      strings.Repeat("x", 200),
			lastApplied: `{"spec":{"containers":[{"env":[{"name":"DB_DSN","value":"` + secret + `"}]}]}}`,
		},
		Spec: corev1.PodSpec{
			NodeName: "worker-1",
			Containers: []corev1.Container{
				{
					Name: "app", Image: "order:1.2.3", Env: secretEnv(),
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
						Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
					},
				},
				{Name: "envoy", Image: "envoy:1.36"},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: "10.42.1.7", QOSClass: corev1.PodQOSBurstable,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady"}},
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app", RestartCount: 3,
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off 40s"}},
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						Reason: "OOMKilled", ExitCode: 137, StartedAt: ago(6 * time.Minute), FinishedAt: ago(5 * time.Minute),
					}},
				},
				{Name: "envoy", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago(3 * time.Hour)}}},
			},
		},
	}
}

func event(ns, name, kind, obj, reason, typ string, count int32, last time.Duration, msg string) *corev1.Event {
	return &corev1.Event{
		Name: name, Namespace: ns,
		InvolvedObject: corev1.ObjectReference{Kind: kind, Name: obj, Namespace: ns},
		Reason:         reason, Type: typ, Count: count, Message: msg,
		FirstTimestamp: ago(last + time.Minute), LastTimestamp: ago(last),
	}
}

func TestKindAllowlist(t *testing.T) {
	for _, k := range []string{"Secret", "secrets", "ConfigMap", "configmaps", "cm", "Ingress", "serviceaccounts", "pod.v1", "", "*"} {
		if _, err := lookupKind(k); err == nil {
			t.Errorf("kind %q must be rejected", k)
		} else if !strings.Contains(err.Error(), "Pod, Node, Deployment") {
			t.Errorf("kind %q: error does not list the allowed kinds: %v", k, err)
		}
	}
	for alias, want := range map[string]string{
		"po": "Pod", "Pods": "Pod", "NODE": "Node", "deploy": "Deployment", "sts": "StatefulSet", "ds": "DaemonSet",
		"rs": "ReplicaSet", "jobs": "Job", "cj": "CronJob", "svc": "Service", "EndpointSlices": "EndpointSlice",
		"hpa": "HorizontalPodAutoscaler", "pvc": "PersistentVolumeClaim", "ev": "Event", "DaemonSets": "DaemonSet",
	} {
		s, err := lookupKind(alias)
		if err != nil || s.kind != want {
			t.Errorf("kind %q = %v, %v; want %s", alias, s, err, want)
		}
	}
	// 通过工具调用也一样被拒，并且不会去调 apiserver。
	c := fake.NewClientset()
	for _, in := range []string{`{"kind":"secret","namespace":"shop","name":"db-cred"}`, `{"kind":"ConfigMap"}`} {
		if _, err := call(t, c, "kube_get", in); err == nil || !strings.Contains(err.Error(), "not allowed") {
			t.Errorf("kube_get %s: err = %v", in, err)
		}
		if _, err := call(t, c, "kube_describe", strings.Replace(in, `}`, `,"name":"x"}`, 1)); err == nil {
			t.Errorf("kube_describe %s must fail", in)
		}
	}
	if n := len(c.Actions()); n != 0 {
		t.Errorf("rejected kinds made %d API calls", n)
	}
}

func TestDescribePodShowsOOMKill(t *testing.T) {
	c := fake.NewClientset(oomPod(),
		event("shop", "e1", "Pod", "order-7d9f-abc", "BackOff", "Warning", 12, time.Minute, "Back-off restarting failed container app"),
		event("shop", "e2", "Pod", "other", "BackOff", "Warning", 1, time.Minute, "not this pod"),
	)
	text := mustCall(t, c, "kube_describe", `{"kind":"po","namespace":"shop","name":"order-7d9f-abc"}`).Text
	for _, want := range []string{
		"Pod: order-7d9f-abc",
		"Node: worker-1",
		"status: CrashLoopBackOff, ready: 1/2, restarts: 3",
		"Ready=False (ContainersNotReady)",
		"Ready: false, restart count: 3",
		"State: Waiting (CrashLoopBackOff: back-off 40s)",
		"Last termination: OOMKilled, exit code 137",
		"finished 2026-11-24T02:55:00Z (5m ago)",
		"Requests: cpu=100m, memory=128Mi",
		"Limits: memory=256Mi",
		"DB_DSN, DB_PASSWORD (from secret db-cred)",
		"short=v",
		"long (value omitted, 200 bytes)",
		"BackOff",
		"Pod/order-7d9f-abc  12",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("describe misses %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "not this pod") {
		t.Errorf("describe shows another pod's events:\n%s", text)
	}
	// 不给命名空间时按名字在所有命名空间里找。
	if res := mustCall(t, c, "kube_describe", `{"kind":"pod","name":"order-7d9f-abc"}`); !strings.Contains(res.Text, "Namespace: shop") {
		t.Errorf("lookup without namespace:\n%s", res.Text)
	}
}

func TestEnvValuesNeverLeak(t *testing.T) {
	tmpl := corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		InitContainers: []corev1.Container{{Name: "migrate", Image: "m:1", Env: secretEnv()}},
		Containers:     []corev1.Container{{Name: "app", Image: "order:1", Env: secretEnv()}},
	}}
	meta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: "shop", Annotations: map[string]string{lastApplied: secret}}
	}
	c := fake.NewClientset(
		oomPod(),
		&appsv1.Deployment{ObjectMeta: meta("order"), Spec: appsv1.DeploymentSpec{Template: tmpl}},
		&appsv1.StatefulSet{ObjectMeta: meta("mysql"), Spec: appsv1.StatefulSetSpec{Template: tmpl}},
		&appsv1.DaemonSet{ObjectMeta: meta("agent"), Spec: appsv1.DaemonSetSpec{Template: tmpl}},
		&appsv1.ReplicaSet{ObjectMeta: meta("order-7d9f"), Spec: appsv1.ReplicaSetSpec{Template: tmpl}},
		&batchv1.Job{ObjectMeta: meta("backfill"), Spec: batchv1.JobSpec{Template: tmpl}},
		&batchv1.CronJob{ObjectMeta: meta("nightly"), Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: tmpl}}}},
	)
	cases := []struct{ tool, input string }{
		{"kube_get", `{"kind":"pod"}`},
		{"kube_get", `{"kind":"pod","namespace":"shop","name":"order-7d9f-abc"}`},
		{"kube_describe", `{"kind":"pod","namespace":"shop","name":"order-7d9f-abc"}`},
	}
	for _, k := range []struct{ kind, name string }{
		{"deploy", "order"}, {"sts", "mysql"}, {"ds", "agent"}, {"rs", "order-7d9f"}, {"job", "backfill"}, {"cronjob", "nightly"},
	} {
		cases = append(cases,
			struct{ tool, input string }{"kube_get", fmt.Sprintf(`{"kind":%q}`, k.kind)},
			struct{ tool, input string }{"kube_describe", fmt.Sprintf(`{"kind":%q,"namespace":"shop","name":%q}`, k.kind, k.name)})
	}
	for _, tc := range cases {
		res := mustCall(t, c, tc.tool, tc.input)
		if strings.Contains(res.Text, "Sup3rS3cret") {
			t.Errorf("%s %s: env value in Text:\n%s", tc.tool, tc.input, res.Text)
		}
		if strings.Contains(string(res.Raw), "Sup3rS3cret") {
			t.Errorf("%s %s: env value in Raw", tc.tool, tc.input)
		}
		if !strings.Contains(string(res.Raw), redacted) {
			t.Errorf("%s %s: Raw has no redaction marker, is it empty? %d bytes", tc.tool, tc.input, len(res.Raw))
		}
		if !jsontext.Value(res.Raw).IsValid() {
			t.Errorf("%s %s: Raw is not valid JSON", tc.tool, tc.input)
		}
		if strings.HasPrefix(tc.tool, "kube_describe") && !strings.Contains(res.Text, "DB_DSN") {
			t.Errorf("%s %s: env names should still be listed:\n%s", tc.tool, tc.input, res.Text)
		}
	}
	// 夹具本身没有被改：脱敏只改这次调用解码出来的副本。
	p, err := c.CoreV1().Pods("shop").Get(t.Context(), "order-7d9f-abc", metav1.GetOptions{})
	if err != nil || p.Spec.Containers[0].Env[0].Value != secret {
		t.Errorf("stored object was modified: %v", err)
	}
}

func TestListCapAndSort(t *testing.T) {
	var objs []runtime.Object
	for i := range 60 {
		p := &corev1.Pod{
			Name: fmt.Sprintf("p%02d", i), Namespace: "shop", CreationTimestamp: ago(time.Hour),
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
			Status: corev1.PodStatus{
				Phase:             corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{Name: "app", Ready: true}},
			},
		}
		switch i {
		case 55: // 没就绪
			p.Status.ContainerStatuses[0].Ready = false
		case 57: // 重启过 5 次
			p.Status.ContainerStatuses[0].RestartCount = 5
		case 58: // 重启过 1 次
			p.Status.ContainerStatuses[0].RestartCount = 1
		}
		objs = append(objs, p)
	}
	objs = append(objs, &corev1.Pod{Name: "other", Namespace: "pay"})
	c := fake.NewClientset(objs...)

	text := mustCall(t, c, "kube_get", `{"kind":"pods","namespace":"shop"}`).Text
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if !strings.HasPrefix(lines[0], `Pod in namespace "shop": 60, showing 50 (not-ready and restarted pods first)`) {
		t.Errorf("header = %q", lines[0])
	}
	if len(lines) != 2+maxItems {
		t.Errorf("got %d lines, want header + column names + %d rows", len(lines), maxItems)
	}
	if !strings.HasPrefix(lines[1], "NAME ") {
		t.Errorf("column line = %q", lines[1])
	}
	for i, want := range []string{"p55", "p57", "p58", "p00"} {
		if !strings.HasPrefix(lines[2+i], want+" ") {
			t.Errorf("row %d = %q, want %s first", i, lines[2+i], want)
		}
	}

	// 不给命名空间时列所有命名空间，带 NAMESPACE 列；总数不到上限时不提截断。
	text = mustCall(t, c, "kube_get", `{"kind":"pod","name":"other"}`).Text
	if !strings.Contains(text, "Pod in all namespaces: 1\nNAMESPACE") || !strings.Contains(text, "pay ") || strings.Contains(text, "showing") {
		t.Errorf("all namespaces:\n%s", text)
	}
}

func TestEvents(t *testing.T) {
	objs := []runtime.Object{
		// 同一个 Pod 的 BackOff 分两条记，要合并成一行，次数相加，消息取最新的。
		event("shop", "a1", "Pod", "order-1", "BackOff", "Warning", 3, 10*time.Minute, "older message"),
		event("shop", "a2", "Pod", "order-1", "BackOff", "Warning", 4, 2*time.Minute, "newer message"),
		event("shop", "b", "Pod", "order-1", "Pulled", "Normal", 1, 5*time.Minute, "pulled"),
		event("shop", "c", "Deployment", "order", "ScalingReplicaSet", "Normal", 1, 30*time.Minute, "scaled"),
		event("shop", "old", "Pod", "order-1", "Killing", "Normal", 1, 3*time.Hour, "too old"),
		event("pay", "p", "Pod", "pay-1", "OOMKilling", "Warning", 1, time.Minute, "other namespace"),
	}
	c := fake.NewClientset(objs...)

	text := mustCall(t, c, "kube_events", `{"namespace":"shop"}`).Text
	if strings.Contains(text, "too old") || strings.Contains(text, "other namespace") || strings.Contains(text, "older message") {
		t.Errorf("filtered events shown:\n%s", text)
	}
	if !strings.Contains(text, "4 events, 3 after merging repeats of the same (object, reason); showing 3, newest first") {
		t.Errorf("counts:\n%s", text)
	}
	backoff, pulled, scaled := strings.Index(text, "BackOff"), strings.Index(text, "Pulled"), strings.Index(text, "ScalingReplicaSet")
	if backoff < 0 || backoff > pulled || pulled > scaled {
		t.Errorf("not newest first:\n%s", text)
	}
	if !strings.Contains(strings.Join(strings.Fields(text), " "), "Warning BackOff Pod/order-1 7 newer message") {
		t.Errorf("merged line wrong:\n%s", text)
	}

	// since 往回看得更远就带上旧的；kind 的别名能用。
	text = mustCall(t, c, "kube_events", `{"namespace":"shop","kind":"po","name":"order-1","since":"-4h"}`).Text
	if !strings.Contains(text, "too old") || strings.Contains(text, "scaled") {
		t.Errorf("since/kind filter:\n%s", text)
	}
	// 所有命名空间时带 NAMESPACE 列。
	text = mustCall(t, c, "kube_events", `{}`).Text
	if !strings.Contains(text, "NAMESPACE") || !strings.Contains(text, "other namespace") {
		t.Errorf("all namespaces:\n%s", text)
	}
	if _, err := call(t, c, "kube_events", `{"since":"yesterday"}`); err == nil {
		t.Error("bad since must be rejected")
	}

	// 合并之后超过 50 条时截断并写明。
	var many []runtime.Object
	for i := range 70 {
		many = append(many, event("shop", fmt.Sprintf("e%d", i), "Pod", fmt.Sprintf("p%d", i), "Unhealthy", "Warning", 1, time.Duration(i)*time.Second, "probe failed"))
	}
	text = mustCall(t, fake.NewClientset(many...), "kube_events", `{"namespace":"shop"}`).Text
	if !strings.Contains(text, "70 events, 70 after merging") || !strings.Contains(text, "showing 50") || !strings.Contains(text, "[20 more merged events not shown") {
		t.Errorf("cap:\n%s", text)
	}
	if !strings.Contains(text, "Pod/p0 ") || strings.Contains(text, "Pod/p69 ") {
		t.Errorf("cap must keep the newest:\n%s", text)
	}
}

// logsReactor 让 fake 的 GetLogs 返回 body，并记下请求的参数。
func logsReactor(c *fake.Clientset, body string, got **corev1.PodLogOptions) {
	c.PrependReactor("get", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "log" {
			return false, nil, nil
		}
		if ga, ok := a.(k8stesting.GenericAction); ok {
			*got, _ = ga.GetValue().(*corev1.PodLogOptions)
		}
		return true, &runtime.Unknown{Raw: []byte(body)}, nil
	})
}

func TestLogs(t *testing.T) {
	single := &corev1.Pod{
		Name: "single", Namespace: "shop",
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "app"}}},
	}

	// fake 默认返回固定的 "fake logs"。
	res := mustCall(t, fake.NewClientset(single), "kube_logs", `{"namespace":"shop","pod":"single"}`)
	if !strings.Contains(res.Text, "Logs of container app in pod shop/single: fetched 1 lines, showing 1\nfake logs\n") {
		t.Errorf("default:\n%s", res.Text)
	}

	c := fake.NewClientset(oomPod())
	var opts *corev1.PodLogOptions
	logsReactor(c, "line 1\nline 2\nline 2\nline 3\n", &opts)
	res = mustCall(t, c, "kube_logs", `{"namespace":"shop","pod":"order-7d9f-abc","container":"app","previous":true,"since":"-10m"}`)
	if !strings.Contains(res.Text, "(previous instance) since 2026-11-24T02:50:00Z: fetched 4 lines, showing 3 (1 repeated lines merged)") ||
		!strings.Contains(res.Text, "line 2  [repeated 2 times]") {
		t.Errorf("previous:\n%s", res.Text)
	}
	if opts == nil || opts.Container != "app" || !opts.Previous || *opts.TailLines != defaultTail || *opts.LimitBytes != maxLogBytes ||
		opts.SinceTime == nil || !opts.SinceTime.Time.Equal(now.Add(-10*time.Minute)) {
		t.Errorf("log options = %+v", opts)
	}
	if string(res.Raw) != "line 1\nline 2\nline 2\nline 3\n" {
		t.Errorf("raw = %q", res.Raw)
	}
	mustCall(t, c, "kube_logs", `{"namespace":"shop","pod":"order-7d9f-abc","container":"app","tail":20}`)
	if *opts.TailLines != 20 {
		t.Errorf("tail = %d", *opts.TailLines)
	}

	for _, tc := range []struct{ input, want string }{
		// 边车的 Pod 有两个容器，没有默认容器注解时要指定。
		{`{"namespace":"shop","pod":"order-7d9f-abc"}`, "several containers (app, envoy); pass container"},
		{`{"namespace":"shop","pod":"order-7d9f-abc","container":"nope"}`, `has no container "nope"; its containers are: app, envoy`},
		{`{"namespace":"shop","pod":"order-7d9f-abc","container":"envoy","previous":true}`, "has not restarted, so there is no previous instance"},
		{`{"namespace":"shop","pod":"order-7d9f-abc","container":"app","tail":501}`, "tail must be between 1 and 500"},
		{`{"namespace":"shop","pod":"gone"}`, `pod "gone" in namespace "shop" not found; use kube_get`},
		{`{"pod":"x"}`, "namespace and pod are required"},
	} {
		_, err := call(t, c, "kube_logs", tc.input)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", tc.input, err, tc.want)
		}
	}
}

func TestFormatLogs(t *testing.T) {
	var many []string
	for i := range 300 {
		many = append(many, fmt.Sprintf("line %d", i))
	}
	body := strings.Join(many, "\r\n") + "\r\n"
	got := formatLogs([]byte(body), 200, false)
	if !strings.HasPrefix(got, "fetched 300 lines, showing 200 (100 older lines dropped)\nline 100\n") || !strings.HasSuffix(got, "line 299\n") {
		t.Errorf("tail:\n%.200s", got)
	}

	long := strings.Repeat("y", 2000)
	got = formatLogs([]byte("a\n"+long+"\npartial"), 200, true)
	if strings.Contains(got, "partial") || !strings.Contains(got, "[+1500 bytes]") || !strings.Contains(got, "exceeded 4 MiB") {
		t.Errorf("byte cut:\n%.300s", got)
	}

	if got := formatLogs(nil, 200, false); got != "no log lines.\n" {
		t.Errorf("empty = %q", got)
	}
}

func TestNotFoundAndForbidden(t *testing.T) {
	c := fake.NewClientset(oomPod())
	for _, tc := range []struct{ tool, input, want string }{
		{"kube_describe", `{"kind":"pod","namespace":"shop","name":"x"}`, `pod "x" in namespace "shop" not found; use kube_get to list`},
		{"kube_describe", `{"kind":"pod","name":"x"}`, `pod "x" not found in any namespace`},
		{"kube_get", `{"kind":"deploy","namespace":"shop","name":"x"}`, `deployment "x" in namespace "shop" not found`},
		{"kube_get", `{"kind":"node","name":"x"}`, `node "x" not found`},
		{"kube_get", `{"kind":"pod","selector":"app in ("}`, "not a valid label selector"},
		{"kube_describe", `{"kind":"pod","namespace":"shop"}`, "name is required"},
	} {
		_, err := call(t, c, tc.tool, tc.input)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s %s: err = %v, want %q", tc.tool, tc.input, err, tc.want)
		}
	}

	c.PrependReactor("list", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(k8sschema.GroupResource{Resource: "nodes"}, "", errors.New("RBAC: system:serviceaccount:monitoring:agent"))
	})
	_, err := call(t, c, "kube_get", `{"kind":"nodes"}`)
	if err == nil || !strings.Contains(err.Error(), "the agent is not permitted to read nodes") || strings.Contains(err.Error(), "serviceaccount") {
		t.Errorf("forbidden: %v", err)
	}

	// 超时要能用 errors.Is 认出来。
	c.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("client rate limiter: %w", context.DeadlineExceeded)
	})
	_, err = call(t, c, "kube_describe", `{"kind":"pod","namespace":"shop","name":"order-7d9f-abc"}`)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("deadline: %v", err)
	}
}

func TestAmbiguousName(t *testing.T) {
	c := fake.NewClientset(
		&corev1.Service{Name: "mysql", Namespace: "shop"},
		&corev1.Service{Name: "mysql", Namespace: "pay"},
	)
	_, err := call(t, c, "kube_describe", `{"kind":"svc","name":"mysql"}`)
	if err == nil || !strings.Contains(err.Error(), "several namespaces (pay, shop); pass namespace") {
		t.Errorf("err = %v", err)
	}
}

func TestUnknownArgumentRejected(t *testing.T) {
	c := fake.NewClientset()
	for name, input := range map[string]string{
		"kube_get":      `{"kind":"pod","nmespace":"shop"}`,
		"kube_describe": `{"kind":"pod","name":"x","container":"app"}`,
		"kube_events":   `{"since":"-1h","limit":10}`,
		"kube_logs":     `{"namespace":"shop","pod":"x","lines":10}`,
	} {
		_, err := call(t, c, name, input)
		if err == nil || !strings.Contains(err.Error(), "invalid arguments") {
			t.Errorf("%s %s: err = %v", name, input, err)
		}
	}
}

// TestEveryKind 给白名单里每种资源放一个对象，get 和 describe 都要走得通，输出里要有名字。
func TestEveryKind(t *testing.T) {
	meta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: "shop", CreationTimestamp: ago(time.Hour)}
	}
	two, util := int32(2), int32(70)
	tmpl := corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app:1"}}}}
	sc, port, ready := "local-path", int32(8080), true
	objs := []runtime.Object{
		oomPod(),
		&corev1.Node{
			Name: "worker-1", Labels: map[string]string{"node-role.kubernetes.io/worker": ""},
			Spec: corev1.NodeSpec{Unschedulable: true, Taints: []corev1.Taint{{Key: "k", Effect: corev1.TaintEffectNoSchedule}}},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}, {Type: corev1.NodeMemoryPressure, Status: corev1.ConditionTrue}},
				Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.168.1.11"}},
				NodeInfo:   corev1.NodeSystemInfo{KubeletVersion: "v1.37.0"},
			},
		},
		&appsv1.Deployment{
			ObjectMeta: meta("order"), Spec: appsv1.DeploymentSpec{
				Replicas: &two, Template: tmpl,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "order"}},
			},
			Status: appsv1.DeploymentStatus{ReadyReplicas: 1},
		},
		&appsv1.StatefulSet{ObjectMeta: meta("mysql"), Spec: appsv1.StatefulSetSpec{Replicas: &two, Template: tmpl}},
		&appsv1.DaemonSet{ObjectMeta: meta("agent"), Spec: appsv1.DaemonSetSpec{Template: tmpl}},
		&appsv1.ReplicaSet{ObjectMeta: meta("order-7d9f"), Spec: appsv1.ReplicaSetSpec{Template: tmpl}},
		&batchv1.Job{
			ObjectMeta: meta("backfill"), Spec: batchv1.JobSpec{Template: tmpl},
			Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}},
		},
		&batchv1.CronJob{ObjectMeta: meta("nightly"), Spec: batchv1.CronJobSpec{
			Schedule:    "0 3 * * *",
			JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: tmpl}},
		}},
		&corev1.Service{ObjectMeta: meta("order-svc"), Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP, ClusterIP: "10.43.0.10",
			Selector: map[string]string{"app": "order"}, Ports: []corev1.ServicePort{{Name: "grpc", Port: 9000, Protocol: corev1.ProtocolTCP}},
		}},
		&discoveryv1.EndpointSlice{
			Name: "order-svc-abc", Namespace: "shop",
			Labels: map[string]string{discoveryv1.LabelServiceName: "order-svc"}, AddressType: discoveryv1.AddressTypeIPv4,
			Ports:     []discoveryv1.EndpointPort{{Port: &port}},
			Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.42.1.7"}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}}},
		},
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: meta("order-hpa"), Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Deployment", Name: "order"}, MaxReplicas: 10,
				Metrics: []autoscalingv2.MetricSpec{{Type: autoscalingv2.ResourceMetricSourceType, Resource: &autoscalingv2.ResourceMetricSource{
					Name: corev1.ResourceCPU, Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: &util},
				}}},
			},
			Status: autoscalingv2.HorizontalPodAutoscalerStatus{CurrentMetrics: []autoscalingv2.MetricStatus{{
				Type:     autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricStatus{Name: corev1.ResourceCPU, Current: autoscalingv2.MetricValueStatus{AverageUtilization: &two}},
			}}},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: meta("data-mysql-0"), Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &sc},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		event("shop", "ev1", "Pod", "order-7d9f-abc", "BackOff", "Warning", 2, time.Minute, "back-off"),
	}
	c := fake.NewClientset(objs...)
	for _, tc := range []struct{ kind, name, wantGet, wantDescribe string }{
		{"Pod", "order-7d9f-abc", "CrashLoopBackOff", "OOMKilled"},
		{"Node", "worker-1", "Ready,MemoryPressure,SchedulingDisabled  worker", "Taints: k:NoSchedule"},
		{"Deployment", "order", "1/2", "Selector: app=order"},
		{"StatefulSet", "mysql", "0/2", "Pod template:"},
		{"DaemonSet", "agent", "app:1", "Pod template:"},
		{"ReplicaSet", "order-7d9f", "order-7d9f", "Pod template:"},
		{"Job", "backfill", "Failed", "Failed=True (BackoffLimitExceeded)"},
		{"CronJob", "nightly", "0 3 * * *", "Schedule: 0 3 * * *"},
		{"Service", "order-svc", "9000/TCP", "grpc 9000/TCP"},
		{"EndpointSlice", "order-svc-abc", "1/1 ready", "10.42.1.7 ready=true"},
		{"HorizontalPodAutoscaler", "order-hpa", "cpu: 2%/70%", "Target: Deployment/order"},
		{"PersistentVolumeClaim", "data-mysql-0", "Bound", "storage class: local-path"},
		{"Event", "ev1", "BackOff", "Message: back-off"},
	} {
		ns := `,"namespace":"shop"`
		if tc.kind == "Node" {
			ns = ""
		}
		got := mustCall(t, c, "kube_get", fmt.Sprintf(`{"kind":%q%s}`, tc.kind, ns)).Text
		// 事件的行里给的是它说的对象，不是事件自己的名字。
		if (tc.kind != "Event" && !strings.Contains(got, tc.name)) || !strings.Contains(got, tc.wantGet) {
			t.Errorf("get %s: want %q:\n%s", tc.kind, tc.wantGet, got)
		}
		got = mustCall(t, c, "kube_describe", fmt.Sprintf(`{"kind":%q%s,"name":%q}`, tc.kind, ns, tc.name)).Text
		if !strings.Contains(got, tc.kind+": "+tc.name) || !strings.Contains(got, tc.wantDescribe) {
			t.Errorf("describe %s: want %q:\n%s", tc.kind, tc.wantDescribe, got)
		}
	}
}

func TestConcurrentCalls(t *testing.T) {
	c := fake.NewClientset(oomPod(), event("shop", "e", "Pod", "order-7d9f-abc", "BackOff", "Warning", 1, time.Minute, "m"))
	ts := tools(t, c)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for _, in := range []struct{ tool, input string }{
				{"kube_get", `{"kind":"pod"}`},
				{"kube_describe", `{"kind":"pod","namespace":"shop","name":"order-7d9f-abc"}`},
				{"kube_events", `{}`},
				{"kube_logs", `{"namespace":"shop","pod":"order-7d9f-abc","container":"app"}`},
			} {
				if _, err := ts[in.tool].Call(t.Context(), jsontext.Value(in.input)); err != nil {
					t.Errorf("%s: %v", in.tool, err)
				}
			}
		})
	}
	wg.Wait()
}
