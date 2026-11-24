package kube

import (
	"encoding/json/jsontext"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

// hiddenCall 用带着隐藏名单的工具调一次。
func hiddenCall(t *testing.T, c *fake.Clientset, name, input string) (tool.Result, error) {
	t.Helper()
	cfg := Config{Client: c, Now: func() time.Time { return now }, Hidden: tool.NewHidden("chaos-mesh", "chaos-mesh.org")}
	for _, tl := range New(cfg) {
		if tl.Def().Name == name {
			return tl.Call(t.Context(), jsontext.Value(input))
		}
	}
	t.Fatalf("no tool %s", name)
	return tool.Result{}, nil
}

func pod(ns, name string) *corev1.Pod {
	return &corev1.Pod{
		Name: name, Namespace: ns, CreationTimestamp: ago(time.Hour),
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "img:1"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// hiddenFixture：一个业务 Pod，隐藏的命名空间里一个 Pod 和一条事件，再加注入工具在业务命名空间里留下的一条事件。
func hiddenFixture() *fake.Clientset {
	trace := event("shop", "trace", "PodNetworkChaos", "order-1", "Updated", "Normal", 3, time.Minute, "Successfully update ObservedGeneration to 4 of resource")
	trace.InvolvedObject.APIVersion = "chaos-mesh.org/v1alpha1"
	plain := event("shop", "plain", "Pod", "order-1", "BackOff", "Warning", 2, 2*time.Minute, "Back-off restarting failed container")
	plain.InvolvedObject.APIVersion = "v1"
	inside := event("chaos-mesh", "inside", "Pod", "chaos-daemon-x", "Pulled", "Normal", 1, time.Minute, "image pulled")
	return fake.NewClientset(pod("shop", "order-1"), pod("chaos-mesh", "chaos-daemon-x"), trace, plain, inside)
}

// 没有指名的调用：结果里（给模型的和落盘的）都不能出现隐藏的东西，别的照常在。
func TestHiddenNeverListed(t *testing.T) {
	for _, c := range []struct{ name, input, keep string }{
		{"kube_get", `{"kind":"pod"}`, "order-1"},
		{"kube_get", `{"kind":"event"}`, "BackOff"},
		{"kube_get", `{"kind":"event","namespace":"shop"}`, "BackOff"},
		{"kube_events", `{}`, "BackOff"},
		{"kube_events", `{"namespace":"shop"}`, "BackOff"},
		{"kube_describe", `{"kind":"pod","name":"order-1"}`, "BackOff"},
	} {
		res, err := hiddenCall(t, hiddenFixture(), c.name, c.input)
		if err != nil {
			t.Errorf("%s %s: %v", c.name, c.input, err)
			continue
		}
		for _, out := range []string{res.Text, string(res.Raw)} {
			if strings.Contains(strings.ToLower(out), "chaos") {
				t.Errorf("%s %s leaks the hidden part:\n%s", c.name, c.input, out)
			}
		}
		if !strings.Contains(res.Text, c.keep) {
			t.Errorf("%s %s lost %q:\n%s", c.name, c.input, c.keep, res.Text)
		}
	}
	// 不带隐藏名单时这些都看得到：上面的断言不是因为夹具里本来就没有。
	if text := mustCall(t, hiddenFixture(), "kube_events", `{}`).Text; !strings.Contains(text, "PodNetworkChaos") || !strings.Contains(text, "chaos-daemon-x") {
		t.Fatalf("without a hidden list the fixture's events should show:\n%s", text)
	}
}

// 指名去读隐藏的命名空间：回答要和一个真的不存在的命名空间一字不差（只差那个名字），
// 否则“不让读”本身就告诉了模型这里有东西。
func TestHiddenLooksNonexistent(t *testing.T) {
	for _, c := range []struct{ name, input string }{
		{"kube_get", `{"kind":"pod","namespace":"NS"}`},
		{"kube_get", `{"kind":"pod","namespace":"NS","name":"chaos-daemon-x"}`},
		{"kube_get", `{"kind":"event","namespace":"NS"}`},
		{"kube_describe", `{"kind":"pod","namespace":"NS","name":"chaos-daemon-x"}`},
		{"kube_describe", `{"kind":"pod","name":"chaos-daemon-x"}`},
		{"kube_events", `{"namespace":"NS"}`},
		{"kube_events", `{"kind":"PodNetworkChaos"}`},
		{"kube_logs", `{"namespace":"NS","pod":"chaos-daemon-x"}`},
	} {
		answer := func(ns string) string {
			// 对照的那一边：同样的夹具去掉隐藏命名空间里的东西，命名空间换成一个不存在的。
			c2 := fake.NewClientset(pod("shop", "order-1"))
			cl := hiddenFixture()
			if ns != "chaos-mesh" {
				cl = c2
			}
			res, err := hiddenCall(t, cl, c.name, strings.ReplaceAll(c.input, "NS", ns))
			out := res.Text
			if err != nil {
				out = "error: " + err.Error()
			}
			return strings.ReplaceAll(out, ns, "NS")
		}
		if got, want := answer("chaos-mesh"), answer("nowhere"); got != want {
			t.Errorf("%s %s:\nhidden:      %s\nnonexistent: %s", c.name, c.input, got, want)
		}
	}
}

// 按名字取一条关于隐藏对象的事件，也是找不到。
func TestHiddenEventByName(t *testing.T) {
	_, err := hiddenCall(t, hiddenFixture(), "kube_get", `{"kind":"event","namespace":"shop","name":"trace"}`)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err = %v, want not found", err)
	}
	if _, err := hiddenCall(t, hiddenFixture(), "kube_get", `{"kind":"event","namespace":"shop","name":"plain"}`); err != nil {
		t.Fatalf("a plain event must still be readable: %v", err)
	}
}
