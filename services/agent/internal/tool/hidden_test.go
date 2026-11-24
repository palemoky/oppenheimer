package tool

import "testing"

func TestHidden(t *testing.T) {
	h := NewHidden(" chaos-mesh, litmus ,", "chaos-mesh.org")
	for _, c := range []struct {
		name string
		got  bool
		want bool
	}{
		{"hidden namespace", h.Namespace("chaos-mesh"), true},
		{"second hidden namespace", h.Namespace("litmus"), true},
		{"other namespace", h.Namespace("oppenheimer"), false},
		{"prefix is not a match", h.Namespace("chaos-mesh-2"), false},
		{"empty namespace (cluster-scoped, all namespaces)", h.Namespace(""), false},
		{"hidden group", h.APIVersion("chaos-mesh.org/v1alpha1"), true},
		{"subdomain of a hidden group", h.APIVersion("x.chaos-mesh.org/v1"), true},
		{"suffix without a dot is another group", h.APIVersion("notchaos-mesh.org/v1"), false},
		{"core group", h.APIVersion("v1"), false},
		{"other group", h.APIVersion("apps/v1"), false},
		{"namespace label", h.Labels(map[string]string{"namespace": "chaos-mesh"}), true},
		{"otlp namespace label", h.Labels(map[string]string{"k8s_namespace_name": "chaos-mesh", "pod": "x"}), true},
		{"exported namespace label", h.Labels(map[string]string{"namespace": "monitoring", "exported_namespace": "chaos-mesh"}), true},
		{"the value on another label does not count", h.Labels(map[string]string{"app": "chaos-mesh"}), false},
		{"no labels", h.Labels(nil), false},
	} {
		if c.got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestNilHiddenHidesNothing(t *testing.T) {
	if h := NewHidden("", " , "); h != nil {
		t.Fatalf("NewHidden with nothing to hide = %+v, want nil", h)
	}
	var h *Hidden
	if h.Namespace("chaos-mesh") || h.APIVersion("chaos-mesh.org/v1alpha1") || h.Labels(map[string]string{"namespace": "chaos-mesh"}) {
		t.Fatal("a nil Hidden must hide nothing")
	}
}
