package kube

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 这个文件是每种资源的投影：列表里的一行（像 kubectl get -o wide），describe 的正文（裁过的 kubectl describe）。
// 字段是手工挑的。环境变量只给名字和来源，永远不给值；卷只给名字不给细节；注解的值超过 80 字节只给键。

const none = "<none>"

func orNone(s string) string {
	if s == "" {
		return none
	}
	return s
}

func i32(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

// kv 把标签、选择器这类小 map 按键排序写成 k=v,k2=v2。
func kv(m map[string]string) string {
	if len(m) == 0 {
		return none
	}
	parts := make([]string, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ",")
}

// resources 把 requests 或 limits 写成 cpu=100m, memory=128Mi。
func resources(rl corev1.ResourceList) string {
	if len(rl) == 0 {
		return none
	}
	parts := make([]string, 0, len(rl))
	for _, k := range slices.Sorted(maps.Keys(rl)) {
		q := rl[k]
		parts = append(parts, string(k)+"="+q.String())
	}
	return strings.Join(parts, ", ")
}

func selectorString(s *metav1.LabelSelector) string {
	if s == nil {
		return none
	}
	sel, err := metav1.LabelSelectorAsSelector(s)
	if err != nil || sel.Empty() {
		return none
	}
	return sel.String()
}

// images 是模板里各容器的镜像，逗号分隔。
func images(ps *corev1.PodSpec) string {
	out := make([]string, len(ps.Containers))
	for i, c := range ps.Containers {
		out[i] = c.Image
	}
	return strings.Join(out, ",")
}

// writeMeta 是 describe 开头的公共部分。注解只给键，值短于 80 字节的才给值（大块的注解没有诊断价值还占地方）。
func writeMeta(b *strings.Builder, kind string, o object, now time.Time) {
	fmt.Fprintf(b, "%s: %s\n", kind, o.GetName())
	if ns := o.GetNamespace(); ns != "" {
		fmt.Fprintf(b, "Namespace: %s\n", ns)
	}
	fmt.Fprintf(b, "Created: %s\n", stamp(o.GetCreationTimestamp().Time, now))
	if dt := o.GetDeletionTimestamp(); dt != nil {
		fmt.Fprintf(b, "Deleting since: %s\n", stamp(dt.Time, now))
	}
	fmt.Fprintf(b, "Labels: %s\n", kv(o.GetLabels()))
	if ann := o.GetAnnotations(); len(ann) > 0 {
		parts := make([]string, 0, len(ann))
		for _, k := range slices.Sorted(maps.Keys(ann)) {
			if v := ann[k]; len(v) < maxAnnotationValue && !strings.ContainsAny(v, "\n") {
				parts = append(parts, k+"="+v)
			} else {
				parts = append(parts, k+" (value omitted, "+strconv.Itoa(len(v))+" bytes)")
			}
		}
		fmt.Fprintf(b, "Annotations: %s\n", strings.Join(parts, "; "))
	}
	if owners := o.GetOwnerReferences(); len(owners) > 0 {
		parts := make([]string, len(owners))
		for i, r := range owners {
			parts[i] = r.Kind + "/" + r.Name
		}
		fmt.Fprintf(b, "Controlled by: %s\n", strings.Join(parts, ", "))
	}
}

// cond 是各种资源的 condition 的公共形状。
type cond struct {
	typ, status, reason, message string
	since                        time.Time
}

func writeConditions(b *strings.Builder, conds []cond, now time.Time) {
	if len(conds) == 0 {
		return
	}
	b.WriteString("Conditions:\n")
	for _, c := range conds {
		fmt.Fprintf(b, "  %s=%s", c.typ, c.status)
		if c.reason != "" {
			fmt.Fprintf(b, " (%s)", c.reason)
		}
		if !c.since.IsZero() {
			fmt.Fprintf(b, " since %s", stamp(c.since, now))
		}
		if c.message != "" {
			fmt.Fprintf(b, ": %s", cut(c.message, maxMessageBytes))
		}
		b.WriteString("\n")
	}
}

// ---- Pod ----

// podCounts 统计就绪的容器数、重启次数和最近一次重启（上一个实例结束）的时间。
func podCounts(p *corev1.Pod) (ready, total int, restarts int32, lastRestart time.Time) {
	total = len(p.Spec.Containers)
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Ready {
			ready++
		}
		restarts += cs.RestartCount
		if t := cs.LastTerminationState.Terminated; t != nil && t.FinishedAt.After(lastRestart) {
			lastRestart = t.FinishedAt.Time
		}
	}
	return ready, total, restarts, lastRestart
}

// podStatus 是 kubectl get pods 的 STATUS 列的简化版：等待或退出的原因优先于 phase。
func podStatus(p *corev1.Pod) string {
	if p.DeletionTimestamp != nil {
		return "Terminating"
	}
	status := string(p.Status.Phase)
	if p.Status.Reason != "" {
		status = p.Status.Reason
	}
	for _, cs := range p.Status.InitContainerStatuses {
		switch {
		case cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0:
			return "Init:" + cmp.Or(cs.State.Terminated.Reason, "Error")
		case cs.State.Waiting != nil && cs.State.Waiting.Reason != "" && cs.State.Waiting.Reason != "PodInitializing":
			return "Init:" + cs.State.Waiting.Reason
		}
	}
	for _, cs := range p.Status.ContainerStatuses {
		switch {
		case cs.State.Waiting != nil && cs.State.Waiting.Reason != "":
			return cs.State.Waiting.Reason
		case cs.State.Terminated != nil && cs.State.Terminated.Reason != "":
			status = cs.State.Terminated.Reason
		}
	}
	return status
}

func podRow(p *corev1.Pod, now time.Time) []string {
	ready, total, restarts, last := podCounts(p)
	r := strconv.Itoa(int(restarts))
	if !last.IsZero() {
		r += " (" + age(last, now) + " ago)"
	}
	return []string{
		p.Name, fmt.Sprintf("%d/%d", ready, total), podStatus(p), r,
		age(p.CreationTimestamp.Time, now), orNone(p.Spec.NodeName), orNone(p.Status.PodIP),
	}
}

// podRank：没就绪的排最前，其次是重启过的，最后是正常的。跑完的（Succeeded）算正常。
func podRank(p *corev1.Pod) int {
	ready, total, restarts, _ := podCounts(p)
	switch {
	case p.Status.Phase == corev1.PodSucceeded:
		return 2
	case ready < total || p.Status.Phase != corev1.PodRunning || p.DeletionTimestamp != nil:
		return 0
	case restarts > 0:
		return 1
	}
	return 2
}

func podOrder(a, b object) int {
	pa, pb := a.(*corev1.Pod), b.(*corev1.Pod)
	if c := cmp.Compare(podRank(pa), podRank(pb)); c != 0 {
		return c
	}
	_, _, ra, _ := podCounts(pa)
	_, _, rb, _ := podCounts(pb)
	if c := cmp.Compare(rb, ra); c != 0 {
		return c
	}
	return byNamespaceName(a, b)
}

func describePod(b *strings.Builder, p *corev1.Pod, now time.Time) {
	ready, total, restarts, _ := podCounts(p)
	fmt.Fprintf(b, "Node: %s\n", orNone(p.Spec.NodeName))
	fmt.Fprintf(b, "Phase: %s, status: %s, ready: %d/%d, restarts: %d\n", p.Status.Phase, podStatus(p), ready, total, restarts)
	if p.Status.Message != "" {
		fmt.Fprintf(b, "Message: %s\n", cut(p.Status.Message, maxMessageBytes))
	}
	fmt.Fprintf(b, "Pod IP: %s, QoS: %s, priority class: %s\n", orNone(p.Status.PodIP), orNone(string(p.Status.QOSClass)), orNone(p.Spec.PriorityClassName))
	if p.Status.StartTime != nil {
		fmt.Fprintf(b, "Started: %s\n", stamp(p.Status.StartTime.Time, now))
	}
	conds := make([]cond, len(p.Status.Conditions))
	for i, c := range p.Status.Conditions {
		conds[i] = cond{string(c.Type), string(c.Status), c.Reason, c.Message, c.LastTransitionTime.Time}
	}
	writeConditions(b, conds, now)
	statuses := make(map[string]corev1.ContainerStatus)
	for _, cs := range slices.Concat(p.Status.InitContainerStatuses, p.Status.ContainerStatuses) {
		statuses[cs.Name] = cs
	}
	writePodSpec(b, &p.Spec, statuses, now)
}

// writePodSpec 写 Pod 规格里有诊断价值的部分：每个容器的镜像、状态、资源、环境变量的名字。
// statuses 为 nil 时（工作负载的模板）不写状态。
func writePodSpec(b *strings.Builder, ps *corev1.PodSpec, statuses map[string]corev1.ContainerStatus, now time.Time) {
	if ps.ServiceAccountName != "" {
		fmt.Fprintf(b, "Service account: %s\n", ps.ServiceAccountName)
	}
	if len(ps.NodeSelector) > 0 {
		fmt.Fprintf(b, "Node selector: %s\n", kv(ps.NodeSelector))
	}
	if len(ps.Volumes) > 0 {
		names := make([]string, len(ps.Volumes))
		for i, v := range ps.Volumes {
			names[i] = v.Name
		}
		fmt.Fprintf(b, "Volumes: %s\n", strings.Join(names, ", "))
	}
	if len(ps.InitContainers) > 0 {
		b.WriteString("Init containers:\n")
		for i := range ps.InitContainers {
			writeContainer(b, &ps.InitContainers[i], statuses, now)
		}
	}
	b.WriteString("Containers:\n")
	for i := range ps.Containers {
		writeContainer(b, &ps.Containers[i], statuses, now)
	}
}

func writeContainer(b *strings.Builder, c *corev1.Container, statuses map[string]corev1.ContainerStatus, now time.Time) {
	fmt.Fprintf(b, "  - %s\n", c.Name)
	fmt.Fprintf(b, "    Image: %s\n", c.Image)
	if cs, ok := statuses[c.Name]; ok {
		fmt.Fprintf(b, "    Ready: %t, restart count: %d\n", cs.Ready, cs.RestartCount)
		fmt.Fprintf(b, "    State: %s\n", stateString(cs.State, now))
		if t := cs.LastTerminationState.Terminated; t != nil {
			fmt.Fprintf(b, "    Last termination: %s\n", terminatedString(t, now))
		}
	} else if statuses != nil {
		b.WriteString("    State: <no status reported>\n")
	}
	if c.RestartPolicy != nil {
		fmt.Fprintf(b, "    Restart policy: %s\n", *c.RestartPolicy)
	}
	fmt.Fprintf(b, "    Requests: %s\n", resources(c.Resources.Requests))
	fmt.Fprintf(b, "    Limits: %s\n", resources(c.Resources.Limits))
	if len(c.Ports) > 0 {
		ports := make([]string, len(c.Ports))
		for i, p := range c.Ports {
			ports[i] = fmt.Sprintf("%d/%s", p.ContainerPort, p.Protocol)
			if p.Name != "" {
				ports[i] = p.Name + ":" + ports[i]
			}
		}
		fmt.Fprintf(b, "    Ports: %s\n", strings.Join(ports, ", "))
	}
	if c.LivenessProbe != nil {
		fmt.Fprintf(b, "    Liveness: %s\n", probeString(c.LivenessProbe))
	}
	if c.ReadinessProbe != nil {
		fmt.Fprintf(b, "    Readiness: %s\n", probeString(c.ReadinessProbe))
	}
	if len(c.Env) > 0 {
		fmt.Fprintf(b, "    Env (names only, values are never shown): %s\n", envNames(c.Env))
	}
	if len(c.EnvFrom) > 0 {
		parts := make([]string, 0, len(c.EnvFrom))
		for _, e := range c.EnvFrom {
			switch {
			case e.SecretRef != nil:
				parts = append(parts, "secret "+e.SecretRef.Name)
			case e.ConfigMapRef != nil:
				parts = append(parts, "configMap "+e.ConfigMapRef.Name)
			}
		}
		fmt.Fprintf(b, "    Env from: %s\n", strings.Join(parts, ", "))
	}
}

// envNames 只给环境变量的名字；valueFrom 的给来源的种类和名字。值永远不出现。
func envNames(env []corev1.EnvVar) string {
	parts := make([]string, len(env))
	for i, e := range env {
		parts[i] = e.Name
		vf := e.ValueFrom
		switch {
		case vf == nil:
		case vf.SecretKeyRef != nil:
			parts[i] += " (from secret " + vf.SecretKeyRef.Name + ")"
		case vf.ConfigMapKeyRef != nil:
			parts[i] += " (from configMap " + vf.ConfigMapKeyRef.Name + ")"
		case vf.FieldRef != nil:
			parts[i] += " (from field " + vf.FieldRef.FieldPath + ")"
		case vf.ResourceFieldRef != nil:
			parts[i] += " (from resource " + vf.ResourceFieldRef.Resource + ")"
		default:
			parts[i] += " (from another source)"
		}
	}
	return strings.Join(parts, ", ")
}

func probeString(p *corev1.Probe) string {
	var how string
	switch {
	case p.HTTPGet != nil:
		how = fmt.Sprintf("http-get :%s%s", p.HTTPGet.Port.String(), p.HTTPGet.Path)
	case p.TCPSocket != nil:
		how = "tcp :" + p.TCPSocket.Port.String()
	case p.GRPC != nil:
		how = fmt.Sprintf("grpc :%d", p.GRPC.Port)
	case p.Exec != nil:
		how = "exec"
	}
	return fmt.Sprintf("%s delay=%ds timeout=%ds period=%ds failure=%d", how,
		p.InitialDelaySeconds, p.TimeoutSeconds, p.PeriodSeconds, p.FailureThreshold)
}

func stateString(s corev1.ContainerState, now time.Time) string {
	switch {
	case s.Running != nil:
		return "Running since " + stamp(s.Running.StartedAt.Time, now)
	case s.Waiting != nil:
		out := "Waiting (" + orNone(s.Waiting.Reason)
		if s.Waiting.Message != "" {
			out += ": " + cut(s.Waiting.Message, maxMessageBytes)
		}
		return out + ")"
	case s.Terminated != nil:
		return "Terminated: " + terminatedString(s.Terminated, now)
	}
	return "<unknown>"
}

// terminatedString 是一次退出：原因、退出码、结束时间。OOMKilled 就是从这里看出来的（reason OOMKilled，exit code 137）。
func terminatedString(t *corev1.ContainerStateTerminated, now time.Time) string {
	out := fmt.Sprintf("%s, exit code %d", cmp.Or(t.Reason, "Terminated"), t.ExitCode)
	if t.Signal != 0 {
		out += fmt.Sprintf(", signal %d", t.Signal)
	}
	if !t.StartedAt.IsZero() {
		out += ", started " + stamp(t.StartedAt.Time, now)
	}
	out += ", finished " + stamp(t.FinishedAt.Time, now)
	if t.Message != "" {
		out += ", message: " + cut(t.Message, maxMessageBytes)
	}
	return out
}

// ---- Node ----

func nodeStatus(n *corev1.Node) string {
	status := "Unknown"
	var pressure []string
	for _, c := range n.Status.Conditions {
		switch {
		case c.Type == corev1.NodeReady:
			if c.Status == corev1.ConditionTrue {
				status = "Ready"
			} else {
				status = "NotReady"
			}
		case c.Status == corev1.ConditionTrue:
			pressure = append(pressure, string(c.Type))
		}
	}
	if n.Spec.Unschedulable {
		pressure = append(pressure, "SchedulingDisabled")
	}
	return strings.Join(append([]string{status}, pressure...), ",")
}

func nodeRoles(n *corev1.Node) string {
	var roles []string
	for k := range n.Labels {
		if r, ok := strings.CutPrefix(k, "node-role.kubernetes.io/"); ok && r != "" {
			roles = append(roles, r)
		}
	}
	slices.Sort(roles)
	return orNone(strings.Join(roles, ","))
}

func nodeAddress(n *corev1.Node, t corev1.NodeAddressType) string {
	for _, a := range n.Status.Addresses {
		if a.Type == t {
			return a.Address
		}
	}
	return none
}

func nodeRow(n *corev1.Node, now time.Time) []string {
	return []string{
		n.Name, nodeStatus(n), nodeRoles(n), age(n.CreationTimestamp.Time, now),
		n.Status.NodeInfo.KubeletVersion, nodeAddress(n, corev1.NodeInternalIP),
	}
}

func describeNode(b *strings.Builder, n *corev1.Node, now time.Time) {
	fmt.Fprintf(b, "Status: %s, roles: %s\n", nodeStatus(n), nodeRoles(n))
	fmt.Fprintf(b, "Internal IP: %s\n", nodeAddress(n, corev1.NodeInternalIP))
	if len(n.Spec.Taints) > 0 {
		taints := make([]string, len(n.Spec.Taints))
		for i, t := range n.Spec.Taints {
			taints[i] = t.ToString()
		}
		fmt.Fprintf(b, "Taints: %s\n", strings.Join(taints, ", "))
	}
	conds := make([]cond, len(n.Status.Conditions))
	for i, c := range n.Status.Conditions {
		conds[i] = cond{string(c.Type), string(c.Status), c.Reason, c.Message, c.LastTransitionTime.Time}
	}
	writeConditions(b, conds, now)
	fmt.Fprintf(b, "Capacity: %s\n", resources(n.Status.Capacity))
	fmt.Fprintf(b, "Allocatable: %s\n", resources(n.Status.Allocatable))
	ni := n.Status.NodeInfo
	fmt.Fprintf(b, "System: kubelet %s, %s, kernel %s, %s, %s/%s\n",
		ni.KubeletVersion, ni.OSImage, ni.KernelVersion, ni.ContainerRuntimeVersion, ni.OperatingSystem, ni.Architecture)
}

// ---- 工作负载 ----

func deploymentRow(d *appsv1.Deployment, now time.Time) []string {
	return []string{
		d.Name, fmt.Sprintf("%d/%d", d.Status.ReadyReplicas, i32(d.Spec.Replicas)),
		strconv.Itoa(int(d.Status.UpdatedReplicas)), strconv.Itoa(int(d.Status.AvailableReplicas)),
		age(d.CreationTimestamp.Time, now), images(&d.Spec.Template.Spec),
	}
}

func describeDeployment(b *strings.Builder, d *appsv1.Deployment, now time.Time) {
	s := d.Status
	fmt.Fprintf(b, "Replicas: %d desired, %d updated, %d total, %d ready, %d available, %d unavailable\n",
		i32(d.Spec.Replicas), s.UpdatedReplicas, s.Replicas, s.ReadyReplicas, s.AvailableReplicas, s.UnavailableReplicas)
	fmt.Fprintf(b, "Strategy: %s, paused: %t, generation: %d observed %d\n", d.Spec.Strategy.Type, d.Spec.Paused, d.Generation, s.ObservedGeneration)
	fmt.Fprintf(b, "Selector: %s\n", selectorString(d.Spec.Selector))
	conds := make([]cond, len(s.Conditions))
	for i, c := range s.Conditions {
		conds[i] = cond{string(c.Type), string(c.Status), c.Reason, c.Message, c.LastTransitionTime.Time}
	}
	writeConditions(b, conds, now)
	b.WriteString("Pod template:\n")
	writePodSpec(b, &d.Spec.Template.Spec, nil, now)
}

func statefulSetRow(s *appsv1.StatefulSet, now time.Time) []string {
	return []string{
		s.Name, fmt.Sprintf("%d/%d", s.Status.ReadyReplicas, i32(s.Spec.Replicas)),
		strconv.Itoa(int(s.Status.UpdatedReplicas)), age(s.CreationTimestamp.Time, now), images(&s.Spec.Template.Spec),
	}
}

func describeStatefulSet(b *strings.Builder, s *appsv1.StatefulSet, now time.Time) {
	st := s.Status
	fmt.Fprintf(b, "Replicas: %d desired, %d current, %d updated, %d ready, %d available\n",
		i32(s.Spec.Replicas), st.CurrentReplicas, st.UpdatedReplicas, st.ReadyReplicas, st.AvailableReplicas)
	fmt.Fprintf(b, "Revisions: current %s, update %s\n", orNone(st.CurrentRevision), orNone(st.UpdateRevision))
	fmt.Fprintf(b, "Service: %s, update strategy: %s, pod management: %s\n", orNone(s.Spec.ServiceName), s.Spec.UpdateStrategy.Type, s.Spec.PodManagementPolicy)
	fmt.Fprintf(b, "Selector: %s\n", selectorString(s.Spec.Selector))
	if len(s.Spec.VolumeClaimTemplates) > 0 {
		names := make([]string, len(s.Spec.VolumeClaimTemplates))
		for i, v := range s.Spec.VolumeClaimTemplates {
			q := v.Spec.Resources.Requests[corev1.ResourceStorage]
			names[i] = v.Name + " (" + q.String() + ")"
		}
		fmt.Fprintf(b, "Volume claim templates: %s\n", strings.Join(names, ", "))
	}
	conds := make([]cond, len(st.Conditions))
	for i, c := range st.Conditions {
		conds[i] = cond{string(c.Type), string(c.Status), c.Reason, c.Message, c.LastTransitionTime.Time}
	}
	writeConditions(b, conds, now)
	b.WriteString("Pod template:\n")
	writePodSpec(b, &s.Spec.Template.Spec, nil, now)
}

func daemonSetRow(d *appsv1.DaemonSet, now time.Time) []string {
	s := d.Status
	return []string{
		d.Name, strconv.Itoa(int(s.DesiredNumberScheduled)), strconv.Itoa(int(s.CurrentNumberScheduled)),
		strconv.Itoa(int(s.NumberReady)), strconv.Itoa(int(s.UpdatedNumberScheduled)), strconv.Itoa(int(s.NumberAvailable)),
		age(d.CreationTimestamp.Time, now), images(&d.Spec.Template.Spec),
	}
}

func describeDaemonSet(b *strings.Builder, d *appsv1.DaemonSet, now time.Time) {
	s := d.Status
	fmt.Fprintf(b, "Pods: %d desired, %d current, %d ready, %d updated, %d available, %d unavailable, %d misscheduled\n",
		s.DesiredNumberScheduled, s.CurrentNumberScheduled, s.NumberReady, s.UpdatedNumberScheduled,
		s.NumberAvailable, s.NumberUnavailable, s.NumberMisscheduled)
	fmt.Fprintf(b, "Update strategy: %s\n", d.Spec.UpdateStrategy.Type)
	fmt.Fprintf(b, "Selector: %s\n", selectorString(d.Spec.Selector))
	conds := make([]cond, len(s.Conditions))
	for i, c := range s.Conditions {
		conds[i] = cond{string(c.Type), string(c.Status), c.Reason, c.Message, c.LastTransitionTime.Time}
	}
	writeConditions(b, conds, now)
	b.WriteString("Pod template:\n")
	writePodSpec(b, &d.Spec.Template.Spec, nil, now)
}

func owner(o object) string {
	for _, r := range o.GetOwnerReferences() {
		if r.Controller != nil && *r.Controller {
			return r.Kind + "/" + r.Name
		}
	}
	return none
}

func replicaSetRow(r *appsv1.ReplicaSet, now time.Time) []string {
	return []string{
		r.Name, strconv.Itoa(int(i32(r.Spec.Replicas))), strconv.Itoa(int(r.Status.Replicas)),
		strconv.Itoa(int(r.Status.ReadyReplicas)), age(r.CreationTimestamp.Time, now), owner(r),
	}
}

func describeReplicaSet(b *strings.Builder, r *appsv1.ReplicaSet, now time.Time) {
	s := r.Status
	fmt.Fprintf(b, "Replicas: %d desired, %d current, %d ready, %d available\n",
		i32(r.Spec.Replicas), s.Replicas, s.ReadyReplicas, s.AvailableReplicas)
	fmt.Fprintf(b, "Selector: %s\n", selectorString(r.Spec.Selector))
	conds := make([]cond, len(s.Conditions))
	for i, c := range s.Conditions {
		conds[i] = cond{string(c.Type), string(c.Status), c.Reason, c.Message, c.LastTransitionTime.Time}
	}
	writeConditions(b, conds, now)
	b.WriteString("Pod template:\n")
	writePodSpec(b, &r.Spec.Template.Spec, nil, now)
}

func jobStatus(j *batchv1.Job) string {
	for _, c := range j.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete, batchv1.JobFailed, batchv1.JobSuspended:
			return string(c.Type)
		case batchv1.JobFailureTarget:
			return "Failing"
		case batchv1.JobSuccessCriteriaMet:
			return "Completing"
		}
	}
	return "Running"
}

func jobRow(j *batchv1.Job, now time.Time) []string {
	completions := "-"
	if j.Spec.Completions != nil {
		completions = strconv.Itoa(int(*j.Spec.Completions))
	}
	return []string{
		j.Name, jobStatus(j), fmt.Sprintf("%d/%s", j.Status.Succeeded, completions),
		strconv.Itoa(int(j.Status.Active)), strconv.Itoa(int(j.Status.Failed)), age(j.CreationTimestamp.Time, now),
	}
}

func describeJob(b *strings.Builder, j *batchv1.Job, now time.Time) {
	s := j.Status
	fmt.Fprintf(b, "Status: %s; %d active, %d succeeded, %d failed\n", jobStatus(j), s.Active, s.Succeeded, s.Failed)
	fmt.Fprintf(b, "Completions: %d, parallelism: %d, backoff limit: %d\n", i32(j.Spec.Completions), i32(j.Spec.Parallelism), i32(j.Spec.BackoffLimit))
	if s.StartTime != nil {
		fmt.Fprintf(b, "Started: %s\n", stamp(s.StartTime.Time, now))
	}
	if s.CompletionTime != nil {
		fmt.Fprintf(b, "Completed: %s\n", stamp(s.CompletionTime.Time, now))
	}
	conds := make([]cond, len(s.Conditions))
	for i, c := range s.Conditions {
		conds[i] = cond{string(c.Type), string(c.Status), c.Reason, c.Message, c.LastTransitionTime.Time}
	}
	writeConditions(b, conds, now)
	b.WriteString("Pod template:\n")
	writePodSpec(b, &j.Spec.Template.Spec, nil, now)
}

func cronJobRow(c *batchv1.CronJob, now time.Time) []string {
	last := none
	if c.Status.LastScheduleTime != nil {
		last = age(c.Status.LastScheduleTime.Time, now) + " ago"
	}
	suspend := c.Spec.Suspend != nil && *c.Spec.Suspend
	return []string{
		c.Name, c.Spec.Schedule, strconv.FormatBool(suspend), strconv.Itoa(len(c.Status.Active)),
		last, age(c.CreationTimestamp.Time, now),
	}
}

func describeCronJob(b *strings.Builder, c *batchv1.CronJob, now time.Time) {
	suspend := c.Spec.Suspend != nil && *c.Spec.Suspend
	fmt.Fprintf(b, "Schedule: %s, suspend: %t, concurrency: %s\n", c.Spec.Schedule, suspend, c.Spec.ConcurrencyPolicy)
	if t := c.Status.LastScheduleTime; t != nil {
		fmt.Fprintf(b, "Last schedule: %s\n", stamp(t.Time, now))
	}
	if t := c.Status.LastSuccessfulTime; t != nil {
		fmt.Fprintf(b, "Last success: %s\n", stamp(t.Time, now))
	}
	if len(c.Status.Active) > 0 {
		names := make([]string, len(c.Status.Active))
		for i, r := range c.Status.Active {
			names[i] = r.Name
		}
		fmt.Fprintf(b, "Active jobs: %s\n", strings.Join(names, ", "))
	}
	b.WriteString("Pod template:\n")
	writePodSpec(b, &c.Spec.JobTemplate.Spec.Template.Spec, nil, now)
}

// ---- 网络 ----

func servicePorts(s *corev1.Service) string {
	if len(s.Spec.Ports) == 0 {
		return none
	}
	parts := make([]string, len(s.Spec.Ports))
	for i, p := range s.Spec.Ports {
		parts[i] = strconv.Itoa(int(p.Port))
		if p.NodePort != 0 {
			parts[i] += ":" + strconv.Itoa(int(p.NodePort))
		}
		parts[i] += "/" + string(p.Protocol)
	}
	return strings.Join(parts, ",")
}

func serviceRow(s *corev1.Service, now time.Time) []string {
	return []string{s.Name, string(s.Spec.Type), orNone(s.Spec.ClusterIP), servicePorts(s), kv(s.Spec.Selector), age(s.CreationTimestamp.Time, now)}
}

func describeService(b *strings.Builder, s *corev1.Service, _ time.Time) {
	fmt.Fprintf(b, "Type: %s, cluster IP: %s\n", s.Spec.Type, orNone(s.Spec.ClusterIP))
	if len(s.Spec.ExternalIPs) > 0 {
		fmt.Fprintf(b, "External IPs: %s\n", strings.Join(s.Spec.ExternalIPs, ", "))
	}
	fmt.Fprintf(b, "Selector: %s\n", kv(s.Spec.Selector))
	b.WriteString("Ports:\n")
	for _, p := range s.Spec.Ports {
		fmt.Fprintf(b, "  - %s %d/%s -> %s", orNone(p.Name), p.Port, p.Protocol, p.TargetPort.String())
		if p.NodePort != 0 {
			fmt.Fprintf(b, " (node port %d)", p.NodePort)
		}
		b.WriteString("\n")
	}
	if s.Spec.SessionAffinity != "" && s.Spec.SessionAffinity != corev1.ServiceAffinityNone {
		fmt.Fprintf(b, "Session affinity: %s\n", s.Spec.SessionAffinity)
	}
	if s.Spec.ExternalTrafficPolicy != "" {
		fmt.Fprintf(b, "External traffic policy: %s\n", s.Spec.ExternalTrafficPolicy)
	}
	b.WriteString("Endpoints: see kube_get kind=EndpointSlice selector=kubernetes.io/service-name=" + s.Name + "\n")
}

func slicePorts(e *discoveryv1.EndpointSlice) string {
	if len(e.Ports) == 0 {
		return none
	}
	parts := make([]string, len(e.Ports))
	for i, p := range e.Ports {
		port := "<all>"
		if p.Port != nil {
			port = strconv.Itoa(int(*p.Port))
		}
		if p.Name != nil && *p.Name != "" {
			port = *p.Name + ":" + port
		}
		parts[i] = port
	}
	return strings.Join(parts, ",")
}

func readyEndpoints(e *discoveryv1.EndpointSlice) int {
	n := 0
	for _, ep := range e.Endpoints {
		if ep.Conditions.Ready == nil || *ep.Conditions.Ready {
			n++
		}
	}
	return n
}

func endpointSliceRow(e *discoveryv1.EndpointSlice, now time.Time) []string {
	return []string{
		e.Name, orNone(e.Labels[discoveryv1.LabelServiceName]), string(e.AddressType), slicePorts(e),
		fmt.Sprintf("%d/%d ready", readyEndpoints(e), len(e.Endpoints)), age(e.CreationTimestamp.Time, now),
	}
}

func describeEndpointSlice(b *strings.Builder, e *discoveryv1.EndpointSlice, _ time.Time) {
	fmt.Fprintf(b, "Service: %s, address type: %s, ports: %s\n", orNone(e.Labels[discoveryv1.LabelServiceName]), e.AddressType, slicePorts(e))
	fmt.Fprintf(b, "Endpoints: %d, ready: %d\n", len(e.Endpoints), readyEndpoints(e))
	flag := func(p *bool) string {
		if p == nil {
			return "?"
		}
		return strconv.FormatBool(*p)
	}
	shown := e.Endpoints[:min(len(e.Endpoints), maxItems)]
	for _, ep := range shown {
		fmt.Fprintf(b, "  - %s ready=%s serving=%s terminating=%s", strings.Join(ep.Addresses, ","),
			flag(ep.Conditions.Ready), flag(ep.Conditions.Serving), flag(ep.Conditions.Terminating))
		if ep.TargetRef != nil {
			fmt.Fprintf(b, " target=%s/%s", ep.TargetRef.Kind, ep.TargetRef.Name)
		}
		if ep.NodeName != nil {
			fmt.Fprintf(b, " node=%s", *ep.NodeName)
		}
		b.WriteString("\n")
	}
	if len(shown) < len(e.Endpoints) {
		fmt.Fprintf(b, "  [showing %d of %d endpoints]\n", len(shown), len(e.Endpoints))
	}
}

// ---- HPA ----

func timeOf(v *metav1.Time) time.Time {
	if v == nil {
		return time.Time{}
	}
	return v.Time
}

// hpaTargets 是每个指标的“当前/目标”，像 kubectl 的 TARGETS 列：cpu: 85%/70%。
func hpaTargets(h *autoscalingv2.HorizontalPodAutoscaler) string {
	if len(h.Spec.Metrics) == 0 {
		return none
	}
	parts := make([]string, len(h.Spec.Metrics))
	for i, m := range h.Spec.Metrics {
		name, target := metricTarget(m)
		current := "<unknown>"
		if i < len(h.Status.CurrentMetrics) {
			current = metricCurrent(h.Status.CurrentMetrics[i])
		}
		parts[i] = name + ": " + current + "/" + target
	}
	return strings.Join(parts, ", ")
}

func targetString(t autoscalingv2.MetricTarget) string {
	switch {
	case t.AverageUtilization != nil:
		return strconv.Itoa(int(*t.AverageUtilization)) + "%"
	case t.AverageValue != nil:
		return t.AverageValue.String() + " (avg)"
	case t.Value != nil:
		return t.Value.String()
	}
	return "?"
}

func valueString(v autoscalingv2.MetricValueStatus) string {
	switch {
	case v.AverageUtilization != nil:
		return strconv.Itoa(int(*v.AverageUtilization)) + "%"
	case v.AverageValue != nil:
		return v.AverageValue.String()
	case v.Value != nil:
		return v.Value.String()
	}
	return "<unknown>"
}

func metricTarget(m autoscalingv2.MetricSpec) (name, target string) {
	switch {
	case m.Resource != nil:
		return string(m.Resource.Name), targetString(m.Resource.Target)
	case m.ContainerResource != nil:
		return m.ContainerResource.Container + "/" + string(m.ContainerResource.Name), targetString(m.ContainerResource.Target)
	case m.Pods != nil:
		return m.Pods.Metric.Name, targetString(m.Pods.Target)
	case m.Object != nil:
		return m.Object.Metric.Name, targetString(m.Object.Target)
	case m.External != nil:
		return m.External.Metric.Name, targetString(m.External.Target)
	}
	return string(m.Type), "?"
}

func metricCurrent(m autoscalingv2.MetricStatus) string {
	switch {
	case m.Resource != nil:
		return valueString(m.Resource.Current)
	case m.ContainerResource != nil:
		return valueString(m.ContainerResource.Current)
	case m.Pods != nil:
		return valueString(m.Pods.Current)
	case m.Object != nil:
		return valueString(m.Object.Current)
	case m.External != nil:
		return valueString(m.External.Current)
	}
	return "<unknown>"
}

func hpaRow(h *autoscalingv2.HorizontalPodAutoscaler, now time.Time) []string {
	ref := h.Spec.ScaleTargetRef.Kind + "/" + h.Spec.ScaleTargetRef.Name
	return []string{
		h.Name, ref, hpaTargets(h), strconv.Itoa(int(i32(h.Spec.MinReplicas))), strconv.Itoa(int(h.Spec.MaxReplicas)),
		strconv.Itoa(int(h.Status.CurrentReplicas)), age(h.CreationTimestamp.Time, now),
	}
}

func describeHPA(b *strings.Builder, h *autoscalingv2.HorizontalPodAutoscaler, now time.Time) {
	fmt.Fprintf(b, "Target: %s/%s\n", h.Spec.ScaleTargetRef.Kind, h.Spec.ScaleTargetRef.Name)
	fmt.Fprintf(b, "Replicas: min %d, max %d, current %d, desired %d\n",
		i32(h.Spec.MinReplicas), h.Spec.MaxReplicas, h.Status.CurrentReplicas, h.Status.DesiredReplicas)
	fmt.Fprintf(b, "Metrics (current/target): %s\n", hpaTargets(h))
	if t := timeOf(h.Status.LastScaleTime); !t.IsZero() {
		fmt.Fprintf(b, "Last scale: %s\n", stamp(t, now))
	}
	conds := make([]cond, len(h.Status.Conditions))
	for i, c := range h.Status.Conditions {
		conds[i] = cond{string(c.Type), string(c.Status), c.Reason, c.Message, c.LastTransitionTime.Time}
	}
	writeConditions(b, conds, now)
}

// ---- PVC ----

func pvcCapacity(p *corev1.PersistentVolumeClaim) string {
	if q, ok := p.Status.Capacity[corev1.ResourceStorage]; ok {
		return q.String()
	}
	return none
}

func pvcAccess(p *corev1.PersistentVolumeClaim) string {
	modes := make([]string, len(p.Spec.AccessModes))
	for i, m := range p.Spec.AccessModes {
		modes[i] = string(m)
	}
	return orNone(strings.Join(modes, ","))
}

func pvcRow(p *corev1.PersistentVolumeClaim, now time.Time) []string {
	sc := none
	if p.Spec.StorageClassName != nil {
		sc = *p.Spec.StorageClassName
	}
	return []string{p.Name, string(p.Status.Phase), orNone(p.Spec.VolumeName), pvcCapacity(p), pvcAccess(p), sc, age(p.CreationTimestamp.Time, now)}
}

func describePVC(b *strings.Builder, p *corev1.PersistentVolumeClaim, now time.Time) {
	sc := none
	if p.Spec.StorageClassName != nil {
		sc = *p.Spec.StorageClassName
	}
	req := p.Spec.Resources.Requests[corev1.ResourceStorage]
	fmt.Fprintf(b, "Status: %s, volume: %s\n", p.Status.Phase, orNone(p.Spec.VolumeName))
	fmt.Fprintf(b, "Requested: %s, capacity: %s, access: %s, storage class: %s\n", req.String(), pvcCapacity(p), pvcAccess(p), sc)
	conds := make([]cond, len(p.Status.Conditions))
	for i, c := range p.Status.Conditions {
		conds[i] = cond{string(c.Type), string(c.Status), c.Reason, c.Message, c.LastTransitionTime.Time}
	}
	writeConditions(b, conds, now)
}

// ---- Event（kube_get kind=Event 的投影；kube_events 有自己的合并和排序） ----

func eventRow(e *corev1.Event, now time.Time) []string {
	return []string{
		age(eventTime(e), now) + " ago", e.Type, e.Reason, e.InvolvedObject.Kind + "/" + e.InvolvedObject.Name,
		strconv.Itoa(int(eventCount(e))), cut(e.Message, maxMessageBytes),
	}
}

// eventOrder：最新的在前。
func eventOrder(a, b object) int {
	return eventTime(b.(*corev1.Event)).Compare(eventTime(a.(*corev1.Event)))
}

func describeEvent(b *strings.Builder, e *corev1.Event, now time.Time) {
	fmt.Fprintf(b, "Type: %s, reason: %s, count: %d\n", e.Type, e.Reason, eventCount(e))
	fmt.Fprintf(b, "Object: %s/%s", e.InvolvedObject.Kind, e.InvolvedObject.Name)
	if e.InvolvedObject.FieldPath != "" {
		fmt.Fprintf(b, " (%s)", e.InvolvedObject.FieldPath)
	}
	b.WriteString("\n")
	fmt.Fprintf(b, "Last seen: %s\n", stamp(eventTime(e), now))
	if !e.FirstTimestamp.IsZero() {
		fmt.Fprintf(b, "First seen: %s\n", stamp(e.FirstTimestamp.Time, now))
	}
	fmt.Fprintf(b, "Source: %s\n", orNone(cmp.Or(e.ReportingController, e.Source.Component)))
	fmt.Fprintf(b, "Message: %s\n", cut(e.Message, 2*maxMessageBytes))
}
