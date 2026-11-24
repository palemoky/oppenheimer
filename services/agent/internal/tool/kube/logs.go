package kube

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

const (
	defaultTail = 200
	maxTail     = 500
	// maxLineBytes 是每行日志的截断长度，和 Loki 的日志工具一样（设计 3.4）。
	maxLineBytes = 500
	// maxLogBytes 是从日志流里最多读的字节数，apiserver 那边也用 limitBytes 限住。
	maxLogBytes = 4 << 20
)

// defaultContainerAnnotation 是 kubectl 选默认容器用的注解。
const defaultContainerAnnotation = "kubectl.kubernetes.io/default-container"

func logsDef() llm.ToolDef {
	return llm.ToolDef{
		Name: "kube_logs",
		Description: "Read the last lines of a container's logs from the kubelet. " +
			"Set previous=true to read the logs of the previous, crashed instance of the container (after an OOM kill or crash loop). " +
			"Consecutive identical lines are merged and long lines are cut. For searching logs across pods or time ranges, prefer the Loki tools.",
		Schema: schema(map[string]any{
			"namespace": str("Namespace of the pod."),
			"pod":       str("Pod name."),
			"container": str("Container name. Required when the pod has several containers and no default container."),
			"previous":  map[string]any{"type": "boolean", "description": "Read the previous (terminated) instance of the container. Default false."},
			"since":     str("Only lines at or after this time: RFC 3339 or relative like -30m. Default: no time limit."),
			"tail":      map[string]any{"type": "integer", "description": "Number of last lines to read. Default 200, max 500."},
		}, "namespace", "pod"),
	}
}

type logsArgs struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Previous  bool   `json:"previous"`
	Since     string `json:"since"`
	Tail      *int   `json:"tail"`
}

func (k *kube) logs(ctx context.Context, input jsontext.Value) (tool.Result, error) {
	var args logsArgs
	if err := tool.Decode(input, &args); err != nil {
		return tool.Result{}, err
	}
	if args.Namespace == "" || args.Pod == "" {
		return tool.Result{}, errors.New("namespace and pod are required; use kube_get kind=pod to find pods")
	}
	tail := defaultTail
	if args.Tail != nil {
		tail = *args.Tail
	}
	if tail < 1 || tail > maxTail {
		return tool.Result{}, fmt.Errorf("tail must be between 1 and %d", maxTail)
	}
	now := k.now()
	opts := &corev1.PodLogOptions{
		Previous:   args.Previous,
		TailLines:  new(int64(tail)),
		LimitBytes: new(int64(maxLogBytes)),
	}
	var since time.Time
	if args.Since != "" {
		t, err := tool.ParseTime(args.Since, now)
		if err != nil {
			return tool.Result{}, fmt.Errorf("since: %w", err)
		}
		since = t
		opts.SinceTime = &metav1.Time{Time: t}
	}

	if k.hidden.Namespace(args.Namespace) {
		return tool.Result{}, notFound(where("Pod", args.Namespace, args.Pod))
	}
	pods := k.client.CoreV1().Pods(args.Namespace)
	pod, err := pods.Get(ctx, args.Pod, metav1.GetOptions{})
	if err != nil {
		return tool.Result{}, apiError(err, where("Pod", args.Namespace, args.Pod))
	}
	container, err := pickContainer(pod, args.Container)
	if err != nil {
		return tool.Result{}, err
	}
	opts.Container = container
	if args.Previous {
		if err := checkPrevious(pod, container); err != nil {
			return tool.Result{}, err
		}
	}

	what := fmt.Sprintf("logs of container %q in pod %q in namespace %q", container, args.Pod, args.Namespace)
	stream, err := pods.GetLogs(args.Pod, opts).Stream(ctx)
	if err != nil {
		if args.Previous && apierrors.IsBadRequest(err) {
			return tool.Result{}, fmt.Errorf("container %q in pod %q has no previous instance to read logs from; omit previous", container, args.Pod)
		}
		return tool.Result{}, apiError(err, what)
	}
	defer func() { _ = stream.Close() }() // 只读的流，关闭的错误没有可做的
	body, err := io.ReadAll(io.LimitReader(stream, maxLogBytes+1))
	if err != nil {
		return tool.Result{}, fmt.Errorf("reading %s: %w", what, err)
	}
	bytesCut := len(body) > maxLogBytes
	body = body[:min(len(body), maxLogBytes)]

	var b strings.Builder
	b.WriteString("Logs of container " + container + " in pod " + args.Namespace + "/" + args.Pod)
	if args.Previous {
		b.WriteString(" (previous instance)")
	}
	if !since.IsZero() {
		b.WriteString(" since " + since.UTC().Format(time.RFC3339))
	}
	b.WriteString(": ")
	b.WriteString(formatLogs(body, tail, bytesCut))
	return tool.Result{Text: b.String(), Raw: body}, nil
}

// formatLogs 把日志正文裁成给模型的文本：按行切开，tool.TrimLines 截行、合并连续相同的行、留最后 tail 行，
// 再在开头写明取到了多少行、给了多少行。bytesCut 表示正文在 maxLogBytes 处被截断了。
func formatLogs(body []byte, tail int, bytesCut bool) string {
	lines := splitLines(body)
	if bytesCut && len(lines) > 0 {
		// 截断点上的那一行不完整，丢掉。
		lines = lines[:len(lines)-1]
	}
	var b strings.Builder
	if len(lines) == 0 {
		b.WriteString("no log lines.\n")
		return b.String()
	}
	shown, dropped := tool.TrimLines(lines, tail, maxLineBytes, true)
	fmt.Fprintf(&b, "fetched %d lines, showing %d", len(lines), len(shown))
	if merged := len(lines) - len(shown) - dropped; merged > 0 {
		fmt.Fprintf(&b, " (%d repeated lines merged)", merged)
	}
	if dropped > 0 {
		fmt.Fprintf(&b, " (%d older lines dropped)", dropped)
	}
	if bytesCut {
		fmt.Fprintf(&b, " [log output exceeded %d MiB and was cut; the newest lines may be missing, use a smaller tail or since]", maxLogBytes>>20)
	}
	b.WriteString("\n")
	for _, l := range shown {
		b.WriteString(l)
		b.WriteString("\n")
	}
	return b.String()
}

// splitLines 按换行切开，去掉行尾的 \r 和末尾的空行。
func splitLines(body []byte) []string {
	body = bytes.TrimRight(body, "\r\n")
	if len(body) == 0 {
		return nil
	}
	lines := strings.Split(string(body), "\n")
	for i, l := range lines {
		lines[i] = strings.TrimSuffix(l, "\r")
	}
	return lines
}

// pickContainer 选要读的容器：给了就核对它存在；没给时用默认容器注解，只有一个容器时用它，否则让模型指定。
func pickContainer(pod *corev1.Pod, name string) (string, error) {
	var names []string
	for _, c := range slices.Concat(pod.Spec.InitContainers, pod.Spec.Containers) {
		names = append(names, c.Name)
	}
	if name != "" {
		if !slices.Contains(names, name) {
			return "", fmt.Errorf("pod %q has no container %q; its containers are: %s", pod.Name, name, strings.Join(names, ", "))
		}
		return name, nil
	}
	if def := pod.Annotations[defaultContainerAnnotation]; def != "" && slices.Contains(names, def) {
		return def, nil
	}
	if len(pod.Spec.Containers) == 1 {
		return pod.Spec.Containers[0].Name, nil
	}
	return "", fmt.Errorf("pod %q has several containers (%s); pass container", pod.Name, strings.Join(names, ", "))
}

// checkPrevious 核对容器有没有上一个实例。没有时 apiserver 回的是一个笼统的 400，这里先说清楚。
func checkPrevious(pod *corev1.Pod, container string) error {
	for _, cs := range slices.Concat(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses) {
		if cs.Name != container {
			continue
		}
		if cs.RestartCount == 0 && cs.LastTerminationState.Terminated == nil {
			return fmt.Errorf("container %q in pod %q has not restarted, so there is no previous instance; omit previous to read the current logs", container, pod.Name)
		}
		return nil
	}
	return fmt.Errorf("container %q in pod %q has no status yet, so there is no previous instance; omit previous", container, pod.Name)
}
