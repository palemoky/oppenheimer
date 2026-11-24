package pprof

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/streaming/pkg/httpstream"
)

// Forwarder 对 Pod 回环上的一个端口发一个 GET，返回 200 的响应体。真的实现经 Kubernetes 的端口转发
// （KubeForwarder），测试里用 httptest 顶替。
type Forwarder interface {
	// Get 返回 GET path 的响应体，最多 limit 字节，超过时返回错误。非 200 返回 *StatusError。
	// ctx 的取消和时限要能打断进行中的请求。
	Get(ctx context.Context, namespace, pod string, port int, path string, limit int64) ([]byte, error)
}

// StatusError 是调试端口返回的非 200。Body 是响应体的开头（net/http/pprof 的出错信息是一行文字）。
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("debug port answered HTTP %d: %s", e.Code, e.Body)
}

// errTooLarge 是剖析超过了 limit。
var errTooLarge = errors.New("profile too large")

// readBody 读响应：200 时读出最多 limit 字节的体，否则返回 *StatusError。
func readBody(resp *http.Response, limit int64) ([]byte, error) {
	if resp.StatusCode != http.StatusOK {
		head, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return nil, &StatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(head))}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read profile: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("%w: over %d bytes", errTooLarge, limit)
	}
	return body, nil
}

// KubeForwarder 经 apiserver 的 pods/portforward（SPDY）连到 Pod 里的端口。
//
// 不像 kubectl port-forward 那样在本机开一个监听端口再转发：直接在转发的数据流上写一个 HTTP 请求、
// 读回响应，用完就关。本机不多出一个端口，一次请求一条连接，也就没有“上一次的连接还开着”的状态。
//
// 只用 SPDY，不走 WebSocket 的回退：SPDY 是 POST，RBAC 只要 pods/portforward 的 create
// （WebSocket 是 GET，有的版本要另给 get）。权限越少越好。
type KubeForwarder struct {
	config *rest.Config
	client rest.Interface
}

// NewKubeForwarder 用 config（Agent 的身份）和它建出的 clientset 建转发器。
func NewKubeForwarder(config *rest.Config, cs kubernetes.Interface) *KubeForwarder {
	return &KubeForwarder{config: config, client: cs.CoreV1().RESTClient()}
}

// Get 实现 Forwarder。
func (f *KubeForwarder) Get(ctx context.Context, namespace, pod string, port int, path string, limit int64) ([]byte, error) {
	// SPDY 的 RoundTripper 只能升级一条连接，每次新建。
	rt, upgrader, err := spdy.RoundTripperFor(f.config)
	if err != nil {
		return nil, fmt.Errorf("port-forward transport: %w", err)
	}
	u := f.client.Post().Resource("pods").Namespace(namespace).Name(pod).SubResource("portforward").URL()
	dialer := spdy.NewDialerForStreaming(upgrader, &http.Client{Transport: rt}, http.MethodPost, u)

	conn, err := dial(ctx, dialer)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	// ctx 到期或取消时关掉连接，进行中的读写随之返回。
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	headers := http.Header{}
	headers.Set(corev1.StreamType, corev1.StreamTypeError)
	headers.Set(corev1.PortHeader, strconv.Itoa(port))
	headers.Set(corev1.PortForwardRequestIDHeader, "0")
	errStream, err := conn.CreateStream(headers)
	if err != nil {
		return nil, fmt.Errorf("port-forward error stream: %w", ctxErr(ctx, err))
	}
	_ = errStream.Close() // 不往错误流里写
	remoteErr := make(chan string, 1)
	go func() {
		msg, _ := io.ReadAll(errStream)
		remoteErr <- strings.TrimSpace(string(msg))
	}()

	headers.Set(corev1.StreamType, corev1.StreamTypeData)
	data, err := conn.CreateStream(headers)
	if err != nil {
		return nil, fmt.Errorf("port-forward data stream: %w", ctxErr(ctx, err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost"+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Close = true
	if err := req.Write(data); err != nil {
		return nil, fmt.Errorf("send request: %w", explain(ctx, err, remoteErr))
	}
	resp, err := http.ReadResponse(bufio.NewReader(data), req)
	if err != nil {
		// 端口上没有在听时，转发的那一端从错误流里说原因（connection refused），数据流直接断开。
		return nil, fmt.Errorf("read response: %w", explain(ctx, err, remoteErr))
	}
	defer func() { _ = resp.Body.Close() }()
	return readBody(resp, limit)
}

// explain 把数据流上的错误和错误流里转发端给的原因合在一起。原因通常在数据流断开的同时到，最多等一秒。
func explain(ctx context.Context, err error, remoteErr <-chan string) error {
	if ctx.Err() != nil {
		return ctxErr(ctx, err)
	}
	select {
	case msg := <-remoteErr:
		if msg != "" {
			return fmt.Errorf("%w (port-forward: %s)", err, msg)
		}
	case <-time.After(time.Second):
	}
	return err
}

// dial 建转发的连接。Dial 不收 ctx：放在别的 goroutine 里等，ctx 先到就不等了，连接建成后马上关掉。
func dial(ctx context.Context, dialer httpstream.Dialer) (httpstream.Connection, error) {
	type result struct {
		conn httpstream.Connection
		err  error
	}
	done := make(chan result, 1)
	go func() {
		conn, _, err := dialer.Dial(portforward.PortForwardProtocolV1Name)
		done <- result{conn, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return nil, fmt.Errorf("port-forward: %w", r.err)
		}
		return r.conn, nil
	case <-ctx.Done():
		go func() {
			if r := <-done; r.conn != nil {
				_ = r.conn.Close()
			}
		}()
		return nil, fmt.Errorf("port-forward: %w", ctx.Err())
	}
}

// ctxErr 在 ctx 已经结束时把它的错误包进去，调用方能用 errors.Is 认出超时。
func ctxErr(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil && !errors.Is(err, cerr) {
		return fmt.Errorf("%w: %w", cerr, err)
	}
	return err
}
