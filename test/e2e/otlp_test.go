package e2e

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
)

const otlpAddrEnv = "E2E_OTLP_ENDPOINT"

// recvSpan 是接收端记下的一个 span，只留断言用得到的字段。
type recvSpan struct {
	service string
	name    string
	kind    tracepb.Span_SpanKind
	traceID string
	spanID  string
	parent  string
	attrs   map[string]string
}

func (s recvSpan) String() string {
	return fmt.Sprintf("%-10s %-8s %-48s span=%s parent=%s", s.service, strings.TrimPrefix(s.kind.String(), "SPAN_KIND_"), s.name, s.spanID, s.parent)
}

// collector 是测试进程里的最小 OTLP（gRPC）接收端：只收链路，按 trace id 存在内存里。
// 指标接口什么都不做，直接返回成功，否则服务的指标导出器会不停报错。
type collector struct {
	coltrace.UnimplementedTraceServiceServer

	mu     sync.Mutex
	traces map[string][]recvSpan
	srv    *grpc.Server
}

var otlp *collector

// startCollector 监听 E2E_OTLP_ENDPOINT（形如 http://127.0.0.1:14317）。变量没设时返回 nil：
// 依赖接收端的用例会跳过。
func startCollector(endpoint string) (*collector, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", otlpAddrEnv, err)
	}
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", u.Host)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", u.Host, err)
	}
	c := &collector{traces: map[string][]recvSpan{}, srv: grpc.NewServer()}
	coltrace.RegisterTraceServiceServer(c.srv, c)
	colmetrics.RegisterMetricsServiceServer(c.srv, &metricsService{})
	go func() { _ = c.srv.Serve(lis) }()
	return c, nil
}

func (c *collector) stop() { c.srv.Stop() }

func (c *collector) Export(_ context.Context, req *coltrace.ExportTraceServiceRequest) (*coltrace.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rs := range req.GetResourceSpans() {
		service := attrString(rs.GetResource().GetAttributes(), "service.name")
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				attrs := make(map[string]string, len(sp.GetAttributes()))
				for _, kv := range sp.GetAttributes() {
					attrs[kv.GetKey()] = anyString(kv.GetValue())
				}
				tid := hex.EncodeToString(sp.GetTraceId())
				c.traces[tid] = append(c.traces[tid], recvSpan{
					service: service,
					name:    sp.GetName(),
					kind:    sp.GetKind(),
					traceID: tid,
					spanID:  hex.EncodeToString(sp.GetSpanId()),
					parent:  hex.EncodeToString(sp.GetParentSpanId()),
					attrs:   attrs,
				})
			}
		}
	}
	return &coltrace.ExportTraceServiceResponse{}, nil
}

// metricsService 满足指标接口，丢弃收到的一切。
type metricsService struct {
	colmetrics.UnimplementedMetricsServiceServer
}

func (*metricsService) Export(context.Context, *colmetrics.ExportMetricsServiceRequest) (*colmetrics.ExportMetricsServiceResponse, error) {
	return &colmetrics.ExportMetricsServiceResponse{}, nil
}

// trace 返回某条链路目前收到的 span 的副本。
func (c *collector) trace(id string) []recvSpan {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.traces[id])
}

// all 返回目前收到的全部 span。
func (c *collector) all() []recvSpan {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []recvSpan
	for _, spans := range c.traces {
		out = append(out, spans...)
	}
	return out
}

func attrString(kvs []*commonpb.KeyValue, key string) string {
	for _, kv := range kvs {
		if kv.GetKey() == key {
			return anyString(kv.GetValue())
		}
	}
	return ""
}

func anyString(v *commonpb.AnyValue) string {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	}
	return ""
}

// dumpSpans 把一组 span 排成可读的列表，失败时打印用。
func dumpSpans(spans []recvSpan) string {
	lines := make([]string, 0, len(spans))
	for _, s := range spans {
		lines = append(lines, "  "+s.String())
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}
