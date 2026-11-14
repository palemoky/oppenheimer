package outbox

import (
	"context"
	"database/sql"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// 本文件测 trace_context 列和 traceparent header。用自己建的 TracerProvider，不碰全局的。

func spanCtx(t *testing.T, sampler sdktrace.Sampler) (context.Context, trace.Span) {
	t.Helper()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(t.Context(), "request")
	t.Cleanup(func() { span.End() })
	return ctx, span
}

func addIn(ctx context.Context, t *testing.T, db *sql.DB, msgs ...Message) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Add(ctx, tx, msgs...); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// traceContexts 返回各行的 trace_context（NULL 用 Valid=false 表示），按 id 升序。
func traceContexts(t *testing.T, db *sql.DB) []sql.NullString {
	t.Helper()
	rs, err := db.QueryContext(t.Context(), "SELECT trace_context FROM outbox ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close() //nolint:errcheck // 测试里的只读查询
	var out []sql.NullString
	for rs.Next() {
		var c sql.NullString
		if err := rs.Scan(&c); err != nil {
			t.Fatal(err)
		}
		out = append(out, c)
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAddStoresTraceparentAndFlushSendsIt(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s)

	ctx, span := spanCtx(t, sdktrace.AlwaysSample())
	want := otelx.Traceparent(ctx)
	if len(want) != 55 {
		t.Fatalf("Traceparent = %q, want 55 chars", want)
	}
	// 一次 Add 写两条：都带同一个上下文。
	addIn(ctx, t, db, msg("a", "p1"), msg("b", "p2"))
	span.End()

	got := traceContexts(t, db)
	if len(got) != 2 {
		t.Fatalf("%d rows, want 2", len(got))
	}
	for i, c := range got {
		if !c.Valid || c.String != want {
			t.Errorf("row %d trace_context = %+v, want %q", i, c, want)
		}
	}

	if n := mustFlush(t, r); n != 2 {
		t.Fatalf("published %d, want 2", n)
	}
	recs := s.received()
	if len(recs) != 2 {
		t.Fatalf("sent %d, want 2", len(recs))
	}
	for i, rec := range recs {
		if h := rec.Headers[kafkax.TraceparentHeader]; h != want {
			t.Errorf("record %d traceparent = %q, want %q", i, h, want)
		}
		if eventID(rec) == "" {
			t.Errorf("record %d lost its event id header", i)
		}
	}
}

func TestAddWithoutSpanStoresNullAndSendsNoTraceparent(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s)

	commitAdd(t, db, msg("a", "p1"))
	got := traceContexts(t, db)
	if len(got) != 1 || got[0].Valid {
		t.Fatalf("trace_context = %+v, want a single NULL", got)
	}

	mustFlush(t, r)
	recs := s.received()
	if len(recs) != 1 {
		t.Fatalf("sent %d, want 1", len(recs))
	}
	if h, ok := recs[0].Headers[kafkax.TraceparentHeader]; ok {
		t.Errorf("record has traceparent %q, want none", h)
	}
	if eventID(recs[0]) == "" {
		t.Error("record lost its event id header")
	}
}

// 有的行有上下文、有的没有：各自对应，不串。
func TestFlushMixesRowsWithAndWithoutTraceparent(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s)

	ctx, span := spanCtx(t, sdktrace.AlwaysSample())
	defer span.End()
	commitAdd(t, db, msg("a", "plain-1"))
	addIn(ctx, t, db, msg("b", "traced"))
	commitAdd(t, db, msg("c", "plain-2"))

	mustFlush(t, r)
	byPayload := map[string]kafkax.Record{}
	for _, rec := range s.received() {
		byPayload[string(rec.Value)] = rec
	}
	if h := byPayload["traced"].Headers[kafkax.TraceparentHeader]; h != otelx.Traceparent(ctx) {
		t.Errorf("traced record traceparent = %q, want %q", h, otelx.Traceparent(ctx))
	}
	for _, p := range []string{"plain-1", "plain-2"} {
		if h, ok := byPayload[p].Headers[kafkax.TraceparentHeader]; ok {
			t.Errorf("%s has traceparent %q, want none", p, h)
		}
	}
}

// 没被采样的 span 也写：采样的决定在标志位里，不写的话下游会各自重新决定。
func TestAddStoresUnsampledTraceparent(t *testing.T) {
	db := newDB(t)
	ctx, span := spanCtx(t, sdktrace.NeverSample())
	defer span.End()
	addIn(ctx, t, db, msg("a", "p"))
	got := traceContexts(t, db)
	if len(got) != 1 || !got[0].Valid {
		t.Fatalf("trace_context = %+v, want a value", got)
	}
	if want := otelx.Traceparent(ctx); got[0].String != want || got[0].String[len(want)-2:] != "00" {
		t.Errorf("trace_context = %q, want %q (flags 00)", got[0].String, want)
	}
}

// 事务回滚时 trace_context 跟着整行一起没有。
func TestAddRollbackLeavesNoTraceContext(t *testing.T) {
	db := newDB(t)
	ctx, span := spanCtx(t, sdktrace.AlwaysSample())
	defer span.End()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Add(ctx, tx, msg("a", "p")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := traceContexts(t, db); len(got) != 0 {
		t.Errorf("%d rows after rollback, want 0", len(got))
	}
}
