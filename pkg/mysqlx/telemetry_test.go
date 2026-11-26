package mysqlx_test

import (
	"context"
	"crypto/rand"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/testdb"
)

// 全局的 TracerProvider 是进程级的，在 TestMain 里设一次：otelsql 在 OpenDB 时取全局的 provider。
// 用例用语句里独有的标记（SQL 注释）区分自己的 span。
var spans = tracetest.NewSpanRecorder()

// 全局的 MeterProvider 同理：连接池的指标在 OpenLazy 里登记到当时的全局 provider 上。
var metrics = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metrics)))
	otel.SetTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(spans),
	))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	os.Exit(m.Run())
}

func attrString(s sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.String(), true
		}
	}
	return "", false
}

// spansOf 返回这条 trace 里已结束的 span。
func spansOf(traceID trace.TraceID) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range spans.Ended() {
		if s.SpanContext().TraceID() == traceID {
			out = append(out, s)
		}
	}
	return out
}

func byName(ss []sdktrace.ReadOnlySpan, name string) []sdktrace.ReadOnlySpan {
	return slices.DeleteFunc(slices.Clone(ss), func(s sdktrace.ReadOnlySpan) bool { return s.Name() != name })
}

func TestQueryAndTransactionSpansAreChildren(t *testing.T) {
	db := testdb.New(t, migrations, migrationsDir)

	ctx, parent := otel.Tracer("test").Start(t.Context(), "request")
	marker := "/* m" + rand.Text() + " */"
	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1 "+marker).Scan(&one); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 参数值不能出现在 span 里：语句用 ? 占位。
	const secret = 987654321
	if _, err := tx.ExecContext(ctx, "INSERT INTO counter (id, value) VALUES (?, ?) "+marker, 1, secret); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	parent.End()

	got := spansOf(parent.SpanContext().TraceID())

	check := func(name string) sdktrace.ReadOnlySpan {
		t.Helper()
		ss := byName(got, name)
		if len(ss) == 0 {
			names := make([]string, len(got))
			for i, s := range got {
				names[i] = s.Name()
			}
			t.Fatalf("no span named %q; have %v", name, names)
		}
		s := ss[0]
		if s.Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Errorf("span %q parent = %s, want the request span %s", name, s.Parent().SpanID(), parent.SpanContext().SpanID())
		}
		if s.SpanKind() != trace.SpanKindClient {
			t.Errorf("span %q kind = %v, want CLIENT", name, s.SpanKind())
		}
		return s
	}

	sel := check("mysql SELECT")
	if q, _ := attrString(sel, "db.query.text"); !strings.Contains(q, marker) {
		t.Errorf("SELECT span db.query.text = %q, want it to contain the statement", q)
	}
	if v, _ := attrString(sel, "db.system.name"); v != "mysql" {
		t.Errorf("db.system.name = %q, want mysql", v)
	}
	ins := check("mysql INSERT")
	check("mysql commit")
	check("mysql begin_tx")

	for _, s := range got {
		for _, kv := range s.Attributes() {
			if strings.Contains(kv.Value.String(), "987654321") {
				t.Errorf("span %q attribute %s = %v leaks a parameter value", s.Name(), kv.Key, kv.Value.String())
			}
		}
	}
	if q, _ := attrString(ins, "db.query.text"); !strings.Contains(q, "?") {
		t.Errorf("INSERT span db.query.text = %q, want placeholders", q)
	}

	// 其余 span 里不该有没有信息量的几类：遍历结果集、连接放回池子时的重置、建连接。
	for _, s := range got {
		for _, bad := range []string{"rows", "reset_session", "connect"} {
			if strings.Contains(s.Name(), bad) {
				t.Errorf("unexpected span %q: these are omitted by configuration", s.Name())
			}
		}
	}
}

func TestRolledBackTransactionSpan(t *testing.T) {
	db := testdb.New(t, migrations, migrationsDir)
	ctx, parent := otel.Tracer("test").Start(t.Context(), "request")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	parent.End()
	if len(byName(spansOf(parent.SpanContext().TraceID()), "mysql rollback")) != 1 {
		t.Error("no `mysql rollback` span")
	}
}

// 没有父 span 的语句不产生 span（后台循环每秒都在查）。
func TestNoSpanWithoutParent(t *testing.T) {
	db := testdb.New(t, migrations, migrationsDir)

	marker := "/* m" + rand.Text() + " */"
	var one int
	if err := db.QueryRowContext(context.Background(), "SELECT 1 "+marker).Scan(&one); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), "INSERT INTO counter (id, value) VALUES (1, 1) "+marker); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	for _, s := range spans.Ended() {
		if q, _ := attrString(s, "db.query.text"); strings.Contains(q, marker) {
			t.Errorf("span %q was created for a statement without a parent span", s.Name())
		}
	}
}

// OpenLazy 打开的连接池也一样带埋点，并且连不上也不报错。
func TestOpenLazyInstrumented(t *testing.T) {
	dsn := os.Getenv(testdb.DSNEnv)
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatalf("%s is not set in CI", testdb.DSNEnv)
		}
		t.Skipf("%s is not set", testdb.DSNEnv)
	}
	db, err := mysqlx.OpenLazy(mysqlx.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx, parent := otel.Tracer("test").Start(t.Context(), "request")
	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatal(err)
	}
	parent.End()
	if len(byName(spansOf(parent.SpanContext().TraceID()), "mysql SELECT")) != 1 {
		t.Error("no `mysql SELECT` span from an OpenLazy pool")
	}
}

// 主库池和从库池连的是同名的库：连接池的指标要靠 db.role 分成两条序列，各报各的上限。
func TestPoolMetricsTellRolesApart(t *testing.T) {
	// 不建连接：OpenLazy 不 ping，连接池的指标读的是 sql.DB 自己的统计。库名每次不同，不和别的用例的池混。
	dbName := "pool_" + strings.ToLower(rand.Text())
	dsn := "u:p@tcp(127.0.0.1:1)/" + dbName
	for role, maxOpen := range map[mysqlx.Role]int{"": 7, mysqlx.RoleReplica: 11, mysqlx.RoleArchive: 3} {
		db, err := mysqlx.OpenLazy(mysqlx.Config{DSN: dsn, MaxOpenConns: maxOpen, Role: role})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}

	var rm metricdata.ResourceMetrics
	if err := metrics.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "db.sql.connection.max_open" {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 gauge", m.Name, m.Data)
			}
			for _, dp := range gauge.DataPoints {
				if ns, _ := dp.Attributes.Value("db.namespace"); ns.AsString() != dbName {
					continue
				}
				role, _ := dp.Attributes.Value("db.role")
				got[role.AsString()] = dp.Value
			}
		}
	}
	want := map[string]int64{"primary": 7, "replica": 11, "archive": 3}
	if !maps.Equal(got, want) {
		t.Errorf("max_open by db.role = %v, want %v", got, want)
	}
}

func TestOpenRejectsUnknownRole(t *testing.T) {
	if _, err := mysqlx.OpenLazy(mysqlx.Config{DSN: "u:p@tcp(127.0.0.1:1)/x", Role: "standby"}); err == nil {
		t.Error("an unknown role was accepted; metric labels must stay a closed set")
	}
}

func TestSpanName(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		method otelsql.Method
		query  string
		want   string
	}{
		{"select", otelsql.MethodConnQuery, "SELECT id FROM orders WHERE id = ?", "mysql SELECT"},
		{"lowercase verb", otelsql.MethodConnExec, "insert into t values (?)", "mysql INSERT"},
		{"leading space", otelsql.MethodConnExec, "   UPDATE t SET a = ?", "mysql UPDATE"},
		{"delete", otelsql.MethodConnExec, "DELETE FROM t", "mysql DELETE"},
		{"single word", otelsql.MethodConnExec, "COMMIT", "mysql COMMIT"},
		{"commit has no query", otelsql.MethodTxCommit, "", "mysql commit"},
		{"rollback has no query", otelsql.MethodTxRollback, "", "mysql rollback"},
		{"begin has no query", otelsql.MethodConnBeginTx, "", "mysql begin_tx"},
		{"blank query falls back to the method", otelsql.MethodConnPing, "  ", "mysql ping"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mysqlx.SpanName(ctx, tt.method, tt.query); got != tt.want {
				t.Errorf("SpanName(%q, %q) = %q, want %q", tt.method, tt.query, got, tt.want)
			}
		})
	}
}

// 语句常常写成多行（动词后面是换行，不是空格）。span 名要是有限的几个值，不能带上换行和后面的内容。
func TestSpanNameMultilineQuery(t *testing.T) {
	got := mysqlx.SpanName(context.Background(), otelsql.MethodConnQuery, "SELECT\n  id\nFROM orders")
	if got != "mysql SELECT" {
		t.Errorf("span name = %q, want %q", got, "mysql SELECT")
	}
	// 以注释开头、或者不认识的动词：落到固定的名字上。
	if got := mysqlx.SpanName(context.Background(), otelsql.MethodConnQuery, "/* hint */ SELECT 1"); got != "mysql query" {
		t.Errorf("span name = %q, want %q", got, "mysql query")
	}
}

// 带参数的语句：go-sql-driver 在没开 interpolateParams 时对它返回 driver.ErrSkip，让 database/sql 改走
// “预处理再执行”。这是驱动和标准库之间的约定，不是失败，span 不能因此被标成 Error
// （集群上第一次接后端时，三分之一的 mysql span 是这样的假错误，设计文档 3a 第 8 节）。
func TestParameterizedStatementSpansAreNotErrors(t *testing.T) {
	db := testdb.New(t, migrations, migrationsDir)

	ctx, parent := otel.Tracer("test").Start(t.Context(), "request")
	if _, err := db.ExecContext(ctx, "INSERT INTO counter (id, value) VALUES (?, ?)", 7, 1); err != nil {
		t.Fatal(err)
	}
	var v int
	if err := db.QueryRowContext(ctx, "SELECT value FROM counter WHERE id = ?", 7).Scan(&v); err != nil {
		t.Fatal(err)
	}
	parent.End()

	got := spansOf(parent.SpanContext().TraceID())
	if len(got) < 3 {
		t.Fatalf("got %d spans, want the request span and at least one per statement", len(got))
	}
	for _, s := range got {
		if s.Status().Code == codes.Error {
			t.Errorf("span %q has status Error (%s); a successful statement must not", s.Name(), s.Status().Description)
		}
		if len(s.Events()) != 0 {
			t.Errorf("span %q has %d events (first: %s); want none", s.Name(), len(s.Events()), s.Events()[0].Name)
		}
	}
}
