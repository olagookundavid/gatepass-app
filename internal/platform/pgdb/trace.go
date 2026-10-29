package pgdb

// This file: OpenTelemetry for SQL. One span per statement, plus pool gauges.
// The statement text is recorded (it uses $1,$2 placeholders so it holds no
// user data); bind arguments are never recorded.

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

type queryTracer struct{}

type spanKey struct{}

func (queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	stmt := strings.Join(strings.Fields(d.SQL), " ")
	if len(stmt) > 300 {
		stmt = stmt[:300]
	}
	ctx, span := telemetry.Tracer().Start(ctx, "db.query", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("db.system", "postgresql"), attribute.String("db.statement", stmt)))
	return context.WithValue(ctx, spanKey{}, span)
}

func (queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	span, _ := ctx.Value(spanKey{}).(trace.Span)
	if span == nil {
		return
	}
	if d.Err != nil {
		span.RecordError(d.Err)
		span.SetStatus(codes.Error, "query failed")
	}
	span.End()
}

// registerPoolMetrics exposes pool saturation: if "acquired" stays at max the
// service is waiting for connections and needs a bigger pool or faster queries.
func registerPoolMetrics(db string, pool *pgxpool.Pool) {
	attr := metric.WithAttributes(attribute.String("db", db))
	for name, fn := range map[string]func() int64{
		"db_pool_connections_acquired": func() int64 { return int64(pool.Stat().AcquiredConns()) },
		"db_pool_connections_idle":     func() int64 { return int64(pool.Stat().IdleConns()) },
		"db_pool_connections_total":    func() int64 { return int64(pool.Stat().TotalConns()) },
		"db_pool_connections_max":      func() int64 { return int64(pool.Stat().MaxConns()) },
	} {
		_, _ = telemetry.Meter().Int64ObservableGauge(name, metric.WithInt64Callback(
			func(_ context.Context, o metric.Int64Observer) error { o.Observe(fn(), attr); return nil }))
	}
}
