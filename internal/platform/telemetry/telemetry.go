// Package telemetry wires OpenTelemetry traces and metrics.
//
// Purpose: give every binary distributed tracing (inbound HTTP, outbound HTTP,
// AWS SDK, SQL, Redis) and business metrics, exported over OTLP/HTTP to the
// collector running in the cluster.
//
// If OTEL_EXPORTER_OTLP_ENDPOINT is unset the global providers stay the OTel
// no-op implementations: instrumentation calls cost almost nothing and nothing
// is exported. That keeps local runs and unit tests dependency-free.
//
// Context propagation: W3C trace context (header "traceparent"). Across HTTP it
// travels as a header; across SQS as the message attribute "traceparent"; across
// EventBridge inside detail.traceparent. TraceparentFromContext /
// ContextWithTraceparent are the two helpers used at those boundaries.
//
// Env: OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES
// (the SDK reads all three itself; see the OpenTelemetry "SDK environment
// variables" specification).
//
// AWS services touched: none directly (the collector may forward to X-Ray / CloudWatch).
package telemetry

import (
	"context"
	"errors"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/olagookundavid/gatepass-app"

// Setup installs the global propagator and, if an OTLP endpoint is configured,
// the trace and metric providers. The returned function flushes and stops them;
// call it during graceful shutdown (it is bounded by ctx).
func Setup(ctx context.Context, service, version string) (func(context.Context) error, error) {
	// Always install the propagator: even with exporting disabled we still want
	// to forward an incoming traceparent to downstream calls.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(), // OTEL_SERVICE_NAME + OTEL_RESOURCE_ATTRIBUTES
		resource.WithAttributes(semconv.ServiceName(service), semconv.ServiceVersion(version)),
	)
	if err != nil {
		return nil, err
	}

	traceExp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)

	metricExp, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(30*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
	}, nil
}

// Tracer returns the application tracer.
func Tracer() trace.Tracer { return otel.Tracer(instrumentationName) }

// Meter returns the application meter.
func Meter() metric.Meter { return otel.Meter(instrumentationName) }

// Counter creates an int counter. Errors (only possible with a bad name) fall
// back to a no-op instrument so metrics can never break request handling.
func Counter(name, description string) metric.Int64Counter {
	c, err := Meter().Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		c, _ = noop.NewMeterProvider().Meter("noop").Int64Counter("noop")
	}
	return c
}

// Histogram creates a float histogram (unit e.g. "s").
func Histogram(name, unit, description string) metric.Float64Histogram {
	h, err := Meter().Float64Histogram(name, metric.WithUnit(unit), metric.WithDescription(description))
	if err != nil {
		h, _ = noop.NewMeterProvider().Meter("noop").Float64Histogram("noop")
	}
	return h
}

// Gauge registers an observable gauge whose value is read by fn at export time.
// Used for "how many rows are waiting in the outbox right now".
func Gauge(name, description string, fn func(ctx context.Context) int64) {
	_, _ = Meter().Int64ObservableGauge(name, metric.WithDescription(description),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(fn(ctx))
			return nil
		}))
}

// TraceparentFromContext serialises the active span context as a W3C
// "traceparent" string ("" when there is no valid span).
func TraceparentFromContext(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// ContextWithTraceparent makes tp (as stored in an SQS attribute, an outbox row
// or an EventBridge detail) the remote parent of any span started from the
// returned context. An empty or malformed tp returns ctx unchanged.
func ContextWithTraceparent(ctx context.Context, tp string) context.Context {
	if tp == "" {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": tp})
}
