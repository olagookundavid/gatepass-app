package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/olagookundavid/gatepass-app/internal/platform/buildinfo"
	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
	"github.com/olagookundavid/gatepass-app/internal/platform/logging"
)

// server builds the full platform chain (spans, request id, access log) around
// the skeleton routes, the same way run() does.
func server(basePath string, force bool, shuttingDown *atomic.Bool) http.Handler {
	return httpserver.New(httpserver.Options{
		Service: service,
		Handler: routes(basePath, force, shuttingDown),
	}).Handler
}

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func body(t *testing.T, rec *httptest.ResponseRecorder) map[string]string {
	t.Helper()
	var m map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("body %q is not JSON: %v", rec.Body.String(), err)
	}
	return m
}

func TestRoutesUnderBasePathAndRoot(t *testing.T) {
	h := server("/skeleton", false, &atomic.Bool{})
	// The base path serves the Ingress; the root serves the kubelet probes.
	for _, prefix := range []string{"/skeleton", ""} {
		if rec := get(h, prefix+"/livez"); rec.Code != 200 || body(t, rec)["status"] != "ok" {
			t.Errorf("%s/livez: %d %s", prefix, rec.Code, rec.Body)
		}
		if rec := get(h, prefix+"/readyz"); rec.Code != 200 || body(t, rec)["status"] != "ready" {
			t.Errorf("%s/readyz: %d %s", prefix, rec.Code, rec.Body)
		}
	}
	if rec := get(h, "/other/livez"); rec.Code != 404 {
		t.Errorf("unknown prefix must be 404, got %d", rec.Code)
	}
	if rec := httptest.NewRecorder(); true {
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/skeleton/livez", nil))
		if rec.Code != 405 {
			t.Errorf("POST must be 405, got %d", rec.Code)
		}
	}
}

func TestCustomBasePath(t *testing.T) {
	h := server("/demo", false, &atomic.Bool{})
	if rec := get(h, "/demo/readyz"); rec.Code != 200 {
		t.Errorf("/demo/readyz: %d", rec.Code)
	}
	if rec := get(h, "/skeleton/readyz"); rec.Code != 404 {
		t.Errorf("default prefix must not exist when BASE_PATH is /demo: %d", rec.Code)
	}
	// An empty BASE_PATH must not register a second, duplicate set of routes (panic).
	h = server("", false, &atomic.Bool{})
	if rec := get(h, "/readyz"); rec.Code != 200 {
		t.Errorf("/readyz with empty base path: %d", rec.Code)
	}
}

func TestForceNotReady(t *testing.T) {
	h := server("/skeleton", true, &atomic.Bool{})
	for _, p := range []string{"/skeleton/readyz", "/readyz"} {
		if rec := get(h, p); rec.Code != 503 || body(t, rec)["status"] != "not ready" {
			t.Errorf("%s: %d %s", p, rec.Code, rec.Body)
		}
	}
	if rec := get(h, "/skeleton/livez"); rec.Code != 200 {
		t.Errorf("livez must stay 200 when not ready (a restart would not help): %d", rec.Code)
	}
}

func TestShuttingDownIsNotReady(t *testing.T) {
	var down atomic.Bool
	h := server("/skeleton", false, &down)
	down.Store(true)
	if rec := get(h, "/skeleton/readyz"); rec.Code != 503 {
		t.Errorf("readyz while shutting down: %d", rec.Code)
	}
}

func TestVersion(t *testing.T) {
	oldV, oldT := buildinfo.Version, buildinfo.BuildTime
	buildinfo.Version, buildinfo.BuildTime = "abc1234", "2026-11-07T10:00:00Z"
	t.Cleanup(func() { buildinfo.Version, buildinfo.BuildTime = oldV, oldT })

	rec := get(server("/skeleton", false, &atomic.Bool{}), "/skeleton/version")
	want := map[string]string{"service": "skeleton-api", "version": "abc1234", "buildTime": "2026-11-07T10:00:00Z", "commit": "abc1234"}
	got := body(t, rec)
	if rec.Code != 200 || len(got) != len(want) {
		t.Fatalf("version: %d %s", rec.Code, rec.Body)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}

func TestConfigDefaultsNeedNothing(t *testing.T) {
	l := config.FromMap(map[string]string{})
	c := loadConfig(l)
	if err := l.Err(); err != nil {
		t.Fatalf("no variable is required, got %v", err)
	}
	if c.HTTPAddr != ":8080" || c.BasePath != "/skeleton" || c.LogLevel != "info" || c.ForceNotReady || c.ShutdownDelay.Seconds() != 5 {
		t.Errorf("defaults: %+v", c)
	}
	c = loadConfig(config.FromMap(map[string]string{"BASE_PATH": "/x/", "FORCE_NOT_READY": "true"}))
	if c.BasePath != "/x" || !c.ForceNotReady {
		t.Errorf("overrides: %+v", c)
	}
	l = config.FromMap(map[string]string{"FORCE_NOT_READY": "maybe"})
	loadConfig(l)
	if l.Err() == nil {
		t.Error("an invalid FORCE_NOT_READY must be reported")
	}
}

// telemetryHarness installs in-memory trace and metric providers and a JSON
// logger writing to a buffer, then restores the globals. It must run BEFORE the
// server is built: otelhttp reads the global providers in NewHandler.
func telemetryHarness(t *testing.T) (*tracetest.SpanRecorder, *sdkmetric.ManualReader, *bytes.Buffer) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	var buf bytes.Buffer

	oldTP, oldMP, oldLog := otel.GetTracerProvider(), otel.GetMeterProvider(), slog.Default()
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	slog.SetDefault(logging.New(&buf, service, "test", "info"))
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTP)
		otel.SetMeterProvider(oldMP)
		slog.SetDefault(oldLog)
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	})
	return rec, reader, &buf
}

func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		out = append(out, m)
	}
	return out
}

func TestVersionRequestHasSpanAndLogWithSameTraceID(t *testing.T) {
	spans, _, buf := telemetryHarness(t)
	h := server("/skeleton", false, &atomic.Bool{})

	get(h, "/skeleton/version")

	ended := spans.Ended()
	if len(ended) != 1 {
		t.Fatalf("want one server span, got %d", len(ended))
	}
	traceID, spanID := ended[0].SpanContext().TraceID().String(), ended[0].SpanContext().SpanID().String()

	lines := logLines(t, buf)
	if len(lines) != 1 {
		t.Fatalf("want one access-log line at info, got %d: %s", len(lines), buf)
	}
	l := lines[0]
	for _, k := range []string{"time", "level", "msg"} {
		if _, ok := l[k]; !ok {
			t.Errorf("log line misses %q: %v", k, l)
		}
	}
	if l["level"] != "INFO" || l["trace_id"] != traceID || l["span_id"] != spanID {
		t.Errorf("log %v does not carry the span's ids (trace %s span %s)", l, traceID, spanID)
	}
}

func TestProbesAreNotLoggedAtInfoAndServerErrorsAreErrors(t *testing.T) {
	_, _, buf := telemetryHarness(t)
	h := server("/skeleton", true, &atomic.Bool{}) // /readyz answers 503

	for _, p := range []string{"/skeleton/livez", "/skeleton/readyz", "/livez", "/readyz"} {
		get(h, p)
	}
	if buf.Len() != 0 {
		t.Errorf("probes (even a failing /readyz) must not log at info or error: %s", buf)
	}

	// A 5xx on a normal route is an error line. The skeleton has no such route
	// of its own, so use a panicking handler behind the same platform chain.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("boom") })
	rec := httptest.NewRecorder()
	httpserver.New(httpserver.Options{Service: service, Handler: mux}).Handler.
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/boom", nil))
	if rec.Code != 500 {
		t.Fatalf("boom: %d", rec.Code)
	}
	var sawError bool
	for _, l := range logLines(t, buf) {
		if l["msg"] == "http request" && l["level"] == "ERROR" && l["status"] == float64(500) {
			sawError = true
		}
	}
	if !sawError {
		t.Errorf("no error-level access-log line for the 500: %s", buf)
	}
}

func TestRequestDurationMetricHasStatusAndRoute(t *testing.T) {
	_, reader, _ := telemetryHarness(t)
	h := server("/skeleton", false, &atomic.Bool{})
	get(h, "/skeleton/version")

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "http.server.request.duration" {
				continue
			}
			if m.Unit != "s" {
				t.Errorf("unit %q, want s", m.Unit)
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || len(hist.DataPoints) == 0 {
				t.Fatalf("unexpected data %T", m.Data)
			}
			attrs := hist.DataPoints[0].Attributes
			if v, _ := attrs.Value("http.response.status_code"); v.AsInt64() != 200 {
				t.Errorf("status attribute: %v", attrs)
			}
			if v, ok := attrs.Value("http.route"); !ok || v.AsString() != "/skeleton/version" {
				t.Errorf("route attribute: %v", attrs)
			}
			return
		}
	}
	t.Fatal("http.server.request.duration was not recorded")
}
