package httpserver

// This file: the middleware chain and the *http.Server constructor.
//
// Chain (outermost first):
//
//	otelhttp   starts the server span, extracts incoming `traceparent`, records
//	           request count/duration metrics
//	requestID  X-Request-Id: reuse the caller's (if sane) or generate one
//	accessLog  one structured log line per request (method, path, status, ms):
//	           info; debug for the probes /livez and /readyz; error for a 5xx
//	recoverer  a panic becomes a 500 problem, not a dead connection
//	bodyLimit  1 MiB cap: without it one client can make us buffer gigabytes
//	routeTag   (innermost) copies the matched route pattern onto the span and the
//	           request-duration metric as http.route
//
// AWS services touched: none.

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/olagookundavid/gatepass-app/internal/platform/logging"
	"github.com/olagookundavid/gatepass-app/internal/platform/problem"
)

// MaxBodyBytes is the request body limit (1 MiB).
const MaxBodyBytes = 1 << 20

// DrainTimeout bounds graceful shutdown of the listener.
const DrainTimeout = 25 * time.Second

// Options configures New.
type Options struct {
	Service string
	Addr    string
	// Handler holds the application routes (the mux with BASE_PATH + "/v1/..."
	// patterns). Health routes are added around it.
	Handler http.Handler
	Health  *Health // nil for listeners that must not expose health routes (internal listener)
}

// New builds the server with timeouts and the middleware chain.
func New(o Options) *http.Server {
	root := http.NewServeMux()
	if o.Health != nil {
		o.Health.Register(root)
	}
	if o.Handler != nil {
		root.Handle("/", o.Handler)
	}
	var h http.Handler = root
	h = routeTag(h)
	h = bodyLimit(h)
	h = recoverer(h)
	h = accessLog(h)
	h = requestID(h)
	h = otelhttp.NewHandler(h, o.Service,
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method + " " + o.Service }))

	return &http.Server{
		Addr:              o.Addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,  // slowloris protection
		ReadTimeout:       15 * time.Second, // whole request incl. body
		WriteTimeout:      30 * time.Second, // must exceed the slowest handler
		IdleTimeout:       60 * time.Second, // keep-alive connections from the load balancer
		MaxHeaderBytes:    32 << 10,
	}
}

// Component adapts an *http.Server to lifecycle.Component.
type Component struct {
	Name_ string
	Srv   *http.Server
}

// Name implements lifecycle.Component.
func (c Component) Name() string { return c.Name_ }

// Run serves until ctx is cancelled, then drains in-flight requests.
func (c Component) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		slog.Info("http listening", "component", c.Name_, "addr", c.Srv.Addr)
		errCh <- c.Srv.ListenAndServe()
	}()
	select {
	case err := <-errCh:
		return err // listener failed (e.g. port in use)
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), DrainTimeout)
	defer cancel()
	if err := c.Srv.Shutdown(sctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

var sane = regexp.MustCompile(`^[A-Za-z0-9._-]{8,64}$`)

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if !sane.MatchString(id) { // never echo arbitrary client text into logs/headers
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-Id", id)
		ctx, _ := logging.WithReqInfo(r.Context(), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(c int)           { s.status = c; s.ResponseWriter.WriteHeader(c) }
func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		// Probes are noisy: log them at debug, wherever they are mounted (root, or
		// under BASE_PATH such as /skeleton/readyz). Every other 5xx is an error line,
		// so "errors in the last 5 minutes" is a query on level=error. A failing probe
		// stays at debug on purpose: a readiness 503 during shutdown or a forced
		// not-ready would otherwise write an error line every probe period.
		lvl := slog.LevelInfo
		switch path.Base(r.URL.Path) {
		case "livez", "readyz":
			lvl = slog.LevelDebug
		default:
			if sw.status >= 500 {
				lvl = slog.LevelError
			}
		}
		slog.Log(r.Context(), lvl, "http request",
			"method", r.Method, "path", r.URL.Path, "status", sw.status,
			"duration_ms", time.Since(start).Milliseconds())
	})
}

func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if rec == http.ErrAbortHandler { //nolint:errorlint // sentinel panic value, documented by net/http
					panic(rec)
				}
				slog.ErrorContext(r.Context(), "panic in handler", "panic", rec)
				problem.Write(w, r, problem.Internal())
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func bodyLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		next.ServeHTTP(w, r)
	})
}

// routeTag records the matched ServeMux pattern ("GET /catalog/v1/events/{id}"
// becomes the route "/catalog/v1/events/{id}") as http.route. otelhttp would do
// it itself, but it reads Request.Pattern from the request it passed down, and
// requestID makes a copy (WithContext), so the pattern the mux sets would be lost
// and every request-duration series would have no route: no per-endpoint RED.
// Patterns are templates, so the label has low cardinality; an unmatched path
// has no pattern and gets no route (never the raw URL).
func routeTag(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		if r.Pattern == "" {
			return
		}
		route := r.Pattern
		if _, after, ok := strings.Cut(route, " "); ok {
			route = after // drop the method
		}
		kv := attribute.String("http.route", route)
		if l, ok := otelhttp.LabelerFromContext(r.Context()); ok {
			l.Add(kv)
		}
		trace.SpanFromContext(r.Context()).SetAttributes(kv)
	})
}
