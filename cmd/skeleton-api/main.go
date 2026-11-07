// Command skeleton-api is the walking-skeleton service: a tiny, dependency-free
// HTTP binary that goes through exactly the same Dockerfile, release workflow,
// chart and Argo CD path as the real services, before the data layer exists.
//
//	docker build -f build/Dockerfile --build-arg BINARY=skeleton-api .
//
// It exists because the real services cannot start without RDS, ElastiCache,
// S3, EventBridge and Cognito. This one needs nothing: no AWS client, no
// database, no network call at startup, no required environment variable.
//
// Routes (served under BASE_PATH, default /skeleton, AND at the root, because
// the kubelet probes the fixed root paths while the Ingress routes BASE_PATH):
//
//	GET /livez    200 {"status":"ok"} while the process runs
//	GET /readyz   200 {"status":"ready"}; 503 {"status":"not ready"} while
//	              FORCE_NOT_READY=true or while shutting down
//	GET /version  service, version (git SHA), buildTime, commit (= version)
//
// FORCE_NOT_READY=true is how the pipeline proves "a broken image is not
// promoted" without a code change: set it in a values file in Git, and the
// PostSync smoke test (which calls /readyz) fails.
//
// Telemetry comes entirely from internal/platform: the httpserver middleware
// starts one server span per request and records http.server.request.duration,
// the logger adds trace_id/span_id, and the access log writes one line per
// request (probes at debug, 5xx at error). Exporting is a no-op unless
// OTEL_EXPORTER_OTLP_ENDPOINT is set.
//
// Environment (all optional): HTTP_ADDR (:8080), BASE_PATH (/skeleton),
// LOG_LEVEL (info), SHUTDOWN_DELAY (5s), FORCE_NOT_READY (false), plus the
// OTEL_* variables. See contracts/skeleton-api.yaml.
//
// AWS services touched: none.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/platform/buildinfo"
	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
	"github.com/olagookundavid/gatepass-app/internal/platform/lifecycle"
	"github.com/olagookundavid/gatepass-app/internal/platform/logging"
	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

const service = "skeleton-api"

type cfg struct {
	LogLevel      string
	HTTPAddr      string
	BasePath      string
	ShutdownDelay time.Duration
	ForceNotReady bool
}

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		slog.Error("fatal", "error", err.Error())
		os.Exit(1)
	}
}

func run() error {
	l := config.FromEnv()
	c := loadConfig(l)
	if err := l.Err(); err != nil {
		return err
	}

	// bootstrap.Init is not used: it loads an AWS config and needs AWS_REGION,
	// and this binary must not touch AWS. These are its logger and telemetry lines.
	slog.SetDefault(logging.New(os.Stdout, service, buildinfo.Version, c.LogLevel))
	flush, err := telemetry.Setup(context.Background(), service, buildinfo.Version)
	if err != nil {
		return err
	}
	slog.Info("starting", "build_time", buildinfo.BuildTime,
		"base_path", c.BasePath, "force_not_ready", c.ForceNotReady)

	var shuttingDown atomic.Bool
	srv := httpserver.New(httpserver.Options{
		Service: service,
		Addr:    c.HTTPAddr,
		Handler: routes(c.BasePath, c.ForceNotReady, &shuttingDown),
	})
	return lifecycle.Run(lifecycle.Options{
		SetNotReady:   func() { shuttingDown.Store(true) },
		ShutdownDelay: c.ShutdownDelay,
		Closers:       []func(context.Context) error{flush},
	}, httpserver.Component{Name_: "public-http", Srv: srv})
}

func loadConfig(l *config.Loader) cfg {
	return cfg{
		LogLevel:      l.String("LOG_LEVEL", "info"),
		HTTPAddr:      l.String("HTTP_ADDR", ":8080"),
		BasePath:      strings.TrimRight(l.String("BASE_PATH", "/skeleton"), "/"),
		ShutdownDelay: l.Duration("SHUTDOWN_DELAY", 5*time.Second),
		ForceNotReady: l.Bool("FORCE_NOT_READY", false),
	}
}

// routes serves the three routes at the root and under basePath (when set).
func routes(basePath string, forceNotReady bool, shuttingDown *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	prefixes := []string{""}
	if basePath != "" {
		prefixes = append(prefixes, basePath)
	}
	for _, p := range prefixes {
		mux.HandleFunc("GET "+p+"/livez", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		})
		mux.HandleFunc("GET "+p+"/readyz", func(w http.ResponseWriter, _ *http.Request) {
			if forceNotReady || shuttingDown.Load() {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		})
		mux.HandleFunc("GET "+p+"/version", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, map[string]string{
				"service":   service,
				"version":   buildinfo.Version,
				"buildTime": buildinfo.BuildTime,
				"commit":    buildinfo.Version,
			})
		})
	}
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
