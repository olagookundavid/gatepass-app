// Package httpserver builds the HTTP listener every binary uses: health routes,
// the standard middleware chain and sane timeouts.
//
// This file: the three health routes, served at the ROOT of the public listener
// (never under BASE_PATH) because the load balancer / kubelet probe fixed paths.
//
//	GET /livez   process is alive. Calls NO dependency: if the database is down
//	             restarting this pod would not help, and a restart storm would
//	             make the outage worse.
//	GET /readyz  critical dependencies reachable (each check 2 s timeout, whole
//	             result cached 5 s so probes cannot flood the database). Returns
//	             503 while shutting down so the Service stops routing here.
//	GET /version git SHA and build time (ldflags).
//
// AWS services touched: none directly; the checks injected by main may ping
// RDS / ElastiCache / DynamoDB.
package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/platform/buildinfo"
)

// Check is one named readiness probe.
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// Health implements the probe endpoints.
type Health struct {
	service string
	checks  []Check

	shuttingDown atomic.Bool

	mu        sync.Mutex
	cachedAt  time.Time
	cachedRes map[string]string
	cachedOK  bool
}

const (
	checkTimeout = 2 * time.Second
	cacheTTL     = 5 * time.Second
)

// NewHealth creates the health handler set.
func NewHealth(service string, checks ...Check) *Health {
	return &Health{service: service, checks: checks}
}

// SetShuttingDown flips /readyz to 503. Called first thing on SIGTERM; the
// process then keeps serving for SHUTDOWN_DELAY so the load balancer notices.
func (h *Health) SetShuttingDown() { h.shuttingDown.Store(true) }

// Register adds the routes to mux.
func (h *Health) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /readyz", h.readyz)
	mux.HandleFunc("GET /version", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"service": h.service, "version": buildinfo.Version, "buildTime": buildinfo.BuildTime,
		})
	})
}

func (h *Health) readyz(w http.ResponseWriter, r *http.Request) {
	if h.shuttingDown.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "shutting_down"})
		return
	}
	res, ok := h.runChecks(r.Context())
	status := http.StatusOK
	body := map[string]any{"status": "ready", "checks": res}
	if !ok {
		status = http.StatusServiceUnavailable
		body["status"] = "not_ready"
	}
	writeJSON(w, status, body)
}

// runChecks runs all checks concurrently or returns the cached result.
// Error strings are replaced by "fail" in the response: they may contain
// hostnames, and /readyz is reachable from the load balancer. Details are in the
// logs of whoever implements the check.
func (h *Health) runChecks(ctx context.Context) (map[string]string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cachedRes != nil && time.Since(h.cachedAt) < cacheTTL {
		return h.cachedRes, h.cachedOK
	}
	res := map[string]string{}
	var rm sync.Mutex
	var wg sync.WaitGroup
	allOK := true
	for _, c := range h.checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, checkTimeout)
			defer cancel()
			err := c.Fn(cctx)
			rm.Lock()
			defer rm.Unlock()
			if err != nil {
				res[c.Name], allOK = "fail", false
			} else {
				res[c.Name] = "ok"
			}
		}()
	}
	wg.Wait()
	h.cachedAt, h.cachedRes, h.cachedOK = time.Now(), res, allOK
	return res, allOK
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
