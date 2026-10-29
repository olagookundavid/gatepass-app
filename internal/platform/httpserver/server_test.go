package httpserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHealthRoutesAndShutdown(t *testing.T) {
	var calls atomic.Int32
	failing := atomic.Bool{}
	h := NewHealth("svc", Check{Name: "db", Fn: func(ctx context.Context) error {
		calls.Add(1)
		if failing.Load() {
			return errors.New("secret-host.internal refused")
		}
		return nil
	}})
	srv := New(Options{Service: "svc", Health: h, Handler: http.NewServeMux()})

	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		return rec
	}
	if rec := get("/livez"); rec.Code != 200 {
		t.Fatalf("livez %d", rec.Code)
	}
	if rec := get("/readyz"); rec.Code != 200 || rec.Header().Get("X-Request-Id") == "" {
		t.Fatalf("readyz %d", rec.Code)
	}
	get("/readyz")
	if calls.Load() != 1 {
		t.Errorf("readiness result not cached: %d calls", calls.Load())
	}
	if rec := get("/version"); !strings.Contains(rec.Body.String(), `"service":"svc"`) {
		t.Errorf("version body %s", rec.Body)
	}
	h.SetShuttingDown()
	if rec := get("/readyz"); rec.Code != 503 {
		t.Errorf("readyz while shutting down: %d", rec.Code)
	}
	if rec := get("/livez"); rec.Code != 200 {
		t.Errorf("livez must stay 200 while draining: %d", rec.Code)
	}
}

func TestReadyzFailureHidesDetails(t *testing.T) {
	h := NewHealth("svc", Check{Name: "db", Fn: func(context.Context) error { return errors.New("secret-host.internal refused") }})
	srv := New(Options{Service: "svc", Health: h})
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != 503 || strings.Contains(rec.Body.String(), "secret-host") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

func TestRequestIDAndBodyLimitAndPanic(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /echo", func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 2<<20)
		if _, err := r.Body.Read(buf); err != nil && !errors.Is(err, http.ErrBodyReadAfterClose) {
			w.WriteHeader(413)
			return
		}
		w.WriteHeader(200)
	})
	mux.HandleFunc("GET /panic", func(http.ResponseWriter, *http.Request) { panic("boom") })
	srv := New(Options{Service: "svc", Handler: mux})

	req := httptest.NewRequest("GET", "/panic", nil)
	req.Header.Set("X-Request-Id", "bad id with spaces")
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != 500 || rec.Header().Get("X-Request-Id") == "bad id with spaces" {
		t.Fatalf("panic/request-id: %d %q", rec.Code, rec.Header().Get("X-Request-Id"))
	}

	req = httptest.NewRequest("GET", "/panic", nil)
	req.Header.Set("X-Request-Id", "abcdef-123456")
	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	if rec.Header().Get("X-Request-Id") != "abcdef-123456" {
		t.Errorf("sane request id not preserved")
	}

	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest("POST", "/echo", strings.NewReader(strings.Repeat("a", 2<<20))))
	if rec.Code != 413 {
		t.Errorf("oversized body: %d", rec.Code)
	}
}
