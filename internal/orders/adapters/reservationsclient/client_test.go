package reservationsclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/orders/domain"
)

const ok = `{"holdId":"h1","eventId":"e1","ticketTypeId":"t1","quantity":2,"unitPriceCents":700,"currency":"USD"}`

func problem(code string) string {
	return `{"type":"urn:gatepass:problem:` + code + `","title":"x","status":409,"code":"` + code + `"}`
}

func TestConsumeMapsAnswers(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   error
	}{
		{404, problem("not_found"), domain.ErrHoldNotFound},
		{403, problem("forbidden"), domain.ErrHoldNotYours},
		{409, problem("hold_expired"), domain.ErrHoldExpired},
		{409, problem("hold_released"), domain.ErrHoldReleased},
		{409, problem("hold_already_consumed"), domain.ErrHoldTaken},
	}
	for _, c := range cases {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(c.status)
			_, _ = w.Write([]byte(c.body))
		}))
		cl := New(srv.URL, "orders-api")
		_, err := cl.Consume(context.Background(), "h1", "o1", "u1")
		srv.Close()
		if !errors.Is(err, c.want) || calls.Load() != 1 {
			t.Errorf("%d %s: err=%v calls=%d (4xx must not be retried)", c.status, c.body, err, calls.Load())
		}
	}
}

func TestConsumeSendsCallerHeaderAndBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]string
		_ = json.NewDecoder(r.Body).Decode(&b)
		if r.Header.Get("X-Caller-Service") != "orders-api" || b["orderId"] != "o1" || b["userId"] != "u1" || r.URL.Path != "/internal/v1/holds/h1/consume" {
			t.Errorf("bad request: %v %v %s", r.Header, b, r.URL.Path)
		}
		_, _ = w.Write([]byte(ok))
	}))
	defer srv.Close()
	h, err := New(srv.URL+"/", "orders-api").Consume(context.Background(), "h1", "o1", "u1")
	if err != nil || h.UnitPriceCents != 700 || h.Quantity != 2 {
		t.Fatalf("%+v %v", h, err)
	}
}

func TestRetriesOnlyServerErrorsAndIsBounded(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(503)
			return
		}
		_, _ = w.Write([]byte(ok))
	}))
	defer srv.Close()
	cl := New(srv.URL, "orders-api")
	cl.Backoff = time.Millisecond
	if _, err := cl.Consume(context.Background(), "h1", "o1", "u1"); err != nil || calls.Load() != 3 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}

	calls.Store(-100) // always 503
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer srv2.Close()
	calls.Store(0)
	cl2 := New(srv2.URL, "orders-api")
	cl2.Backoff = time.Millisecond
	_, err := cl2.Consume(context.Background(), "h1", "o1", "u1")
	if !errors.Is(err, domain.ErrReservationsDown) || calls.Load() != 3 {
		t.Fatalf("err=%v calls=%d, want 3 bounded attempts", err, calls.Load())
	}
}
