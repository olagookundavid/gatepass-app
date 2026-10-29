package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/orders/app"
	"github.com/olagookundavid/gatepass-app/internal/orders/domain"
	"github.com/olagookundavid/gatepass-app/internal/orders/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
)

type verifier struct{}

func (verifier) Verify(_ context.Context, t string) (*auth.Claims, error) {
	switch t {
	case "u1", "u2":
		return &auth.Claims{Subject: t}, nil
	case "admin":
		return &auth.Claims{Subject: "admin", Groups: []string{"admin"}}, nil
	}
	return nil, auth.ErrInvalidToken
}

type repo struct {
	ports.Orders
	orders map[string]domain.Order
	idem   map[string]ports.IdempotencyRecord
}

func (r *repo) CreateOrder(_ context.Context, n ports.NewOrder) error {
	r.orders[n.Order.ID] = n.Order
	r.idem[n.Order.UserID+n.IdemKey] = n.Idempotency
	return nil
}
func (r *repo) FindIdempotency(_ context.Context, u, k string) (ports.IdempotencyRecord, bool, error) {
	x, ok := r.idem[u+k]
	return x, ok, nil
}
func (r *repo) Get(_ context.Context, id string) (domain.Order, error) {
	if o, ok := r.orders[id]; ok {
		return o, nil
	}
	return domain.Order{}, domain.ErrNotFound
}
func (r *repo) MarkPaymentEnqueued(context.Context, string, time.Time) error { return nil }
func (r *repo) Transition(_ context.Context, id string, _ string, decide func(domain.Order) (domain.Change, error)) (domain.Order, bool, error) {
	o := r.orders[id]
	ch, err := decide(o)
	if err == nil && ch.Changed {
		o.Status = ch.NewStatus
		r.orders[id] = o
	}
	return o, ch.Changed, err
}

type holds struct{ err error }

func (h holds) Consume(_ context.Context, id, _, _ string) (ports.ConsumedHold, error) {
	if h.err != nil {
		return ports.ConsumedHold{}, h.err
	}
	return ports.ConsumedHold{HoldID: id, EventID: "e", TicketTypeID: "t", Quantity: 1, UnitPriceCents: 500, Currency: "USD"}, nil
}

type pay struct{}

func (pay) Enqueue(context.Context, domain.PaymentRequest) error { return nil }

const hold = "11111111-1111-7111-8111-111111111111"

func srv(h holds) http.Handler {
	r := &repo{orders: map[string]domain.Order{}, idem: map[string]ports.IdempotencyRecord{}}
	svc := app.New(app.Service{Orders: r, Holds: h, Payments: pay{}})
	mux := http.NewServeMux()
	(&Handler{Svc: svc, Verifier: verifier{}, BasePath: "/orders"}).Routes(mux)
	return httpserver.New(httpserver.Options{Service: "t", Handler: mux}).Handler
}

func do(h http.Handler, method, path, token, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestOrderLifecycleOverHTTP(t *testing.T) {
	h := srv(holds{})
	idem := map[string]string{"Idempotency-Key": "key-12345678"}
	body := `{"holdId":"` + hold + `"}`

	if rec := do(h, "POST", "/orders/v1/orders", "", body, idem); rec.Code != 401 {
		t.Errorf("no token %d", rec.Code)
	}
	if rec := do(h, "POST", "/orders/v1/orders", "u1", body, nil); rec.Code != 422 {
		t.Errorf("missing key %d", rec.Code)
	}
	if rec := do(h, "POST", "/orders/v1/orders", "u1", `{"holdId":"nope"}`, idem); rec.Code != 422 {
		t.Errorf("bad hold id %d", rec.Code)
	}
	rec := do(h, "POST", "/orders/v1/orders", "u1", body, idem)
	if rec.Code != 201 {
		t.Fatalf("create %d %s", rec.Code, rec.Body)
	}
	var o domain.Order
	_ = json.Unmarshal(rec.Body.Bytes(), &o)
	if o.Status != domain.StatusPendingPayment || o.AmountCents != 500 {
		t.Fatalf("%+v", o)
	}
	rec = do(h, "POST", "/orders/v1/orders", "u1", body, idem)
	if rec.Code != 201 || rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Errorf("replay %d %v", rec.Code, rec.Header())
	}
	if rec := do(h, "POST", "/orders/v1/orders", "u1", `{"holdId":"22222222-2222-7222-8222-222222222222"}`, idem); rec.Code != 409 {
		t.Errorf("key reuse %d", rec.Code)
	}

	path := "/orders/v1/orders/" + o.ID
	if rec := do(h, "GET", path, "u2", "", nil); rec.Code != 404 {
		t.Errorf("other user %d", rec.Code)
	}
	if rec := do(h, "GET", path, "admin", "", nil); rec.Code != 200 {
		t.Errorf("admin %d", rec.Code)
	}
	if rec := do(h, "POST", path+"/cancel", "u2", "", nil); rec.Code != 404 {
		t.Errorf("cancel by other user %d", rec.Code)
	}
	if rec := do(h, "POST", path+"/cancel", "u1", "", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "cancelled") {
		t.Errorf("cancel %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, "POST", path+"/cancel", "u1", `{"reason":"again"}`, nil); rec.Code != 200 {
		t.Errorf("cancel again %d", rec.Code)
	}
}

func TestHoldErrorsBecomeProblems(t *testing.T) {
	for err, want := range map[error]int{
		domain.ErrHoldExpired: 409, domain.ErrHoldTaken: 409, domain.ErrHoldNotFound: 422, domain.ErrReservationsDown: 503,
	} {
		rec := do(srv(holds{err: err}), "POST", "/orders/v1/orders", "u1", `{"holdId":"`+hold+`"}`, map[string]string{"Idempotency-Key": "key-12345678"})
		if rec.Code != want || rec.Header().Get("Content-Type") != "application/problem+json" {
			t.Errorf("%v: got %d", err, rec.Code)
		}
	}
}
