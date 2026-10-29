package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
	"github.com/olagookundavid/gatepass-app/internal/reservations/app"
	"github.com/olagookundavid/gatepass-app/internal/reservations/domain"
	"github.com/olagookundavid/gatepass-app/internal/reservations/ports"
)

type fakeVerifier struct{}

func (fakeVerifier) Verify(_ context.Context, tok string) (*auth.Claims, error) {
	if tok == "u1" || tok == "u2" {
		return &auth.Claims{Subject: tok}, nil
	}
	return nil, auth.ErrInvalidToken
}

// store implements only what the tested routes call.
type store struct {
	ports.Store
	inv   domain.Inventory
	holds map[string]domain.Hold
	idem  map[string]ports.IdempotencyRecord
}

func (s *store) GetInventory(context.Context, string, string) (domain.Inventory, error) {
	return s.inv, nil
}
func (s *store) GetIdempotency(_ context.Context, u, k string) (ports.IdempotencyRecord, bool, error) {
	r, ok := s.idem[u+k]
	return r, ok, nil
}
func (s *store) CreateHold(_ context.Context, h domain.Hold, k string, r ports.IdempotencyRecord, _ time.Duration) error {
	s.holds[h.ID] = h
	s.idem[h.UserID+k] = r
	s.inv.Available -= h.Quantity
	return nil
}
func (s *store) GetHold(_ context.Context, id string) (domain.Hold, error) {
	if h, ok := s.holds[id]; ok {
		return h, nil
	}
	return domain.Hold{}, domain.ErrNotFound
}
func (s *store) ConsumeHold(_ context.Context, id, order, user string, now time.Time) (domain.Hold, error) {
	h, ok := s.holds[id]
	if !ok {
		return domain.Hold{}, domain.ErrNotFound
	}
	if _, err := domain.ConsumeDecision(h, order, user, now); err != nil {
		return domain.Hold{}, err
	}
	h.Status, h.OrderID = domain.StatusConsumed, order
	s.holds[id] = h
	return h, nil
}

type okSched struct{}

func (okSched) ScheduleExpiry(context.Context, string, time.Time) error { return nil }
func (okSched) CancelExpiry(context.Context, string) error              { return nil }

type allowAll struct{}

func (allowAll) Allow(context.Context, string, int, time.Duration) (bool, error) { return true, nil }

const (
	ev = "11111111-1111-7111-8111-111111111111"
	tt = "22222222-2222-7222-8222-222222222222"
)

func setup() (public, internal http.Handler) {
	st := &store{inv: domain.Inventory{EventID: ev, TicketTypeID: tt, Capacity: 10, Available: 10, PriceCents: 700, Currency: "USD"},
		holds: map[string]domain.Hold{}, idem: map[string]ports.IdempotencyRecord{}}
	svc := app.New(app.Service{Store: st, Scheduler: okSched{}, Limiter: allowAll{}})
	h := &Handler{Svc: svc, Verifier: fakeVerifier{}, BasePath: "/reservations"}
	pm, im := http.NewServeMux(), http.NewServeMux()
	h.PublicRoutes(pm)
	h.InternalRoutes(im)
	return httpserver.New(httpserver.Options{Service: "t", Handler: pm}).Handler, httpserver.New(httpserver.Options{Service: "t", Handler: im}).Handler
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

const body = `{"eventId":"` + ev + `","ticketTypeId":"` + tt + `","quantity":2}`

func TestPublicHoldFlow(t *testing.T) {
	pub, _ := setup()
	idem := map[string]string{"Idempotency-Key": "key-12345678"}

	if rec := do(pub, "POST", "/reservations/v1/holds", "", body, idem); rec.Code != 401 {
		t.Errorf("no token: %d", rec.Code)
	}
	if rec := do(pub, "POST", "/reservations/v1/holds", "u1", body, nil); rec.Code != 422 {
		t.Errorf("missing Idempotency-Key must be rejected: %d", rec.Code)
	}
	if rec := do(pub, "POST", "/reservations/v1/holds", "u1", `{"eventId":"x","ticketTypeId":"y","quantity":9}`, idem); rec.Code != 422 || !strings.Contains(rec.Body.String(), "quantity") {
		t.Errorf("bad body: %d %s", rec.Code, rec.Body)
	}

	rec := do(pub, "POST", "/reservations/v1/holds", "u1", body, idem)
	if rec.Code != 201 {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	var h map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &h)
	if h["status"] != "active" || h["expiresAt"] == nil || h["userId"] != nil {
		t.Fatalf("hold body: %v", h)
	}
	id := h["holdId"].(string)

	// replay returns the same hold, flagged
	rec = do(pub, "POST", "/reservations/v1/holds", "u1", body, idem)
	var h2 map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &h2)
	if rec.Code != 201 || h2["holdId"] != id || rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Errorf("replay: %d %v", rec.Code, h2)
	}
	// same key, different body -> 409
	if rec := do(pub, "POST", "/reservations/v1/holds", "u1", strings.Replace(body, `"quantity":2`, `"quantity":3`, 1), idem); rec.Code != 409 {
		t.Errorf("key reuse: %d", rec.Code)
	}
	// owner only
	if rec := do(pub, "GET", "/reservations/v1/holds/"+id, "u2", "", nil); rec.Code != 404 {
		t.Errorf("other user's hold: %d", rec.Code)
	}
	if rec := do(pub, "GET", "/reservations/v1/holds/"+id, "u1", "", nil); rec.Code != 200 {
		t.Errorf("own hold: %d", rec.Code)
	}
}

func TestInternalConsume(t *testing.T) {
	pub, internal := setup()
	rec := do(pub, "POST", "/reservations/v1/holds", "u1", body, map[string]string{"Idempotency-Key": "key-12345678"})
	var h map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &h)
	path := "/internal/v1/holds/" + h["holdId"].(string) + "/consume"
	caller := map[string]string{"X-Caller-Service": "orders-api"}
	order1 := `{"orderId":"33333333-3333-7333-8333-333333333333","userId":"u1"}`
	order2 := `{"orderId":"44444444-4444-7444-8444-444444444444","userId":"u1"}`

	if rec := do(internal, "POST", path, "", order1, nil); rec.Code != 400 {
		t.Errorf("missing caller header: %d", rec.Code)
	}
	rec = do(internal, "POST", path, "", order1, caller)
	var c map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &c)
	if rec.Code != 200 || c["unitPriceCents"] != float64(700) || c["quantity"] != float64(2) || c["currency"] != "USD" {
		t.Fatalf("consume: %d %s", rec.Code, rec.Body)
	}
	if rec := do(internal, "POST", path, "", order1, caller); rec.Code != 200 {
		t.Errorf("same order again: %d", rec.Code)
	}
	if rec := do(internal, "POST", path, "", order2, caller); rec.Code != 409 {
		t.Errorf("different order: %d", rec.Code)
	}
	// the internal route does not exist on the public listener
	if rec := do(pub, "POST", path, "u1", order1, caller); rec.Code != 404 {
		t.Errorf("internal route reachable on public listener: %d", rec.Code)
	}
}
