package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/notifications/app"
	"github.com/olagookundavid/gatepass-app/internal/notifications/domain"
	"github.com/olagookundavid/gatepass-app/internal/notifications/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
)

type verifier struct{}

func (verifier) Verify(_ context.Context, t string) (*auth.Claims, error) {
	if t == "u1" || t == "u2" {
		return &auth.Claims{Subject: t}, nil
	}
	return nil, auth.ErrInvalidToken
}

type store struct {
	ports.Store
	prefs   map[string]domain.Preferences
	tickets map[string]string
}

func (s *store) GetPreferences(_ context.Context, u string) (domain.Preferences, error) {
	if p, ok := s.prefs[u]; ok {
		return p, nil
	}
	return domain.Preferences{EmailEnabled: true}, nil
}
func (s *store) PutPreferences(_ context.Context, u string, p domain.Preferences) error {
	s.prefs[u] = p
	return nil
}
func (s *store) GetTicketRecord(_ context.Context, u, o string) (string, bool, error) {
	k, ok := s.tickets[u+"/"+o]
	return k, ok, nil
}
func (s *store) ListNotifications(_ context.Context, u string, _ *ports.NotificationCursor, _ int) ([]domain.Notification, *ports.NotificationCursor, error) {
	return []domain.Notification{{ID: "n1", UserID: u, Type: domain.TypeOrderPaid, OrderID: "o1", Channel: "email", Status: "sent", MessageID: "secret-ses-id", CreatedAt: time.Unix(1, 0).UTC()}}, nil, nil
}

type tk struct{}

func (tk) Put(context.Context, string, []byte) error { return nil }
func (tk) PresignGet(_ context.Context, k, _ string, _ time.Duration) (string, error) {
	return "https://s3.example.com/" + k, nil
}

func server() http.Handler {
	st := &store{prefs: map[string]domain.Preferences{}, tickets: map[string]string{"u1/order-1": "tickets/u1/order-1.png"}}
	mux := http.NewServeMux()
	(&Handler{API: &app.API{Store: st, Tickets: tk{}}, Verifier: verifier{}, BasePath: "/notifications"}).Routes(mux)
	return httpserver.New(httpserver.Options{Service: "t", Handler: mux}).Handler
}

func do(h http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPreferencesLifecycle(t *testing.T) {
	h := server()
	if rec := do(h, "GET", "/notifications/v1/me/preferences", "", ""); rec.Code != 401 {
		t.Errorf("no token %d", rec.Code)
	}
	if rec := do(h, "GET", "/notifications/v1/me/preferences", "u1", ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"emailEnabled":true`) {
		t.Errorf("default %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, "PUT", "/notifications/v1/me/preferences", "u1", `{}`); rec.Code != 422 {
		t.Errorf("missing field must be rejected, got %d", rec.Code)
	}
	if rec := do(h, "PUT", "/notifications/v1/me/preferences", "u1", `{"emailEnabled":false}`); rec.Code != 200 {
		t.Errorf("put %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, "GET", "/notifications/v1/me/preferences", "u1", ""); !strings.Contains(rec.Body.String(), `"emailEnabled":false`) {
		t.Errorf("not persisted: %s", rec.Body)
	}
	if rec := do(h, "GET", "/notifications/v1/me/preferences", "u2", ""); !strings.Contains(rec.Body.String(), `"emailEnabled":true`) {
		t.Errorf("preferences leaked across users: %s", rec.Body)
	}
}

func TestTicketURLOwnerOnlyAndLogHidesProviderIDs(t *testing.T) {
	h := server()
	if rec := do(h, "GET", "/notifications/v1/orders/order-1/ticket-url", "u1", ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), "tickets/u1/order-1.png") {
		t.Errorf("owner %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, "GET", "/notifications/v1/orders/order-1/ticket-url", "u2", ""); rec.Code != 404 {
		t.Errorf("other user %d", rec.Code)
	}
	if rec := do(h, "GET", "/notifications/v1/orders/..%2Fx/ticket-url", "u1", ""); rec.Code != 404 {
		t.Errorf("odd id %d", rec.Code)
	}
	rec := do(h, "GET", "/notifications/v1/me/notifications", "u1", "")
	if rec.Code != 200 || strings.Contains(rec.Body.String(), "secret-ses-id") || strings.Contains(rec.Body.String(), "userId") {
		t.Errorf("list %d %s", rec.Code, rec.Body)
	}
}
