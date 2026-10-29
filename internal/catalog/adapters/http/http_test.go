package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/catalog/app"
	"github.com/olagookundavid/gatepass-app/internal/catalog/domain"
	"github.com/olagookundavid/gatepass-app/internal/catalog/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
)

// fakeVerifier maps the literal token "admin"/"user" to claims.
type fakeVerifier struct{}

func (fakeVerifier) Verify(_ context.Context, tok string) (*auth.Claims, error) {
	switch tok {
	case "admin":
		return &auth.Claims{Subject: "a1", Groups: []string{"admin"}}, nil
	case "user":
		return &auth.Claims{Subject: "u1"}, nil
	}
	return nil, auth.ErrInvalidToken
}

type memEvents struct{ m map[string]domain.Event }

func (r *memEvents) Create(_ context.Context, e domain.Event) error { r.m[e.ID] = e; return nil }
func (r *memEvents) Get(_ context.Context, id string) (domain.Event, error) {
	if e, ok := r.m[id]; ok {
		return e, nil
	}
	return domain.Event{}, domain.ErrNotFound
}
func (r *memEvents) Save(_ context.Context, e domain.Event, v int, _ bool) (domain.Event, error) {
	if r.m[e.ID].Version != v {
		return domain.Event{}, domain.ErrVersionConflict
	}
	e.Version = v + 1
	r.m[e.ID] = e
	return e, nil
}
func (r *memEvents) ListPublished(context.Context, ports.EventFilter, *ports.EventCursor, int) ([]domain.Event, error) {
	return nil, nil
}

type memVenues struct{}

func (memVenues) Create(context.Context, domain.Venue) error { return nil }
func (memVenues) Get(_ context.Context, id string) (domain.Venue, error) {
	return domain.Venue{ID: id, Capacity: 1000}, nil
}
func (memVenues) List(context.Context, *ports.VenueCursor, int) ([]domain.Venue, error) {
	return nil, nil
}

type nopCache struct{}

func (nopCache) GetEvent(context.Context, string) (*domain.Event, bool, error) {
	return nil, false, nil
}
func (nopCache) SetEvent(context.Context, domain.Event, time.Duration) error  { return nil }
func (nopCache) DeleteEvent(context.Context, string) error                    { return nil }
func (nopCache) GetList(context.Context, string) ([]byte, bool, error)        { return nil, false, nil }
func (nopCache) SetList(context.Context, string, []byte, time.Duration) error { return nil }
func (nopCache) ListGeneration(context.Context) (int64, error)                { return 0, nil }
func (nopCache) InvalidateLists(context.Context) error                        { return nil }

type okAnnouncer struct{}

func (okAnnouncer) AnnouncePublished(context.Context, domain.EventPublishedData) error { return nil }

func server() http.Handler {
	svc := app.New(app.Service{Events: &memEvents{m: map[string]domain.Event{}}, Venues: memVenues{}, Cache: nopCache{}, Announcer: okAnnouncer{}, CDNBase: "https://cdn.example.com"})
	mux := http.NewServeMux()
	(&Handler{Svc: svc, Verifier: fakeVerifier{}, BasePath: "/catalog"}).Routes(mux)
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

func TestAuthorisationAndErrorShape(t *testing.T) {
	h := server()
	body := `{"venueId":"11111111-1111-7111-8111-111111111111","title":"X","startsAt":"2099-01-01T10:00:00Z","endsAt":"2099-01-01T12:00:00Z","ticketTypes":[{"name":"GA","priceCents":100,"currency":"USD","capacity":10}]}`

	if rec := do(h, "POST", "/catalog/v1/events", "", body, nil); rec.Code != 401 {
		t.Errorf("no token: %d", rec.Code)
	}
	if rec := do(h, "POST", "/catalog/v1/events", "user", body, nil); rec.Code != 403 {
		t.Errorf("non-admin: %d", rec.Code)
	}
	rec := do(h, "POST", "/catalog/v1/events", "admin", body, nil)
	if rec.Code != 201 || rec.Header().Get("ETag") != `"1"` {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	var e domain.Event
	_ = json.Unmarshal(rec.Body.Bytes(), &e)

	// invalid body -> 422 problem+json with field errors
	rec = do(h, "POST", "/catalog/v1/events", "admin", `{"title":""}`, nil)
	if rec.Code != 422 || rec.Header().Get("Content-Type") != "application/problem+json" || !strings.Contains(rec.Body.String(), `"field"`) {
		t.Errorf("validation: %d %s", rec.Code, rec.Body)
	}
	// unknown JSON field -> 400
	if rec := do(h, "POST", "/catalog/v1/venues", "admin", `{"nme":"x"}`, nil); rec.Code != 400 {
		t.Errorf("unknown field: %d", rec.Code)
	}
	// drafts are 404 for the public, 200 for admins
	if rec := do(h, "GET", "/catalog/v1/events/"+e.ID, "", "", nil); rec.Code != 404 {
		t.Errorf("public draft: %d", rec.Code)
	}
	if rec := do(h, "GET", "/catalog/v1/events/"+e.ID, "admin", "", nil); rec.Code != 200 {
		t.Errorf("admin draft: %d", rec.Code)
	}
	// an invalid token on a public endpoint is rejected, not ignored
	if rec := do(h, "GET", "/catalog/v1/events/"+e.ID, "garbage", "", nil); rec.Code != 401 {
		t.Errorf("bad token on optional-auth endpoint: %d", rec.Code)
	}
	if rec := do(h, "GET", "/catalog/v1/events/not-a-uuid", "", "", nil); rec.Code != 404 {
		t.Errorf("bad id: %d", rec.Code)
	}
}

func TestIfMatchFlow(t *testing.T) {
	h := server()
	body := `{"venueId":"11111111-1111-7111-8111-111111111111","title":"X","startsAt":"2099-01-01T10:00:00Z","endsAt":"2099-01-01T12:00:00Z","ticketTypes":[{"name":"GA","priceCents":100,"currency":"USD","capacity":10}]}`
	var e domain.Event
	_ = json.Unmarshal(do(h, "POST", "/catalog/v1/events", "admin", body, nil).Body.Bytes(), &e)
	path := "/catalog/v1/events/" + e.ID

	if rec := do(h, "PATCH", path, "admin", `{"title":"Y"}`, nil); rec.Code != 428 {
		t.Errorf("missing If-Match: %d", rec.Code)
	}
	if rec := do(h, "PATCH", path, "admin", `{"title":"Y"}`, map[string]string{"If-Match": `"7"`}); rec.Code != 412 {
		t.Errorf("stale If-Match: %d", rec.Code)
	}
	rec := do(h, "PATCH", path, "admin", `{"title":"Y"}`, map[string]string{"If-Match": `"1"`})
	if rec.Code != 200 || rec.Header().Get("ETag") != `"2"` {
		t.Errorf("good If-Match: %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, "POST", path+"/publish", "admin", "", nil); rec.Code != 200 {
		t.Errorf("publish: %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, "POST", path+"/publish", "admin", "", nil); rec.Code != 200 {
		t.Errorf("publish again must be idempotent: %d", rec.Code)
	}
}

func TestHealthRoutesAreNotUnderBasePath(t *testing.T) {
	h := server()
	// the test server was built without Health; this documents that /catalog/livez is not a route
	if rec := do(h, "GET", "/catalog/livez", "", "", nil); rec.Code != 404 {
		t.Errorf("unexpected route: %d", rec.Code)
	}
}
