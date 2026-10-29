package app

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/catalog/domain"
	"github.com/olagookundavid/gatepass-app/internal/catalog/ports"
)

// ---- in-memory fakes of OUR ports (not mocks of the AWS SDK) ----

type memEvents struct {
	m     map[string]domain.Event
	reads int
}

func (r *memEvents) Create(_ context.Context, e domain.Event) error { r.m[e.ID] = e; return nil }
func (r *memEvents) Get(_ context.Context, id string) (domain.Event, error) {
	r.reads++
	e, ok := r.m[id]
	if !ok {
		return domain.Event{}, domain.ErrNotFound
	}
	return e, nil
}
func (r *memEvents) Save(_ context.Context, e domain.Event, expected int, _ bool) (domain.Event, error) {
	cur, ok := r.m[e.ID]
	if !ok {
		return domain.Event{}, domain.ErrNotFound
	}
	if cur.Version != expected {
		return domain.Event{}, domain.ErrVersionConflict
	}
	e.Version = expected + 1
	r.m[e.ID] = e
	return e, nil
}
func (r *memEvents) ListPublished(_ context.Context, _ ports.EventFilter, _ *ports.EventCursor, limit int) ([]domain.Event, error) {
	r.reads++
	var out []domain.Event
	for _, e := range r.m {
		if e.Status == domain.StatusPublished {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

type memVenues struct{ m map[string]domain.Venue }

func (r *memVenues) Create(_ context.Context, v domain.Venue) error { r.m[v.ID] = v; return nil }
func (r *memVenues) Get(_ context.Context, id string) (domain.Venue, error) {
	v, ok := r.m[id]
	if !ok {
		return domain.Venue{}, domain.ErrNotFound
	}
	return v, nil
}
func (r *memVenues) List(context.Context, *ports.VenueCursor, int) ([]domain.Venue, error) {
	return nil, nil
}

type memCache struct {
	fail   bool
	events map[string]domain.Event
	lists  map[string][]byte
	gen    int64
}

var errCache = errors.New("redis down")

func (c *memCache) GetEvent(_ context.Context, id string) (*domain.Event, bool, error) {
	if c.fail {
		return nil, false, errCache
	}
	e, ok := c.events[id]
	return &e, ok, nil
}
func (c *memCache) SetEvent(_ context.Context, e domain.Event, _ time.Duration) error {
	if c.fail {
		return errCache
	}
	c.events[e.ID] = e
	return nil
}
func (c *memCache) DeleteEvent(_ context.Context, id string) error {
	if c.fail {
		return errCache
	}
	delete(c.events, id)
	return nil
}
func (c *memCache) GetList(_ context.Context, k string) ([]byte, bool, error) {
	if c.fail {
		return nil, false, errCache
	}
	b, ok := c.lists[k]
	return b, ok, nil
}
func (c *memCache) SetList(_ context.Context, k string, b []byte, _ time.Duration) error {
	if c.fail {
		return errCache
	}
	c.lists[k] = b
	return nil
}
func (c *memCache) ListGeneration(context.Context) (int64, error) {
	if c.fail {
		return 0, errCache
	}
	return c.gen, nil
}
func (c *memCache) InvalidateLists(context.Context) error {
	if c.fail {
		return errCache
	}
	c.gen++
	return nil
}

type recAnnouncer struct {
	n    int
	fail bool
}

func (a *recAnnouncer) AnnouncePublished(context.Context, domain.EventPublishedData) error {
	a.n++
	if a.fail {
		return errors.New("eventbridge down")
	}
	return nil
}

type fixture struct {
	svc    *Service
	events *memEvents
	cache  *memCache
	ann    *recAnnouncer
	venue  domain.Venue
}

var base = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func newFixture(t *testing.T) *fixture {
	t.Helper()
	n := 0
	f := &fixture{events: &memEvents{m: map[string]domain.Event{}}, ann: &recAnnouncer{},
		cache: &memCache{events: map[string]domain.Event{}, lists: map[string][]byte{}}}
	venues := &memVenues{m: map[string]domain.Venue{}}
	f.venue = domain.Venue{ID: "11111111-1111-7111-8111-111111111111", Name: "Arena", City: "Lagos", Address: "1 Road", Capacity: 500}
	venues.m[f.venue.ID] = f.venue
	f.svc = New(Service{Events: f.events, Venues: venues, Cache: f.cache, Announcer: f.ann, CDNBase: "https://cdn.example.com",
		Now: func() time.Time { return base },
		NewID: func() string {
			n++
			return fmt.Sprintf("00000000-0000-7000-8000-%012d", n)
		}})
	return f
}

func (f *fixture) createDraft(t *testing.T) domain.Event {
	t.Helper()
	e, err := f.svc.CreateEvent(context.Background(), CreateEventInput{
		VenueID: f.venue.ID, Title: "Show", StartsAt: base.Add(72 * time.Hour), EndsAt: base.Add(75 * time.Hour),
		TicketTypes: []TicketTypeInput{{Name: "GA", PriceCents: 1000, Currency: "USD", Capacity: 100}},
	}, "admin-1")
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestCacheAsideHitMissAndInvalidation(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	e := f.createDraft(t)
	if _, err := f.svc.PublishEvent(ctx, e.ID); err != nil {
		t.Fatal(err)
	}

	f.events.reads = 0
	if _, err := f.svc.GetEvent(ctx, e.ID, false); err != nil { // miss -> DB
		t.Fatal(err)
	}
	if _, err := f.svc.GetEvent(ctx, e.ID, false); err != nil { // hit
		t.Fatal(err)
	}
	if f.events.reads != 1 {
		t.Fatalf("db reads = %d, want 1 (second read must come from cache)", f.events.reads)
	}

	// list: first call fills, second call hits
	f.events.reads = 0
	p1, _ := f.svc.ListEvents(ctx, ports.EventFilter{City: "Lagos"}, nil, 10)
	p2, _ := f.svc.ListEvents(ctx, ports.EventFilter{City: "Lagos"}, nil, 10)
	if f.events.reads != 1 || len(p1.Items) != 1 || len(p2.Items) != 1 {
		t.Fatalf("list reads=%d items=%d/%d", f.events.reads, len(p1.Items), len(p2.Items))
	}

	// an update invalidates both the event key and every cached list
	title := "Renamed"
	if _, err := f.svc.UpdateEvent(ctx, e.ID, UpdateEventInput{Title: &title}, 2); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.cache.events[e.ID]; ok {
		t.Fatal("event key survived update")
	}
	f.events.reads = 0
	_, _ = f.svc.ListEvents(ctx, ports.EventFilter{City: "Lagos"}, nil, 10)
	if f.events.reads != 1 {
		t.Fatal("list should be re-read from DB after invalidation")
	}
}

func TestCacheOutageFallsBackToDatabase(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	e := f.createDraft(t)
	_, _ = f.svc.PublishEvent(ctx, e.ID)
	f.cache.fail = true

	got, err := f.svc.GetEvent(ctx, e.ID, false)
	if err != nil || got.ID != e.ID {
		t.Fatalf("event read must survive a cache outage: %v", err)
	}
	if page, err := f.svc.ListEvents(ctx, ports.EventFilter{}, nil, 10); err != nil || len(page.Items) != 1 {
		t.Fatalf("list must survive a cache outage: %v %d", err, len(page.Items))
	}
	title := "x"
	if _, err := f.svc.UpdateEvent(ctx, e.ID, UpdateEventInput{Title: &title}, 2); err != nil {
		t.Fatalf("write must survive failed invalidation: %v", err)
	}
}

func TestDraftsAreHiddenFromPublic(t *testing.T) {
	f := newFixture(t)
	e := f.createDraft(t)
	if _, err := f.svc.GetEvent(context.Background(), e.ID, false); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("public saw a draft: %v", err)
	}
	if _, err := f.svc.GetEvent(context.Background(), e.ID, true); err != nil {
		t.Fatalf("admin cannot see draft: %v", err)
	}
}

func TestPublishIdempotentReannouncesAndReportsAnnounceFailure(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	e := f.createDraft(t)

	f.ann.fail = true
	_, err := f.svc.PublishEvent(ctx, e.ID)
	if !errors.Is(err, ErrAnnounceFailed) {
		t.Fatalf("want ErrAnnounceFailed, got %v", err)
	}
	if f.events.m[e.ID].Status != domain.StatusPublished {
		t.Fatal("DB state must be committed even if the announcement failed")
	}

	f.ann.fail = false
	got, err := f.svc.PublishEvent(ctx, e.ID) // retry = re-announce
	if err != nil || got.Status != domain.StatusPublished || f.ann.n != 2 {
		t.Fatalf("retry: err=%v announcements=%d", err, f.ann.n)
	}
	if got.Version != 2 {
		t.Fatalf("re-publish must not bump the version again, got %d", got.Version)
	}
}

func TestOptimisticLockAndDraftOnlyTicketTypes(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	e := f.createDraft(t)
	title := "Edited"
	if _, err := f.svc.UpdateEvent(ctx, e.ID, UpdateEventInput{Title: &title}, 99); !errors.Is(err, domain.ErrVersionConflict) {
		t.Fatalf("stale version accepted: %v", err)
	}
	_, _ = f.svc.PublishEvent(ctx, e.ID)
	tts := []TicketTypeInput{{Name: "VIP", PriceCents: 1, Currency: "USD", Capacity: 1}}
	if _, err := f.svc.UpdateEvent(ctx, e.ID, UpdateEventInput{TicketTypes: &tts}, 2); !errors.Is(err, domain.ErrNotDraft) {
		t.Fatalf("ticket types editable after publish: %v", err)
	}
}

func TestCreateValidatesVenueAndCapacity(t *testing.T) {
	f := newFixture(t)
	_, err := f.svc.CreateEvent(context.Background(), CreateEventInput{
		VenueID: f.venue.ID, Title: "Big", StartsAt: base.Add(time.Hour), EndsAt: base.Add(2 * time.Hour),
		TicketTypes: []TicketTypeInput{{Name: "GA", PriceCents: 1, Currency: "USD", Capacity: 501}},
	}, "a")
	var ve *domain.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("over-capacity accepted: %v", err)
	}
	_, err = f.svc.CreateEvent(context.Background(), CreateEventInput{
		VenueID: "99999999-9999-7999-8999-999999999999", Title: "X", StartsAt: base.Add(time.Hour), EndsAt: base.Add(2 * time.Hour),
		TicketTypes: []TicketTypeInput{{Name: "GA", PriceCents: 1, Currency: "USD", Capacity: 1}},
	}, "a")
	if !errors.As(err, &ve) {
		t.Fatalf("unknown venue accepted: %v", err)
	}
}

type fakeSigner struct{ key, ct string }

func (s *fakeSigner) PresignPut(_ context.Context, key, ct string, _ int64, _ time.Duration) (string, error) {
	s.key, s.ct = key, ct
	return "https://s3.example.com/" + key, nil
}

func TestImageUploadURL(t *testing.T) {
	f := newFixture(t)
	e := f.createDraft(t)
	sg := &fakeSigner{}
	f.svc.Images = sg
	ctx := context.Background()

	u, err := f.svc.ImageUploadURL(ctx, e.ID, "image/png", 1024)
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := "images/events/" + e.ID + "/"
	if len(u.ObjectKey) < len(wantPrefix) || u.ObjectKey[:len(wantPrefix)] != wantPrefix || u.PublicURL != "https://cdn.example.com/"+u.ObjectKey {
		t.Fatalf("%+v", u)
	}
	if _, err := f.svc.ImageUploadURL(ctx, e.ID, "application/pdf", 1024); err == nil {
		t.Fatal("pdf accepted")
	}
	if _, err := f.svc.ImageUploadURL(ctx, e.ID, "image/png", 6<<20); err == nil {
		t.Fatal("oversized image accepted")
	}
	// imageUrl in PATCH must be one of OUR urls for THIS event
	evil := "https://evil.example.com/x.png"
	if _, err := f.svc.UpdateEvent(ctx, e.ID, UpdateEventInput{ImageURL: &evil}, 1); err == nil {
		t.Fatal("foreign image url accepted")
	}
	if _, err := f.svc.UpdateEvent(ctx, e.ID, UpdateEventInput{ImageURL: &u.PublicURL}, 1); err != nil {
		t.Fatal(err)
	}
}
