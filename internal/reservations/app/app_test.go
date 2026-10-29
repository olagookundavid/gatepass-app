package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
	"github.com/olagookundavid/gatepass-app/internal/reservations/domain"
	"github.com/olagookundavid/gatepass-app/internal/reservations/ports"
)

// memStore is an in-memory fake of ports.Store whose operations take one mutex,
// emulating the all-or-nothing behaviour of DynamoDB conditional transactions.
type memStore struct {
	mu        sync.Mutex
	inv       map[string]domain.Inventory
	holds     map[string]domain.Hold
	idem      map[string]ports.IdempotencyRecord
	processed map[string]bool
	markers   map[string]bool
}

func newStore() *memStore {
	return &memStore{inv: map[string]domain.Inventory{}, holds: map[string]domain.Hold{}, idem: map[string]ports.IdempotencyRecord{},
		processed: map[string]bool{}, markers: map[string]bool{}}
}

func ik(e, t string) string { return e + "/" + t }

func (m *memStore) GetInventory(_ context.Context, e, t string) (domain.Inventory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i, ok := m.inv[ik(e, t)]
	if !ok {
		return domain.Inventory{}, domain.ErrNotFound
	}
	return i, nil
}
func (m *memStore) ListInventory(_ context.Context, e string) ([]domain.Inventory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []domain.Inventory
	for _, i := range m.inv {
		if i.EventID == e {
			out = append(out, i)
		}
	}
	return out, nil
}
func (m *memStore) CreateInventoryIfAbsent(_ context.Context, i domain.Inventory) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.inv[ik(i.EventID, i.TicketTypeID)]; ok {
		return false, nil
	}
	m.inv[ik(i.EventID, i.TicketTypeID)] = i
	return true, nil
}
func (m *memStore) CreateHold(_ context.Context, h domain.Hold, key string, rec ports.IdempotencyRecord, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.idem[h.UserID+"/"+key]; ok {
		return domain.ErrIdempotencyInUse
	}
	i := m.inv[ik(h.EventID, h.TicketTypeID)]
	n, err := domain.AfterReserve(i.Available, h.Quantity)
	if err != nil {
		return err
	}
	i.Available = n
	m.inv[ik(h.EventID, h.TicketTypeID)] = i
	m.holds[h.ID] = h
	m.idem[h.UserID+"/"+key] = rec
	return nil
}
func (m *memStore) GetHold(_ context.Context, id string) (domain.Hold, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.holds[id]
	if !ok {
		return domain.Hold{}, domain.ErrNotFound
	}
	return h, nil
}
func (m *memStore) GetIdempotency(_ context.Context, u, k string) (ports.IdempotencyRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.idem[u+"/"+k]
	return r, ok, nil
}
func (m *memStore) ListUserHolds(context.Context, string, *ports.HoldCursor, int) ([]domain.Hold, *ports.HoldCursor, error) {
	return nil, nil, nil
}
func (m *memStore) ConsumeHold(_ context.Context, id, order, user string, now time.Time) (domain.Hold, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.holds[id]
	if !ok {
		return domain.Hold{}, domain.ErrNotFound
	}
	out, err := domain.ConsumeDecision(h, order, user, now)
	if err != nil {
		return domain.Hold{}, err
	}
	if out == domain.ConsumeNow {
		h.Status, h.OrderID = domain.StatusConsumed, order
		m.holds[id] = h
	}
	return h, nil
}
func (m *memStore) ReleaseHold(_ context.Context, id string, from []domain.HoldStatus, _ time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.holds[id]
	if !ok {
		return false, domain.ErrNotFound
	}
	match := false
	for _, f := range from {
		match = match || h.Status == f
	}
	if !match {
		return false, nil
	}
	k := ik(h.EventID, h.TicketTypeID)
	i := m.inv[k]
	n, ok := domain.AfterRelease(i.Available, h.Quantity, i.Capacity)
	if !ok {
		return false, errors.New("would exceed capacity")
	}
	i.Available = n
	m.inv[k] = i
	h.Status = domain.StatusReleased
	m.holds[id] = h
	return true, nil
}
func (m *memStore) ReleaseDeletedHold(_ context.Context, h domain.Hold, _ time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.markers[h.ID] {
		return false, nil
	}
	m.markers[h.ID] = true
	k := ik(h.EventID, h.TicketTypeID)
	i := m.inv[k]
	i.Available += h.Quantity
	m.inv[k] = i
	return true, nil
}
func (m *memStore) EventProcessed(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.processed[id], nil
}
func (m *memStore) MarkEventProcessed(_ context.Context, id string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed[id] = true
	return nil
}

type fakeSched struct {
	mu        sync.Mutex
	scheduled map[string]time.Time
	failN     int32
	cancelled []string
}

func (f *fakeSched) ScheduleExpiry(_ context.Context, id string, at time.Time) error {
	if atomic.LoadInt32(&f.failN) > 0 {
		atomic.AddInt32(&f.failN, -1)
		return errors.New("scheduler down")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scheduled[id] = at
	return nil
}
func (f *fakeSched) CancelExpiry(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, id)
	return nil
}

type fakeLimiter struct {
	allow bool
	err   error
}

func (l fakeLimiter) Allow(context.Context, string, int, time.Duration) (bool, error) {
	return l.allow, l.err
}

var base = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

type fx struct {
	svc   *Service
	store *memStore
	sched *fakeSched
	now   *time.Time
	lim   *fakeLimiter
}

func newFx(capacity int) *fx {
	f := &fx{store: newStore(), sched: &fakeSched{scheduled: map[string]time.Time{}}, lim: &fakeLimiter{allow: true}}
	now := base
	f.now = &now
	n := 0
	f.svc = New(Service{Store: f.store, Scheduler: f.sched, Limiter: limiterFunc(func() (bool, error) { return f.lim.allow, f.lim.err }),
		Now:   func() time.Time { return *f.now },
		NewID: func() string { n++; return fmt.Sprintf("00000000-0000-7000-8000-%012d", n) }})
	f.store.inv[ik("ev", "tt")] = domain.Inventory{EventID: "ev", TicketTypeID: "tt", Capacity: capacity, Available: capacity, PriceCents: 1500, Currency: "USD"}
	return f
}

type limiterFunc func() (bool, error)

func (l limiterFunc) Allow(context.Context, string, int, time.Duration) (bool, error) { return l() }

func (f *fx) avail() int { return f.store.inv[ik("ev", "tt")].Available }

var req = CreateHoldInput{EventID: "ev", TicketTypeID: "tt", Quantity: 2}

func TestCreateHoldSchedulesExpiryAndSnapshotsPrice(t *testing.T) {
	f := newFx(10)
	res, err := f.svc.CreateHold(context.Background(), "u1", "key-00000001", req)
	if err != nil {
		t.Fatal(err)
	}
	if f.avail() != 8 || res.Hold.UnitPriceCents != 1500 {
		t.Fatalf("avail=%d hold=%+v", f.avail(), res.Hold)
	}
	if at := f.sched.scheduled[res.Hold.ID]; !at.Equal(base.Add(10 * time.Minute)) {
		t.Fatalf("schedule at %v", at)
	}
}

func TestFiftyConcurrentHoldsOfOneTicketAgainstTenAvailable(t *testing.T) {
	f := newFx(10)
	var ok, soldOut atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.svc.CreateHold(context.Background(), fmt.Sprintf("user-%d", i), fmt.Sprintf("key-%08d", i),
				CreateHoldInput{EventID: "ev", TicketTypeID: "tt", Quantity: 1})
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, domain.ErrSoldOut):
				soldOut.Add(1)
			default:
				t.Errorf("unexpected: %v", err)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 10 || soldOut.Load() != 40 || f.avail() != 0 {
		t.Fatalf("ok=%d soldOut=%d avail=%d", ok.Load(), soldOut.Load(), f.avail())
	}
}

func TestIdempotentCreate(t *testing.T) {
	f := newFx(10)
	ctx := context.Background()
	first, err := f.svc.CreateHold(ctx, "u1", "key-00000001", req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.svc.CreateHold(ctx, "u1", "key-00000001", req)
	if err != nil || !again.Replayed || again.Hold.ID != first.Hold.ID || f.avail() != 8 {
		t.Fatalf("replay: %+v err=%v avail=%d", again, err, f.avail())
	}
	different := req
	different.Quantity = 3
	if _, err := f.svc.CreateHold(ctx, "u1", "key-00000001", different); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("same key, different body: %v", err)
	}
	// the same key from another user is a different key
	if r, err := f.svc.CreateHold(ctx, "u2", "key-00000001", req); err != nil || r.Replayed {
		t.Fatalf("keys must be scoped per user: %v %+v", err, r)
	}
}

func TestRateLimitRejectsAndFailsOpen(t *testing.T) {
	f := newFx(10)
	f.lim.allow = false
	if _, err := f.svc.CreateHold(context.Background(), "u1", "key-00000001", req); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("want rate limited, got %v", err)
	}
	f.lim.err = errors.New("redis down")
	if _, err := f.svc.CreateHold(context.Background(), "u1", "key-00000002", req); err != nil {
		t.Fatalf("limiter outage must fail open: %v", err)
	}
}

func TestSchedulingFailureRollsTheHoldBack(t *testing.T) {
	f := newFx(10)
	f.sched.failN = 99
	_, err := f.svc.CreateHold(context.Background(), "u1", "key-00000001", req)
	if !errors.Is(err, ErrSchedulingFailed) {
		t.Fatalf("got %v", err)
	}
	if f.avail() != 10 {
		t.Fatalf("tickets leaked: available=%d", f.avail())
	}
}

func TestConsumeIdempotentPerOrderAndSchedulesDeleted(t *testing.T) {
	f := newFx(10)
	ctx := context.Background()
	r, _ := f.svc.CreateHold(ctx, "u1", "key-00000001", req)
	if _, err := f.svc.Consume(ctx, r.Hold.ID, "order-1", "u1"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Consume(ctx, r.Hold.ID, "order-1", "u1"); err != nil {
		t.Fatalf("same order again must succeed: %v", err)
	}
	if _, err := f.svc.Consume(ctx, r.Hold.ID, "order-2", "u1"); !errors.Is(err, domain.ErrAlreadyConsumed) {
		t.Fatalf("different order: %v", err)
	}
	if len(f.sched.cancelled) == 0 {
		t.Fatal("expiry schedule was not deleted after consume")
	}
}

func TestExpiryPathsAreIdempotentAndRespectConsume(t *testing.T) {
	f := newFx(10)
	ctx := context.Background()
	r, _ := f.svc.CreateHold(ctx, "u1", "key-00000001", req)

	if _, err := f.svc.ExpireHold(ctx, r.Hold.ID); !errors.Is(err, domain.ErrNotYetExpired) {
		t.Fatalf("early fire must error so Scheduler retries: %v", err)
	}
	*f.now = base.Add(11 * time.Minute)
	rel, err := f.svc.ExpireHold(ctx, r.Hold.ID)
	if err != nil || !rel || f.avail() != 10 {
		t.Fatalf("expire: %v %v avail=%d", rel, err, f.avail())
	}
	rel, err = f.svc.ExpireHold(ctx, r.Hold.ID) // duplicate delivery
	if err != nil || rel || f.avail() != 10 {
		t.Fatalf("second expire must be a no-op: %v %v avail=%d", rel, err, f.avail())
	}

	// a consumed hold must not be released by the expirer
	*f.now = base
	r2, _ := f.svc.CreateHold(ctx, "u1", "key-00000002", req)
	_, _ = f.svc.Consume(ctx, r2.Hold.ID, "o1", "u1")
	*f.now = base.Add(time.Hour)
	if rel, _ := f.svc.ExpireHold(ctx, r2.Hold.ID); rel {
		t.Fatal("expirer released a consumed hold")
	}
	// unknown hold: success
	if rel, err := f.svc.ExpireHold(ctx, "gone"); rel || err != nil {
		t.Fatalf("missing hold: %v %v", rel, err)
	}
}

func TestStreamSafetyNetReleasesOnce(t *testing.T) {
	f := newFx(10)
	ctx := context.Background()
	r, _ := f.svc.CreateHold(ctx, "u1", "key-00000001", req)
	delete(f.store.holds, r.Hold.ID) // TTL removed the row
	for i := 0; i < 3; i++ {
		_, _ = f.svc.ExpireDeletedHold(ctx, r.Hold)
	}
	if f.avail() != 10 {
		t.Fatalf("available=%d, want 10 (released exactly once)", f.avail())
	}
	released := r.Hold
	released.Status = domain.StatusReleased
	if rel, _ := f.svc.ExpireDeletedHold(ctx, released); rel {
		t.Fatal("non-active image processed")
	}
}

func ev(id, typ string, data any) ebpub.Received {
	b, _ := json.Marshal(data)
	return ebpub.Received{Detail: ebpub.Detail{EventID: id, EventType: typ, Data: b}}
}

func TestConsumerEventPublishedIsIdempotentAndKeepsCounters(t *testing.T) {
	f := newFx(10)
	ctx := context.Background()
	data := map[string]any{"eventId": "ev2", "ticketTypes": []map[string]any{{"id": "ttA", "name": "GA", "priceCents": 900, "currency": "USD", "capacity": 20}}}
	if err := f.svc.HandleEvent(ctx, ev("e1", "EventPublished", data)); err != nil {
		t.Fatal(err)
	}
	// simulate sales, then the same event again AND a re-announce with a new eventId
	i := f.store.inv[ik("ev2", "ttA")]
	i.Available = 5
	f.store.inv[ik("ev2", "ttA")] = i
	_ = f.svc.HandleEvent(ctx, ev("e1", "EventPublished", data))
	_ = f.svc.HandleEvent(ctx, ev("e2", "EventPublished", data))
	if got := f.store.inv[ik("ev2", "ttA")]; got.Available != 5 || got.PriceCents != 900 {
		t.Fatalf("re-delivery reset the counter: %+v", got)
	}
}

func TestConsumerReleasesConsumedHoldOnceOnPaymentFailedAndCancel(t *testing.T) {
	f := newFx(10)
	ctx := context.Background()
	r, _ := f.svc.CreateHold(ctx, "u1", "key-00000001", req)
	_, _ = f.svc.Consume(ctx, r.Hold.ID, "o1", "u1")
	data := domain.OrderEventData{OrderID: "o1", HoldID: r.Hold.ID}

	for i, typ := range []string{"PaymentFailed", "PaymentFailed", "OrderCancelled"} {
		if err := f.svc.HandleEvent(ctx, ev(fmt.Sprintf("x%d", i), typ, data)); err != nil {
			t.Fatal(err)
		}
	}
	if f.avail() != 10 {
		t.Fatalf("available=%d; the hold must be released exactly once", f.avail())
	}
	// unknown hold: acknowledged, not an error (it would otherwise loop into the DLQ)
	if err := f.svc.HandleEvent(ctx, ev("y", "OrderCancelled", domain.OrderEventData{HoldID: "nope"})); err != nil {
		t.Fatal(err)
	}
	// unrelated event types are ignored
	if err := f.svc.HandleEvent(ctx, ev("z", "OrderPaid", data)); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteHold(t *testing.T) {
	f := newFx(10)
	ctx := context.Background()
	r, _ := f.svc.CreateHold(ctx, "u1", "key-00000001", req)
	if err := f.svc.DeleteHold(ctx, "u2", r.Hold.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("other user's hold must look absent: %v", err)
	}
	if err := f.svc.DeleteHold(ctx, "u1", r.Hold.ID); err != nil || f.avail() != 10 {
		t.Fatalf("%v avail=%d", err, f.avail())
	}
	if err := f.svc.DeleteHold(ctx, "u1", r.Hold.ID); err != nil {
		t.Fatalf("second delete must be idempotent: %v", err)
	}
	r2, _ := f.svc.CreateHold(ctx, "u1", "key-00000002", req)
	_, _ = f.svc.Consume(ctx, r2.Hold.ID, "o", "u1")
	if err := f.svc.DeleteHold(ctx, "u1", r2.Hold.ID); !errors.Is(err, domain.ErrHoldConsumed) {
		t.Fatalf("deleting a consumed hold: %v", err)
	}
}
