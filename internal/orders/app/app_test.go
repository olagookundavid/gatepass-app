package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/orders/domain"
	"github.com/olagookundavid/gatepass-app/internal/orders/ports"
)

// ---- in-memory fakes of OUR ports ----

type outboxEvt struct {
	ID, Type, Order string
	Published       bool
}

type memOrders struct {
	mu       sync.Mutex
	orders   map[string]domain.Order
	history  map[string][]domain.HistoryEntry
	idem     map[string]ports.IdempotencyRecord
	outbox   []*outboxEvt
	enqueued map[string]bool
	failNext bool // make the next CreateOrder fail (simulated crash before commit)
}

func newOrders() *memOrders {
	return &memOrders{orders: map[string]domain.Order{}, history: map[string][]domain.HistoryEntry{}, idem: map[string]ports.IdempotencyRecord{}, enqueued: map[string]bool{}}
}

func (m *memOrders) CreateOrder(_ context.Context, n ports.NewOrder) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return errors.New("db connection lost") // nothing is written: atomic
	}
	if _, ok := m.idem[n.Order.UserID+"/"+n.IdemKey]; ok {
		return domain.ErrIdempotencyInUse
	}
	for _, o := range m.orders {
		if o.HoldID == n.Order.HoldID {
			return domain.ErrOrderExists
		}
	}
	m.orders[n.Order.ID] = n.Order
	m.history[n.Order.ID] = []domain.HistoryEntry{{Status: n.Order.Status, At: n.Order.CreatedAt}}
	m.idem[n.Order.UserID+"/"+n.IdemKey] = n.Idempotency
	m.outbox = append(m.outbox, &outboxEvt{ID: "evt-" + n.Order.ID, Type: domain.EventOrderPlaced, Order: n.Order.ID})
	return nil
}
func (m *memOrders) FindIdempotency(_ context.Context, u, k string) (ports.IdempotencyRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.idem[u+"/"+k]
	return r, ok, nil
}
func (m *memOrders) Get(_ context.Context, id string) (domain.Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.orders[id]
	if !ok {
		return domain.Order{}, domain.ErrNotFound
	}
	return o, nil
}
func (m *memOrders) ListByUser(context.Context, string, *ports.OrderCursor, int) ([]domain.Order, error) {
	return nil, nil
}
func (m *memOrders) History(_ context.Context, id string) ([]domain.HistoryEntry, error) {
	return m.history[id], nil
}
func (m *memOrders) Transition(_ context.Context, id string, _ string, decide func(domain.Order) (domain.Change, error)) (domain.Order, bool, error) {
	m.mu.Lock() // the mutex plays the role of SELECT ... FOR UPDATE
	defer m.mu.Unlock()
	o, ok := m.orders[id]
	if !ok {
		return domain.Order{}, false, domain.ErrNotFound
	}
	ch, err := decide(o)
	if err != nil || !ch.Changed {
		return o, false, err
	}
	now := time.Now().UTC()
	o.Status, o.UpdatedAt, o.Version = ch.NewStatus, now, o.Version+1
	m.orders[id] = o
	m.history[id] = append(m.history[id], domain.HistoryEntry{Status: ch.NewStatus, Reason: ch.Reason, At: now})
	m.outbox = append(m.outbox, &outboxEvt{ID: "evt-" + id + "-" + ch.EventType, Type: ch.EventType, Order: id})
	return o, true, nil
}
func (m *memOrders) MarkPaymentEnqueued(_ context.Context, id string, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enqueued[id] = true
	return nil
}

type fakeHolds struct {
	mu       sync.Mutex
	consumed map[string]string // holdID -> orderID that consumed it
	calls    []string          // orderIDs presented
	err      error
}

func (h *fakeHolds) Consume(_ context.Context, holdID, orderID, userID string) (ports.ConsumedHold, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, orderID)
	if h.err != nil {
		return ports.ConsumedHold{}, h.err
	}
	if prev, ok := h.consumed[holdID]; ok && prev != orderID {
		return ports.ConsumedHold{}, domain.ErrHoldTaken
	}
	h.consumed[holdID] = orderID
	return ports.ConsumedHold{HoldID: holdID, EventID: "ev", TicketTypeID: "tt", Quantity: 2, UnitPriceCents: 1000, Currency: "USD"}, nil
}

type fakePayments struct {
	mu   sync.Mutex
	sent []domain.PaymentRequest
	fail bool
}

func (p *fakePayments) Enqueue(_ context.Context, r domain.PaymentRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("sqs down")
	}
	p.sent = append(p.sent, r)
	return nil
}

type fx struct {
	svc      *Service
	orders   *memOrders
	holds    *fakeHolds
	payments *fakePayments
}

func newFx() *fx {
	f := &fx{orders: newOrders(), holds: &fakeHolds{consumed: map[string]string{}}, payments: &fakePayments{}}
	f.svc = New(Service{Orders: f.orders, Holds: f.holds, Payments: f.payments})
	return f
}

const hold1 = "11111111-1111-7111-8111-111111111111"

var ctx = context.Background()

func TestCreateOrderIsIdempotentAndConsumesOnce(t *testing.T) {
	f := newFx()
	r1, err := f.svc.CreateOrder(ctx, "u1", "key-00000001", hold1)
	if err != nil || r1.Code != 201 || r1.Replayed {
		t.Fatalf("first: %+v %v", r1, err)
	}
	r2, err := f.svc.CreateOrder(ctx, "u1", "key-00000001", hold1)
	if err != nil || !r2.Replayed || string(r2.Body) != string(r1.Body) {
		t.Fatalf("replay must return the stored response: %+v %v", r2, err)
	}
	if len(f.holds.calls) != 1 {
		t.Fatalf("hold consumed %d times, want 1 (replay must not call reservations)", len(f.holds.calls))
	}
	if len(f.orders.orders) != 1 || len(f.orders.outbox) != 1 || f.orders.outbox[0].Type != domain.EventOrderPlaced {
		t.Fatalf("orders=%d outbox=%d", len(f.orders.orders), len(f.orders.outbox))
	}
	if len(f.payments.sent) != 1 || f.payments.sent[0].AmountCents != 2000 || !f.orders.enqueued[f.payments.sent[0].OrderID] {
		t.Fatalf("payments: %+v", f.payments.sent)
	}
	if _, err := f.svc.CreateOrder(ctx, "u1", "key-00000001", "22222222-2222-7222-8222-222222222222"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("same key, different body: %v", err)
	}
}

func TestCrashAfterConsumeThenRetryWithSameKeyStillCreatesTheOrder(t *testing.T) {
	f := newFx()
	f.orders.failNext = true // the database commit fails AFTER reservations consumed the hold
	if _, err := f.svc.CreateOrder(ctx, "u1", "key-00000001", hold1); err == nil {
		t.Fatal("expected failure")
	}
	if len(f.orders.orders) != 0 || len(f.orders.outbox) != 0 {
		t.Fatal("a failed transaction left rows behind")
	}
	r, err := f.svc.CreateOrder(ctx, "u1", "key-00000001", hold1)
	if err != nil || r.Code != 201 {
		t.Fatalf("retry must succeed (deterministic order id): %v", err)
	}
	if len(f.holds.calls) != 2 || f.holds.calls[0] != f.holds.calls[1] {
		t.Fatalf("both consume calls must present the SAME order id: %v", f.holds.calls)
	}
}

func TestHoldErrorsAreReturnedAndNothingIsWritten(t *testing.T) {
	f := newFx()
	f.holds.err = domain.ErrHoldExpired
	if _, err := f.svc.CreateOrder(ctx, "u1", "key-00000001", hold1); !errors.Is(err, domain.ErrHoldExpired) {
		t.Fatalf("%v", err)
	}
	if len(f.orders.orders) != 0 || len(f.orders.idem) != 0 {
		t.Fatal("rows written for a failed order")
	}
}

func TestPaymentEnqueueFailureDoesNotFailTheOrder(t *testing.T) {
	f := newFx()
	f.payments.fail = true
	r, err := f.svc.CreateOrder(ctx, "u1", "key-00000001", hold1)
	if err != nil || r.Code != 201 {
		t.Fatalf("order must still be created: %v", err)
	}
	for id := range f.orders.orders {
		if f.orders.enqueued[id] {
			t.Fatal("must not be marked enqueued after a failed send")
		}
	}
}

func TestCancelFlowsAndOwnership(t *testing.T) {
	f := newFx()
	_, _ = f.svc.CreateOrder(ctx, "u1", "key-00000001", hold1)
	var id string
	for k := range f.orders.orders {
		id = k
	}
	if _, err := f.svc.CancelOrder(ctx, "intruder", id, "x"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("other user's order: %v", err)
	}
	o, err := f.svc.CancelOrder(ctx, "u1", id, "changed my mind")
	if err != nil || o.Status != domain.StatusCancelled {
		t.Fatalf("%+v %v", o, err)
	}
	if _, err := f.svc.CancelOrder(ctx, "u1", id, "again"); err != nil {
		t.Fatalf("second cancel must be idempotent: %v", err)
	}
	cancelEvents := 0
	for _, e := range f.orders.outbox {
		if e.Type == domain.EventOrderCancelled {
			cancelEvents++
		}
	}
	if cancelEvents != 1 {
		t.Fatalf("OrderCancelled written %d times, want 1", cancelEvents)
	}
	if _, err := f.svc.GetOrder(ctx, Actor{UserID: "other"}, id); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("non-owner read an order")
	}
	if _, err := f.svc.GetOrder(ctx, Actor{UserID: "admin", IsAdmin: true}, id); err != nil {
		t.Fatal("admin cannot read an order")
	}
}

// ---- payment worker ----

func worker(o ports.Orders, roll float64) *PaymentWorker {
	return NewPaymentWorker(PaymentWorker{Orders: o, SuccessRate: 0.8,
		Latency: func() time.Duration { return 0 }, Roll: func() float64 { return roll },
		Sleep: func(context.Context, time.Duration) error { return nil }})
}

func pendingOrder(t *testing.T, f *fx) string {
	t.Helper()
	_, err := f.svc.CreateOrder(ctx, "u1", "key-00000001", hold1)
	if err != nil {
		t.Fatal(err)
	}
	for id := range f.orders.orders {
		return id
	}
	return ""
}

func TestPaymentOutcomes(t *testing.T) {
	cases := []struct {
		roll   float64
		status domain.Status
		err    error
		event  string
	}{
		{0.1, domain.StatusPaid, nil, domain.EventOrderPaid},
		{0.85, domain.StatusPaymentFailed, nil, domain.EventPaymentFailed},
		{0.95, domain.StatusPendingPayment, ErrTransientPayment, ""},
	}
	for _, c := range cases {
		f := newFx()
		id := pendingOrder(t, f)
		err := worker(f.orders, c.roll).Process(ctx, domain.PaymentRequest{OrderID: id, AmountCents: 2000, Attempt: 1})
		if !errors.Is(err, c.err) || f.orders.orders[id].Status != c.status {
			t.Errorf("roll %.2f: status=%s err=%v", c.roll, f.orders.orders[id].Status, err)
		}
		found := false
		for _, e := range f.orders.outbox {
			found = found || e.Type == c.event
		}
		if c.event != "" && !found {
			t.Errorf("roll %.2f: event %s not in outbox", c.roll, c.event)
		}
	}
}

func TestDuplicatePaymentMessageAndPoison(t *testing.T) {
	f := newFx()
	id := pendingOrder(t, f)
	w := worker(f.orders, 0.1)
	req := domain.PaymentRequest{OrderID: id, AmountCents: 2000, Attempt: 1}
	for i := 0; i < 3; i++ {
		if err := w.Process(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	paid := 0
	for _, e := range f.orders.outbox {
		if e.Type == domain.EventOrderPaid {
			paid++
		}
	}
	if paid != 1 {
		t.Fatalf("OrderPaid published %d times for a redelivered message, want 1", paid)
	}
	if err := w.Process(ctx, domain.PaymentRequest{OrderID: id, Poison: true}); !errors.Is(err, ErrPoison) {
		t.Fatalf("poison must always fail: %v", err)
	}
}

func TestPaymentForCancelledOrderIsIgnored(t *testing.T) {
	f := newFx()
	id := pendingOrder(t, f)
	if _, err := f.svc.CancelOrder(ctx, "u1", id, "x"); err != nil {
		t.Fatal(err)
	}
	if err := worker(f.orders, 0.1).Process(ctx, domain.PaymentRequest{OrderID: id, AmountCents: 2000, Attempt: 1}); err != nil {
		t.Fatal(err)
	}
	if f.orders.orders[id].Status != domain.StatusCancelled {
		t.Fatalf("a late payment resurrected a cancelled order: %s", f.orders.orders[id].Status)
	}
}

func TestCancelAndPaymentRaceEndsInALegalState(t *testing.T) {
	for i := 0; i < 50; i++ {
		f := newFx()
		id := pendingOrder(t, f)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = f.svc.CancelOrder(ctx, "u1", id, "x") }()
		go func() {
			defer wg.Done()
			_ = worker(f.orders, 0.1).Process(ctx, domain.PaymentRequest{OrderID: id, AmountCents: 2000, Attempt: 1})
		}()
		wg.Wait()
		st := f.orders.orders[id].Status
		if st != domain.StatusCancelled && st != domain.StatusPaid {
			t.Fatalf("illegal end state %s", st)
		}
	}
}

// ---- relay ----

type fakeOutbox struct {
	mu         sync.Mutex
	rows       []ports.OutboxRow
	done       map[string]bool
	reconcile  []domain.PaymentRequest
	reconciled []string
}

func (o *fakeOutbox) Claim(_ context.Context, batch int, fn func([]ports.OutboxRow) ([]string, []string, error)) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var todo []ports.OutboxRow
	for _, r := range o.rows {
		if !o.done[r.ID] && len(todo) < batch {
			todo = append(todo, r)
		}
	}
	ok, _, err := fn(todo)
	for _, id := range ok {
		o.done[id] = true
	}
	return len(todo), err
}
func (o *fakeOutbox) CountUnpublished(context.Context) (int64, error)       { return 0, nil }
func (o *fakeOutbox) Cleanup(context.Context, time.Duration) (int64, error) { return 0, nil }
func (o *fakeOutbox) ClaimUnenqueuedPayments(_ context.Context, _ time.Duration, _ int, fn func([]domain.PaymentRequest) ([]string, error)) (int, error) {
	done, err := fn(o.reconcile)
	o.reconciled = append(o.reconciled, done...)
	return len(o.reconcile), err
}

type flakyPublisher struct{ failIDs map[string]bool }

func (p *flakyPublisher) Publish(_ context.Context, rows []ports.OutboxRow) []error {
	errs := make([]error, len(rows))
	for i, r := range rows {
		if p.failIDs[r.ID] {
			errs[i] = errors.New("throttled")
		}
	}
	return errs
}

func TestRelayMarksOnlyAcceptedRowsAndRetriesTheRest(t *testing.T) {
	ob := &fakeOutbox{done: map[string]bool{}, rows: []ports.OutboxRow{{ID: "a", CreatedAt: time.Now()}, {ID: "b", CreatedAt: time.Now()}}}
	pub := &flakyPublisher{failIDs: map[string]bool{"b": true}}
	r := &Relay{Outbox: ob, Publisher: pub}
	if _, err := r.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if !ob.done["a"] || ob.done["b"] {
		t.Fatalf("done = %v", ob.done)
	}
	pub.failIDs = nil
	if _, err := r.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if !ob.done["b"] {
		t.Fatal("failed row was not retried")
	}
}

func TestReconcilerQueuesLostPayments(t *testing.T) {
	ob := &fakeOutbox{done: map[string]bool{}, reconcile: []domain.PaymentRequest{{OrderID: "o1", AmountCents: 5, Attempt: 1}, {OrderID: "o2", AmountCents: 6, Attempt: 1}}}
	pay := &fakePayments{}
	r := &Relay{Outbox: ob, Payments: pay}
	if _, err := r.ReconcilePayments(ctx); err != nil || len(pay.sent) != 2 || len(ob.reconciled) != 2 {
		t.Fatalf("%v sent=%d reconciled=%v", err, len(pay.sent), ob.reconciled)
	}
	pay.fail = true
	ob.reconciled = nil
	_, _ = r.ReconcilePayments(ctx)
	if len(ob.reconciled) != 0 {
		t.Fatal("rows marked reconciled although the send failed")
	}
}
