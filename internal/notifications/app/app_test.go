package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/notifications/domain"
	"github.com/olagookundavid/gatepass-app/internal/notifications/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
)

type store struct {
	mu        sync.Mutex
	notifs    []domain.Notification
	prefs     map[string]domain.Preferences
	tickets   map[string]string
	processed map[string]bool
}

func newStore() *store {
	return &store{prefs: map[string]domain.Preferences{}, tickets: map[string]string{}, processed: map[string]bool{}}
}
func (s *store) PutNotification(_ context.Context, n domain.Notification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notifs = append(s.notifs, n)
	return nil
}
func (s *store) ListNotifications(context.Context, string, *ports.NotificationCursor, int) ([]domain.Notification, *ports.NotificationCursor, error) {
	return s.notifs, nil, nil
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
func (s *store) PutTicketRecord(_ context.Context, u, o, k string) error {
	s.tickets[u+"/"+o] = k
	return nil
}
func (s *store) GetTicketRecord(_ context.Context, u, o string) (string, bool, error) {
	k, ok := s.tickets[u+"/"+o]
	return k, ok, nil
}
func (s *store) EventProcessed(_ context.Context, id string) (bool, error) {
	return s.processed[id], nil
}
func (s *store) MarkEventProcessed(_ context.Context, id string, _ time.Duration) error {
	s.processed[id] = true
	return nil
}

type tickets struct{ puts map[string][]byte }

func (t *tickets) Put(_ context.Context, k string, b []byte) error { t.puts[k] = b; return nil }
func (t *tickets) PresignGet(_ context.Context, k, _ string, ttl time.Duration) (string, error) {
	return "https://s3.example.com/" + k + "?ttl=" + ttl.String(), nil
}

type renderer struct{}

func (renderer) Render(t domain.Ticket) ([]byte, error) { return []byte("PNG:" + t.Token), nil }

type directory struct {
	email string
	err   error
	calls int
}

func (d *directory) EmailOf(context.Context, string) (string, error) {
	d.calls++
	return d.email, d.err
}

type mailer struct {
	sent []struct {
		To string
		M  domain.Email
	}
	err error
}

func (m *mailer) Send(_ context.Context, to string, e domain.Email) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.sent = append(m.sent, struct {
		To string
		M  domain.Email
	}{to, e})
	return "ses-msg-1", nil
}

type fx struct {
	w   *Worker
	st  *store
	tk  *tickets
	dir *directory
	ml  *mailer
}

func newFx() *fx {
	f := &fx{st: newStore(), tk: &tickets{puts: map[string][]byte{}}, dir: &directory{email: "jane@example.com"}, ml: &mailer{}}
	f.w = NewWorker(Worker{Store: f.st, Tickets: f.tk, Renderer: renderer{}, Directory: f.dir, Mailer: f.ml, SigningKey: []byte("0123456789abcdef0123456789abcdef")})
	return f
}

func event(id, typ string) ebpub.Received {
	d, _ := json.Marshal(domain.OrderEventData{OrderID: "order-1", UserID: "user-1", EventID: "ev-1", Quantity: 2, AmountCents: 2500, Currency: "USD"})
	return ebpub.Received{Detail: ebpub.Detail{EventID: id, EventType: typ, Data: d}}
}

var ctx = context.Background()

func TestOrderPaidRendersUploadsAndEmailsOnce(t *testing.T) {
	f := newFx()
	for i := 0; i < 3; i++ { // the same event delivered three times
		if err := f.w.HandleEvent(ctx, event("evt-1", "OrderPaid")); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.ml.sent) != 1 {
		t.Fatalf("%d e-mails for one event, want 1", len(f.ml.sent))
	}
	key := "tickets/user-1/order-1.png"
	if !strings.HasPrefix(string(f.tk.puts[key]), "PNG:GP1.order-1.") {
		t.Fatalf("ticket not stored under %s: %v", key, f.tk.puts)
	}
	if f.st.tickets["user-1/order-1"] != key {
		t.Fatal("ticket record missing")
	}
	if !strings.Contains(f.ml.sent[0].M.Text, "https://s3.example.com/"+key) || !strings.Contains(f.ml.sent[0].M.Text, "ttl=24h") {
		t.Fatalf("mail lacks the 24h link: %s", f.ml.sent[0].M.Text)
	}
	if len(f.st.notifs) != 1 || f.st.notifs[0].Status != domain.StatusSent || f.st.notifs[0].MessageID != "ses-msg-1" {
		t.Fatalf("log: %+v", f.st.notifs)
	}
	// the address must never be stored in the notification log
	b, _ := json.Marshal(f.st.notifs)
	if strings.Contains(string(b), "jane@example.com") {
		t.Fatal("e-mail address leaked into the notification log")
	}
}

func TestPreferencesDisableEmailButKeepTheTicket(t *testing.T) {
	f := newFx()
	f.st.prefs["user-1"] = domain.Preferences{EmailEnabled: false}
	if err := f.w.HandleEvent(ctx, event("evt-2", "OrderPaid")); err != nil {
		t.Fatal(err)
	}
	if len(f.ml.sent) != 0 || f.dir.calls != 0 {
		t.Fatal("mail sent or Cognito called although e-mail is disabled")
	}
	if len(f.st.notifs) != 1 || f.st.notifs[0].Status != domain.StatusSkipped {
		t.Fatalf("%+v", f.st.notifs)
	}
	if _, ok := f.tk.puts["tickets/user-1/order-1.png"]; !ok {
		t.Fatal("ticket must still be generated so ticket-url works")
	}
}

func TestPermanentFailuresAreRecordedAndAcknowledged(t *testing.T) {
	f := newFx()
	f.ml.err = domain.ErrMailRejected
	if err := f.w.HandleEvent(ctx, event("evt-3", "PaymentFailed")); err != nil {
		t.Fatalf("permanent failure must be acknowledged: %v", err)
	}
	if f.st.notifs[0].Status != domain.StatusFailed || f.st.notifs[0].Reason != "address_rejected" {
		t.Fatalf("%+v", f.st.notifs)
	}

	f2 := newFx()
	f2.dir.err = domain.ErrNoEmail
	if err := f2.w.HandleEvent(ctx, event("evt-4", "OrderCancelled")); err != nil || f2.st.notifs[0].Reason != "no_email_address" {
		t.Fatalf("no e-mail: %v %+v", err, f2.st.notifs)
	}
}

func TestTemporaryFailuresAreRetriedAndNotMarkedProcessed(t *testing.T) {
	f := newFx()
	f.ml.err = domain.ErrMailThrottled
	if err := f.w.HandleEvent(ctx, event("evt-5", "OrderPaid")); !errors.Is(err, domain.ErrMailThrottled) {
		t.Fatalf("throttling must surface as an error so SQS retries: %v", err)
	}
	if f.st.processed["evt-5"] || len(f.st.notifs) != 0 {
		t.Fatal("event marked processed after a temporary failure")
	}
	f.ml.err = nil // SES recovered; SQS redelivers
	if err := f.w.HandleEvent(ctx, event("evt-5", "OrderPaid")); err != nil || len(f.ml.sent) != 1 {
		t.Fatalf("retry: %v sent=%d", err, len(f.ml.sent))
	}

	f.dir.err = errors.New("cognito 500")
	if err := f.w.HandleEvent(ctx, event("evt-6", "OrderPaid")); err == nil {
		t.Fatal("temporary directory failure must be retried")
	}
}

func TestRecipientOverrideRoutesEverythingToOneAddress(t *testing.T) {
	f := newFx()
	f.w.RecipientOverride = "sandbox@example.com"
	_ = f.w.HandleEvent(ctx, event("evt-7", "OrderCancelled"))
	if len(f.ml.sent) != 1 || f.ml.sent[0].To != "sandbox@example.com" {
		t.Fatalf("%+v", f.ml.sent)
	}
}

func TestUnknownEventTypesAndBadIDs(t *testing.T) {
	f := newFx()
	if err := f.w.HandleEvent(ctx, event("evt-8", "OrderPlaced")); err != nil || len(f.ml.sent) != 0 {
		t.Fatalf("OrderPlaced must be ignored: %v", err)
	}
	bad := event("evt-9", "OrderPaid")
	bad.Data = json.RawMessage(`{"orderId":"../../x","userId":"u"}`)
	if err := f.w.HandleEvent(ctx, bad); err == nil {
		t.Fatal("path-traversal order id accepted")
	}
}

func TestTicketURLIsOwnerOnly(t *testing.T) {
	f := newFx()
	_ = f.w.HandleEvent(ctx, event("evt-10", "OrderPaid"))
	api := &API{Store: f.st, Tickets: f.tk}
	got, err := api.TicketURL(ctx, "user-1", "order-1")
	if err != nil || !strings.Contains(got.URL, "tickets/user-1/order-1.png") || !strings.Contains(got.URL, "ttl=5m") {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err := api.TicketURL(ctx, "user-2", "order-1"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("another user must get not found: %v", err)
	}
	if _, err := api.TicketURL(ctx, "user-1", "no-such-order"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing ticket: %v", err)
	}
}
