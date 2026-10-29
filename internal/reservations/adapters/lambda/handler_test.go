package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/olagookundavid/gatepass-app/internal/reservations/app"
	"github.com/olagookundavid/gatepass-app/internal/reservations/domain"
	"github.com/olagookundavid/gatepass-app/internal/reservations/ports"
)

// recStore records which release path was used. It is a fake of OUR port.
type recStore struct {
	ports.Store
	mu       sync.Mutex
	hold     domain.Hold
	found    bool
	released []string
	failWith error
}

func (s *recStore) GetHold(context.Context, string) (domain.Hold, error) {
	if !s.found {
		return domain.Hold{}, domain.ErrNotFound
	}
	return s.hold, nil
}
func (s *recStore) ReleaseHold(_ context.Context, id string, _ []domain.HoldStatus, _ time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failWith != nil {
		return false, s.failWith
	}
	s.released = append(s.released, "table:"+id)
	return true, nil
}
func (s *recStore) ReleaseDeletedHold(_ context.Context, h domain.Hold, _ time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failWith != nil {
		return false, s.failWith
	}
	s.released = append(s.released, "stream:"+h.ID)
	return true, nil
}

func newHandler(s *recStore, now time.Time) (*Handler, *int) {
	emitted := 0
	svc := app.New(app.Service{Store: s, Now: func() time.Time { return now }})
	return &Handler{Svc: svc, Emit: func(_ context.Context, _ string, n int) { emitted += n }}, &emitted
}

var t0 = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func activeHold() domain.Hold {
	return domain.Hold{ID: "h1", UserID: "u", EventID: "e", TicketTypeID: "t", Quantity: 2, Status: domain.StatusActive, ExpiresAt: t0, CreatedAt: t0.Add(-10 * time.Minute)}
}

func TestSchedulerShape(t *testing.T) {
	s := &recStore{hold: activeHold(), found: true}
	h, emitted := newHandler(s, t0.Add(time.Second))
	out, err := h.Handle(context.Background(), json.RawMessage(`{"holdId":"h1"}`))
	if err != nil || !out.(SchedulerOutput).Released || len(s.released) != 1 || *emitted != 1 {
		t.Fatalf("out=%v err=%v released=%v emitted=%d", out, err, s.released, *emitted)
	}

	// consumed hold: success without releasing
	s2 := &recStore{hold: activeHold(), found: true}
	s2.hold.Status = domain.StatusConsumed
	h2, e2 := newHandler(s2, t0.Add(time.Hour))
	out, err = h2.Handle(context.Background(), json.RawMessage(`{"holdId":"h1"}`))
	if err != nil || out.(SchedulerOutput).Released || *e2 != 0 {
		t.Fatalf("consumed: out=%v err=%v", out, err)
	}

	// missing hold: success
	h3, _ := newHandler(&recStore{}, t0)
	if _, err := h3.Handle(context.Background(), json.RawMessage(`{"holdId":"gone"}`)); err != nil {
		t.Fatalf("missing hold must succeed: %v", err)
	}

	// early fire: error so Scheduler retries
	h4, _ := newHandler(&recStore{hold: activeHold(), found: true}, t0.Add(-time.Minute))
	if _, err := h4.Handle(context.Background(), json.RawMessage(`{"holdId":"h1"}`)); !errors.Is(err, domain.ErrNotYetExpired) {
		t.Fatalf("early fire: %v", err)
	}
}

const ttlRecord = `{"Records":[{
 "eventID":"1","eventName":"REMOVE","eventSource":"aws:dynamodb","awsRegion":"us-east-1",
 "userIdentity":{"type":"Service","principalId":"dynamodb.amazonaws.com"},
 "dynamodb":{"SequenceNumber":"111","OldImage":{
   "PK":{"S":"HOLD#h1"},"SK":{"S":"HOLD"},"holdId":{"S":"h1"},"userId":{"S":"u"},"eventId":{"S":"e"},"ticketTypeId":{"S":"t"},
   "quantity":{"N":"2"},"status":{"S":"%s"},"expiresAt":{"N":"1780315800"},"createdAt":{"N":"1780315200"}}}}]}`

func TestStreamShapeOnlyTTLDeletedActiveHolds(t *testing.T) {
	s := &recStore{}
	h, emitted := newHandler(s, t0)
	out, err := h.Handle(context.Background(), json.RawMessage(fmt.Sprintf(ttlRecord, "active")))
	if err != nil || len(s.released) != 1 || s.released[0] != "stream:h1" || *emitted != 1 {
		t.Fatalf("active ttl delete: out=%v err=%v released=%v", out, err, s.released)
	}

	// already released/consumed when TTL removed it: ignore
	s2 := &recStore{}
	h2, _ := newHandler(s2, t0)
	_, _ = h2.Handle(context.Background(), json.RawMessage(fmt.Sprintf(ttlRecord, "released")))
	_, _ = h2.Handle(context.Background(), json.RawMessage(fmt.Sprintf(ttlRecord, "consumed")))
	if len(s2.released) != 0 {
		t.Fatalf("processed non-active images: %v", s2.released)
	}

	// deletion by a human (no Service identity) is not an expiry
	s3 := &recStore{}
	h3, _ := newHandler(s3, t0)
	human := `{"Records":[{"eventID":"1","eventName":"REMOVE","eventSource":"aws:dynamodb",
	  "dynamodb":{"SequenceNumber":"1","OldImage":{"PK":{"S":"HOLD#h1"},"SK":{"S":"HOLD"},"holdId":{"S":"h1"},"status":{"S":"active"}}}}]}`
	_, _ = h3.Handle(context.Background(), json.RawMessage(human))
	if len(s3.released) != 0 {
		t.Fatal("user-initiated delete treated as TTL")
	}
}

func TestStreamReportsPartialBatchFailures(t *testing.T) {
	s := &recStore{failWith: errors.New("dynamodb throttled")}
	h, _ := newHandler(s, t0)
	out, err := h.Handle(context.Background(), json.RawMessage(fmt.Sprintf(ttlRecord, "active")))
	if err != nil {
		t.Fatal(err)
	}
	resp, ok := out.(events.DynamoDBEventResponse)
	if !ok || len(resp.BatchItemFailures) != 1 || resp.BatchItemFailures[0].ItemIdentifier != "111" {
		t.Fatalf("want the failed record's sequence number reported, got %+v", out)
	}
}

func TestUnknownPayload(t *testing.T) {
	h, _ := newHandler(&recStore{}, t0)
	if _, err := h.Handle(context.Background(), json.RawMessage(`{"hello":"world"}`)); err == nil {
		t.Fatal("unknown payload accepted")
	}
}
