package ebpub

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

// fakeEB fails the second entry of the first call (partial failure), then succeeds.
type fakeEB struct {
	calls   int
	details [][]string
}

func (f *fakeEB) PutEvents(_ context.Context, in *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	f.calls++
	var ds []string
	out := &eventbridge.PutEventsOutput{}
	for i, e := range in.Entries {
		ds = append(ds, aws.ToString(e.Detail))
		r := types.PutEventsResultEntry{EventId: aws.String("x")}
		if f.calls == 1 && i == 1 {
			r = types.PutEventsResultEntry{ErrorCode: aws.String("ThrottlingException"), ErrorMessage: aws.String("slow down")}
			out.FailedEntryCount++
		}
		out.Entries = append(out.Entries, r)
	}
	f.details = append(f.details, ds)
	return out, nil
}

func TestPartialFailureRetriesOnlyFailedEntriesWithSameEventID(t *testing.T) {
	f := &fakeEB{}
	p := &Publisher{Client: f, Bus: "bus", Source: "gatepass.test", MaxAttempts: 3, Backoff: time.Millisecond}
	entries := []Entry{
		{EventID: "e1", Type: "OrderPaid", Data: json.RawMessage(`{"orderId":"o1"}`)},
		{EventID: "e2", Type: "OrderPaid", Data: json.RawMessage(`{"orderId":"o2"}`)},
	}
	errs := p.PublishBatch(context.Background(), entries)
	for i, e := range errs {
		if e != nil {
			t.Fatalf("entry %d: %v", i, e)
		}
	}
	if f.calls != 2 || len(f.details[1]) != 1 {
		t.Fatalf("calls=%d second-call-entries=%d; want one retry of one entry", f.calls, len(f.details[1]))
	}
	var d Detail
	_ = json.Unmarshal([]byte(f.details[1][0]), &d)
	if d.EventID != "e2" {
		t.Fatalf("retry changed eventId: %s", d.EventID)
	}
}

type errEB struct{}

func (errEB) PutEvents(context.Context, *eventbridge.PutEventsInput, ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	return nil, errors.New("access denied")
}

func TestBoundedRetriesThenError(t *testing.T) {
	p := &Publisher{Client: errEB{}, Bus: "b", Source: "s", MaxAttempts: 2, Backoff: time.Millisecond}
	if err := p.Publish(context.Background(), Entry{Type: "X", Data: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("expected error after bounded retries")
	}
}

func TestParseSQSBody(t *testing.T) {
	wrapped := `{"version":"0","id":"abc","detail-type":"OrderPaid","source":"gatepass.orders",
	 "detail":{"eventId":"e1","eventType":"OrderPaid","eventVersion":1,"occurredAt":"2026-01-02T03:04:05Z","data":{"orderId":"o1"}}}`
	r, err := ParseSQSBody(wrapped)
	if err != nil || r.EventID != "e1" || r.DetailType != "OrderPaid" || r.Source != "gatepass.orders" {
		t.Fatalf("%+v %v", r, err)
	}
	bare := `{"eventId":"e2","eventType":"PaymentFailed","eventVersion":1,"occurredAt":"2026-01-02T03:04:05Z","data":{}}`
	if r, err := ParseSQSBody(bare); err != nil || r.EventID != "e2" {
		t.Fatalf("%+v %v", r, err)
	}
	for _, bad := range []string{`nope`, `{"detail":{"eventType":"X"}}`, `{"hello":"world"}`} {
		if _, err := ParseSQSBody(bad); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}
