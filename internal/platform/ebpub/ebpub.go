// Package ebpub publishes domain events to Amazon EventBridge and parses them
// on the consuming side.
//
// Envelope (the `detail` JSON of every Gatepass event, see PLAN section 6):
//
//	{ "eventId": "<UUIDv7>", "eventType": "OrderPaid", "eventVersion": 1,
//	  "occurredAt": "2026-01-02T03:04:05Z", "traceparent": "00-...", "data": {...} }
//
// `source` is "gatepass.<service>", `detail-type` is the event name, the bus is
// EVENT_BUS_NAME. Events carry ids only - never an email address or other
// personal data - because buses, archives and rule targets have broader read
// access than the services' own databases.
//
// Delivery is AT-LEAST-ONCE: consumers dedupe on eventId (generated once, before
// the first publish attempt, so a retry re-sends the SAME id).
//
// IAM: events:PutEvents on the bus ARN.
package ebpub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/google/uuid"

	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

// Detail is the envelope inside EventBridge `detail`.
type Detail struct {
	EventID      string          `json:"eventId"`
	EventType    string          `json:"eventType"`
	EventVersion int             `json:"eventVersion"`
	OccurredAt   time.Time       `json:"occurredAt"`
	Traceparent  string          `json:"traceparent,omitempty"`
	Data         json.RawMessage `json:"data"`
}

// NewEventID returns a UUIDv7 (time-ordered: friendlier to B-tree indexes than random v4).
func NewEventID() string {
	id, err := uuid.NewV7()
	if err != nil { // only if the system clock/entropy source fails
		return uuid.NewString()
	}
	return id.String()
}

// Entry is one event to publish. Zero EventID/OccurredAt are filled in by Build.
type Entry struct {
	EventID     string
	Type        string
	Data        json.RawMessage
	Traceparent string
	OccurredAt  time.Time
}

// Build marshals data into an Entry, taking the traceparent from ctx.
func Build(ctx context.Context, eventType string, data any) (Entry, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Entry{}, err
	}
	return Entry{EventID: NewEventID(), Type: eventType, Data: raw,
		Traceparent: telemetry.TraceparentFromContext(ctx), OccurredAt: time.Now().UTC()}, nil
}

// API is the subset of *eventbridge.Client used.
type API interface {
	PutEvents(ctx context.Context, in *eventbridge.PutEventsInput, opts ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error)
}

// Publisher sends events to one bus as one source.
type Publisher struct {
	Client      API
	Bus         string
	Source      string // "gatepass.catalog"
	MaxAttempts int    // total tries per entry, default 3
	Backoff     time.Duration
}

// maxBatch is the PutEvents limit (10 entries per call).
const maxBatch = 10

// PublishBatch sends entries and returns one error slot per entry (nil =
// accepted by EventBridge). It retries ONLY the entries that failed, with
// bounded exponential backoff.
//
// Why per-entry results: PutEvents is not atomic. The API call can succeed
// (HTTP 200) while FailedEntryCount > 0 (e.g. throttled or internal error for
// some entries). Ignoring that would silently lose events.
func (p *Publisher) PublishBatch(ctx context.Context, entries []Entry) []error {
	errs := make([]error, len(entries))
	attempts := p.MaxAttempts
	if attempts <= 0 {
		attempts = 3
	}
	backoff := p.Backoff
	if backoff <= 0 {
		backoff = 200 * time.Millisecond
	}

	pending := make([]int, len(entries))
	for i := range entries {
		pending[i] = i
		errs[i] = errors.New("not attempted")
	}

	for attempt := 1; attempt <= attempts && len(pending) > 0; attempt++ {
		var still []int
		for start := 0; start < len(pending); start += maxBatch {
			chunk := pending[start:min(start+maxBatch, len(pending))]
			failed := p.putChunk(ctx, entries, chunk, errs)
			still = append(still, failed...)
		}
		pending = still
		if len(pending) > 0 && attempt < attempts {
			select {
			case <-time.After(backoff << (attempt - 1)):
			case <-ctx.Done():
				return errs
			}
		}
	}
	return errs
}

// putChunk performs one PutEvents call for the given entry indexes, writes
// results into errs, and returns the indexes that should be retried.
func (p *Publisher) putChunk(ctx context.Context, entries []Entry, idx []int, errs []error) []int {
	sent := make([]int, 0, len(idx)) // indexes actually in the request, in order
	reqEntries := make([]types.PutEventsRequestEntry, 0, len(idx))
	for _, i := range idx {
		e := entries[i]
		if e.OccurredAt.IsZero() {
			e.OccurredAt = time.Now().UTC()
		}
		detail, err := json.Marshal(Detail{EventID: e.EventID, EventType: e.Type, EventVersion: 1,
			OccurredAt: e.OccurredAt, Traceparent: e.Traceparent, Data: e.Data})
		if err != nil {
			errs[i] = fmt.Errorf("marshal event: %w", err) // permanent: retrying cannot help
			continue
		}
		sent = append(sent, i)
		reqEntries = append(reqEntries, types.PutEventsRequestEntry{
			EventBusName: aws.String(p.Bus), Source: aws.String(p.Source),
			DetailType: aws.String(e.Type), Detail: aws.String(string(detail)),
		})
	}
	if len(reqEntries) == 0 {
		return nil
	}

	// events:PutEvents - put up to 10 events on the bus. Failure modes: the call
	// itself fails (AccessDenied if the role lacks events:PutEvents on the bus,
	// ResourceNotFound for a wrong bus name, throttling, timeout) -> every entry
	// of the chunk is retried; OR the call succeeds with per-entry ErrorCode
	// (InternalFailure / ThrottlingException) -> only those entries are retried.
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := p.Client.PutEvents(cctx, &eventbridge.PutEventsInput{Entries: reqEntries})
	if err != nil {
		slog.WarnContext(ctx, "eventbridge PutEvents call failed", "error", err.Error())
		for _, i := range sent {
			errs[i] = fmt.Errorf("put events: %w", err)
		}
		return sent
	}

	// Results are positional: out.Entries[k] belongs to reqEntries[k] == sent[k].
	var retry []int
	for k, i := range sent {
		if k >= len(out.Entries) {
			errs[i] = errors.New("put events: missing result entry")
			retry = append(retry, i)
			continue
		}
		res := out.Entries[k]
		if res.ErrorCode != nil {
			errs[i] = fmt.Errorf("eventbridge entry failed: %s: %s", aws.ToString(res.ErrorCode), aws.ToString(res.ErrorMessage))
			retry = append(retry, i)
		} else {
			errs[i] = nil
		}
	}
	return retry
}

// Publish is the convenience form for a single event; it returns the final error.
func (p *Publisher) Publish(ctx context.Context, e Entry) error {
	if e.EventID == "" {
		e.EventID = NewEventID() // generated ONCE so internal retries re-send the same id
	}
	return p.PublishBatch(ctx, []Entry{e})[0]
}

// ---- consuming side -------------------------------------------------------

// Received is a parsed incoming event.
type Received struct {
	Detail
	Source     string
	DetailType string
}

// ParseSQSBody extracts the event from an SQS message body.
//
// When an EventBridge rule targets an SQS queue, the message body is the whole
// EventBridge event ({"version","id","detail-type","source","detail":{...}}).
// For tests and manual debugging we also accept a bare Detail document.
func ParseSQSBody(body string) (Received, error) {
	var env struct {
		Source     string          `json:"source"`
		DetailType string          `json:"detail-type"`
		Detail     json.RawMessage `json:"detail"`
	}
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		return Received{}, fmt.Errorf("body is not JSON: %w", err)
	}
	raw := env.Detail
	if len(raw) == 0 { // bare Detail document
		raw = json.RawMessage(body)
	}
	var d Detail
	if err := json.Unmarshal(raw, &d); err != nil {
		return Received{}, fmt.Errorf("detail is not a Gatepass envelope: %w", err)
	}
	if d.EventID == "" || d.EventType == "" {
		return Received{}, errors.New("envelope lacks eventId or eventType")
	}
	r := Received{Detail: d, Source: env.Source, DetailType: env.DetailType}
	if r.DetailType == "" {
		r.DetailType = d.EventType
	}
	return r, nil
}
