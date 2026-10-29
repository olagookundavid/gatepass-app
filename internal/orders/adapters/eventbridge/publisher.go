// Package eventbridge (orders adapter) implements ports.Publisher: it sends
// outbox rows to the Gatepass event bus.
//
// HEXAGONAL BOUNDARY: adapter. The envelope and the partial-failure handling of
// PutEvents are in platform/ebpub; this file maps outbox rows to entries.
// The outbox row id becomes `eventId`, so a row published twice (relay crashed
// before marking it) carries the SAME eventId and consumers can dedupe.
//
// AWS: EventBridge PutEvents (IAM events:PutEvents on the bus), source gatepass.orders.
package eventbridge

import (
	"context"
	"encoding/json"

	"github.com/olagookundavid/gatepass-app/internal/orders/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
)

// Publisher implements ports.Publisher.
type Publisher struct{ Pub *ebpub.Publisher }

var _ ports.Publisher = (*Publisher)(nil)

// Publish returns one error per row.
func (p *Publisher) Publish(ctx context.Context, rows []ports.OutboxRow) []error {
	entries := make([]ebpub.Entry, len(rows))
	for i, r := range rows {
		entries[i] = ebpub.Entry{EventID: r.ID, Type: r.EventType, Data: json.RawMessage(r.Payload),
			Traceparent: r.Traceparent, OccurredAt: r.CreatedAt}
	}
	return p.Pub.PublishBatch(ctx, entries)
}
