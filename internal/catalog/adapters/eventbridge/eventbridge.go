// Package eventbridge (catalog adapter) implements ports.EventAnnouncer by
// putting the EventPublished event on the Gatepass bus.
//
// HEXAGONAL BOUNDARY: adapter. The envelope, retry and partial-failure handling
// live in platform/ebpub; this file only chooses the event name and payload.
//
// AWS service: Amazon EventBridge. IAM: events:PutEvents on the bus ARN.
// Delivery: at-least-once and WITHOUT an outbox: the call happens after the
// database commit, so a crash in between loses the announcement (the app makes
// the publish endpoint re-announce to recover; see app.PublishEvent).
package eventbridge

import (
	"context"

	"github.com/olagookundavid/gatepass-app/internal/catalog/domain"
	"github.com/olagookundavid/gatepass-app/internal/catalog/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
)

// Announcer implements ports.EventAnnouncer.
type Announcer struct{ Pub *ebpub.Publisher }

var _ ports.EventAnnouncer = (*Announcer)(nil)

// EventPublished is the detail-type consumed by reservations.
const EventPublished = "EventPublished"

// AnnouncePublished sends the event; retries are bounded inside the publisher.
func (a *Announcer) AnnouncePublished(ctx context.Context, d domain.EventPublishedData) error {
	e, err := ebpub.Build(ctx, EventPublished, d)
	if err != nil {
		return err
	}
	return a.Pub.Publish(ctx, e)
}
