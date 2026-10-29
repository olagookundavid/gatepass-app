// Package sqs (reservations adapter) turns SQS messages into application calls.
//
// HEXAGONAL BOUNDARY: adapter, "driving" side (like HTTP). The generic polling,
// visibility-extension and delete-on-success logic is in platform/sqsconsumer; this
// file only parses the message and calls app.HandleEvent.
//
// The queue is fed by EventBridge rules (EventPublished, PaymentFailed,
// OrderCancelled -> inventory queue). A message body is the whole EventBridge event.
// Returning an error leaves the message for redelivery and, after maxReceiveCount,
// for the dead-letter queue.
package sqs

import (
	"context"
	"fmt"

	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
	"github.com/olagookundavid/gatepass-app/internal/platform/sqsconsumer"
	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
	"github.com/olagookundavid/gatepass-app/internal/reservations/app"
)

// Handler builds the message handler for the inventory queue.
func Handler(svc *app.Service) sqsconsumer.Handler {
	return func(ctx context.Context, m sqsconsumer.Message) error {
		ev, err := ebpub.ParseSQSBody(m.Body)
		if err != nil {
			// A malformed message will never succeed: erroring lets SQS move it to
			// the DLQ after maxReceiveCount where a human can inspect it.
			return fmt.Errorf("unparseable message %s: %w", m.ID, err)
		}
		// EventBridge->SQS carries the trace in detail.traceparent, not as an SQS attribute.
		ctx = telemetry.ContextWithTraceparent(ctx, ev.Traceparent)
		return svc.HandleEvent(ctx, ev)
	}
}
