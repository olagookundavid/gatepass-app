// Package sqs (notifications adapter) feeds queue messages to the worker.
//
// HEXAGONAL BOUNDARY: adapter, driving side. The queue is filled by EventBridge
// rules (OrderPaid, PaymentFailed, OrderCancelled -> notifications queue); the body
// is the whole EventBridge envelope. An error leaves the message for redelivery and,
// after maxReceiveCount, for the DLQ.
package sqs

import (
	"context"
	"fmt"

	"github.com/olagookundavid/gatepass-app/internal/notifications/app"
	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
	"github.com/olagookundavid/gatepass-app/internal/platform/sqsconsumer"
	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

// Handler adapts the worker to the shared consumer loop.
func Handler(w *app.Worker) sqsconsumer.Handler {
	return func(ctx context.Context, m sqsconsumer.Message) error {
		ev, err := ebpub.ParseSQSBody(m.Body)
		if err != nil {
			return fmt.Errorf("unparseable message %s: %w", m.ID, err)
		}
		ctx = telemetry.ContextWithTraceparent(ctx, ev.Traceparent)
		return w.HandleEvent(ctx, ev)
	}
}
