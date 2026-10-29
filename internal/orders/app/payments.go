package app

// This file: the payment worker use case (orders-worker).
//
// The worker consumes the payments queue and SIMULATES a payment provider:
// random latency, a success rate, permanent declines, transient errors.
//
// How SQS turns this into retries (no retry code here, on purpose):
//   - transient error  -> handler returns an error -> message is NOT deleted ->
//     it reappears after the visibility timeout -> tried again;
//   - after maxReceiveCount (5) failed receives SQS moves it to the dead-letter
//     queue, where an operator can inspect it. A message with "poison": true
//     fails on every attempt, which exercises exactly that path;
//   - permanent decline -> the order becomes payment_failed and the message is
//     acknowledged (retrying a declined card would be wrong).
//
// Idempotency: the message can arrive twice. The worker only acts on orders that
// are still pending_payment (Order.Pay / FailPayment are no-ops otherwise), and
// the status change happens under a row lock, so two workers cannot both win.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/olagookundavid/gatepass-app/internal/orders/domain"
	"github.com/olagookundavid/gatepass-app/internal/orders/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

// ErrTransientPayment is returned for provider hiccups; SQS will redeliver.
var ErrTransientPayment = errors.New("payment provider temporarily unavailable")

// ErrPoison is returned for messages flagged "poison": they always fail.
var ErrPoison = errors.New("poison message: always fails by design")

// PaymentWorker processes payment requests.
type PaymentWorker struct {
	Orders      ports.Orders
	SuccessRate float64
	Now         func() time.Time
	// Injectable for tests; defaults give 100-1500 ms latency and real randomness.
	Latency func() time.Duration
	Roll    func() float64
	Sleep   func(ctx context.Context, d time.Duration) error

	paid, failed metric.Int64Counter
	duration     metric.Float64Histogram
}

// NewPaymentWorker applies defaults and registers metrics.
func NewPaymentWorker(w PaymentWorker) *PaymentWorker {
	if w.Now == nil {
		w.Now = func() time.Time { return time.Now().UTC() }
	}
	if w.Latency == nil {
		w.Latency = func() time.Duration { return time.Duration(100+rand.IntN(1401)) * time.Millisecond }
	}
	if w.Roll == nil {
		w.Roll = rand.Float64
	}
	if w.Sleep == nil {
		w.Sleep = func(ctx context.Context, d time.Duration) error {
			select {
			case <-time.After(d):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	w.paid = telemetry.Counter("orders_paid_total", "Orders that became paid")
	w.failed = telemetry.Counter("payments_failed_total", "Payments that failed, by reason")
	w.duration = telemetry.Histogram("payment_processing_duration_seconds", "s", "Time to process one payment request")
	return &w
}

// Process handles one payment request. Returning an error means "retry via SQS".
func (w *PaymentWorker) Process(ctx context.Context, req domain.PaymentRequest) error {
	start := time.Now()
	defer func() { w.duration.Record(ctx, time.Since(start).Seconds()) }()

	if req.Poison {
		return ErrPoison
	}
	if req.OrderID == "" || req.AmountCents < 0 {
		// Malformed: retrying cannot help; failing sends it to the DLQ for a human.
		return fmt.Errorf("invalid payment request %+v", req)
	}
	order, err := w.Orders.Get(ctx, req.OrderID)
	if err != nil {
		return err // not found (replication lag? deleted?) -> retry, then DLQ
	}
	if order.Status != domain.StatusPendingPayment {
		slog.InfoContext(ctx, "payment request ignored: order is not pending", "order_id", order.ID, "status", string(order.Status))
		return nil
	}

	if err := w.Sleep(ctx, w.Latency()); err != nil { // the provider's response time
		return err
	}
	switch domain.SimulatePayment(w.SuccessRate, w.Roll()) {
	case domain.PaymentApproved:
		_, changed, err := w.Orders.Transition(ctx, order.ID, telemetry.TraceparentFromContext(ctx),
			func(o domain.Order) (domain.Change, error) { return o.Pay() })
		if err != nil {
			return err
		}
		if changed {
			w.paid.Add(ctx, 1)
		}
	case domain.PaymentDeclined:
		const reason = "card_declined"
		_, changed, err := w.Orders.Transition(ctx, order.ID, telemetry.TraceparentFromContext(ctx),
			func(o domain.Order) (domain.Change, error) { return o.FailPayment(reason) })
		if err != nil {
			return err
		}
		if changed {
			w.failed.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
		}
	default:
		w.failed.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "transient")))
		return ErrTransientPayment
	}
	return nil
}
