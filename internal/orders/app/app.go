// Package app (orders) contains the use cases of the orders service: creating
// an order from a hold, reading orders, cancelling, and (in payments.go and
// relay.go) processing payments and relaying the outbox.
//
// HEXAGONAL BOUNDARY: depends only on domain and ports.
//
// The core problem of this service is the "dual write": one business action must
// change our database AND tell other systems (reservations, notifications). Two
// independent writes can half-fail. The answer used here is the TRANSACTIONAL
// OUTBOX:
//
//  1. the state change and an `outbox` row describing the event are written in
//     ONE Postgres transaction (both happen or neither);
//  2. a separate process (orders-outbox-relay) reads unpublished outbox rows and
//     puts them on EventBridge, marking them published afterwards.
//
// If the relay dies between PutEvents and the UPDATE, the row is published again
// later: events are AT-LEAST-ONCE, and every consumer dedupes on eventId (the
// outbox row id).
//
// AWS services touched (through ports): RDS PostgreSQL, SQS, EventBridge.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/olagookundavid/gatepass-app/internal/orders/domain"
	"github.com/olagookundavid/gatepass-app/internal/orders/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/idempotency"
	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

// IdempotencyRetention is how long a stored response can be replayed.
const IdempotencyRetention = 24 * time.Hour

// Actor is the authenticated caller.
type Actor struct {
	UserID  string
	IsAdmin bool
}

// ErrIdempotencyConflict: same key, different request.
var ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")

// Service implements the API use cases.
type Service struct {
	Orders   ports.Orders
	Holds    ports.Holds
	Payments ports.Payments
	Now      func() time.Time

	created, enqueueFailures metric.Int64Counter
}

// New applies defaults and registers metrics.
func New(s Service) *Service {
	if s.Now == nil {
		s.Now = func() time.Time { return time.Now().UTC() }
	}
	s.created = telemetry.Counter("orders_created_total", "Orders created")
	s.enqueueFailures = telemetry.Counter("orders_payment_enqueue_failures_total", "Payment request could not be queued right after commit (the reconciler will retry)")
	return &s
}

// Response is an HTTP-shaped result, so that a replay can return the stored
// bytes unchanged.
type Response struct {
	Code     int
	Body     []byte
	Replayed bool
}

type createRequest struct {
	HoldID string `json:"holdId"`
}

// CreateOrder turns a hold into an order.
//
// Sequence and the failure window it closes:
//
//  1. Replay check: (user, Idempotency-Key) seen before? -> return the stored response.
//  2. Consume the hold in reservations for a DETERMINISTIC order id (see
//     domain.DeterministicOrderID). Idempotent per order id, so safe to retry.
//  3. ONE Postgres transaction: order + status history + OrderPlaced outbox row +
//     idempotency record. Either all four exist or none.
//  4. After commit, queue the payment request. If that fails the order stays
//     `pending_payment` with payment_enqueued_at NULL and the relay's reconciler
//     queues it a moment later; the API call still succeeds.
//
// Crash between 2 and 3: the hold is consumed for order id X, no order row yet.
// The client retries with the same Idempotency-Key -> same X -> consume succeeds
// again -> step 3 runs. Nothing is lost or double-sold.
func (s *Service) CreateOrder(ctx context.Context, userID, idemKey, holdID string) (Response, error) {
	canonical, _ := json.Marshal(createRequest{HoldID: holdID})
	hash := idempotency.Hash(canonical)

	if resp, done, err := s.replay(ctx, userID, idemKey, hash); done || err != nil {
		return resp, err
	}

	orderID := domain.DeterministicOrderID(userID, idemKey, holdID)
	hold, err := s.Holds.Consume(ctx, holdID, orderID, userID)
	if err != nil {
		return Response{}, err
	}
	order, err := domain.NewOrder(orderID, userID, hold.HoldID, hold.EventID, hold.TicketTypeID, hold.Quantity, hold.UnitPriceCents, hold.Currency, s.Now())
	if err != nil {
		return Response{}, err
	}
	body, err := json.Marshal(order)
	if err != nil {
		return Response{}, err
	}

	err = s.Orders.CreateOrder(ctx, ports.NewOrder{
		Order: order, IdemKey: idemKey, IdemExpires: s.Now().Add(IdempotencyRetention),
		Idempotency: ports.IdempotencyRecord{RequestHash: hash, ResponseCode: 201, ResponseBody: body},
		Traceparent: telemetry.TraceparentFromContext(ctx),
	})
	switch {
	case errors.Is(err, domain.ErrIdempotencyInUse):
		// A concurrent duplicate request committed first: replay its result.
		resp, done, rerr := s.replay(ctx, userID, idemKey, hash)
		if done || rerr != nil {
			return resp, rerr
		}
		return Response{}, err
	case err != nil:
		return Response{}, err
	}
	s.created.Add(ctx, 1)
	s.enqueuePayment(ctx, order)
	return Response{Code: 201, Body: body}, nil
}

func (s *Service) replay(ctx context.Context, userID, key, hash string) (Response, bool, error) {
	rec, found, err := s.Orders.FindIdempotency(ctx, userID, key)
	if err != nil {
		return Response{}, false, err
	}
	switch idempotency.Decide(found, rec.RequestHash, hash) {
	case idempotency.Conflict:
		return Response{}, true, ErrIdempotencyConflict
	case idempotency.Replay:
		return Response{Code: rec.ResponseCode, Body: rec.ResponseBody, Replayed: true}, true, nil
	}
	return Response{}, false, nil
}

func (s *Service) enqueuePayment(ctx context.Context, o domain.Order) {
	err := s.Payments.Enqueue(ctx, domain.PaymentRequest{OrderID: o.ID, AmountCents: o.AmountCents, Attempt: 1})
	if err != nil {
		s.enqueueFailures.Add(ctx, 1)
		slog.ErrorContext(ctx, "could not queue payment; reconciler will retry", "order_id", o.ID, "error", err.Error())
		return
	}
	if err := s.Orders.MarkPaymentEnqueued(ctx, o.ID, s.Now()); err != nil {
		// Harmless: the reconciler may queue a duplicate payment request; the worker
		// only acts on orders still pending_payment, so a duplicate is a no-op.
		slog.WarnContext(ctx, "could not record payment_enqueued_at", "order_id", o.ID, "error", err.Error())
	}
}

// GetOrder returns an order to its owner or an admin. Anyone else gets
// ErrNotFound (404, not 403: do not reveal which ids exist).
func (s *Service) GetOrder(ctx context.Context, a Actor, id string) (domain.Order, error) {
	o, err := s.Orders.Get(ctx, id)
	if err != nil {
		return domain.Order{}, err
	}
	if o.UserID != a.UserID && !a.IsAdmin {
		return domain.Order{}, domain.ErrNotFound
	}
	return o, nil
}

// ListMyOrders pages the caller's orders, newest first. Returns limit items and
// the cursor of the next page (nil at the end).
func (s *Service) ListMyOrders(ctx context.Context, userID string, after *ports.OrderCursor, limit int) ([]domain.Order, *ports.OrderCursor, error) {
	rows, err := s.Orders.ListByUser(ctx, userID, after, limit+1)
	if err != nil {
		return nil, nil, err
	}
	if len(rows) > limit {
		last := rows[limit-1]
		return rows[:limit], &ports.OrderCursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
	}
	return rows, nil, nil
}

// History returns the audit trail (owner or admin).
func (s *Service) History(ctx context.Context, a Actor, id string) ([]domain.HistoryEntry, error) {
	if _, err := s.GetOrder(ctx, a, id); err != nil {
		return nil, err
	}
	return s.Orders.History(ctx, id)
}

// CancelOrder cancels the caller's own order. Idempotent.
//
// Concurrency: Transition locks the order row, so a cancel and a payment worker
// processing the same order are serialised by Postgres; whichever runs second
// sees the other's result and the domain rules decide (e.g. a payment for a
// cancelled order is ignored).
func (s *Service) CancelOrder(ctx context.Context, userID, id, reason string) (domain.Order, error) {
	cur, err := s.Orders.Get(ctx, id)
	if err != nil {
		return domain.Order{}, err
	}
	if cur.UserID != userID {
		return domain.Order{}, domain.ErrNotFound
	}
	o, _, err := s.Orders.Transition(ctx, id, telemetry.TraceparentFromContext(ctx),
		func(o domain.Order) (domain.Change, error) { return o.Cancel(reason) })
	if err != nil {
		return domain.Order{}, fmt.Errorf("cancel order: %w", err)
	}
	return o, nil
}
