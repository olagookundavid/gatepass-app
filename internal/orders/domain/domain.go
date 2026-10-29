// Package domain (orders) holds the order state machine and the shape of the
// domain events orders publishes.
//
// HEXAGONAL BOUNDARY: pure Go, no SQL/HTTP/AWS. The state machine is the part
// most worth unit testing: payments, cancellations and retries all race against
// each other in a distributed system, and every race must end in a legal state.
//
//	                 +--> paid ----------+
//	pending_payment -+                   +--> cancelled
//	                 +--> payment_failed   (cancelling a failed order is refused)
//	                 +----------------------> cancelled
//
// Every transition is IDEMPOTENT where repeating it is harmless (paying a paid
// order, cancelling a cancelled one): messages are delivered at least once.
//
// AWS services touched: none.
package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Status of an order.
type Status string

const (
	StatusPendingPayment Status = "pending_payment"
	StatusPaid           Status = "paid"
	StatusPaymentFailed  Status = "payment_failed"
	StatusCancelled      Status = "cancelled"
)

// Event type names (EventBridge detail-type).
const (
	EventOrderPlaced    = "OrderPlaced"
	EventOrderPaid      = "OrderPaid"
	EventPaymentFailed  = "PaymentFailed"
	EventOrderCancelled = "OrderCancelled"
)

// Errors shared across layers.
var (
	ErrNotFound          = errors.New("order not found")
	ErrInvalidTransition = errors.New("transition not allowed from the current status")
	ErrIdempotencyInUse  = errors.New("idempotency key already recorded")
	ErrOrderExists       = errors.New("an order already exists for this hold")

	// Reservations (the hold) outcomes seen through the consume call.
	ErrHoldNotFound     = errors.New("hold not found")
	ErrHoldNotYours     = errors.New("hold belongs to another user")
	ErrHoldExpired      = errors.New("hold expired")
	ErrHoldReleased     = errors.New("hold was released")
	ErrHoldTaken        = errors.New("hold already used by another order")
	ErrReservationsDown = errors.New("reservations service unavailable")
)

// Order is the aggregate.
type Order struct {
	ID           string    `json:"id"`
	UserID       string    `json:"userId"`
	HoldID       string    `json:"holdId"`
	EventID      string    `json:"eventId"`
	TicketTypeID string    `json:"ticketTypeId"`
	Quantity     int       `json:"quantity"`
	AmountCents  int64     `json:"amountCents"`
	Currency     string    `json:"currency"`
	Status       Status    `json:"status"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
	Version      int       `json:"version"`
}

// HistoryEntry is one row of the audit trail.
type HistoryEntry struct {
	Status Status    `json:"status"`
	Reason string    `json:"reason,omitempty"`
	At     time.Time `json:"at"`
}

// ordersNamespace seeds deterministic order ids (UUID v5).
var ordersNamespace = uuid.MustParse("6f1b0a52-7f0e-5c0e-9a53-3d9d4c8a1f10")

// DeterministicOrderID derives the order id from (user, idempotency key, hold).
//
// Why not a random id: the order is created in two steps that cannot be made
// atomic across services: (1) reservations consumes the hold FOR THIS order id,
// (2) we commit the order row. If we crash between the two and the client
// retries with the same Idempotency-Key, a RANDOM id would make the retry look
// like a different order and reservations would answer "already consumed by
// another order": the buyer would be stuck. With a deterministic id the retry
// presents the SAME order id, consume is idempotent for it, and the order is
// created. Including the hold id keeps a reused key (after the 24 h
// idempotency retention) from colliding with an old order.
func DeterministicOrderID(userID, idemKey, holdID string) string {
	return uuid.NewSHA1(ordersNamespace, []byte(userID+"\x00"+idemKey+"\x00"+holdID)).String()
}

// NewOrder builds a pending order from the consumed hold.
func NewOrder(id, userID, holdID, eventID, ticketTypeID string, quantity int, unitPriceCents int64, currency string, now time.Time) (Order, error) {
	if quantity < 1 || quantity > 8 {
		return Order{}, fmt.Errorf("quantity %d out of range", quantity)
	}
	if unitPriceCents < 0 {
		return Order{}, errors.New("negative price")
	}
	amount := int64(quantity) * unitPriceCents
	if unitPriceCents != 0 && amount/unitPriceCents != int64(quantity) {
		return Order{}, errors.New("amount overflow")
	}
	return Order{ID: id, UserID: userID, HoldID: holdID, EventID: eventID, TicketTypeID: ticketTypeID, Quantity: quantity,
		AmountCents: amount, Currency: currency, Status: StatusPendingPayment, CreatedAt: now.UTC(), UpdatedAt: now.UTC(), Version: 1}, nil
}

// Change describes the result of applying a transition: whether anything
// changed and what to record. The repository persists it in ONE transaction
// together with the history row and the outbox event.
type Change struct {
	Changed   bool
	NewStatus Status
	Reason    string
	EventType string // outbox event to publish when Changed
}

// Pay applies a successful payment. Paying a paid order is a no-op. A payment
// that arrives for a CANCELLED order (the user cancelled while the payment was
// in flight) is ignored here; a real system would also trigger a refund.
func (o Order) Pay() (Change, error) {
	switch o.Status {
	case StatusPendingPayment:
		return Change{Changed: true, NewStatus: StatusPaid, EventType: EventOrderPaid}, nil
	case StatusPaid, StatusCancelled, StatusPaymentFailed:
		return Change{}, nil
	}
	return Change{}, ErrInvalidTransition
}

// FailPayment applies a permanent payment failure.
func (o Order) FailPayment(reason string) (Change, error) {
	switch o.Status {
	case StatusPendingPayment:
		return Change{Changed: true, NewStatus: StatusPaymentFailed, Reason: reason, EventType: EventPaymentFailed}, nil
	case StatusPaymentFailed, StatusPaid, StatusCancelled:
		return Change{}, nil
	}
	return Change{}, ErrInvalidTransition
}

// Cancel applies a user cancellation. Allowed from pending_payment and paid.
// Cancelling a cancelled order is a no-op (idempotent); a payment_failed order
// has nothing to cancel (its tickets were already released by PaymentFailed).
func (o Order) Cancel(reason string) (Change, error) {
	switch o.Status {
	case StatusPendingPayment, StatusPaid:
		return Change{Changed: true, NewStatus: StatusCancelled, Reason: reason, EventType: EventOrderCancelled}, nil
	case StatusCancelled:
		return Change{}, nil
	}
	return Change{}, ErrInvalidTransition
}

// EventData is the `data` of every order event (ids and numbers only; never an
// email address or other personal data).
type EventData struct {
	OrderID      string `json:"orderId"`
	UserID       string `json:"userId"`
	HoldID       string `json:"holdId"`
	EventID      string `json:"eventId"`
	TicketTypeID string `json:"ticketTypeId"`
	Quantity     int    `json:"quantity"`
	AmountCents  int64  `json:"amountCents"`
	Currency     string `json:"currency"`
	Reason       string `json:"reason,omitempty"`
}

// EventDataFor builds the payload for o.
func EventDataFor(o Order, reason string) EventData {
	return EventData{OrderID: o.ID, UserID: o.UserID, HoldID: o.HoldID, EventID: o.EventID, TicketTypeID: o.TicketTypeID,
		Quantity: o.Quantity, AmountCents: o.AmountCents, Currency: o.Currency, Reason: reason}
}

// PaymentRequest is the message on the payments queue.
type PaymentRequest struct {
	OrderID     string `json:"orderId"`
	AmountCents int64  `json:"amountCents"`
	Attempt     int    `json:"attempt"`
	// Poison makes the worker fail every time; used to exercise the DLQ.
	Poison bool `json:"poison,omitempty"`
}

// PaymentOutcome is what the (simulated) provider answered.
type PaymentOutcome int

const (
	PaymentApproved  PaymentOutcome = iota
	PaymentDeclined                 // permanent: do not retry
	PaymentTransient                // temporary provider problem: retry later
)

// SimulatePayment maps two random draws in [0,1) to an outcome.
//
// successRate is the probability of approval. Of the remaining probability mass,
// half are permanent declines and half transient errors, so a default rate of 0.8
// gives 80% approved, 10% declined, 10% transient.
func SimulatePayment(successRate, roll float64) PaymentOutcome {
	switch {
	case roll < successRate:
		return PaymentApproved
	case roll < successRate+(1-successRate)/2:
		return PaymentDeclined
	default:
		return PaymentTransient
	}
}
