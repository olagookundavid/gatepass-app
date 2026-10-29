// Package ports (orders) declares what the use cases need from outside.
//
// HEXAGONAL BOUNDARY: interfaces owned by the application; implemented by
// adapters (Postgres, the reservations HTTP client, SQS, EventBridge).
package ports

import (
	"context"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/orders/domain"
)

// IdempotencyRecord is a stored response for (user, key).
type IdempotencyRecord struct {
	RequestHash  string
	ResponseCode int
	ResponseBody []byte
}

// NewOrder is everything CreateOrder writes in ONE database transaction.
type NewOrder struct {
	Order       domain.Order
	IdemKey     string
	Idempotency IdempotencyRecord
	IdemExpires time.Time
	Traceparent string
}

// OrderCursor is the keyset position for GET /me/orders (newest first).
type OrderCursor struct {
	CreatedAt time.Time `json:"c"`
	ID        string    `json:"i"`
}

// Orders is the persistence port (PostgreSQL).
type Orders interface {
	// CreateOrder inserts order + history row + OrderPlaced outbox row +
	// idempotency record atomically. domain.ErrIdempotencyInUse if the (user,key)
	// exists; domain.ErrOrderExists if the hold already has an order.
	CreateOrder(ctx context.Context, n NewOrder) error
	FindIdempotency(ctx context.Context, userID, key string) (IdempotencyRecord, bool, error)
	Get(ctx context.Context, id string) (domain.Order, error)
	ListByUser(ctx context.Context, userID string, after *OrderCursor, limit int) ([]domain.Order, error)
	History(ctx context.Context, id string) ([]domain.HistoryEntry, error)
	// Transition locks the order row (SELECT ... FOR UPDATE), asks decide what
	// to do, and if it says Changed writes the new status, a history row and the
	// outbox row in the SAME transaction. Returns the order after the call and
	// whether it changed. Timestamps are taken AFTER the lock is acquired (from the
	// database clock), so they follow the true commit order of competing changes.
	Transition(ctx context.Context, id string, traceparent string, decide func(domain.Order) (domain.Change, error)) (domain.Order, bool, error)
	// MarkPaymentEnqueued records that the payment request reached the queue.
	MarkPaymentEnqueued(ctx context.Context, id string, at time.Time) error
}

// ConsumedHold is the answer of reservations' consume endpoint.
type ConsumedHold struct {
	HoldID         string
	EventID        string
	TicketTypeID   string
	Quantity       int
	UnitPriceCents int64
	Currency       string
}

// Holds is the client port for reservations.
type Holds interface {
	// Consume must be idempotent for the same orderID. It maps reservations'
	// answers to domain.ErrHold* errors.
	Consume(ctx context.Context, holdID, orderID, userID string) (ConsumedHold, error)
}

// Payments enqueues payment requests.
type Payments interface {
	Enqueue(ctx context.Context, r domain.PaymentRequest) error
}

// OutboxRow is one event waiting to be published.
type OutboxRow struct {
	ID          string
	AggregateID string
	EventType   string
	Payload     []byte
	Traceparent string
	CreatedAt   time.Time
	Attempts    int
}

// Outbox is the relay's view of the outbox table.
type Outbox interface {
	// Claim runs fn inside a transaction holding row locks on up to batch
	// UNPUBLISHED rows (FOR UPDATE SKIP LOCKED): other relay replicas skip these
	// rows meanwhile. fn returns the ids that were published successfully and the
	// ids that failed; both are updated before commit. Returns the number of rows claimed.
	Claim(ctx context.Context, batch int, fn func(rows []OutboxRow) (published, failed []string, err error)) (int, error)
	CountUnpublished(ctx context.Context) (int64, error)
	// Cleanup deletes published rows older than retention and expired idempotency keys.
	Cleanup(ctx context.Context, retention time.Duration) (int64, error)
	// ClaimUnenqueuedPayments is the reconciler: pending orders whose payment
	// request was never recorded as enqueued.
	ClaimUnenqueuedPayments(ctx context.Context, olderThan time.Duration, batch int, fn func(reqs []domain.PaymentRequest) (enqueued []string, err error)) (int, error)
}

// Publisher sends outbox rows to the bus.
type Publisher interface {
	// Publish returns one error slot per row (nil = accepted by EventBridge).
	Publish(ctx context.Context, rows []OutboxRow) []error
}
