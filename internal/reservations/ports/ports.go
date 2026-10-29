// Package ports (reservations) declares what the application needs from storage,
// the scheduler and the rate limiter.
//
// HEXAGONAL BOUNDARY: owned by the application; implemented by adapters
// (DynamoDB, EventBridge Scheduler, Redis). Method names state the BUSINESS
// operation; each documents the atomicity it promises, because that promise is
// what keeps tickets from being oversold.
package ports

import (
	"context"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/reservations/domain"
)

// IdempotencyRecord remembers the outcome of a POST /holds for a (user, key).
type IdempotencyRecord struct {
	RequestHash string
	Hold        domain.Hold // snapshot returned on replay
}

// HoldCursor is the DynamoDB LastEvaluatedKey for GET /me/holds.
type HoldCursor struct {
	PK     string `json:"p"`
	SK     string `json:"s"`
	GSI1PK string `json:"g"`
	GSI1SK string `json:"k"`
}

// Store is the persistence port (implemented on DynamoDB).
type Store interface {
	// ---- inventory ----
	GetInventory(ctx context.Context, eventID, ticketTypeID string) (domain.Inventory, error) // ErrNotFound if absent
	ListInventory(ctx context.Context, eventID string) ([]domain.Inventory, error)
	// CreateInventoryIfAbsent writes the counter only if it does not exist; it never
	// overwrites (so a re-delivered or re-announced event cannot reset `available`).
	CreateInventoryIfAbsent(ctx context.Context, inv domain.Inventory) (created bool, err error)

	// ---- holds ----
	// CreateHold ATOMICALLY (all or nothing): decrements available if
	// available >= quantity, writes the hold if it does not exist, and records the
	// idempotency key. Returns domain.ErrSoldOut, or domain.ErrIdempotencyInUse
	// when the (user,key) pair already exists.
	CreateHold(ctx context.Context, h domain.Hold, idemKey string, rec IdempotencyRecord, idemTTL time.Duration) error
	GetHold(ctx context.Context, holdID string) (domain.Hold, error) // ErrNotFound if absent
	GetIdempotency(ctx context.Context, userID, key string) (IdempotencyRecord, bool, error)
	ListUserHolds(ctx context.Context, userID string, after *HoldCursor, limit int) ([]domain.Hold, *HoldCursor, error)
	// ConsumeHold moves active -> consumed iff active, unexpired and owned by userID;
	// idempotent for the same orderID. Atomic (single conditional write).
	ConsumeHold(ctx context.Context, holdID, orderID, userID string, now time.Time) (domain.Hold, error)
	// ReleaseHold moves the hold from one of `from` to released AND returns the
	// tickets to the counter, ATOMICALLY. released=false means it was already not in
	// `from` (someone else released/consumed it): a harmless no-op. ErrNotFound if
	// the hold row no longer exists.
	ReleaseHold(ctx context.Context, holdID string, from []domain.HoldStatus, now time.Time) (released bool, err error)
	// ReleaseDeletedHold returns tickets for a hold row that DynamoDB TTL already
	// deleted (only its last image is known), using a marker item for idempotency.
	ReleaseDeletedHold(ctx context.Context, h domain.Hold, now time.Time) (released bool, err error)

	// ---- consumer bookkeeping ----
	EventProcessed(ctx context.Context, eventID string) (bool, error)
	MarkEventProcessed(ctx context.Context, eventID string, ttl time.Duration) error
}

// Scheduler creates and deletes the one-time "expire this hold" schedules.
type Scheduler interface {
	ScheduleExpiry(ctx context.Context, holdID string, at time.Time) error
	// CancelExpiry deletes the schedule; a missing schedule is NOT an error.
	CancelExpiry(ctx context.Context, holdID string) error
}

// RateLimiter decides whether a user may create another hold now.
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error)
}
