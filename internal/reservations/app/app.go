// Package app (reservations) contains the use cases for holds and inventory.
//
// HEXAGONAL BOUNDARY: depends only on domain and ports.
//
// Use cases and the distributed-systems problem each one solves:
//
//	CreateHold   oversell protection (atomic conditional decrement), retry safety
//	             (Idempotency-Key), abuse protection (rate limit, fail-open),
//	             timely expiry (a one-time EventBridge Scheduler schedule)
//	Consume      "exactly one order per hold" (conditional write, idempotent per orderId)
//	Release*     give tickets back exactly once, whoever triggers it (user delete,
//	             Scheduler->Lambda, DynamoDB TTL safety net, OrderCancelled event)
//	HandleEvent  idempotent processing of at-least-once events from SQS
//
// AWS services touched (through ports): DynamoDB, EventBridge Scheduler, ElastiCache.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
	"github.com/olagookundavid/gatepass-app/internal/platform/idempotency"
	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
	"github.com/olagookundavid/gatepass-app/internal/reservations/domain"
	"github.com/olagookundavid/gatepass-app/internal/reservations/ports"
)

// Retention / limits.
const (
	IdempotencyTTL    = 24 * time.Hour
	ProcessedEventTTL = 7 * 24 * time.Hour
	RateLimitWindow   = time.Minute
)

// ErrRateLimited: too many holds per minute for this user.
var ErrRateLimited = errors.New("rate limited")

// ErrIdempotencyConflict: same key, different request body.
var ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")

// ErrSchedulingFailed: the hold could not get its expiry schedule and was rolled back.
var ErrSchedulingFailed = errors.New("could not schedule hold expiry")

// Service holds dependencies.
type Service struct {
	Store     ports.Store
	Scheduler ports.Scheduler
	Limiter   ports.RateLimiter
	HoldTTL   time.Duration
	RateLimit int // holds per user per minute

	Now   func() time.Time
	NewID func() string

	created, rejected, consumed, expiredReleased, eventsProcessed, limiterErrors metric.Int64Counter
}

// New applies defaults and registers metrics.
func New(s Service) *Service {
	if s.Now == nil {
		s.Now = func() time.Time { return time.Now().UTC() }
	}
	if s.NewID == nil {
		s.NewID = func() string {
			id, err := uuid.NewV7()
			if err != nil {
				return uuid.NewString()
			}
			return id.String()
		}
	}
	if s.HoldTTL <= 0 {
		s.HoldTTL = domain.HoldTTL
	}
	if s.RateLimit <= 0 {
		s.RateLimit = 10
	}
	s.created = telemetry.Counter("reservations_holds_created_total", "Holds created")
	s.rejected = telemetry.Counter("reservations_holds_rejected_total", "Holds rejected, by reason sold_out|rate_limited|invalid")
	s.consumed = telemetry.Counter("reservations_holds_consumed_total", "Holds consumed by an order")
	s.expiredReleased = telemetry.Counter("reservations_holds_expired_released_total", "Holds released because they expired")
	s.eventsProcessed = telemetry.Counter("reservations_inventory_events_processed_total", "Events handled by the inventory consumer")
	s.limiterErrors = telemetry.Counter("reservations_rate_limiter_errors_total", "Rate limiter unavailable (request allowed)")
	return &s
}

func reason(r string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("reason", r))
}

// ---- create ---------------------------------------------------------------

// CreateHoldInput is the validated request.
type CreateHoldInput struct {
	EventID, TicketTypeID string
	Quantity              int
}

// CreateHoldResult carries the hold and whether it is a replay.
type CreateHoldResult struct {
	Hold     domain.Hold
	Replayed bool
}

// CreateHold places a hold.
func (s *Service) CreateHold(ctx context.Context, userID, idemKey string, in CreateHoldInput) (CreateHoldResult, error) {
	if err := domain.ValidateQuantity(in.Quantity); err != nil {
		s.rejected.Add(ctx, 1, reason("invalid"))
		return CreateHoldResult{}, err
	}

	// 1. Rate limit FIRST, before touching DynamoDB, so abuse costs us a cheap
	// Redis call, not table capacity. FAIL OPEN: if Redis is down we allow the
	// request. Reason: the limiter protects against abuse; the hard guarantees
	// (no oversell, one hold per idempotency key) live in DynamoDB. Failing
	// closed would let a cache outage stop all ticket sales.
	allowed, err := s.Limiter.Allow(ctx, "holds:"+userID, s.RateLimit, RateLimitWindow)
	if err != nil {
		s.limiterErrors.Add(ctx, 1)
		slog.WarnContext(ctx, "rate limiter unavailable, allowing request", "error", err.Error())
	} else if !allowed {
		s.rejected.Add(ctx, 1, reason("rate_limited"))
		return CreateHoldResult{}, ErrRateLimited
	}

	// 2. Idempotency: have we already handled this exact request?
	canonical, _ := json.Marshal(in)
	hash := idempotency.Hash(canonical)
	if res, done, err := s.replay(ctx, userID, idemKey, hash); done || err != nil {
		return res, err
	}

	// 3. Cheap pre-check (also gives "unknown ticket type" vs "sold out").
	inv, err := s.Store.GetInventory(ctx, in.EventID, in.TicketTypeID)
	if err != nil {
		return CreateHoldResult{}, err // ErrNotFound -> 404
	}
	if !domain.CanReserve(inv, in.Quantity) {
		s.rejected.Add(ctx, 1, reason("sold_out"))
		return CreateHoldResult{}, domain.ErrSoldOut
	}

	hold, err := domain.NewHold(s.NewID(), userID, inv, in.Quantity, s.Now(), s.HoldTTL)
	if err != nil {
		return CreateHoldResult{}, err
	}

	// 4. The atomic part. The pre-check above is only an optimisation: between it
	// and this call another request may take the last tickets. CreateHold repeats
	// the check INSIDE a DynamoDB transaction (condition available >= quantity),
	// which is what really prevents overselling.
	err = s.Store.CreateHold(ctx, hold, idemKey, ports.IdempotencyRecord{RequestHash: hash, Hold: hold}, IdempotencyTTL)
	switch {
	case errors.Is(err, domain.ErrSoldOut):
		s.rejected.Add(ctx, 1, reason("sold_out"))
		return CreateHoldResult{}, err
	case errors.Is(err, domain.ErrIdempotencyInUse):
		// Two identical requests raced; the other one won. Treat as a replay.
		res, done, rerr := s.replay(ctx, userID, idemKey, hash)
		if rerr != nil || done {
			return res, rerr
		}
		return CreateHoldResult{}, err
	case err != nil:
		return CreateHoldResult{}, err
	}

	// 5. Schedule the expiry. If this fails the hold would only be released by the
	// slow TTL safety net (days), so we roll the hold back instead of leaking tickets.
	if err := s.scheduleWithRetry(ctx, hold); err != nil {
		slog.ErrorContext(ctx, "scheduling hold expiry failed; rolling the hold back", "hold_id", hold.ID, "error", err.Error())
		if _, rerr := s.Store.ReleaseHold(ctx, hold.ID, []domain.HoldStatus{domain.StatusActive}, s.Now()); rerr != nil {
			slog.ErrorContext(ctx, "rollback release failed; TTL safety net will release it", "hold_id", hold.ID, "error", rerr.Error())
		}
		return CreateHoldResult{}, fmt.Errorf("%w: %w", ErrSchedulingFailed, err)
	}
	s.created.Add(ctx, 1)
	return CreateHoldResult{Hold: hold}, nil
}

func (s *Service) scheduleWithRetry(ctx context.Context, h domain.Hold) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = s.Scheduler.ScheduleExpiry(ctx, h.ID, h.ExpiresAt); err == nil {
			return nil
		}
		select {
		case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

// replay applies the idempotency decision. done=true means "return this result".
func (s *Service) replay(ctx context.Context, userID, key, hash string) (CreateHoldResult, bool, error) {
	rec, found, err := s.Store.GetIdempotency(ctx, userID, key)
	if err != nil {
		return CreateHoldResult{}, false, err
	}
	switch idempotency.Decide(found, rec.RequestHash, hash) {
	case idempotency.Conflict:
		return CreateHoldResult{}, true, ErrIdempotencyConflict
	case idempotency.Replay:
		// Show the hold's CURRENT state (it may have been released since), falling
		// back to the stored snapshot.
		h := rec.Hold
		if cur, err := s.Store.GetHold(ctx, h.ID); err == nil {
			h = cur
		}
		return CreateHoldResult{Hold: h, Replayed: true}, true, nil
	}
	return CreateHoldResult{}, false, nil
}

// ---- reads ---------------------------------------------------------------------

// GetHold returns the caller's hold. Someone else's hold is reported as not
// found (no information leak about which ids exist).
func (s *Service) GetHold(ctx context.Context, userID, holdID string) (domain.Hold, error) {
	h, err := s.Store.GetHold(ctx, holdID)
	if err != nil {
		return domain.Hold{}, err
	}
	if h.UserID != userID {
		return domain.Hold{}, domain.ErrNotFound
	}
	return h, nil
}

// ListMyHolds pages the caller's holds, newest first.
func (s *Service) ListMyHolds(ctx context.Context, userID string, after *ports.HoldCursor, limit int) ([]domain.Hold, *ports.HoldCursor, error) {
	return s.Store.ListUserHolds(ctx, userID, after, limit)
}

// Inventory lists availability for an event.
func (s *Service) Inventory(ctx context.Context, eventID string) ([]domain.Inventory, error) {
	return s.Store.ListInventory(ctx, eventID)
}

// ---- delete / consume ---------------------------------------------------------

// DeleteHold releases an active hold on behalf of its owner. Idempotent:
// deleting an already released or expired hold succeeds.
func (s *Service) DeleteHold(ctx context.Context, userID, holdID string) error {
	h, err := s.GetHold(ctx, userID, holdID)
	if err != nil {
		return err
	}
	switch h.Status {
	case domain.StatusConsumed:
		return domain.ErrHoldConsumed
	case domain.StatusReleased:
		return nil
	}
	if _, err := s.Store.ReleaseHold(ctx, holdID, []domain.HoldStatus{domain.StatusActive}, s.Now()); err != nil {
		return err
	}
	s.cancelSchedule(ctx, holdID)
	return nil
}

// cancelSchedule deletes the expiry schedule. Failure is only logged: the
// schedule would fire later and the expirer finds the hold consumed/released
// and does nothing.
func (s *Service) cancelSchedule(ctx context.Context, holdID string) {
	if err := s.Scheduler.CancelExpiry(ctx, holdID); err != nil {
		slog.WarnContext(ctx, "could not delete expiry schedule (harmless: the expirer will no-op)", "hold_id", holdID, "error", err.Error())
	}
}

// Consume is called by orders (internal endpoint).
func (s *Service) Consume(ctx context.Context, holdID, orderID, userID string) (domain.Hold, error) {
	h, err := s.Store.ConsumeHold(ctx, holdID, orderID, userID, s.Now())
	if err != nil {
		return domain.Hold{}, err
	}
	s.consumed.Add(ctx, 1)
	s.cancelSchedule(ctx, holdID)
	return h, nil
}

// ---- expiry (Lambda) -----------------------------------------------------------

// ExpireHold is the Scheduler path: release the hold if it is still active and expired.
// Returns released=true only when this call gave the tickets back.
func (s *Service) ExpireHold(ctx context.Context, holdID string) (bool, error) {
	h, err := s.Store.GetHold(ctx, holdID)
	if errors.Is(err, domain.ErrNotFound) {
		return false, nil // already cleaned up (TTL or released+deleted): nothing to do
	}
	if err != nil {
		return false, err
	}
	if h.Status != domain.StatusActive {
		return false, nil // consumed or released: nothing to do (idempotent)
	}
	if !h.Expired(s.Now()) {
		// Scheduler fired slightly early (clock skew). Return an error so the
		// Scheduler retries per its retry policy rather than dropping the schedule.
		return false, domain.ErrNotYetExpired
	}
	released, err := s.Store.ReleaseHold(ctx, holdID, []domain.HoldStatus{domain.StatusActive}, s.Now())
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if released {
		s.expiredReleased.Add(ctx, 1)
	}
	return released, nil
}

// ExpireDeletedHold is the DynamoDB-Streams safety-net path for an ACTIVE hold
// whose row was deleted by TTL before anyone released it.
func (s *Service) ExpireDeletedHold(ctx context.Context, h domain.Hold) (bool, error) {
	if h.Status != domain.StatusActive {
		return false, nil
	}
	released, err := s.Store.ReleaseDeletedHold(ctx, h, s.Now())
	if err != nil {
		return false, err
	}
	if released {
		s.expiredReleased.Add(ctx, 1)
	}
	return released, nil
}

// ---- event consumer ------------------------------------------------------------

// HandleEvent processes one parsed event from the inventory queue. It is
// idempotent: SQS delivers at-least-once, EventBridge may also duplicate.
//
// Two layers of idempotency, on purpose:
//  1. a "processed event" marker (by eventId) skips exact redeliveries cheaply;
//  2. the operations themselves are idempotent (create-if-absent; release only
//     flips consumed->released once), which also covers a DIFFERENT eventId
//     carrying the same business fact (catalog re-announces on a repeated publish).
//
// The marker is written AFTER the work: a crash in between just repeats
// idempotent work; writing it BEFORE could lose the work.
func (s *Service) HandleEvent(ctx context.Context, ev ebpub.Received) error {
	switch ev.EventType {
	case "EventPublished", "PaymentFailed", "OrderCancelled":
	default:
		slog.InfoContext(ctx, "ignoring event type", "event_type", ev.EventType)
		return nil
	}
	done, err := s.Store.EventProcessed(ctx, ev.EventID)
	if err != nil {
		return err
	}
	if done {
		s.eventsProcessed.Add(ctx, 1, metric.WithAttributes(attribute.String("type", ev.EventType), attribute.String("result", "duplicate")))
		return nil
	}

	switch ev.EventType {
	case "EventPublished":
		err = s.onEventPublished(ctx, ev)
	default:
		err = s.onReleaseEvent(ctx, ev)
	}
	if err != nil {
		return err
	}
	if err := s.Store.MarkEventProcessed(ctx, ev.EventID, ProcessedEventTTL); err != nil {
		return err
	}
	s.eventsProcessed.Add(ctx, 1, metric.WithAttributes(attribute.String("type", ev.EventType), attribute.String("result", "ok")))
	return nil
}

func (s *Service) onEventPublished(ctx context.Context, ev ebpub.Received) error {
	var d domain.EventPublishedData
	if err := json.Unmarshal(ev.Data, &d); err != nil {
		return fmt.Errorf("bad EventPublished data: %w", err)
	}
	if d.EventID == "" {
		return errors.New("bad EventPublished data: eventId missing")
	}
	for _, t := range d.TicketTypes {
		if t.ID == "" || t.Capacity < 1 || t.PriceCents < 0 || t.Currency == "" {
			return fmt.Errorf("bad ticket type in EventPublished %s", ev.EventID)
		}
		created, err := s.Store.CreateInventoryIfAbsent(ctx, domain.Inventory{
			EventID: d.EventID, TicketTypeID: t.ID, Capacity: t.Capacity, Available: t.Capacity,
			PriceCents: t.PriceCents, Currency: t.Currency, Version: 1})
		if err != nil {
			return err
		}
		slog.InfoContext(ctx, "inventory", "event_id", d.EventID, "ticket_type_id", t.ID, "created", created)
	}
	return nil
}

func (s *Service) onReleaseEvent(ctx context.Context, ev ebpub.Received) error {
	var d domain.OrderEventData
	if err := json.Unmarshal(ev.Data, &d); err != nil {
		return fmt.Errorf("bad %s data: %w", ev.EventType, err)
	}
	if d.HoldID == "" {
		return fmt.Errorf("bad %s data: holdId missing", ev.EventType)
	}
	released, err := s.Store.ReleaseHold(ctx, d.HoldID, []domain.HoldStatus{domain.StatusConsumed}, s.Now())
	switch {
	case errors.Is(err, domain.ErrNotFound):
		slog.WarnContext(ctx, "hold not found for release event (already cleaned up?)", "hold_id", d.HoldID, "event_type", ev.EventType)
		return nil
	case err != nil:
		return err
	}
	slog.InfoContext(ctx, "released hold after order event", "hold_id", d.HoldID, "event_type", ev.EventType, "released", released)
	return nil
}
