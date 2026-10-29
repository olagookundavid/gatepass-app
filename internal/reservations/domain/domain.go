// Package domain (reservations) holds the rules for inventory counters and
// ticket holds.
//
// HEXAGONAL BOUNDARY: no AWS SDK, no HTTP, no Redis in here. The functions are
// pure so the arithmetic that must never go wrong (available never below 0,
// never above capacity, a hold is consumed at most once) is unit tested without
// any infrastructure.
//
// The model in one paragraph: for each (event, ticket type) there is an
// INVENTORY counter `available`. A user "holds" `quantity` tickets for 10
// minutes: available goes down and a HOLD record is written. The hold ends in
// exactly one of three ways: it is CONSUMED by an order, RELEASED (cancelled by
// the user, expired, or payment failed/order cancelled) giving the tickets back, or
// it stays active until it expires (treated as released by the expirer).
//
// AWS services touched: none.
package domain

import (
	"errors"
	"time"
)

// Business constants.
const (
	MaxQuantity = 8
	HoldTTL     = 10 * time.Minute
)

// HoldStatus is the lifecycle state stored on the hold.
type HoldStatus string

const (
	StatusActive   HoldStatus = "active"
	StatusConsumed HoldStatus = "consumed"
	StatusReleased HoldStatus = "released"
	// StatusExpired is NEVER stored. It is what readers report for an active hold
	// whose expiresAt has passed but which the expirer has not processed yet.
	StatusExpired HoldStatus = "expired"
)

// Errors the layers share.
var (
	ErrNotFound         = errors.New("not found")
	ErrSoldOut          = errors.New("not enough tickets available")
	ErrWrongOwner       = errors.New("hold belongs to another user")
	ErrHoldExpired      = errors.New("hold expired")
	ErrHoldReleased     = errors.New("hold was released")
	ErrAlreadyConsumed  = errors.New("hold already consumed by another order")
	ErrNotYetExpired    = errors.New("hold has not expired yet")
	ErrIdempotencyInUse = errors.New("idempotency key already recorded")
	ErrInvalidQuantity  = errors.New("quantity must be between 1 and 8")
	ErrContention       = errors.New("too many concurrent updates; retry shortly")
	ErrHoldConsumed     = errors.New("hold is already consumed by an order; cancel the order instead")
)

// Inventory is the counter for one ticket type of one event.
// PriceCents/Currency are a SNAPSHOT copied from the EventPublished event when
// the inventory was created: later catalog edits never change the price of
// inventory that already exists (and orders compute amounts from this snapshot,
// so reservations is the single source of truth for what a hold costs).
type Inventory struct {
	EventID      string `json:"eventId"`
	TicketTypeID string `json:"ticketTypeId"`
	Capacity     int    `json:"capacity"`
	Available    int    `json:"available"`
	PriceCents   int64  `json:"priceCents"`
	Currency     string `json:"currency"`
	Version      int    `json:"-"`
}

// Hold is a time-limited claim on tickets.
type Hold struct {
	ID             string     `json:"holdId"`
	UserID         string     `json:"-"` // never returned to other users; owner-only API
	EventID        string     `json:"eventId"`
	TicketTypeID   string     `json:"ticketTypeId"`
	Quantity       int        `json:"quantity"`
	UnitPriceCents int64      `json:"unitPriceCents"`
	Currency       string     `json:"currency"`
	Status         HoldStatus `json:"status"`
	ExpiresAt      time.Time  `json:"expiresAt"`
	CreatedAt      time.Time  `json:"createdAt"`
	OrderID        string     `json:"orderId,omitempty"`
}

// ValidateQuantity enforces 1..8.
func ValidateQuantity(q int) error {
	if q < 1 || q > MaxQuantity {
		return ErrInvalidQuantity
	}
	return nil
}

// NewHold builds an active hold expiring HoldTTL after now. The price is taken
// from the inventory snapshot.
func NewHold(id, userID string, inv Inventory, quantity int, now time.Time, ttl time.Duration) (Hold, error) {
	if err := ValidateQuantity(quantity); err != nil {
		return Hold{}, err
	}
	if ttl <= 0 {
		ttl = HoldTTL
	}
	return Hold{ID: id, UserID: userID, EventID: inv.EventID, TicketTypeID: inv.TicketTypeID, Quantity: quantity,
		UnitPriceCents: inv.PriceCents, Currency: inv.Currency, Status: StatusActive,
		CreatedAt: now.UTC().Truncate(time.Second), ExpiresAt: now.UTC().Truncate(time.Second).Add(ttl)}, nil
}

// Expired reports whether the hold's time is up (regardless of stored status).
func (h Hold) Expired(now time.Time) bool { return !now.Before(h.ExpiresAt) }

// EffectiveStatus is the status readers should show: an active hold past its
// expiry reads as "expired" even though the row (and the stored status) still
// say active until the expirer runs. DynamoDB TTL deletes "typically within a
// few days", so the stored row cannot be trusted as a timer.
func (h Hold) EffectiveStatus(now time.Time) HoldStatus {
	if h.Status == StatusActive && h.Expired(now) {
		return StatusExpired
	}
	return h.Status
}

// ConsumeOutcome says what ConsumeDecision found.
type ConsumeOutcome int

const (
	// ConsumeNow: the hold is active, unexpired and owned by the caller.
	ConsumeNow ConsumeOutcome = iota
	// AlreadyConsumedBySame: a repeat of a successful call (same orderId).
	AlreadyConsumedBySame
)

// ConsumeDecision applies the consume rules to a hold read from storage.
// (The repository additionally enforces them atomically with a conditional
// write, because two requests can read the same state at the same time.)
func ConsumeDecision(h Hold, orderID, userID string, now time.Time) (ConsumeOutcome, error) {
	if h.UserID != userID {
		return 0, ErrWrongOwner
	}
	switch h.Status {
	case StatusConsumed:
		if h.OrderID == orderID {
			return AlreadyConsumedBySame, nil
		}
		return 0, ErrAlreadyConsumed
	case StatusReleased:
		return 0, ErrHoldReleased
	}
	if h.Expired(now) {
		return 0, ErrHoldExpired
	}
	return ConsumeNow, nil
}

// CanReserve reports whether quantity tickets can be taken from the counter.
func CanReserve(inv Inventory, quantity int) bool {
	return quantity >= 1 && inv.Available >= quantity
}

// AfterReserve returns the counter after taking quantity tickets (never below 0).
func AfterReserve(available, quantity int) (int, error) {
	if quantity > available {
		return available, ErrSoldOut
	}
	return available - quantity, nil
}

// AfterRelease returns the counter after giving tickets back, never above capacity.
// The second result is false when the increment would exceed capacity (a bug
// somewhere else: the caller logs it loudly instead of corrupting the counter).
func AfterRelease(available, quantity, capacity int) (int, bool) {
	n := available + quantity
	if n > capacity {
		return available, false
	}
	return n, true
}

// EventPublishedData mirrors the `data` of the EventPublished event (catalog).
type EventPublishedData struct {
	EventID     string `json:"eventId"`
	Title       string `json:"title"`
	VenueID     string `json:"venueId"`
	TicketTypes []struct {
		ID         string `json:"id"`
		Name       string `json:"name"`
		PriceCents int64  `json:"priceCents"`
		Currency   string `json:"currency"`
		Capacity   int    `json:"capacity"`
	} `json:"ticketTypes"`
}

// OrderEventData mirrors the `data` of OrderPaid/PaymentFailed/OrderCancelled (orders).
type OrderEventData struct {
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
