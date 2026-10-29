// Package domain (catalog) holds the business entities and rules for venues,
// events and ticket types.
//
// HEXAGONAL BOUNDARY: this package is the inside. It imports no AWS SDK, no SQL,
// no Redis and no net/http. Everything it needs from the outside world arrives
// through plain values or the interfaces in package ports. That is what makes
// the rules below unit-testable in microseconds.
//
// JSON tags exist because the same structs are the API representation and the
// cache payload; tags are metadata, not a dependency.
//
// AWS services touched: none.
package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

// Status of an event.
type Status string

const (
	StatusDraft     Status = "draft"
	StatusPublished Status = "published"
	StatusCancelled Status = "cancelled"
)

// Venue is a place events happen.
type Venue struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	City      string    `json:"city"`
	Address   string    `json:"address"`
	Capacity  int       `json:"capacity"`
	CreatedAt time.Time `json:"createdAt"`
}

// TicketType is a sellable category of ticket (e.g. "General", "VIP").
type TicketType struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	PriceCents int64  `json:"priceCents"`
	Currency   string `json:"currency"`
	Capacity   int    `json:"capacity"`
}

// Event is the aggregate root: ticket types only exist inside an event.
type Event struct {
	ID          string       `json:"id"`
	VenueID     string       `json:"venueId"`
	Title       string       `json:"title"`
	Description string       `json:"description"`
	StartsAt    time.Time    `json:"startsAt"`
	EndsAt      time.Time    `json:"endsAt"`
	Status      Status       `json:"status"`
	ImageURL    string       `json:"imageUrl,omitempty"`
	CreatedBy   string       `json:"createdBy,omitempty"`
	CreatedAt   time.Time    `json:"createdAt"`
	UpdatedAt   time.Time    `json:"updatedAt"`
	Version     int          `json:"version"`
	TicketTypes []TicketType `json:"ticketTypes"`
}

// Sentinel errors the app and adapters share.
var (
	ErrNotFound        = errors.New("not found")
	ErrVersionConflict = errors.New("version conflict")
	ErrNotDraft        = errors.New("event is not a draft")
	ErrCancelled       = errors.New("event is cancelled")
)

// FieldError is one invalid field.
type FieldError struct{ Field, Reason string }

// ValidationError aggregates field errors.
type ValidationError struct{ Fields []FieldError }

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Reason
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

type validator struct{ fields []FieldError }

func (v *validator) add(field, reason string) { v.fields = append(v.fields, FieldError{field, reason}) }
func (v *validator) err() error {
	if len(v.fields) == 0 {
		return nil
	}
	return &ValidationError{Fields: v.fields}
}

// ValidateVenue checks venue input.
func ValidateVenue(v Venue) error {
	var val validator
	if n := len([]rune(strings.TrimSpace(v.Name))); n < 1 || n > 200 {
		val.add("name", "must be 1-200 characters")
	}
	if n := len([]rune(strings.TrimSpace(v.City))); n < 1 || n > 100 {
		val.add("city", "must be 1-100 characters")
	}
	if n := len([]rune(strings.TrimSpace(v.Address))); n < 1 || n > 300 {
		val.add("address", "must be 1-300 characters")
	}
	if v.Capacity < 1 || v.Capacity > 1_000_000 {
		val.add("capacity", "must be between 1 and 1000000")
	}
	return val.err()
}

// ValidateEventFields checks the fields common to create and update.
// now is injected so tests are deterministic.
func ValidateEventFields(e Event, now time.Time, requireFuture bool) error {
	var val validator
	if n := len([]rune(strings.TrimSpace(e.Title))); n < 1 || n > 200 {
		val.add("title", "must be 1-200 characters")
	}
	if len([]rune(e.Description)) > 5000 {
		val.add("description", "must be at most 5000 characters")
	}
	if e.StartsAt.IsZero() {
		val.add("startsAt", "is required")
	} else if requireFuture && !e.StartsAt.After(now) {
		val.add("startsAt", "must be in the future")
	}
	if e.EndsAt.IsZero() {
		val.add("endsAt", "is required")
	} else if !e.StartsAt.IsZero() && !e.EndsAt.After(e.StartsAt) {
		val.add("endsAt", "must be after startsAt")
	}
	if e.VenueID == "" {
		val.add("venueId", "is required")
	}
	return val.err()
}

// ValidateTicketTypes checks the ticket type list of a draft.
func ValidateTicketTypes(tts []TicketType) error {
	var val validator
	if len(tts) < 1 || len(tts) > 10 {
		val.add("ticketTypes", "must contain 1 to 10 entries")
	}
	seen := map[string]bool{}
	for i, t := range tts {
		p := fmt.Sprintf("ticketTypes[%d]", i)
		name := strings.ToLower(strings.TrimSpace(t.Name))
		if n := len([]rune(name)); n < 1 || n > 100 {
			val.add(p+".name", "must be 1-100 characters")
		}
		if seen[name] {
			val.add(p+".name", "must be unique within the event")
		}
		seen[name] = true
		if t.PriceCents < 0 || t.PriceCents > 100_000_00 {
			val.add(p+".priceCents", "must be between 0 and 10000000")
		}
		if !validCurrency(t.Currency) {
			val.add(p+".currency", "must be a 3-letter upper-case ISO 4217 code")
		}
		if t.Capacity < 1 || t.Capacity > 1_000_000 {
			val.add(p+".capacity", "must be between 1 and 1000000")
		}
	}
	return val.err()
}

func validCurrency(c string) bool {
	if len(c) != 3 {
		return false
	}
	for _, r := range c {
		if !unicode.IsUpper(r) || r > 'Z' {
			return false
		}
	}
	return true
}

// FitsVenue enforces: the sum of ticket-type capacities cannot exceed the
// venue's capacity (you cannot sell more tickets than seats).
func FitsVenue(tts []TicketType, v Venue) error {
	total := 0
	for _, t := range tts {
		total += t.Capacity
	}
	if total > v.Capacity {
		return &ValidationError{Fields: []FieldError{{"ticketTypes", fmt.Sprintf("total capacity %d exceeds venue capacity %d", total, v.Capacity)}}}
	}
	return nil
}

// Publish moves a draft to published. It returns (changed=false) when the event
// is already published: publishing is IDEMPOTENT. A cancelled event cannot be
// published.
func (e *Event) Publish(now time.Time) (changed bool, err error) {
	switch e.Status {
	case StatusPublished:
		return false, nil
	case StatusCancelled:
		return false, ErrCancelled
	}
	if len(e.TicketTypes) == 0 {
		return false, &ValidationError{Fields: []FieldError{{"ticketTypes", "at least one ticket type is required to publish"}}}
	}
	if !e.StartsAt.After(now) {
		return false, &ValidationError{Fields: []FieldError{{"startsAt", "event has already started"}}}
	}
	e.Status = StatusPublished
	return true, nil
}

// Visible reports whether a caller may see the event. Drafts and cancelled
// events are visible to admins only.
func (e *Event) Visible(isAdmin bool) bool {
	return isAdmin || e.Status == StatusPublished
}

// EventPublishedData is the `data` of the EventPublished domain event (PLAN section 6).
type EventPublishedData struct {
	EventID     string            `json:"eventId"`
	Title       string            `json:"title"`
	StartsAt    time.Time         `json:"startsAt"`
	VenueID     string            `json:"venueId"`
	TicketTypes []TicketTypeEvent `json:"ticketTypes"`
}

// TicketTypeEvent is one ticket type inside EventPublishedData.
type TicketTypeEvent struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	PriceCents int64  `json:"priceCents"`
	Currency   string `json:"currency"`
	Capacity   int    `json:"capacity"`
}

// PublishedData builds the event payload from the aggregate. Ids only, no
// personal data.
func (e *Event) PublishedData() EventPublishedData {
	d := EventPublishedData{EventID: e.ID, Title: e.Title, StartsAt: e.StartsAt, VenueID: e.VenueID}
	for _, t := range e.TicketTypes {
		d.TicketTypes = append(d.TicketTypes, TicketTypeEvent(t))
	}
	return d
}

// ImageContentTypes lists the accepted upload types and their file extensions.
var ImageContentTypes = map[string]string{
	"image/jpeg": "jpg",
	"image/png":  "png",
	"image/webp": "webp",
}

// MaxImageBytes is the largest accepted image (5 MiB).
const MaxImageBytes = 5 << 20
