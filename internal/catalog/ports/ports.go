// Package ports (catalog) declares the interfaces the application layer needs
// from the outside world.
//
// HEXAGONAL BOUNDARY: ports are owned by the application (inside); adapters
// (outside) implement them. The arrow of dependency points inward: adapters
// import ports and domain, never the other way round. Swapping Postgres for
// another store, or the real cache for an in-memory fake in tests, changes no
// application code.
package ports

import (
	"context"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/catalog/domain"
)

// EventFilter narrows an event listing. Only PUBLISHED events are listed.
type EventFilter struct {
	City string // case-insensitive exact match on the venue city; "" = any
	From *time.Time
	To   *time.Time
}

// EventCursor is the keyset position: events are ordered by (starts_at, id).
type EventCursor struct {
	StartsAt time.Time `json:"s"`
	ID       string    `json:"i"`
}

// VenueCursor: venues are ordered by id (UUIDv7, i.e. creation order).
type VenueCursor struct {
	ID string `json:"i"`
}

// EventRepository persists events with their ticket types (system of record: Postgres).
type EventRepository interface {
	// Create inserts the event and its ticket types in one transaction.
	Create(ctx context.Context, e domain.Event) error
	// Get returns domain.ErrNotFound when absent.
	Get(ctx context.Context, id string) (domain.Event, error)
	// Save writes the event (and replaces its ticket types when replaceTicketTypes)
	// if the stored version equals expectedVersion; it increments the version.
	// Returns domain.ErrVersionConflict or domain.ErrNotFound otherwise.
	Save(ctx context.Context, e domain.Event, expectedVersion int, replaceTicketTypes bool) (domain.Event, error)
	// ListPublished returns up to limit+1 events after cursor (the extra row tells
	// the caller whether another page exists).
	ListPublished(ctx context.Context, f EventFilter, after *EventCursor, limit int) ([]domain.Event, error)
}

// VenueRepository persists venues.
type VenueRepository interface {
	Create(ctx context.Context, v domain.Venue) error
	Get(ctx context.Context, id string) (domain.Venue, error)
	List(ctx context.Context, after *VenueCursor, limit int) ([]domain.Venue, error)
}

// Cache is an OPTIONAL accelerator. Every method may fail; the application
// treats any error as a miss and continues with the repository.
type Cache interface {
	GetEvent(ctx context.Context, id string) (*domain.Event, bool, error)
	SetEvent(ctx context.Context, e domain.Event, ttl time.Duration) error
	DeleteEvent(ctx context.Context, id string) error
	// List pages are cached as opaque bytes under a key built by the app.
	GetList(ctx context.Context, key string) ([]byte, bool, error)
	SetList(ctx context.Context, key string, payload []byte, ttl time.Duration) error
	// ListGeneration returns the current list-cache generation; InvalidateLists
	// bumps it, making every cached list page unreachable at once (O(1)).
	ListGeneration(ctx context.Context) (int64, error)
	InvalidateLists(ctx context.Context) error
}

// EventAnnouncer publishes the EventPublished domain event.
type EventAnnouncer interface {
	AnnouncePublished(ctx context.Context, d domain.EventPublishedData) error
}

// ImageSigner creates time-limited upload URLs.
type ImageSigner interface {
	PresignPut(ctx context.Context, objectKey, contentType string, contentLength int64, ttl time.Duration) (url string, err error)
}
