// Package app (catalog) contains the use cases: the sequence of steps for
// "create an event", "publish an event", "read an event with caching" and so on.
//
// HEXAGONAL BOUNDARY: this package depends only on domain and ports (+ the
// metrics helpers). It never sees SQL, Redis commands, S3 or EventBridge types.
//
// Distributed-systems decisions made here (each explained where it happens):
//   - cache-aside with a cache that may fail without failing the request;
//   - "publish" is idempotent AND re-announces, which is the recovery path for
//     the lack of an outbox in this service (contrast: orders uses an outbox);
//   - cache invalidation is best-effort, bounded by TTLs.
//
// AWS services touched (through ports): RDS Postgres, ElastiCache, S3, EventBridge.
package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/olagookundavid/gatepass-app/internal/catalog/domain"
	"github.com/olagookundavid/gatepass-app/internal/catalog/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

// Cache TTLs from the spec.
const (
	EventTTL = 5 * time.Minute
	ListTTL  = 60 * time.Second
	// UploadURLTTL is how long the presigned PUT URL works.
	UploadURLTTL = 5 * time.Minute
)

// ErrAnnounceFailed means the event was published in the database but the
// EventPublished domain event could not be sent. Clients retry the (idempotent)
// publish call, which announces again.
var ErrAnnounceFailed = errors.New("event published but announcement failed")

// ErrImageType / ErrImageSize are validation failures of the upload request.
var ErrImageType = &domain.ValidationError{Fields: []domain.FieldError{{Field: "contentType", Reason: "must be image/jpeg, image/png or image/webp"}}}

// Service holds the dependencies. Construct with New.
type Service struct {
	Events    ports.EventRepository
	Venues    ports.VenueRepository
	Cache     ports.Cache
	Announcer ports.EventAnnouncer
	Images    ports.ImageSigner
	CDNBase   string // e.g. https://cdn.example.com (no trailing slash)

	Now   func() time.Time
	NewID func() string

	cacheHits, cacheMisses, cacheErrors, published, announceFailures metric.Int64Counter
}

// New wires the service and its metrics.
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
	s.CDNBase = strings.TrimRight(s.CDNBase, "/")
	s.cacheHits = telemetry.Counter("catalog_cache_hits_total", "Cache hits by key type")
	s.cacheMisses = telemetry.Counter("catalog_cache_misses_total", "Cache misses by key type")
	s.cacheErrors = telemetry.Counter("catalog_cache_errors_total", "Cache failures (request continued without cache)")
	s.published = telemetry.Counter("catalog_events_published_total", "Events moved to published")
	s.announceFailures = telemetry.Counter("catalog_event_announce_failures_total", "EventPublished could not be sent")
	return &s
}

func keyType(t string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("key_type", t))
}

// ---- venues ---------------------------------------------------------------

// CreateVenueInput is the admin request.
type CreateVenueInput struct {
	Name, City, Address string
	Capacity            int
}

// CreateVenue validates and stores a venue.
func (s *Service) CreateVenue(ctx context.Context, in CreateVenueInput) (domain.Venue, error) {
	v := domain.Venue{ID: s.NewID(), Name: strings.TrimSpace(in.Name), City: strings.TrimSpace(in.City),
		Address: strings.TrimSpace(in.Address), Capacity: in.Capacity, CreatedAt: s.Now()}
	if err := domain.ValidateVenue(v); err != nil {
		return domain.Venue{}, err
	}
	return v, s.Venues.Create(ctx, v)
}

// VenuePage is one page of venues.
type VenuePage struct {
	Items []domain.Venue
	Next  *ports.VenueCursor
}

// ListVenues pages through venues (not cached: rarely read, cheap query).
func (s *Service) ListVenues(ctx context.Context, after *ports.VenueCursor, limit int) (VenuePage, error) {
	rows, err := s.Venues.List(ctx, after, limit+1)
	if err != nil {
		return VenuePage{}, err
	}
	p := VenuePage{Items: rows}
	if len(rows) > limit {
		p.Items = rows[:limit]
		p.Next = &ports.VenueCursor{ID: rows[limit-1].ID}
	}
	return p, nil
}

// ---- events: reads (cache-aside) -------------------------------------------

// EventPage is one page of events.
type EventPage struct {
	Items []domain.Event     `json:"items"`
	Next  *ports.EventCursor `json:"next,omitempty"`
}

// ListEvents returns published events. The FIRST page (after == nil) is cached
// for 60 s per filter/limit.
//
// Cache-aside, step by step:
//  1. look in the cache; a hit returns immediately;
//  2. on a miss (or any cache error) read the database;
//  3. try to store the result; ignore failure.
//
// Why only the first page: it is by far the most requested (the home page);
// deeper pages have many key variations and a low hit rate.
//
// Why a "generation" in the key: when an event is published or edited, the
// cached lists are stale. Deleting keys by pattern needs SCAN (slow, and not
// atomic); instead the key contains a generation number and invalidation just
// increments it, so old keys are never read again and expire by TTL.
func (s *Service) ListEvents(ctx context.Context, f ports.EventFilter, after *ports.EventCursor, limit int) (EventPage, error) {
	var key string
	if after == nil {
		gen, err := s.Cache.ListGeneration(ctx)
		if err != nil {
			s.cacheErr(ctx, "list", err)
		} else {
			key = listKey(gen, f, limit)
			if b, hit, err := s.Cache.GetList(ctx, key); err != nil {
				s.cacheErr(ctx, "list", err)
			} else if hit {
				var page EventPage
				if json.Unmarshal(b, &page) == nil {
					s.cacheHits.Add(ctx, 1, keyType("list"))
					return page, nil
				}
			}
			s.cacheMisses.Add(ctx, 1, keyType("list"))
		}
	}

	rows, err := s.Events.ListPublished(ctx, f, after, limit+1)
	if err != nil {
		return EventPage{}, err
	}
	page := EventPage{Items: rows}
	if len(rows) > limit {
		page.Items = rows[:limit]
		last := rows[limit-1]
		page.Next = &ports.EventCursor{StartsAt: last.StartsAt, ID: last.ID}
	}
	if key != "" {
		if b, err := json.Marshal(page); err == nil {
			if err := s.Cache.SetList(ctx, key, b, ListTTL); err != nil {
				s.cacheErr(ctx, "list", err)
			}
		}
	}
	return page, nil
}

func listKey(gen int64, f ports.EventFilter, limit int) string {
	parts := []string{strings.ToLower(f.City)}
	for _, t := range []*time.Time{f.From, f.To} {
		if t == nil {
			parts = append(parts, "")
		} else {
			parts = append(parts, t.UTC().Format(time.RFC3339))
		}
	}
	// Hash the filter so user-controlled text never becomes part of a Redis key verbatim.
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return fmt.Sprintf("catalog:events:list:g%d:l%d:%s", gen, limit, hex.EncodeToString(sum[:8]))
}

// GetEvent returns one event. Non-admins get ErrNotFound for drafts (a 404
// rather than 403 so draft ids cannot be probed).
func (s *Service) GetEvent(ctx context.Context, id string, isAdmin bool) (domain.Event, error) {
	if _, err := uuid.Parse(id); err != nil {
		return domain.Event{}, domain.ErrNotFound
	}
	e, err := s.getCached(ctx, id)
	if err != nil {
		return domain.Event{}, err
	}
	if !e.Visible(isAdmin) {
		return domain.Event{}, domain.ErrNotFound
	}
	return e, nil
}

func (s *Service) getCached(ctx context.Context, id string) (domain.Event, error) {
	if e, hit, err := s.Cache.GetEvent(ctx, id); err != nil {
		s.cacheErr(ctx, "event", err)
	} else if hit {
		s.cacheHits.Add(ctx, 1, keyType("event"))
		return *e, nil
	} else {
		s.cacheMisses.Add(ctx, 1, keyType("event"))
	}
	e, err := s.Events.Get(ctx, id)
	if err != nil {
		return domain.Event{}, err
	}
	if err := s.Cache.SetEvent(ctx, e, EventTTL); err != nil {
		s.cacheErr(ctx, "event", err)
	}
	return e, nil
}

// cacheErr records a cache failure. A cache problem must never fail a request:
// the cache is an optimisation, Postgres is the truth.
func (s *Service) cacheErr(ctx context.Context, kt string, err error) {
	s.cacheErrors.Add(ctx, 1, keyType(kt))
	slog.WarnContext(ctx, "cache unavailable, falling back to database", "key_type", kt, "error", err.Error())
}

// invalidate drops cached data after a write. Best effort: if it fails the stale
// copy lives at most EventTTL (5 min) / ListTTL (60 s).
func (s *Service) invalidate(ctx context.Context, eventID string) {
	if err := s.Cache.DeleteEvent(ctx, eventID); err != nil {
		s.cacheErr(ctx, "event", err)
	}
	if err := s.Cache.InvalidateLists(ctx); err != nil {
		s.cacheErr(ctx, "list", err)
	}
}

// ---- events: writes -----------------------------------------------------------

// TicketTypeInput is a requested ticket type.
type TicketTypeInput struct {
	Name       string
	PriceCents int64
	Currency   string
	Capacity   int
}

func (s *Service) buildTicketTypes(in []TicketTypeInput) []domain.TicketType {
	out := make([]domain.TicketType, len(in))
	for i, t := range in {
		out[i] = domain.TicketType{ID: s.NewID(), Name: strings.TrimSpace(t.Name), PriceCents: t.PriceCents,
			Currency: t.Currency, Capacity: t.Capacity}
	}
	return out
}

// CreateEventInput is the admin request.
type CreateEventInput struct {
	VenueID, Title, Description string
	StartsAt, EndsAt            time.Time
	TicketTypes                 []TicketTypeInput
}

// CreateEvent creates a DRAFT event with its ticket types.
func (s *Service) CreateEvent(ctx context.Context, in CreateEventInput, actor string) (domain.Event, error) {
	now := s.Now()
	e := domain.Event{ID: s.NewID(), VenueID: in.VenueID, Title: strings.TrimSpace(in.Title), Description: in.Description,
		StartsAt: in.StartsAt.UTC(), EndsAt: in.EndsAt.UTC(), Status: domain.StatusDraft, CreatedBy: actor,
		CreatedAt: now, UpdatedAt: now, Version: 1, TicketTypes: s.buildTicketTypes(in.TicketTypes)}
	if err := validateAll(e, now, true); err != nil {
		return domain.Event{}, err
	}
	if err := s.checkVenue(ctx, e); err != nil {
		return domain.Event{}, err
	}
	if err := s.Events.Create(ctx, e); err != nil {
		return domain.Event{}, err
	}
	s.invalidate(ctx, e.ID) // drafts are not listed, but keep one rule: every write invalidates
	return e, nil
}

func validateAll(e domain.Event, now time.Time, requireFuture bool) error {
	var fields []domain.FieldError
	for _, err := range []error{domain.ValidateEventFields(e, now, requireFuture), domain.ValidateTicketTypes(e.TicketTypes)} {
		var ve *domain.ValidationError
		if errors.As(err, &ve) {
			fields = append(fields, ve.Fields...)
		}
	}
	if len(fields) > 0 {
		return &domain.ValidationError{Fields: fields}
	}
	return nil
}

// checkVenue verifies the venue exists and fits the ticket capacity.
func (s *Service) checkVenue(ctx context.Context, e domain.Event) error {
	v, err := s.Venues.Get(ctx, e.VenueID)
	if errors.Is(err, domain.ErrNotFound) {
		return &domain.ValidationError{Fields: []domain.FieldError{{Field: "venueId", Reason: "venue does not exist"}}}
	}
	if err != nil {
		return err
	}
	return domain.FitsVenue(e.TicketTypes, v)
}

// UpdateEventInput holds the optional fields of a PATCH.
type UpdateEventInput struct {
	VenueID     *string
	Title       *string
	Description *string
	StartsAt    *time.Time
	EndsAt      *time.Time
	ImageURL    *string
	TicketTypes *[]TicketTypeInput
}

// UpdateEvent applies a partial update with optimistic locking.
//
// Optimistic locking in plain words: the client read the event at version N and
// sends `If-Match: N`. We write only if the row is STILL at version N. If an
// admin in another tab saved in between, the row is at N+1, our write matches
// zero rows and the client gets 412 instead of silently overwriting the other
// admin's change ("lost update").
//
// Price note: ticket types are editable only in draft. Inventory in the
// reservations service keeps a price SNAPSHOT taken when the event was
// published, so even other edits never change what existing inventory costs.
func (s *Service) UpdateEvent(ctx context.Context, id string, in UpdateEventInput, expectedVersion int) (domain.Event, error) {
	cur, err := s.Events.Get(ctx, id) // always read the database, never the cache, before writing
	if err != nil {
		return domain.Event{}, err
	}
	if cur.Status == domain.StatusCancelled {
		return domain.Event{}, domain.ErrCancelled
	}
	replace := false
	e := cur
	if in.Title != nil {
		e.Title = strings.TrimSpace(*in.Title)
	}
	if in.Description != nil {
		e.Description = *in.Description
	}
	if in.StartsAt != nil {
		e.StartsAt = in.StartsAt.UTC()
	}
	if in.EndsAt != nil {
		e.EndsAt = in.EndsAt.UTC()
	}
	if in.ImageURL != nil {
		if *in.ImageURL != "" && !strings.HasPrefix(*in.ImageURL, s.CDNBase+"/images/events/"+id+"/") {
			return domain.Event{}, &domain.ValidationError{Fields: []domain.FieldError{{Field: "imageUrl", Reason: "must be an URL returned by image-upload-url for this event"}}}
		}
		e.ImageURL = *in.ImageURL
	}
	if in.VenueID != nil || in.TicketTypes != nil {
		if cur.Status != domain.StatusDraft {
			return domain.Event{}, domain.ErrNotDraft
		}
		if in.VenueID != nil {
			e.VenueID = *in.VenueID
		}
		if in.TicketTypes != nil {
			e.TicketTypes = s.buildTicketTypes(*in.TicketTypes)
			replace = true
		}
	}
	// A published event already started selling: only require "future" for drafts.
	if err := validateAll(e, s.Now(), cur.Status == domain.StatusDraft); err != nil {
		return domain.Event{}, err
	}
	if in.VenueID != nil || in.TicketTypes != nil {
		if err := s.checkVenue(ctx, e); err != nil {
			return domain.Event{}, err
		}
	}
	e.UpdatedAt = s.Now()
	saved, err := s.Events.Save(ctx, e, expectedVersion, replace)
	if err != nil {
		return domain.Event{}, err
	}
	s.invalidate(ctx, id)
	return saved, nil
}

// PublishEvent moves a draft to published and announces it.
//
// Ordering and its trade-off (no outbox here, by design - see orders for the
// outbox version): 1) commit to Postgres, 2) send EventPublished. If the process
// dies between 1 and 2, or EventBridge is down, the event is published but
// reservations never learns about it. We therefore (a) retry PutEvents a few
// times, (b) on final failure return ErrAnnounceFailed (HTTP 503) so the admin
// knows, and (c) make this endpoint IDEMPOTENT AND RE-ANNOUNCING: calling it again
// on an already-published event announces again. Consumers dedupe by event id /
// create inventory only if absent, so a duplicate announcement is harmless.
func (s *Service) PublishEvent(ctx context.Context, id string) (domain.Event, error) {
	cur, err := s.Events.Get(ctx, id)
	if err != nil {
		return domain.Event{}, err
	}
	e := cur
	changed, err := e.Publish(s.Now())
	if err != nil {
		return domain.Event{}, err
	}
	if changed {
		e.UpdatedAt = s.Now()
		if e, err = s.Events.Save(ctx, e, cur.Version, false); err != nil {
			return domain.Event{}, err
		}
		s.published.Add(ctx, 1)
		s.invalidate(ctx, id)
	}
	if err := s.Announcer.AnnouncePublished(ctx, e.PublishedData()); err != nil {
		s.announceFailures.Add(ctx, 1)
		slog.ErrorContext(ctx, "EventPublished announcement failed", "event_id", e.ID, "error", err.Error())
		return e, fmt.Errorf("%w: %w", ErrAnnounceFailed, err)
	}
	return e, nil
}

// ---- images --------------------------------------------------------------------

// UploadURL is the result of ImageUploadURL.
type UploadURL struct {
	UploadURL string    `json:"uploadUrl"`
	ObjectKey string    `json:"objectKey"`
	PublicURL string    `json:"publicUrl"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// ImageUploadURL returns a presigned S3 PUT URL for one image.
//
// The browser uploads straight to S3, so image bytes never pass through the API
// pods (no memory spike, no 1 MiB body limit problem). Security comes from what
// the URL pins: the exact object key, the Content-Type and the Content-Length
// are part of the signature, so the URL cannot be reused to upload a different
// kind of file or a 2 GB file.
func (s *Service) ImageUploadURL(ctx context.Context, eventID, contentType string, contentLength int64) (UploadURL, error) {
	ext, ok := domain.ImageContentTypes[contentType]
	if !ok {
		return UploadURL{}, ErrImageType
	}
	if contentLength < 1 || contentLength > domain.MaxImageBytes {
		return UploadURL{}, &domain.ValidationError{Fields: []domain.FieldError{{Field: "contentLength", Reason: "must be between 1 and 5242880 bytes"}}}
	}
	if _, err := s.Events.Get(ctx, eventID); err != nil {
		return UploadURL{}, err
	}
	key := fmt.Sprintf("images/events/%s/%s.%s", eventID, s.NewID(), ext)
	url, err := s.Images.PresignPut(ctx, key, contentType, contentLength, UploadURLTTL)
	if err != nil {
		return UploadURL{}, err
	}
	return UploadURL{UploadURL: url, ObjectKey: key, PublicURL: s.CDNBase + "/" + key, ExpiresAt: s.Now().Add(UploadURLTTL)}, nil
}
