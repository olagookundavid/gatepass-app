// Package http (catalog adapter) exposes the application over HTTP/JSON.
//
// HEXAGONAL BOUNDARY: adapter, the "driving" side. It parses and validates the
// transport (headers, JSON, query strings), calls one use case, and maps the
// result or error to a status code. It contains no business rules.
//
// Routes (all under BASE_PATH, default /catalog; the Ingress does not strip prefixes):
//
//	GET  /v1/events                          public   list published events
//	GET  /v1/events/{eventId}                public   (admins also see drafts)
//	POST /v1/events                          admin    create draft
//	PATCH /v1/events/{eventId}               admin    partial update, If-Match
//	POST /v1/events/{eventId}/publish        admin    idempotent
//	POST /v1/events/{eventId}/image-upload-url admin  presigned S3 PUT
//	GET  /v1/venues                          public
//	POST /v1/venues                          admin
//
// AWS services touched: none directly.
package http

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/olagookundavid/gatepass-app/internal/catalog/app"
	"github.com/olagookundavid/gatepass-app/internal/catalog/domain"
	"github.com/olagookundavid/gatepass-app/internal/catalog/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/pagination"
	"github.com/olagookundavid/gatepass-app/internal/platform/problem"
)

// Handler serves the catalog API.
type Handler struct {
	Svc      *app.Service
	Verifier auth.Verifier
	BasePath string
}

// Routes registers every endpoint on mux.
func (h *Handler) Routes(mux *http.ServeMux) {
	b := h.BasePath + "/v1"
	mux.Handle("GET "+b+"/events", problem.Handle(h.listEvents))
	mux.Handle("GET "+b+"/events/{eventId}", auth.Optional(h.Verifier, problem.Handle(h.getEvent)))
	mux.Handle("POST "+b+"/events", auth.RequireAdmin(h.Verifier, problem.Handle(h.createEvent)))
	mux.Handle("PATCH "+b+"/events/{eventId}", auth.RequireAdmin(h.Verifier, problem.Handle(h.updateEvent)))
	mux.Handle("POST "+b+"/events/{eventId}/publish", auth.RequireAdmin(h.Verifier, problem.Handle(h.publishEvent)))
	mux.Handle("POST "+b+"/events/{eventId}/image-upload-url", auth.RequireAdmin(h.Verifier, problem.Handle(h.imageUploadURL)))
	mux.Handle("GET "+b+"/venues", problem.Handle(h.listVenues))
	mux.Handle("POST "+b+"/venues", auth.RequireAdmin(h.Verifier, problem.Handle(h.createVenue)))
}

// mapErr converts domain/application errors to RFC 9457 problems.
func mapErr(err error) error {
	var ve *domain.ValidationError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &ve):
		fe := make([]problem.FieldError, len(ve.Fields))
		for i, f := range ve.Fields {
			fe[i] = problem.FieldError{Field: f.Field, Reason: f.Reason}
		}
		return problem.Validation(fe...)
	case errors.Is(err, domain.ErrNotFound):
		return problem.NotFound("The resource")
	case errors.Is(err, domain.ErrVersionConflict):
		return problem.PreconditionFailed("The event was modified by someone else. GET it again and retry with the new ETag.")
	case errors.Is(err, domain.ErrNotDraft):
		return problem.Conflict("event_not_draft", "This change is only allowed while the event is a draft.")
	case errors.Is(err, domain.ErrCancelled):
		return problem.Conflict("event_cancelled", "The event is cancelled.")
	case errors.Is(err, app.ErrAnnounceFailed):
		return problem.Unavailable("The event is published but could not be announced to other services yet. Repeat the publish call; it is safe to repeat.")
	default:
		return err // problem.Handle turns unknown errors into a generic 500
	}
}

func eventID(r *http.Request) (string, error) {
	id := r.PathValue("eventId")
	if _, err := uuid.Parse(id); err != nil {
		return "", problem.NotFound("The event")
	}
	return id, nil
}

// ---- events ----

func parseTime(field, s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return &t, nil
		}
	}
	return nil, problem.Validation(problem.FieldError{Field: field, Reason: "must be RFC 3339 or YYYY-MM-DD"})
}

func (h *Handler) listEvents(w http.ResponseWriter, r *http.Request) error {
	limit, err := pagination.Limit(r)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	from, err := parseTime("from", q.Get("from"))
	if err != nil {
		return err
	}
	to, err := parseTime("to", q.Get("to"))
	if err != nil {
		return err
	}
	if len(q.Get("city")) > 100 {
		return problem.Validation(problem.FieldError{Field: "city", Reason: "must be at most 100 characters"})
	}
	var cur ports.EventCursor
	has, err := pagination.Decode(q.Get("cursor"), &cur)
	if err != nil {
		return err
	}
	var after *ports.EventCursor
	if has {
		if _, perr := uuid.Parse(cur.ID); perr != nil {
			return problem.Validation(problem.FieldError{Field: "cursor", Reason: "is not a valid cursor"})
		}
		after = &cur
	}
	page, err := h.Svc.ListEvents(r.Context(), ports.EventFilter{City: strings.TrimSpace(q.Get("city")), From: from, To: to}, after, limit)
	if err != nil {
		return mapErr(err)
	}
	out := pagination.Page[domain.Event]{Items: page.Items}
	if out.Items == nil {
		out.Items = []domain.Event{}
	}
	if page.Next != nil {
		out.NextCursor = pagination.Encode(page.Next)
	}
	// Public, cacheable by browsers/CDN for a short time (matches the server-side list TTL).
	w.Header().Set("Cache-Control", "public, max-age=30")
	return problem.WriteJSON(w, http.StatusOK, out)
}

func (h *Handler) getEvent(w http.ResponseWriter, r *http.Request) error {
	id, err := eventID(r)
	if err != nil {
		return err
	}
	isAdmin := false
	if c, ok := auth.FromContext(r.Context()); ok {
		isAdmin = c.IsAdmin()
	}
	e, err := h.Svc.GetEvent(r.Context(), id, isAdmin)
	if err != nil {
		return mapErr(err)
	}
	w.Header().Set("ETag", `"`+strconv.Itoa(e.Version)+`"`)
	if e.Status == domain.StatusPublished {
		w.Header().Set("Cache-Control", "public, max-age=30")
	} else {
		w.Header().Set("Cache-Control", "private, no-store")
	}
	return problem.WriteJSON(w, http.StatusOK, e)
}

type ticketTypeBody struct {
	Name       string `json:"name"`
	PriceCents int64  `json:"priceCents"`
	Currency   string `json:"currency"`
	Capacity   int    `json:"capacity"`
}

func toInputs(in []ticketTypeBody) []app.TicketTypeInput {
	out := make([]app.TicketTypeInput, len(in))
	for i, t := range in {
		out[i] = app.TicketTypeInput{Name: t.Name, PriceCents: t.PriceCents, Currency: t.Currency, Capacity: t.Capacity}
	}
	return out
}

type createEventBody struct {
	VenueID     string           `json:"venueId"`
	Title       string           `json:"title"`
	Description string           `json:"description"`
	StartsAt    time.Time        `json:"startsAt"`
	EndsAt      time.Time        `json:"endsAt"`
	TicketTypes []ticketTypeBody `json:"ticketTypes"`
}

func (h *Handler) createEvent(w http.ResponseWriter, r *http.Request) error {
	var b createEventBody
	if err := problem.DecodeJSON(r, &b); err != nil {
		return err
	}
	claims, _ := auth.FromContext(r.Context())
	e, err := h.Svc.CreateEvent(r.Context(), app.CreateEventInput{VenueID: b.VenueID, Title: b.Title, Description: b.Description,
		StartsAt: b.StartsAt, EndsAt: b.EndsAt, TicketTypes: toInputs(b.TicketTypes)}, claims.Subject)
	if err != nil {
		return mapErr(err)
	}
	w.Header().Set("Location", h.BasePath+"/v1/events/"+e.ID)
	w.Header().Set("ETag", `"`+strconv.Itoa(e.Version)+`"`)
	return problem.WriteJSON(w, http.StatusCreated, e)
}

type updateEventBody struct {
	VenueID     *string           `json:"venueId"`
	Title       *string           `json:"title"`
	Description *string           `json:"description"`
	StartsAt    *time.Time        `json:"startsAt"`
	EndsAt      *time.Time        `json:"endsAt"`
	ImageURL    *string           `json:"imageUrl"`
	TicketTypes *[]ticketTypeBody `json:"ticketTypes"`
}

// parseIfMatch accepts If-Match: "3" or 3.
func parseIfMatch(r *http.Request) (int, error) {
	h := strings.Trim(strings.TrimSpace(r.Header.Get("If-Match")), `"`)
	if h == "" {
		return 0, problem.New(http.StatusPreconditionRequired, "if_match_required", "Precondition required",
			"Send the event's ETag in the If-Match header (from GET /events/{id}).")
	}
	n, err := strconv.Atoi(h)
	if err != nil || n < 1 {
		return 0, problem.BadRequest("If-Match must be the numeric ETag returned by GET.")
	}
	return n, nil
}

func (h *Handler) updateEvent(w http.ResponseWriter, r *http.Request) error {
	id, err := eventID(r)
	if err != nil {
		return err
	}
	version, err := parseIfMatch(r)
	if err != nil {
		return err
	}
	var b updateEventBody
	if err := problem.DecodeJSON(r, &b); err != nil {
		return err
	}
	in := app.UpdateEventInput{VenueID: b.VenueID, Title: b.Title, Description: b.Description, StartsAt: b.StartsAt, EndsAt: b.EndsAt, ImageURL: b.ImageURL}
	if b.TicketTypes != nil {
		tts := toInputs(*b.TicketTypes)
		in.TicketTypes = &tts
	}
	e, err := h.Svc.UpdateEvent(r.Context(), id, in, version)
	if err != nil {
		return mapErr(err)
	}
	w.Header().Set("ETag", `"`+strconv.Itoa(e.Version)+`"`)
	return problem.WriteJSON(w, http.StatusOK, e)
}

func (h *Handler) publishEvent(w http.ResponseWriter, r *http.Request) error {
	id, err := eventID(r)
	if err != nil {
		return err
	}
	e, err := h.Svc.PublishEvent(r.Context(), id)
	if err != nil {
		return mapErr(err)
	}
	w.Header().Set("ETag", `"`+strconv.Itoa(e.Version)+`"`)
	return problem.WriteJSON(w, http.StatusOK, e)
}

type uploadBody struct {
	ContentType   string `json:"contentType"`
	ContentLength int64  `json:"contentLength"`
}

func (h *Handler) imageUploadURL(w http.ResponseWriter, r *http.Request) error {
	id, err := eventID(r)
	if err != nil {
		return err
	}
	var b uploadBody
	if err := problem.DecodeJSON(r, &b); err != nil {
		return err
	}
	u, err := h.Svc.ImageUploadURL(r.Context(), id, b.ContentType, b.ContentLength)
	if err != nil {
		return mapErr(err)
	}
	w.Header().Set("Cache-Control", "no-store")
	return problem.WriteJSON(w, http.StatusOK, u)
}

// ---- venues ----

func (h *Handler) listVenues(w http.ResponseWriter, r *http.Request) error {
	limit, err := pagination.Limit(r)
	if err != nil {
		return err
	}
	var cur ports.VenueCursor
	has, err := pagination.Decode(r.URL.Query().Get("cursor"), &cur)
	if err != nil {
		return err
	}
	var after *ports.VenueCursor
	if has {
		if _, perr := uuid.Parse(cur.ID); perr != nil {
			return problem.Validation(problem.FieldError{Field: "cursor", Reason: "is not a valid cursor"})
		}
		after = &cur
	}
	page, err := h.Svc.ListVenues(r.Context(), after, limit)
	if err != nil {
		return mapErr(err)
	}
	out := pagination.Page[domain.Venue]{Items: page.Items}
	if out.Items == nil {
		out.Items = []domain.Venue{}
	}
	if page.Next != nil {
		out.NextCursor = pagination.Encode(page.Next)
	}
	return problem.WriteJSON(w, http.StatusOK, out)
}

type venueBody struct {
	Name     string `json:"name"`
	City     string `json:"city"`
	Address  string `json:"address"`
	Capacity int    `json:"capacity"`
}

func (h *Handler) createVenue(w http.ResponseWriter, r *http.Request) error {
	var b venueBody
	if err := problem.DecodeJSON(r, &b); err != nil {
		return err
	}
	v, err := h.Svc.CreateVenue(r.Context(), app.CreateVenueInput{Name: b.Name, City: b.City, Address: b.Address, Capacity: b.Capacity})
	if err != nil {
		return mapErr(err)
	}
	w.Header().Set("Location", h.BasePath+"/v1/venues/"+v.ID)
	return problem.WriteJSON(w, http.StatusCreated, v)
}
