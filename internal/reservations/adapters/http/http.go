// Package http (reservations adapter) exposes the public and internal HTTP APIs.
//
// HEXAGONAL BOUNDARY: adapter, "driving" side. Parses transport details, calls
// one use case, maps domain errors to RFC 9457 problems.
//
// Public (BASE_PATH, default /reservations; Ingress does not strip it):
//
//	POST   /v1/holds                      auth, Idempotency-Key required
//	GET    /v1/holds/{holdId}             auth, owner only
//	DELETE /v1/holds/{holdId}             auth, owner only
//	GET    /v1/me/holds                   auth, paginated
//	GET    /v1/events/{eventId}/inventory public
//
// Internal listener (never exposed by the Ingress):
//
//	POST /internal/v1/holds/{holdId}/consume   X-Caller-Service required, no JWT
//
// AWS services touched: none directly.
package http

import (
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/idempotency"
	"github.com/olagookundavid/gatepass-app/internal/platform/pagination"
	"github.com/olagookundavid/gatepass-app/internal/platform/problem"
	"github.com/olagookundavid/gatepass-app/internal/reservations/app"
	"github.com/olagookundavid/gatepass-app/internal/reservations/domain"
	"github.com/olagookundavid/gatepass-app/internal/reservations/ports"
)

// Handler serves both APIs.
type Handler struct {
	Svc      *app.Service
	Verifier auth.Verifier
	BasePath string
	Now      func() time.Time
}

func (h *Handler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now().UTC()
}

// PublicRoutes registers the public endpoints.
func (h *Handler) PublicRoutes(mux *http.ServeMux) {
	b := h.BasePath + "/v1"
	mux.Handle("POST "+b+"/holds", auth.Require(h.Verifier, problem.Handle(h.createHold)))
	mux.Handle("GET "+b+"/holds/{holdId}", auth.Require(h.Verifier, problem.Handle(h.getHold)))
	mux.Handle("DELETE "+b+"/holds/{holdId}", auth.Require(h.Verifier, problem.Handle(h.deleteHold)))
	mux.Handle("GET "+b+"/me/holds", auth.Require(h.Verifier, problem.Handle(h.myHolds)))
	mux.Handle("GET "+b+"/events/{eventId}/inventory", problem.Handle(h.inventory))
}

// InternalRoutes registers the internal endpoint (internal listener only).
func (h *Handler) InternalRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /internal/v1/holds/{holdId}/consume", auth.RequireCaller(problem.Handle(h.consume)))
}

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return problem.NotFound("The resource")
	case errors.Is(err, domain.ErrContention):
		return problem.Unavailable("The system is busy with this item. Please retry in a moment.")
	case errors.Is(err, domain.ErrInvalidQuantity):
		return problem.Validation(problem.FieldError{Field: "quantity", Reason: "must be between 1 and 8"})
	case errors.Is(err, domain.ErrSoldOut):
		return problem.Conflict("sold_out", "Not enough tickets are available.")
	case errors.Is(err, app.ErrRateLimited):
		return problem.TooManyRequests("Too many holds. Try again in a minute.")
	case errors.Is(err, app.ErrIdempotencyConflict):
		return idempotency.ConflictProblem()
	case errors.Is(err, app.ErrSchedulingFailed):
		return problem.Unavailable("The hold could not be completed. Please try again.")
	case errors.Is(err, domain.ErrHoldExpired):
		return problem.Conflict("hold_expired", "The hold has expired.")
	case errors.Is(err, domain.ErrHoldReleased):
		return problem.Conflict("hold_released", "The hold was released.")
	case errors.Is(err, domain.ErrAlreadyConsumed):
		return problem.Conflict("hold_already_consumed", "The hold was already consumed by another order.")
	case errors.Is(err, domain.ErrHoldConsumed):
		return problem.Conflict("hold_consumed", "The hold belongs to an order; cancel the order instead.")
	case errors.Is(err, domain.ErrWrongOwner):
		return problem.Forbidden("The hold belongs to another user.")
	default:
		return err
	}
}

func pathUUID(r *http.Request, name, what string) (string, error) {
	v := r.PathValue(name)
	if _, err := uuid.Parse(v); err != nil {
		return "", problem.NotFound(what)
	}
	return v, nil
}

// holdView is the JSON returned to clients: the stored status is replaced by the
// effective one (an active hold past expiresAt reads as "expired").
type holdView struct {
	domain.Hold
	Status domain.HoldStatus `json:"status"`
}

func (h *Handler) view(x domain.Hold) holdView {
	return holdView{Hold: x, Status: x.EffectiveStatus(h.now())}
}

type createHoldBody struct {
	EventID      string `json:"eventId"`
	TicketTypeID string `json:"ticketTypeId"`
	Quantity     int    `json:"quantity"`
}

func (h *Handler) createHold(w http.ResponseWriter, r *http.Request) error {
	claims, _ := auth.FromContext(r.Context())
	key := r.Header.Get(idempotency.Header)
	if key == "" {
		return problem.Validation(problem.FieldError{Field: idempotency.Header, Reason: "header is required"})
	}
	if err := idempotency.ValidateKey(key); err != nil {
		return err
	}
	var b createHoldBody
	if err := problem.DecodeJSON(r, &b); err != nil {
		return err
	}
	var fe []problem.FieldError
	if _, err := uuid.Parse(b.EventID); err != nil {
		fe = append(fe, problem.FieldError{Field: "eventId", Reason: "must be a UUID"})
	}
	if _, err := uuid.Parse(b.TicketTypeID); err != nil {
		fe = append(fe, problem.FieldError{Field: "ticketTypeId", Reason: "must be a UUID"})
	}
	if b.Quantity < 1 || b.Quantity > domain.MaxQuantity {
		fe = append(fe, problem.FieldError{Field: "quantity", Reason: "must be between 1 and 8"})
	}
	if len(fe) > 0 {
		return problem.Validation(fe...)
	}
	res, err := h.Svc.CreateHold(r.Context(), claims.Subject, key, app.CreateHoldInput{EventID: b.EventID, TicketTypeID: b.TicketTypeID, Quantity: b.Quantity})
	if err != nil {
		if errors.Is(err, app.ErrRateLimited) {
			w.Header().Set("Retry-After", "60")
		}
		if errors.Is(err, domain.ErrContention) {
			w.Header().Set("Retry-After", "1")
		}
		return mapErr(err)
	}
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("Location", h.BasePath+"/v1/holds/"+res.Hold.ID)
	return problem.WriteJSON(w, http.StatusCreated, h.view(res.Hold))
}

func (h *Handler) getHold(w http.ResponseWriter, r *http.Request) error {
	claims, _ := auth.FromContext(r.Context())
	id, err := pathUUID(r, "holdId", "The hold")
	if err != nil {
		return err
	}
	hold, err := h.Svc.GetHold(r.Context(), claims.Subject, id)
	if err != nil {
		return mapErr(err)
	}
	w.Header().Set("Cache-Control", "private, no-store")
	return problem.WriteJSON(w, http.StatusOK, h.view(hold))
}

func (h *Handler) deleteHold(w http.ResponseWriter, r *http.Request) error {
	claims, _ := auth.FromContext(r.Context())
	id, err := pathUUID(r, "holdId", "The hold")
	if err != nil {
		return err
	}
	if err := h.Svc.DeleteHold(r.Context(), claims.Subject, id); err != nil {
		return mapErr(err)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (h *Handler) myHolds(w http.ResponseWriter, r *http.Request) error {
	claims, _ := auth.FromContext(r.Context())
	limit, err := pagination.Limit(r)
	if err != nil {
		return err
	}
	var cur ports.HoldCursor
	has, err := pagination.Decode(r.URL.Query().Get("cursor"), &cur)
	if err != nil {
		return err
	}
	var after *ports.HoldCursor
	if has {
		after = &cur
	}
	holds, next, err := h.Svc.ListMyHolds(r.Context(), claims.Subject, after, limit)
	if err != nil {
		// The adapter refuses cursors that point into another user's data.
		if after != nil {
			return problem.Validation(problem.FieldError{Field: "cursor", Reason: "is not a valid cursor"})
		}
		return err
	}
	out := pagination.Page[holdView]{Items: make([]holdView, len(holds))}
	for i, x := range holds {
		out.Items[i] = h.view(x)
	}
	if next != nil {
		out.NextCursor = pagination.Encode(next)
	}
	w.Header().Set("Cache-Control", "private, no-store")
	return problem.WriteJSON(w, http.StatusOK, out)
}

type inventoryView struct {
	TicketTypeID string `json:"ticketTypeId"`
	Available    int    `json:"available"`
	Capacity     int    `json:"capacity"`
	PriceCents   int64  `json:"priceCents"`
	Currency     string `json:"currency"`
}

func (h *Handler) inventory(w http.ResponseWriter, r *http.Request) error {
	id, err := pathUUID(r, "eventId", "The event")
	if err != nil {
		return err
	}
	inv, err := h.Svc.Inventory(r.Context(), id)
	if err != nil {
		return mapErr(err)
	}
	items := make([]inventoryView, len(inv))
	for i, x := range inv {
		items[i] = inventoryView{x.TicketTypeID, x.Available, x.Capacity, x.PriceCents, x.Currency}
	}
	w.Header().Set("Cache-Control", "public, max-age=5") // availability changes constantly
	return problem.WriteJSON(w, http.StatusOK, map[string]any{"eventId": id, "ticketTypes": items})
}

type consumeBody struct {
	OrderID string `json:"orderId"`
	UserID  string `json:"userId"`
}

// consumeResponse is what orders needs to price the order without calling catalog.
type consumeResponse struct {
	HoldID         string `json:"holdId"`
	EventID        string `json:"eventId"`
	TicketTypeID   string `json:"ticketTypeId"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unitPriceCents"`
	Currency       string `json:"currency"`
}

func (h *Handler) consume(w http.ResponseWriter, r *http.Request) error {
	id, err := pathUUID(r, "holdId", "The hold")
	if err != nil {
		return err
	}
	var b consumeBody
	if err := problem.DecodeJSON(r, &b); err != nil {
		return err
	}
	var fe []problem.FieldError
	if _, err := uuid.Parse(b.OrderID); err != nil {
		fe = append(fe, problem.FieldError{Field: "orderId", Reason: "must be a UUID"})
	}
	if b.UserID == "" {
		fe = append(fe, problem.FieldError{Field: "userId", Reason: "is required"})
	}
	if len(fe) > 0 {
		return problem.Validation(fe...)
	}
	hold, err := h.Svc.Consume(r.Context(), id, b.OrderID, b.UserID)
	if err != nil {
		return mapErr(err)
	}
	return problem.WriteJSON(w, http.StatusOK, consumeResponse{hold.ID, hold.EventID, hold.TicketTypeID, hold.Quantity, hold.UnitPriceCents, hold.Currency})
}
