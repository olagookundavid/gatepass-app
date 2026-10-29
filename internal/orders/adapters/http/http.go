// Package http (orders adapter) exposes the public orders API.
//
// HEXAGONAL BOUNDARY: adapter, driving side. Parses the transport, calls one use
// case, maps errors to RFC 9457 problems.
//
// Routes (under BASE_PATH, default /orders; the Ingress does not strip it). All
// require a Cognito access token:
//
//	POST /v1/orders                  Idempotency-Key required, body {"holdId": "..."}
//	GET  /v1/orders/{orderId}        owner or admin
//	GET  /v1/me/orders               paginated, newest first
//	POST /v1/orders/{orderId}/cancel owner; from pending_payment or paid
//	GET  /v1/orders/{orderId}/history owner or admin
//
// AWS services touched: none directly.
package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/olagookundavid/gatepass-app/internal/orders/app"
	"github.com/olagookundavid/gatepass-app/internal/orders/domain"
	"github.com/olagookundavid/gatepass-app/internal/orders/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/idempotency"
	"github.com/olagookundavid/gatepass-app/internal/platform/pagination"
	"github.com/olagookundavid/gatepass-app/internal/platform/problem"
)

// Handler serves the orders API.
type Handler struct {
	Svc      *app.Service
	Verifier auth.Verifier
	BasePath string
}

// Routes registers the endpoints.
func (h *Handler) Routes(mux *http.ServeMux) {
	b := h.BasePath + "/v1"
	mux.Handle("POST "+b+"/orders", auth.Require(h.Verifier, problem.Handle(h.create)))
	mux.Handle("GET "+b+"/orders/{orderId}", auth.Require(h.Verifier, problem.Handle(h.get)))
	mux.Handle("GET "+b+"/me/orders", auth.Require(h.Verifier, problem.Handle(h.mine)))
	mux.Handle("POST "+b+"/orders/{orderId}/cancel", auth.Require(h.Verifier, problem.Handle(h.cancel)))
	mux.Handle("GET "+b+"/orders/{orderId}/history", auth.Require(h.Verifier, problem.Handle(h.history)))
}

func actor(r *http.Request) app.Actor {
	c, _ := auth.FromContext(r.Context())
	return app.Actor{UserID: c.Subject, IsAdmin: c.IsAdmin()}
}

func orderID(r *http.Request) (string, error) {
	id := r.PathValue("orderId")
	if _, err := uuid.Parse(id); err != nil {
		return "", problem.NotFound("The order")
	}
	return id, nil
}

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return problem.NotFound("The order")
	case errors.Is(err, app.ErrIdempotencyConflict):
		return idempotency.ConflictProblem()
	case errors.Is(err, domain.ErrHoldNotFound):
		return problem.Validation(problem.FieldError{Field: "holdId", Reason: "hold not found"})
	case errors.Is(err, domain.ErrHoldNotYours):
		return problem.Validation(problem.FieldError{Field: "holdId", Reason: "hold not found"}) // same answer: do not reveal other users' holds
	case errors.Is(err, domain.ErrHoldExpired):
		return problem.Conflict("hold_expired", "The hold has expired. Place a new hold.")
	case errors.Is(err, domain.ErrHoldReleased):
		return problem.Conflict("hold_released", "The hold was released. Place a new hold.")
	case errors.Is(err, domain.ErrHoldTaken):
		return problem.Conflict("hold_already_used", "The hold is already used by another order.")
	case errors.Is(err, domain.ErrOrderExists):
		return problem.Conflict("order_exists", "An order already exists for this hold.")
	case errors.Is(err, domain.ErrReservationsDown):
		return problem.Unavailable("Ticket reservations are temporarily unavailable. Please retry.")
	case errors.Is(err, domain.ErrInvalidTransition):
		return problem.Conflict("invalid_transition", "The order cannot be changed from its current status.")
	default:
		return err
	}
}

type createBody struct {
	HoldID string `json:"holdId"`
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) error {
	a := actor(r)
	key := r.Header.Get(idempotency.Header)
	if key == "" {
		return problem.Validation(problem.FieldError{Field: idempotency.Header, Reason: "header is required"})
	}
	if err := idempotency.ValidateKey(key); err != nil {
		return err
	}
	var b createBody
	if err := problem.DecodeJSON(r, &b); err != nil {
		return err
	}
	if _, err := uuid.Parse(b.HoldID); err != nil {
		return problem.Validation(problem.FieldError{Field: "holdId", Reason: "must be a UUID"})
	}
	resp, err := h.Svc.CreateOrder(r.Context(), a.UserID, key, b.HoldID)
	if err != nil {
		return mapErr(err)
	}
	if resp.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.Code)
	_, _ = w.Write(resp.Body)
	return nil
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) error {
	id, err := orderID(r)
	if err != nil {
		return err
	}
	o, err := h.Svc.GetOrder(r.Context(), actor(r), id)
	if err != nil {
		return mapErr(err)
	}
	w.Header().Set("ETag", `"`+strconv.Itoa(o.Version)+`"`)
	w.Header().Set("Cache-Control", "private, no-store")
	return problem.WriteJSON(w, http.StatusOK, o)
}

func (h *Handler) mine(w http.ResponseWriter, r *http.Request) error {
	limit, err := pagination.Limit(r)
	if err != nil {
		return err
	}
	var cur ports.OrderCursor
	has, err := pagination.Decode(r.URL.Query().Get("cursor"), &cur)
	if err != nil {
		return err
	}
	var after *ports.OrderCursor
	if has {
		if _, perr := uuid.Parse(cur.ID); perr != nil || cur.CreatedAt.IsZero() {
			return problem.Validation(problem.FieldError{Field: "cursor", Reason: "is not a valid cursor"})
		}
		after = &cur
	}
	orders, next, err := h.Svc.ListMyOrders(r.Context(), actor(r).UserID, after, limit)
	if err != nil {
		return mapErr(err)
	}
	out := pagination.Page[domain.Order]{Items: orders}
	if out.Items == nil {
		out.Items = []domain.Order{}
	}
	if next != nil {
		out.NextCursor = pagination.Encode(next)
	}
	w.Header().Set("Cache-Control", "private, no-store")
	return problem.WriteJSON(w, http.StatusOK, out)
}

type cancelBody struct {
	Reason string `json:"reason"`
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) error {
	id, err := orderID(r)
	if err != nil {
		return err
	}
	var b cancelBody
	// The body is optional: an empty body is fine, a malformed one is not.
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil && !errors.Is(err, io.EOF) {
		return problem.BadRequest("Request body is not valid JSON.")
	}
	if utf8.RuneCountInString(b.Reason) > 200 {
		return problem.Validation(problem.FieldError{Field: "reason", Reason: "must be at most 200 characters"})
	}
	if b.Reason == "" {
		b.Reason = "cancelled by user"
	}
	o, err := h.Svc.CancelOrder(r.Context(), actor(r).UserID, id, b.Reason)
	if err != nil {
		return mapErr(err)
	}
	return problem.WriteJSON(w, http.StatusOK, o)
}

func (h *Handler) history(w http.ResponseWriter, r *http.Request) error {
	id, err := orderID(r)
	if err != nil {
		return err
	}
	hist, err := h.Svc.History(r.Context(), actor(r), id)
	if err != nil {
		return mapErr(err)
	}
	w.Header().Set("Cache-Control", "private, no-store")
	return problem.WriteJSON(w, http.StatusOK, map[string]any{"orderId": id, "history": hist})
}
