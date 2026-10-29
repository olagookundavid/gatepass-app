// Package http (notifications adapter) exposes the public notifications API.
//
// HEXAGONAL BOUNDARY: adapter, driving side.
//
// Routes (under BASE_PATH, default /notifications; Ingress does not strip it), all
// requiring a Cognito access token:
//
//	GET /v1/me/notifications               the caller's log, newest first
//	GET /v1/me/preferences                 {emailEnabled}
//	PUT /v1/me/preferences                 body {"emailEnabled": true|false}
//	GET /v1/orders/{orderId}/ticket-url    fresh presigned URL (5 min); owner only
//
// AWS services touched: none directly (S3 presigning happens in the adapter).
package http

import (
	"errors"
	"net/http"
	"strings"

	"github.com/olagookundavid/gatepass-app/internal/notifications/app"
	"github.com/olagookundavid/gatepass-app/internal/notifications/domain"
	"github.com/olagookundavid/gatepass-app/internal/notifications/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/pagination"
	"github.com/olagookundavid/gatepass-app/internal/platform/problem"
)

// Handler serves the API.
type Handler struct {
	API      *app.API
	Verifier auth.Verifier
	BasePath string
}

// Routes registers the endpoints.
func (h *Handler) Routes(mux *http.ServeMux) {
	b := h.BasePath + "/v1"
	mux.Handle("GET "+b+"/me/notifications", auth.Require(h.Verifier, problem.Handle(h.list)))
	mux.Handle("GET "+b+"/me/preferences", auth.Require(h.Verifier, problem.Handle(h.getPrefs)))
	mux.Handle("PUT "+b+"/me/preferences", auth.Require(h.Verifier, problem.Handle(h.putPrefs)))
	mux.Handle("GET "+b+"/orders/{orderId}/ticket-url", auth.Require(h.Verifier, problem.Handle(h.ticketURL)))
}

func user(r *http.Request) string {
	c, _ := auth.FromContext(r.Context())
	return c.Subject
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) error {
	limit, err := pagination.Limit(r)
	if err != nil {
		return err
	}
	var cur ports.NotificationCursor
	has, err := pagination.Decode(r.URL.Query().Get("cursor"), &cur)
	if err != nil {
		return err
	}
	var after *ports.NotificationCursor
	if has {
		if !strings.HasPrefix(cur.SK, "NOTIF#") {
			return problem.Validation(problem.FieldError{Field: "cursor", Reason: "is not a valid cursor"})
		}
		after = &cur
	}
	items, next, err := h.API.ListNotifications(r.Context(), user(r), after, limit)
	if err != nil {
		return err
	}
	out := pagination.Page[domain.Notification]{Items: items}
	if out.Items == nil {
		out.Items = []domain.Notification{}
	}
	if next != nil {
		out.NextCursor = pagination.Encode(next)
	}
	w.Header().Set("Cache-Control", "private, no-store")
	return problem.WriteJSON(w, http.StatusOK, out)
}

func (h *Handler) getPrefs(w http.ResponseWriter, r *http.Request) error {
	p, err := h.API.GetPreferences(r.Context(), user(r))
	if err != nil {
		return err
	}
	w.Header().Set("Cache-Control", "private, no-store")
	return problem.WriteJSON(w, http.StatusOK, p)
}

type prefsBody struct {
	// A pointer, so that a missing field is an error instead of silently "false".
	EmailEnabled *bool `json:"emailEnabled"`
}

func (h *Handler) putPrefs(w http.ResponseWriter, r *http.Request) error {
	var b prefsBody
	if err := problem.DecodeJSON(r, &b); err != nil {
		return err
	}
	if b.EmailEnabled == nil {
		return problem.Validation(problem.FieldError{Field: "emailEnabled", Reason: "is required (true or false)"})
	}
	p, err := h.API.SetPreferences(r.Context(), user(r), *b.EmailEnabled)
	if err != nil {
		return err
	}
	return problem.WriteJSON(w, http.StatusOK, p)
}

func (h *Handler) ticketURL(w http.ResponseWriter, r *http.Request) error {
	u, err := h.API.TicketURL(r.Context(), user(r), r.PathValue("orderId"))
	if errors.Is(err, domain.ErrNotFound) {
		return problem.NotFound("A ticket for this order")
	}
	if err != nil {
		return err
	}
	w.Header().Set("Cache-Control", "private, no-store")
	return problem.WriteJSON(w, http.StatusOK, u)
}
