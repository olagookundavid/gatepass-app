// Package problem implements RFC 9457 "Problem Details for HTTP APIs".
//
// Purpose: every non-2xx response of every service has the same JSON shape,
// Content-Type application/problem+json, a stable machine-readable `code`, and
// NEVER stack traces, SQL text, ARNs or other internals.
//
// Handler style used across the repo: handlers return an error. If it is a
// *Problem it is rendered as-is; anything else becomes a generic 500 and the
// real error is logged server-side with the request id (the client gets the
// request id so support can find the log line).
//
// AWS services touched: none.
package problem

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/olagookundavid/gatepass-app/internal/platform/logging"
)

// TypePrefix namespaces problem type URIs. A URN is used because the API has no
// public documentation site to dereference.
const TypePrefix = "urn:gatepass:problem:"

// FieldError describes one invalid input field (400/422 responses).
type FieldError struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

// Problem is the response body and also an error value.
type Problem struct {
	Type      string       `json:"type"`
	Title     string       `json:"title"`
	Status    int          `json:"status"`
	Detail    string       `json:"detail,omitempty"`
	Code      string       `json:"code"`
	RequestID string       `json:"requestId,omitempty"`
	Errors    []FieldError `json:"errors,omitempty"`
}

func (p *Problem) Error() string { return p.Code + ": " + p.Detail }

// New builds a Problem. code is the stable identifier clients may switch on,
// e.g. "sold_out"; it becomes the tail of the type URN.
func New(status int, code, title, detail string) *Problem {
	return &Problem{Type: TypePrefix + code, Title: title, Status: status, Detail: detail, Code: code}
}

// Common constructors. Codes are part of the API contract: do not rename them.
func BadRequest(detail string) *Problem {
	return New(http.StatusBadRequest, "bad_request", "Bad request", detail)
}
func Validation(errs ...FieldError) *Problem {
	p := New(http.StatusUnprocessableEntity, "validation_failed", "Validation failed", "One or more fields are invalid.")
	p.Errors = errs
	return p
}
func Unauthorized(detail string) *Problem {
	return New(http.StatusUnauthorized, "unauthorized", "Authentication required", detail)
}
func Forbidden(detail string) *Problem {
	return New(http.StatusForbidden, "forbidden", "Forbidden", detail)
}
func NotFound(what string) *Problem {
	return New(http.StatusNotFound, "not_found", "Not found", what+" was not found.")
}
func Conflict(code, detail string) *Problem {
	return New(http.StatusConflict, code, "Conflict", detail)
}
func TooManyRequests(detail string) *Problem {
	return New(http.StatusTooManyRequests, "rate_limited", "Too many requests", detail)
}
func PreconditionFailed(detail string) *Problem {
	return New(http.StatusPreconditionFailed, "version_mismatch", "Precondition failed", detail)
}
func Internal() *Problem {
	return New(http.StatusInternalServerError, "internal", "Internal error", "The server could not process the request.")
}
func Unavailable(detail string) *Problem {
	return New(http.StatusServiceUnavailable, "unavailable", "Service unavailable", detail)
}

// Write renders p as application/problem+json.
func Write(w http.ResponseWriter, r *http.Request, p *Problem) {
	out := *p
	out.RequestID = logging.RequestID(r.Context())
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(out.Status)
	_ = json.NewEncoder(w).Encode(out)
}

// HandlerFunc is a handler that reports failure by returning an error.
type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

// Handle adapts a HandlerFunc to http.HandlerFunc, rendering errors.
func Handle(h HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := h(w, r)
		if err == nil {
			return
		}
		var p *Problem
		if errors.As(err, &p) {
			if p.Status >= 500 {
				slog.ErrorContext(r.Context(), "request failed", "code", p.Code, "error", err.Error())
			}
			Write(w, r, p)
			return
		}
		// Unknown error: log the truth, tell the client nothing.
		slog.ErrorContext(r.Context(), "unhandled error", "error", err.Error())
		Write(w, r, Internal())
	}
}

// DecodeJSON reads a JSON body into dst (strict: unknown fields are rejected so
// a typo like "quantiy" is reported rather than silently ignored). The body
// size limit is enforced by the server middleware (1 MiB).
func DecodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return New(http.StatusRequestEntityTooLarge, "body_too_large", "Body too large", "Request body exceeds the 1 MiB limit.")
		}
		return BadRequest("Request body is not valid JSON for this endpoint.")
	}
	return nil
}

// WriteJSON writes a successful JSON response.
func WriteJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(v)
}
