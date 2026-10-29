// Package logging builds the structured JSON logger used by every binary.
//
// Purpose: one log line = one JSON object with the same field names everywhere
// (service, version, request_id, trace_id, span_id, user_id), so CloudWatch
// Logs Insights / Loki queries work across services without per-service parsing.
//
// How the correlation fields get in: callers log with the *Context variants
// (slog.InfoContext(ctx, ...)). Our handler wrapper reads the request id and user
// id from a small struct stored in the context, and the trace/span ids from the
// active OpenTelemetry span, and adds them to the record.
//
// What is never logged: tokens, passwords, secret values, full email addresses.
// Use MaskEmail if an address must be mentioned at all.
//
// AWS services touched: none (stdout is collected by the cluster's log agent).
package logging

import (
	"context"
	"io"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

type ctxKey struct{}

// ReqInfo is the per-request mutable bag the log handler reads. It is a pointer
// stored in the context so that middleware that runs later (authentication) can
// set UserID and the outer access-log middleware still sees it.
type ReqInfo struct {
	RequestID string
	UserID    string
}

// WithReqInfo returns a context carrying a fresh ReqInfo.
func WithReqInfo(ctx context.Context, requestID string) (context.Context, *ReqInfo) {
	ri := &ReqInfo{RequestID: requestID}
	return context.WithValue(ctx, ctxKey{}, ri), ri
}

// FromContext returns the ReqInfo, or nil outside a request.
func FromContext(ctx context.Context) *ReqInfo {
	ri, _ := ctx.Value(ctxKey{}).(*ReqInfo)
	return ri
}

// SetUserID records the authenticated user for the rest of the request.
func SetUserID(ctx context.Context, userID string) {
	if ri := FromContext(ctx); ri != nil {
		ri.UserID = userID
	}
}

// RequestID returns the request id in ctx ("" outside a request).
func RequestID(ctx context.Context) string {
	if ri := FromContext(ctx); ri != nil {
		return ri.RequestID
	}
	return ""
}

// New builds the JSON logger. level is debug|info|warn|error (default info).
func New(w io.Writer, service, version, level string) *slog.Logger {
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(strings.ToLower(level))); err != nil {
		lv = slog.LevelInfo
	}
	base := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lv})
	return slog.New(&ctxHandler{inner: base}).With("service", service, "version", version)
}

// ctxHandler decorates every record with correlation fields from the context.
type ctxHandler struct{ inner slog.Handler }

func (h *ctxHandler) Enabled(ctx context.Context, l slog.Level) bool { return h.inner.Enabled(ctx, l) }

func (h *ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	if ri := FromContext(ctx); ri != nil {
		if ri.RequestID != "" {
			r.AddAttrs(slog.String("request_id", ri.RequestID))
		}
		if ri.UserID != "" {
			r.AddAttrs(slog.String("user_id", ri.UserID))
		}
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.inner.Handle(ctx, r)
}

func (h *ctxHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return &ctxHandler{inner: h.inner.WithAttrs(a)}
}

func (h *ctxHandler) WithGroup(n string) slog.Handler {
	return &ctxHandler{inner: h.inner.WithGroup(n)}
}

// MaskEmail turns "jane.doe@example.com" into "j***@example.com" for the rare
// log line that must mention an address.
func MaskEmail(e string) string {
	at := strings.LastIndex(e, "@")
	if at <= 0 {
		return "***"
	}
	return e[:1] + "***" + e[at:]
}
