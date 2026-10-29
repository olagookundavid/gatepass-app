// Package app (notifications) contains the use cases: the event-driven worker
// that renders tickets and sends e-mail, and the small query use cases of the API.
//
// HEXAGONAL BOUNDARY: depends only on domain and ports.
//
// The worker's central problem: side effects that cannot be undone (an e-mail
// was sent) inside an at-least-once pipeline (the same event can arrive twice).
//
//   - Exact redeliveries are dropped by a "processed event" marker (by eventId).
//   - The marker is written AFTER the work. If the process dies after sending the
//     mail but before writing the marker, the retry sends a SECOND e-mail. That is
//     the deliberate trade-off: SES has no idempotency key, and losing a ticket
//     e-mail is worse than receiving it twice. (The alternative, marker first, gives
//     at-most-once and can silently lose mail.)
//   - Failures are classified. TEMPORARY (SES throttling, Cognito 5xx, S3 hiccup):
//     return an error -> SQS redelivers later -> after 5 receives the DLQ.
//     PERMANENT (address rejected, user without e-mail): record `failed` in the
//     notification log and ACKNOWLEDGE, because retrying cannot help.
//
// AWS services touched (through ports): DynamoDB, S3, SES, Cognito.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/olagookundavid/gatepass-app/internal/notifications/domain"
	"github.com/olagookundavid/gatepass-app/internal/notifications/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
	"github.com/olagookundavid/gatepass-app/internal/platform/logging"
	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

// ProcessedEventTTL is how long event markers live.
const ProcessedEventTTL = 7 * 24 * time.Hour

// Worker handles events from the notifications queue.
type Worker struct {
	Store     ports.Store
	Tickets   ports.Tickets
	Renderer  ports.Renderer
	Directory ports.Directory
	Mailer    ports.Mailer

	SigningKey []byte
	// RecipientOverride, if set, receives EVERY e-mail instead of the user. For the
	// SES sandbox, where only verified addresses can receive mail.
	RecipientOverride string
	LinkTTL           time.Duration // presigned link validity in the e-mail, default 24h

	Now   func() time.Time
	NewID func() string

	sent, failed, sesThrottled metric.Int64Counter
	renderDuration             metric.Float64Histogram
}

// NewWorker applies defaults and registers metrics.
func NewWorker(w Worker) *Worker {
	if w.Now == nil {
		w.Now = func() time.Time { return time.Now().UTC() }
	}
	if w.NewID == nil {
		w.NewID = func() string {
			id, err := uuid.NewV7()
			if err != nil {
				return uuid.NewString()
			}
			return id.String()
		}
	}
	if w.LinkTTL <= 0 {
		w.LinkTTL = 24 * time.Hour
	}
	w.sent = telemetry.Counter("notifications_sent_total", "Notifications sent, by type and channel")
	w.failed = telemetry.Counter("notifications_failed_total", "Notifications that failed permanently, by type and reason")
	w.sesThrottled = telemetry.Counter("ses_throttled_total", "SES throttled a send (will be retried via SQS)")
	w.renderDuration = telemetry.Histogram("ticket_render_duration_seconds", "s", "Time to render a ticket PNG")
	return &w
}

// HandleEvent processes one event. Idempotent per eventId (see package comment).
func (w *Worker) HandleEvent(ctx context.Context, ev ebpub.Received) error {
	typ := domain.TypeForEvent(ev.EventType)
	if typ == "" {
		slog.InfoContext(ctx, "ignoring event type", "event_type", ev.EventType)
		return nil
	}
	done, err := w.Store.EventProcessed(ctx, ev.EventID)
	if err != nil {
		return err
	}
	if done {
		slog.InfoContext(ctx, "duplicate event ignored", "event_id", ev.EventID)
		return nil
	}

	var d domain.OrderEventData
	if err := json.Unmarshal(ev.Data, &d); err != nil {
		return fmt.Errorf("bad %s data: %w", ev.EventType, err)
	}
	if err := domain.ValidateID(d.UserID); err != nil {
		return fmt.Errorf("bad %s data: userId: %w", ev.EventType, err)
	}
	if err := domain.ValidateID(d.OrderID); err != nil {
		return fmt.Errorf("bad %s data: orderId: %w", ev.EventType, err)
	}
	logging.SetUserID(ctx, d.UserID)

	if err := w.process(ctx, typ, d); err != nil {
		return err
	}
	return w.Store.MarkEventProcessed(ctx, ev.EventID, ProcessedEventTTL)
}

func (w *Worker) process(ctx context.Context, typ string, d domain.OrderEventData) error {
	link := ""
	if typ == domain.TypeOrderPaid {
		key, err := w.makeTicket(ctx, d)
		if err != nil {
			return err
		}
		// Presigned GET. KNOWN LIMIT: a presigned URL stops working when the
		// credentials that signed it expire. With IRSA / Pod Identity those are role
		// session credentials (about 1 hour), so the "24h" link may die sooner.
		// The reliable path is GET /notifications/v1/orders/{orderId}/ticket-url,
		// which signs a fresh short-lived URL on demand. UNVERIFIED in a real account.
		link, err = w.Tickets.PresignGet(ctx, key, "ticket-"+d.OrderID+".png", w.LinkTTL)
		if err != nil {
			return fmt.Errorf("presign ticket: %w", err)
		}
	}

	prefs, err := w.Store.GetPreferences(ctx, d.UserID)
	if err != nil {
		return err
	}
	if !prefs.EmailEnabled {
		return w.record(ctx, typ, d, domain.StatusSkipped, "email_disabled", "")
	}

	// Look up the address ONLY now (after the preference check, to save a Cognito
	// call) and keep it in this stack frame: it is never logged or stored.
	to, err := w.Directory.EmailOf(ctx, d.UserID)
	switch {
	case errors.Is(err, domain.ErrUserNotFound), errors.Is(err, domain.ErrNoEmail):
		return w.recordFailure(ctx, typ, d, "no_email_address")
	case err != nil:
		return fmt.Errorf("look up user: %w", err) // temporary: retry via SQS
	}
	if w.RecipientOverride != "" {
		to = w.RecipientOverride
	}

	msgID, err := w.Mailer.Send(ctx, to, domain.BuildEmail(typ, d, link))
	switch {
	case errors.Is(err, domain.ErrMailThrottled):
		w.sesThrottled.Add(ctx, 1)
		return err // SQS redelivers later: natural back-off
	case errors.Is(err, domain.ErrMailRejected):
		return w.recordFailure(ctx, typ, d, "address_rejected")
	case err != nil:
		return fmt.Errorf("send email: %w", err)
	}
	return w.record(ctx, typ, d, domain.StatusSent, "", msgID)
}

// makeTicket renders, uploads and registers the ticket of a paid order.
func (w *Worker) makeTicket(ctx context.Context, d domain.OrderEventData) (string, error) {
	key, err := domain.TicketKey(d.UserID, d.OrderID)
	if err != nil {
		return "", err
	}
	start := time.Now()
	png, err := w.Renderer.Render(domain.Ticket{OrderID: d.OrderID, EventID: d.EventID, Quantity: d.Quantity,
		Token: domain.SignToken(w.SigningKey, d.OrderID)})
	w.renderDuration.Record(ctx, time.Since(start).Seconds())
	if err != nil {
		return "", fmt.Errorf("render ticket: %w", err)
	}
	// Re-running overwrites the same key with an identical image: idempotent.
	if err := w.Tickets.Put(ctx, key, png); err != nil {
		return "", fmt.Errorf("store ticket: %w", err)
	}
	if err := w.Store.PutTicketRecord(ctx, d.UserID, d.OrderID, key); err != nil {
		return "", err
	}
	return key, nil
}

func (w *Worker) recordFailure(ctx context.Context, typ string, d domain.OrderEventData, reason string) error {
	w.failed.Add(ctx, 1, metric.WithAttributes(attribute.String("type", typ), attribute.String("reason", reason)))
	slog.WarnContext(ctx, "notification failed permanently", "type", typ, "order_id", d.OrderID, "reason", reason)
	return w.record(ctx, typ, d, domain.StatusFailed, reason, "")
}

func (w *Worker) record(ctx context.Context, typ string, d domain.OrderEventData, status, reason, msgID string) error {
	if status == domain.StatusSent {
		w.sent.Add(ctx, 1, metric.WithAttributes(attribute.String("type", typ), attribute.String("channel", domain.ChannelEmail)))
	}
	return w.Store.PutNotification(ctx, domain.Notification{ID: w.NewID(), UserID: d.UserID, Type: typ, OrderID: d.OrderID,
		Channel: domain.ChannelEmail, Status: status, Reason: reason, MessageID: msgID, CreatedAt: w.Now()})
}

// ---- API use cases ----------------------------------------------------------------

// API serves the public queries.
type API struct {
	Store   ports.Store
	Tickets ports.Tickets
	Now     func() time.Time
	// URLTTL is the lifetime of URLs from ticket-url (5 minutes).
	URLTTL time.Duration
}

// ListNotifications pages the caller's log, newest first.
func (a *API) ListNotifications(ctx context.Context, userID string, after *ports.NotificationCursor, limit int) ([]domain.Notification, *ports.NotificationCursor, error) {
	return a.Store.ListNotifications(ctx, userID, after, limit)
}

// GetPreferences returns the caller's preferences (default: e-mail on).
func (a *API) GetPreferences(ctx context.Context, userID string) (domain.Preferences, error) {
	return a.Store.GetPreferences(ctx, userID)
}

// SetPreferences stores the caller's preferences.
func (a *API) SetPreferences(ctx context.Context, userID string, emailEnabled bool) (domain.Preferences, error) {
	now := a.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	p := domain.Preferences{EmailEnabled: emailEnabled, UpdatedAt: now()}
	return p, a.Store.PutPreferences(ctx, userID, p)
}

// TicketURL is a fresh download link for the caller's ticket.
type TicketURL struct {
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// TicketURL returns a short-lived URL or domain.ErrNotFound. Ownership is
// enforced by the storage layout: the ticket record is stored in the OWNER's
// partition (USER#<id>), so another user's lookup simply finds nothing and gets
// a 404 (not 403: we do not confirm that the order exists).
func (a *API) TicketURL(ctx context.Context, userID, orderID string) (TicketURL, error) {
	if err := domain.ValidateID(orderID); err != nil {
		return TicketURL{}, domain.ErrNotFound
	}
	key, found, err := a.Store.GetTicketRecord(ctx, userID, orderID)
	if err != nil {
		return TicketURL{}, err
	}
	if !found {
		return TicketURL{}, domain.ErrNotFound
	}
	ttl := a.URLTTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	url, err := a.Tickets.PresignGet(ctx, key, "ticket-"+orderID+".png", ttl)
	if err != nil {
		return TicketURL{}, err
	}
	now := a.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return TicketURL{URL: url, ExpiresAt: now().Add(ttl)}, nil
}
