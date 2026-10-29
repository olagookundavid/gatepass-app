// Package reservationsclient (orders adapter) implements ports.Holds: the HTTP
// client orders uses to call reservations' INTERNAL consume endpoint.
//
// HEXAGONAL BOUNDARY: adapter ("driven" side, outbound HTTP).
//
// Resilience decisions:
//   - a timeout per attempt (3 s): a hung dependency must not hang our request;
//   - retries ONLY on network errors and 5xx (and 429), never on 4xx: a 409
//     "hold expired" will not change by asking again;
//   - retrying consume is SAFE because it is idempotent per order id (the order id
//     is deterministic, see domain.DeterministicOrderID);
//   - bounded attempts with a small backoff: retrying forever turns one slow
//     dependency into an outage of the caller as well;
//   - traceparent is propagated by otelhttp, X-Caller-Service identifies us.
//
// The internal URL (RESERVATIONS_INTERNAL_URL) points at a cluster-internal
// Service; the endpoint has no JWT, the network is the protection.
package reservationsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/olagookundavid/gatepass-app/internal/orders/domain"
	"github.com/olagookundavid/gatepass-app/internal/orders/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/logging"
)

// Client implements ports.Holds.
type Client struct {
	BaseURL     string
	HTTP        *http.Client
	Caller      string // value of X-Caller-Service
	MaxAttempts int    // default 3
	Backoff     time.Duration
}

var _ ports.Holds = (*Client)(nil)

// New builds a client with a traced transport and a per-attempt timeout.
func New(baseURL, caller string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Caller: caller,
		HTTP: &http.Client{Timeout: 3 * time.Second, Transport: otelhttp.NewTransport(http.DefaultTransport)}}
}

type consumeResponse struct {
	HoldID         string `json:"holdId"`
	EventID        string `json:"eventId"`
	TicketTypeID   string `json:"ticketTypeId"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unitPriceCents"`
	Currency       string `json:"currency"`
}

// Consume calls POST /internal/v1/holds/{holdId}/consume.
func (c *Client) Consume(ctx context.Context, holdID, orderID, userID string) (ports.ConsumedHold, error) {
	body, _ := json.Marshal(map[string]string{"orderId": orderID, "userId": userID})
	endpoint := c.BaseURL + "/internal/v1/holds/" + url.PathEscape(holdID) + "/consume"
	attempts := c.MaxAttempts
	if attempts <= 0 {
		attempts = 3
	}
	backoff := c.Backoff
	if backoff <= 0 {
		backoff = 100 * time.Millisecond
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		hold, retryable, err := c.once(ctx, endpoint, body)
		if err == nil {
			return hold, nil
		}
		lastErr = err
		if !retryable || attempt == attempts {
			break
		}
		slog.WarnContext(ctx, "reservations consume failed, retrying", "attempt", attempt, "error", err.Error())
		select {
		case <-time.After(backoff << (attempt - 1)):
		case <-ctx.Done():
			return ports.ConsumedHold{}, ctx.Err()
		}
	}
	return ports.ConsumedHold{}, lastErr
}

// once performs one HTTP attempt. retryable says whether trying again may help.
func (c *Client) once(ctx context.Context, endpoint string, body []byte) (ports.ConsumedHold, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return ports.ConsumedHold{}, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(auth.CallerHeader, c.Caller)
	if id := logging.RequestID(ctx); id != "" {
		req.Header.Set("X-Request-Id", id)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return ports.ConsumedHold{}, true, fmt.Errorf("%w: %w", domain.ErrReservationsDown, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	switch {
	case resp.StatusCode == http.StatusOK:
		var r consumeResponse
		if err := json.Unmarshal(raw, &r); err != nil || r.HoldID == "" {
			return ports.ConsumedHold{}, false, fmt.Errorf("reservations returned an unreadable consume response")
		}
		return ports.ConsumedHold(r), false, nil
	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests:
		return ports.ConsumedHold{}, true, fmt.Errorf("%w: status %d", domain.ErrReservationsDown, resp.StatusCode)
	}
	return ports.ConsumedHold{}, false, mapClientError(resp.StatusCode, raw)
}

// mapClientError translates reservations' problem+json codes to domain errors.
func mapClientError(status int, raw []byte) error {
	var p struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(raw, &p)
	switch {
	case status == http.StatusNotFound:
		return domain.ErrHoldNotFound
	case status == http.StatusForbidden:
		return domain.ErrHoldNotYours
	case status == http.StatusConflict && p.Code == "hold_expired":
		return domain.ErrHoldExpired
	case status == http.StatusConflict && p.Code == "hold_released":
		return domain.ErrHoldReleased
	case status == http.StatusConflict:
		return domain.ErrHoldTaken
	}
	return errors.New("reservations rejected the consume request: status " + http.StatusText(status))
}
