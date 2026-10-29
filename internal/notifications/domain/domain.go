// Package domain (notifications) holds the notification model, ticket token
// signing, and the pure functions that build the e-mails.
//
// HEXAGONAL BOUNDARY: no AWS SDK, no HTTP. Stdlib crypto (HMAC) is allowed here
// because it is pure computation.
//
// Privacy rule: an e-mail ADDRESS never appears in this package, in events, in the
// notification log or in logs. The worker fetches it from Cognito at send time,
// uses it for one SES call and drops it.
//
// AWS services touched: none.
package domain

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Notification types (one per domain event we react to).
const (
	TypeOrderPaid      = "order_paid"
	TypePaymentFailed  = "payment_failed"
	TypeOrderCancelled = "order_cancelled"
)

// Statuses of a notification attempt.
const (
	StatusSent    = "sent"
	StatusSkipped = "skipped" // user disabled e-mail
	StatusFailed  = "failed"  // permanent failure (no address, address rejected)
)

// ChannelEmail is the only channel today.
const ChannelEmail = "email"

// Notification is one row of the user's notification log.
type Notification struct {
	ID        string    `json:"id"`
	UserID    string    `json:"-"`
	Type      string    `json:"type"`
	OrderID   string    `json:"orderId"`
	Channel   string    `json:"channel"`
	Status    string    `json:"status"`
	Reason    string    `json:"reason,omitempty"`
	MessageID string    `json:"-"` // SES message id: for support, not for clients
	CreatedAt time.Time `json:"createdAt"`
}

// Preferences are per-user settings. The default (no record) is e-mail enabled.
type Preferences struct {
	EmailEnabled bool      `json:"emailEnabled"`
	UpdatedAt    time.Time `json:"updatedAt,omitempty"`
}

// Errors shared by layers.
var (
	ErrNotFound        = errors.New("not found")
	ErrUserNotFound    = errors.New("user not found in the user pool")
	ErrNoEmail         = errors.New("user has no e-mail address")
	ErrMailRejected    = errors.New("mail provider permanently rejected the message")
	ErrMailThrottled   = errors.New("mail provider throttled the request")
	ErrInvalidIdentity = errors.New("identifier has unexpected characters")
)

var safeID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// ValidateID guards ids that become part of an S3 key or a DynamoDB key.
func ValidateID(id string) error {
	if !safeID.MatchString(id) {
		return ErrInvalidIdentity
	}
	return nil
}

// TicketKey is the S3 object key of an order's ticket.
func TicketKey(userID, orderID string) (string, error) {
	if err := ValidateID(userID); err != nil {
		return "", err
	}
	if err := ValidateID(orderID); err != nil {
		return "", err
	}
	return fmt.Sprintf("tickets/%s/%s.png", userID, orderID), nil
}

// Ticket is what is printed on the PNG. Ids and numbers only.
type Ticket struct {
	OrderID  string
	EventID  string
	Quantity int
	Token    string // content of the QR code
}

// tokenPrefix versions the QR format so scanners can evolve.
const tokenPrefix = "GP1"

// SignToken builds the QR content "GP1.<orderId>.<base64url(HMAC-SHA256(key, orderId))>".
//
// Why an HMAC: the QR is shown at the door. A gate scanner that shares the key can
// check the signature OFFLINE, so a forged QR (someone typing an order id) is
// rejected without a network call. The key never leaves the worker and the gate
// service (a mounted secret, env TICKET_SIGNING_KEY_FILE).
func SignToken(key []byte, orderID string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(tokenPrefix + "." + orderID))
	return tokenPrefix + "." + orderID + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// VerifyToken checks a token and returns the order id it was issued for.
func VerifyToken(key []byte, token string) (string, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != tokenPrefix {
		return "", false
	}
	want := SignToken(key, parts[1])
	// hmac.Equal compares in constant time: a normal == would leak, byte by byte,
	// how much of a forged signature was correct.
	if !hmac.Equal([]byte(want), []byte(token)) {
		return "", false
	}
	return parts[1], true
}

// OrderEventData mirrors the `data` of OrderPaid / PaymentFailed / OrderCancelled.
type OrderEventData struct {
	OrderID      string `json:"orderId"`
	UserID       string `json:"userId"`
	HoldID       string `json:"holdId"`
	EventID      string `json:"eventId"`
	TicketTypeID string `json:"ticketTypeId"`
	Quantity     int    `json:"quantity"`
	AmountCents  int64  `json:"amountCents"`
	Currency     string `json:"currency"`
	Reason       string `json:"reason,omitempty"`
}

// TypeForEvent maps a detail-type to a notification type ("" if unhandled).
func TypeForEvent(detailType string) string {
	switch detailType {
	case "OrderPaid":
		return TypeOrderPaid
	case "PaymentFailed":
		return TypePaymentFailed
	case "OrderCancelled":
		return TypeOrderCancelled
	}
	return ""
}

// Email is a rendered message (no recipient: that is supplied at send time).
type Email struct{ Subject, Text, HTML string }

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func money(cents int64, cur string) string {
	return fmt.Sprintf("%d.%02d %s", cents/100, cents%100, cur)
}

// BuildEmail renders the message for a notification type. link is the ticket
// download URL for order_paid and is ignored otherwise. No personal data besides
// ids and amounts is used.
func BuildEmail(typ string, d OrderEventData, link string) Email {
	ref := shortID(d.OrderID)
	switch typ {
	case TypeOrderPaid:
		return Email{
			Subject: "Your Gatepass tickets (order " + ref + ")",
			Text: fmt.Sprintf("Thank you! Payment of %s for order %s was received (%d ticket(s)).\n\nDownload your ticket:\n%s\n\nThe link works for a limited time. You can always get a fresh link from the app.\n",
				money(d.AmountCents, d.Currency), ref, d.Quantity, link),
			HTML: fmt.Sprintf(`<p>Thank you! Payment of %s for order <b>%s</b> was received (%d ticket(s)).</p><p><a href="%s">Download your ticket</a></p><p>The link works for a limited time. You can always get a fresh link from the app.</p>`,
				money(d.AmountCents, d.Currency), ref, d.Quantity, link),
		}
	case TypePaymentFailed:
		return Email{
			Subject: "Payment failed for order " + ref,
			Text:    fmt.Sprintf("We could not take payment for order %s. Your tickets were released. You can place a new order from the app.\n", ref),
			HTML:    fmt.Sprintf("<p>We could not take payment for order <b>%s</b>. Your tickets were released. You can place a new order from the app.</p>", ref),
		}
	default: // order_cancelled
		return Email{
			Subject: "Order " + ref + " cancelled",
			Text:    fmt.Sprintf("Your order %s was cancelled and the tickets were released.\n", ref),
			HTML:    fmt.Sprintf("<p>Your order <b>%s</b> was cancelled and the tickets were released.</p>", ref),
		}
	}
}
