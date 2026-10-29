// Package ports (notifications) declares the dependencies of the use cases.
//
// HEXAGONAL BOUNDARY: interfaces owned by the application; implemented by
// adapters for DynamoDB, S3, SES, Cognito and PNG rendering.
package ports

import (
	"context"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/notifications/domain"
)

// NotificationCursor is the DynamoDB LastEvaluatedKey for the log (PK is implied
// by the user; only SK is needed).
type NotificationCursor struct {
	SK string `json:"s"`
}

// Store is the DynamoDB-backed persistence port.
type Store interface {
	PutNotification(ctx context.Context, n domain.Notification) error
	ListNotifications(ctx context.Context, userID string, after *NotificationCursor, limit int) ([]domain.Notification, *NotificationCursor, error)
	GetPreferences(ctx context.Context, userID string) (domain.Preferences, error) // default: email enabled
	PutPreferences(ctx context.Context, userID string, p domain.Preferences) error
	// PutTicketRecord remembers that the ticket of orderID (owned by userID) exists
	// at s3Key. The record lives in the OWNER's partition, so only the owner can find it.
	PutTicketRecord(ctx context.Context, userID, orderID, s3Key string) error
	GetTicketRecord(ctx context.Context, userID, orderID string) (s3Key string, found bool, err error)
	EventProcessed(ctx context.Context, eventID string) (bool, error)
	MarkEventProcessed(ctx context.Context, eventID string, ttl time.Duration) error
}

// Tickets stores ticket files and creates download links (S3).
type Tickets interface {
	Put(ctx context.Context, key string, png []byte) error
	PresignGet(ctx context.Context, key, downloadName string, ttl time.Duration) (string, error)
}

// Renderer draws the ticket PNG.
type Renderer interface {
	Render(t domain.Ticket) ([]byte, error)
}

// Directory resolves a user id to an e-mail address (Cognito). The address must
// not be stored or logged by callers.
type Directory interface {
	EmailOf(ctx context.Context, userID string) (string, error) // domain.ErrUserNotFound / ErrNoEmail
}

// Mailer sends one e-mail (SES).
type Mailer interface {
	// Send returns the provider message id. domain.ErrMailThrottled means retry
	// later; domain.ErrMailRejected means it will never succeed.
	Send(ctx context.Context, to string, m domain.Email) (messageID string, err error)
}
