// Package dynamodb (notifications adapter) implements ports.Store on the
// notifications DynamoDB table (on-demand).
//
// HEXAGONAL BOUNDARY: adapter.
//
// Table layout (env TABLE_NAME; key attributes PK and SK, both strings; no GSI;
// TTL attribute `ttl`):
//
//	item            PK              SK                          attributes
//	notification    USER#<userId>   NOTIF#<createdAt>#<id>       type, orderId, channel, status, reason, messageId
//	preferences     USER#<userId>   PREFS                       emailEnabled, updatedAt
//	ticket record   USER#<userId>   TICKET#<orderId>            s3Key
//	processed event EVT#<eventId>   EVT                         ttl
//
// Everything about a user lives in ONE partition, so "my notifications" is a
// single Query (SK begins_with NOTIF#) and ownership of a ticket needs no extra
// check: another user's partition simply has no such item.
// createdAt in the sort key is RFC 3339 UTC with fixed width (nanosecond digits
// padded), so lexicographic order == chronological order.
//
// IAM: dynamodb:GetItem, PutItem, Query on the table ARN.
package dynamodb

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/olagookundavid/gatepass-app/internal/notifications/domain"
	"github.com/olagookundavid/gatepass-app/internal/notifications/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/awscfg"
)

// API is the subset of *dynamodb.Client used.
type API interface {
	GetItem(ctx context.Context, in *ddb.GetItemInput, opts ...func(*ddb.Options)) (*ddb.GetItemOutput, error)
	PutItem(ctx context.Context, in *ddb.PutItemInput, opts ...func(*ddb.Options)) (*ddb.PutItemOutput, error)
	Query(ctx context.Context, in *ddb.QueryInput, opts ...func(*ddb.Options)) (*ddb.QueryOutput, error)
}

// Store implements ports.Store.
type Store struct {
	Client API
	Table  string
}

var _ ports.Store = (*Store)(nil)

type item = map[string]types.AttributeValue

func s(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func gs(it item, k string) string {
	if v, ok := it[k].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

func (st *Store) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, awscfg.DefaultCallTimeout)
}

const timeLayout = "2006-01-02T15:04:05.000000000Z"

func userPK(u string) string { return "USER#" + u }

// PutNotification appends one entry to the user's log.
func (st *Store) PutNotification(ctx context.Context, n domain.Notification) error {
	ctx, cancel := st.ctx(ctx)
	defer cancel()
	it := item{"PK": s(userPK(n.UserID)), "SK": s("NOTIF#" + n.CreatedAt.UTC().Format(timeLayout) + "#" + n.ID),
		"entity": s("NOTIF"), "id": s(n.ID), "type": s(n.Type), "orderId": s(n.OrderID), "channel": s(n.Channel),
		"status": s(n.Status), "createdAt": s(n.CreatedAt.UTC().Format(time.RFC3339Nano))}
	if n.Reason != "" {
		it["reason"] = s(n.Reason)
	}
	if n.MessageID != "" {
		it["messageId"] = s(n.MessageID)
	}
	_, err := st.Client.PutItem(ctx, &ddb.PutItemInput{TableName: &st.Table, Item: it})
	awscfg.LogThrottle(ctx, "dynamodb.PutItem", err)
	return err
}

// ListNotifications returns the newest entries first. It asks for limit+1 items
// to know whether another page exists without returning an empty last page.
// The cursor holds only the sort key: the partition is always the caller's, so a
// forged cursor cannot reach another user's data.
func (st *Store) ListNotifications(ctx context.Context, userID string, after *ports.NotificationCursor, limit int) ([]domain.Notification, *ports.NotificationCursor, error) {
	ctx, cancel := st.ctx(ctx)
	defer cancel()
	in := &ddb.QueryInput{TableName: &st.Table, ScanIndexForward: aws.Bool(false), Limit: aws.Int32(int32(limit + 1)),
		KeyConditionExpression:    aws.String("PK = :pk AND begins_with(SK, :p)"),
		ExpressionAttributeValues: item{":pk": s(userPK(userID)), ":p": s("NOTIF#")}}
	if after != nil {
		in.ExclusiveStartKey = item{"PK": s(userPK(userID)), "SK": s(after.SK)}
	}
	out, err := st.Client.Query(ctx, in)
	if err != nil {
		return nil, nil, err
	}
	items := out.Items
	var next *ports.NotificationCursor
	if len(items) > limit {
		items = items[:limit]
		next = &ports.NotificationCursor{SK: gs(items[limit-1], "SK")}
	}
	res := make([]domain.Notification, len(items))
	for i, it := range items {
		created, _ := time.Parse(time.RFC3339Nano, gs(it, "createdAt"))
		res[i] = domain.Notification{ID: gs(it, "id"), UserID: userID, Type: gs(it, "type"), OrderID: gs(it, "orderId"),
			Channel: gs(it, "channel"), Status: gs(it, "status"), Reason: gs(it, "reason"), MessageID: gs(it, "messageId"), CreatedAt: created}
	}
	return res, next, nil
}

// GetPreferences returns stored preferences or the default (e-mail enabled).
func (st *Store) GetPreferences(ctx context.Context, userID string) (domain.Preferences, error) {
	ctx, cancel := st.ctx(ctx)
	defer cancel()
	out, err := st.Client.GetItem(ctx, &ddb.GetItemInput{TableName: &st.Table, ConsistentRead: aws.Bool(true),
		Key: item{"PK": s(userPK(userID)), "SK": s("PREFS")}})
	if err != nil {
		return domain.Preferences{}, err
	}
	if out.Item == nil {
		return domain.Preferences{EmailEnabled: true}, nil
	}
	enabled := true
	if v, ok := out.Item["emailEnabled"].(*types.AttributeValueMemberBOOL); ok {
		enabled = v.Value
	}
	updated, _ := time.Parse(time.RFC3339Nano, gs(out.Item, "updatedAt"))
	return domain.Preferences{EmailEnabled: enabled, UpdatedAt: updated}, nil
}

// PutPreferences replaces the preferences item (last write wins; it is one boolean).
func (st *Store) PutPreferences(ctx context.Context, userID string, p domain.Preferences) error {
	ctx, cancel := st.ctx(ctx)
	defer cancel()
	_, err := st.Client.PutItem(ctx, &ddb.PutItemInput{TableName: &st.Table, Item: item{
		"PK": s(userPK(userID)), "SK": s("PREFS"), "entity": s("PREFS"),
		"emailEnabled": &types.AttributeValueMemberBOOL{Value: p.EmailEnabled}, "updatedAt": s(p.UpdatedAt.UTC().Format(time.RFC3339Nano))}})
	return err
}

// PutTicketRecord stores the ticket pointer in the owner's partition (overwrites: idempotent).
func (st *Store) PutTicketRecord(ctx context.Context, userID, orderID, s3Key string) error {
	ctx, cancel := st.ctx(ctx)
	defer cancel()
	_, err := st.Client.PutItem(ctx, &ddb.PutItemInput{TableName: &st.Table, Item: item{
		"PK": s(userPK(userID)), "SK": s("TICKET#" + orderID), "entity": s("TICKET"), "s3Key": s(s3Key), "orderId": s(orderID)}})
	return err
}

// GetTicketRecord looks the pointer up in the caller's own partition.
func (st *Store) GetTicketRecord(ctx context.Context, userID, orderID string) (string, bool, error) {
	ctx, cancel := st.ctx(ctx)
	defer cancel()
	out, err := st.Client.GetItem(ctx, &ddb.GetItemInput{TableName: &st.Table, ConsistentRead: aws.Bool(true),
		Key: item{"PK": s(userPK(userID)), "SK": s("TICKET#" + orderID)}})
	if err != nil || out.Item == nil {
		return "", false, err
	}
	return gs(out.Item, "s3Key"), true, nil
}

// EventProcessed checks the dedupe marker.
func (st *Store) EventProcessed(ctx context.Context, eventID string) (bool, error) {
	ctx, cancel := st.ctx(ctx)
	defer cancel()
	out, err := st.Client.GetItem(ctx, &ddb.GetItemInput{TableName: &st.Table, ConsistentRead: aws.Bool(true),
		Key: item{"PK": s("EVT#" + eventID), "SK": s("EVT")}})
	if err != nil {
		return false, err
	}
	if out.Item == nil {
		return false, nil
	}
	// TTL deletion is lazy: ignore an already-expired marker.
	if v, ok := out.Item["ttl"].(*types.AttributeValueMemberN); ok {
		if n, perr := strconv.ParseInt(v.Value, 10, 64); perr == nil && time.Now().Unix() >= n {
			return false, nil
		}
	}
	return true, nil
}

// MarkEventProcessed writes the marker with a TTL.
func (st *Store) MarkEventProcessed(ctx context.Context, eventID string, ttl time.Duration) error {
	ctx, cancel := st.ctx(ctx)
	defer cancel()
	_, err := st.Client.PutItem(ctx, &ddb.PutItemInput{TableName: &st.Table, Item: item{
		"PK": s("EVT#" + eventID), "SK": s("EVT"), "entity": s("EVT"),
		"ttl": &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Add(ttl).Unix(), 10)}}})
	return err
}
