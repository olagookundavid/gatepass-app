// Package dynamodb (reservations adapter) implements ports.Store on a SINGLE
// Amazon DynamoDB table (on-demand capacity).
//
// HEXAGONAL BOUNDARY: adapter. All knowledge of keys, attribute names, condition
// expressions and DynamoDB error shapes is here.
//
// Single-table layout (env TABLE_NAME). Key attributes are strings PK and SK;
// there is one global secondary index GSI1 (GSI1PK, GSI1SK) with projection ALL.
//
//	entity       PK                      SK       other attributes
//	inventory    EVENT#<eventId>         TT#<ttId> capacity, available, priceCents, currency, version
//	hold         HOLD#<holdId>           HOLD      userId, eventId, ticketTypeId, quantity, unitPriceCents,
//	                                               currency, status, expiresAt, createdAt, orderId?, ttl,
//	                                               GSI1PK=USER#<userId>, GSI1SK=<createdAt RFC3339>#<holdId>
//	idempotency  IDEMP#<userId>#<key>    IDEMP     requestHash, hold (JSON snapshot), ttl
//	processed    EVT#<eventId>           EVT       ttl
//	release mark REL#<holdId>            REL       ttl   (only for holds whose row TTL already deleted)
//
// TTL attribute: `ttl` (epoch seconds) on hold, idempotency, processed-event and
// release-marker items. Deliberate deviation from "expiresAt is the TTL
// attribute": a hold's logical expiry (`expiresAt`, 10 minutes) must stay
// readable, while its TTL is moved far into the future once the hold is consumed
// or released. If TTL were `expiresAt`, DynamoDB would delete CONSUMED holds
// ~10 minutes after creation and a later OrderCancelled event could no longer
// find the hold to return its tickets.
//
// Why every multi-item change is a TransactWriteItems: DynamoDB conditions apply
// per item. A transaction makes "decrement the counter AND create the hold AND
// remember the idempotency key" all-or-nothing, so a crash or a lost race can
// never leave a decremented counter without a hold (a leak) or a hold without a
// decrement (an oversell).
//
// IAM (table + index): dynamodb:GetItem, PutItem, UpdateItem, Query on
// arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME> and
// .../table/<TABLE_NAME>/index/GSI1 (for Query). UNVERIFIED: DynamoDB
// transactions are authorised by the IAM actions of the items they contain
// (here PutItem and UpdateItem); to my knowledge there is no separate
// TransactWriteItems IAM action. Confirm with the first real call.
package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/olagookundavid/gatepass-app/internal/platform/awscfg"
	"github.com/olagookundavid/gatepass-app/internal/reservations/domain"
	"github.com/olagookundavid/gatepass-app/internal/reservations/ports"
)

// API is the subset of *dynamodb.Client used.
type API interface {
	GetItem(ctx context.Context, in *ddb.GetItemInput, opts ...func(*ddb.Options)) (*ddb.GetItemOutput, error)
	PutItem(ctx context.Context, in *ddb.PutItemInput, opts ...func(*ddb.Options)) (*ddb.PutItemOutput, error)
	UpdateItem(ctx context.Context, in *ddb.UpdateItemInput, opts ...func(*ddb.Options)) (*ddb.UpdateItemOutput, error)
	Query(ctx context.Context, in *ddb.QueryInput, opts ...func(*ddb.Options)) (*ddb.QueryOutput, error)
	TransactWriteItems(ctx context.Context, in *ddb.TransactWriteItemsInput, opts ...func(*ddb.Options)) (*ddb.TransactWriteItemsOutput, error)
}

// Store implements ports.Store.
type Store struct {
	Client API
	Table  string
}

var _ ports.Store = (*Store)(nil)

const (
	// RetainConsumed is how long a consumed hold row is kept (orders may still
	// cancel/refund within this period and need the hold to return tickets).
	RetainConsumed = 30 * 24 * time.Hour
	// RetainReleased is how long a released hold row is kept for audit/idempotency.
	RetainReleased = 7 * 24 * time.Hour
)

type item = map[string]types.AttributeValue

func str(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func num(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}
func numI(v int) types.AttributeValue { return num(int64(v)) }
func key(pk, sk string) item          { return item{"PK": str(pk), "SK": str(sk)} }
func holdKey(id string) item          { return key("HOLD#"+id, "HOLD") }
func invKey(e, t string) item         { return key("EVENT#"+e, "TT#"+t) }
func gstr(it item, k string) string {
	if v, ok := it[k].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}
func gnum(it item, k string) int64 {
	if v, ok := it[k].(*types.AttributeValueMemberN); ok {
		n, _ := strconv.ParseInt(v.Value, 10, 64)
		return n
	}
	return 0
}

func (s *Store) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, awscfg.DefaultCallTimeout)
}

// ---- (de)serialisation ----------------------------------------------------------

func invFromItem(it item) domain.Inventory {
	return domain.Inventory{EventID: gstr(it, "eventId"), TicketTypeID: gstr(it, "ticketTypeId"),
		Capacity: int(gnum(it, "capacity")), Available: int(gnum(it, "available")),
		PriceCents: gnum(it, "priceCents"), Currency: gstr(it, "currency"), Version: int(gnum(it, "version"))}
}

func holdToItem(h domain.Hold, ttl time.Time) item {
	it := key("HOLD#"+h.ID, "HOLD")
	it["entity"] = str("HOLD")
	it["holdId"], it["userId"], it["eventId"], it["ticketTypeId"] = str(h.ID), str(h.UserID), str(h.EventID), str(h.TicketTypeID)
	it["quantity"], it["unitPriceCents"], it["currency"] = numI(h.Quantity), num(h.UnitPriceCents), str(h.Currency)
	it["status"] = str(string(h.Status))
	it["expiresAt"], it["createdAt"] = num(h.ExpiresAt.Unix()), num(h.CreatedAt.Unix())
	it["ttl"] = num(ttl.Unix())
	it["GSI1PK"] = str("USER#" + h.UserID)
	it["GSI1SK"] = str(h.CreatedAt.UTC().Format(time.RFC3339) + "#" + h.ID)
	if h.OrderID != "" {
		it["orderId"] = str(h.OrderID)
	}
	return it
}

// HoldFromItem converts a DynamoDB item (also used by the Lambda for stream images).
func HoldFromItem(it item) domain.Hold {
	return domain.Hold{ID: gstr(it, "holdId"), UserID: gstr(it, "userId"), EventID: gstr(it, "eventId"),
		TicketTypeID: gstr(it, "ticketTypeId"), Quantity: int(gnum(it, "quantity")),
		UnitPriceCents: gnum(it, "unitPriceCents"), Currency: gstr(it, "currency"),
		Status: domain.HoldStatus(gstr(it, "status")), ExpiresAt: time.Unix(gnum(it, "expiresAt"), 0).UTC(),
		CreatedAt: time.Unix(gnum(it, "createdAt"), 0).UTC(), OrderID: gstr(it, "orderId")}
}

// ---- error helpers --------------------------------------------------------------

func isCondFail(err error) bool {
	var c *types.ConditionalCheckFailedException
	return errors.As(err, &c)
}

// cancelCodes returns the per-item cancellation codes of a failed transaction
// ("ConditionalCheckFailed", "TransactionConflict", "None", ...), or nil.
func cancelCodes(err error) []string {
	var tc *types.TransactionCanceledException
	if !errors.As(err, &tc) {
		return nil
	}
	codes := make([]string, len(tc.CancellationReasons))
	for i, r := range tc.CancellationReasons {
		codes[i] = aws.ToString(r.Code)
	}
	return codes
}

func at(codes []string, i int, code string) bool { return i < len(codes) && codes[i] == code }

// transact runs a TransactWriteItems and retries it when DynamoDB cancels it
// because of contention.
//
// Why this matters (and why DynamoDB Local does not show it): when many
// transactions touch the SAME item at once (50 buyers racing for the last
// tickets), real DynamoDB does not queue them. Losing transactions are cancelled
// immediately with CancellationReason "TransactionConflict" - a signal that
// means "try again", not "your conditions failed". The AWS SDK does not retry
// TransactionCanceledException on its own, so we do, with exponential backoff
// and jitter (random wait, so the losers do not collide again in lockstep).
// The retry is bounded; if contention persists we return domain.ErrContention
// (HTTP 503 + Retry-After) rather than blocking the caller for long.
// Retrying is safe: either the transaction was cancelled (nothing applied) or,
// for CreateHold, ClientRequestToken makes a duplicate a no-op.
func (s *Store) transact(ctx context.Context, in *ddb.TransactWriteItemsInput) error {
	var err error
	for attempt := 0; attempt < 8; attempt++ {
		_, err = s.Client.TransactWriteItems(ctx, in)
		if err == nil || !isContention(err) {
			return err
		}
		backoff := min(10*time.Millisecond<<attempt, 250*time.Millisecond)
		select {
		case <-time.After(backoff/2 + rand.N(backoff/2+1)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("%w: %w", domain.ErrContention, err)
}

// isContention: cancelled because another transaction holds an item, or throttled.
func isContention(err error) bool {
	for _, c := range cancelCodes(err) {
		if c == "TransactionConflict" || c == "ThrottlingError" || c == "ProvisionedThroughputExceeded" {
			return true
		}
	}
	return false
}

// ---- inventory ------------------------------------------------------------------

// GetInventory reads one counter. ConsistentRead: after a sale the next reader
// should see the new number (eventually consistent reads can lag ~1 s).
//
// IAM: dynamodb:GetItem. Failure: ProvisionedThroughputExceeded cannot happen on
// on-demand tables except at extreme spikes (ThrottlingException).
func (s *Store) GetInventory(ctx context.Context, eventID, ticketTypeID string) (domain.Inventory, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	out, err := s.Client.GetItem(ctx, &ddb.GetItemInput{TableName: &s.Table, Key: invKey(eventID, ticketTypeID), ConsistentRead: aws.Bool(true)})
	if err != nil {
		awscfg.LogThrottle(ctx, "dynamodb.GetItem", err)
		return domain.Inventory{}, err
	}
	if out.Item == nil {
		return domain.Inventory{}, domain.ErrNotFound
	}
	return invFromItem(out.Item), nil
}

// ListInventory queries all counters of an event (one partition, SK prefix TT#).
// IAM: dynamodb:Query.
func (s *Store) ListInventory(ctx context.Context, eventID string) ([]domain.Inventory, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	out, err := s.Client.Query(ctx, &ddb.QueryInput{TableName: &s.Table, ConsistentRead: aws.Bool(true),
		KeyConditionExpression:    aws.String("PK = :pk AND begins_with(SK, :sk)"),
		ExpressionAttributeValues: item{":pk": str("EVENT#" + eventID), ":sk": str("TT#")}})
	if err != nil {
		return nil, err
	}
	res := make([]domain.Inventory, 0, len(out.Items))
	for _, it := range out.Items {
		res = append(res, invFromItem(it))
	}
	return res, nil
}

// CreateInventoryIfAbsent: PutItem with attribute_not_exists(PK). The condition
// is what makes redelivery of EventPublished harmless: a second Put would
// otherwise RESET `available` to capacity and re-sell tickets already sold.
// IAM: dynamodb:PutItem. A failed condition is the normal "already exists" answer.
func (s *Store) CreateInventoryIfAbsent(ctx context.Context, inv domain.Inventory) (bool, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	it := invKey(inv.EventID, inv.TicketTypeID)
	it["entity"] = str("INV")
	it["eventId"], it["ticketTypeId"] = str(inv.EventID), str(inv.TicketTypeID)
	it["capacity"], it["available"], it["priceCents"], it["currency"], it["version"] = numI(inv.Capacity), numI(inv.Available), num(inv.PriceCents), str(inv.Currency), numI(1)
	_, err := s.Client.PutItem(ctx, &ddb.PutItemInput{TableName: &s.Table, Item: it, ConditionExpression: aws.String("attribute_not_exists(PK)")})
	if isCondFail(err) {
		return false, nil
	}
	return err == nil, err
}

// ---- holds ----------------------------------------------------------------------

// CreateHold is the heart of the service: ONE transaction, three items.
//
//  0. Update inventory:  available = available - :q   IF available >= :q
//  1. Put hold:          IF the hold id does not exist
//  2. Put idempotency:   IF the (user,key) does not exist
//
// All three succeed or none does. Under 50 concurrent requests for the last 10
// tickets, DynamoDB serialises conflicting writers on the counter item, and the
// condition `available >= :q` is evaluated against the latest committed value, so
// exactly 10 succeed (an UPDATE with a condition is a compare-and-set; reading
// the number and then writing it back would let two requests both see "1 left").
//
// ClientRequestToken makes SDK-level retries of THIS transaction safe: if a
// timeout hides a success and the SDK retries, DynamoDB recognises the token and
// does not apply the transaction twice (valid for 10 minutes).
//
// IAM: dynamodb:UpdateItem + dynamodb:PutItem (transactions are authorised per item).
// Failure shapes: TransactionCanceledException with CancellationReasons[i]:
// ConditionalCheckFailed (our conditions), TransactionConflict (another
// transaction is touching an item right now; safe to retry), ThrottlingError.
func (s *Store) CreateHold(ctx context.Context, h domain.Hold, idemKey string, rec ports.IdempotencyRecord, idemTTL time.Duration) error {
	snapshot, err := json.Marshal(rec.Hold)
	if err != nil {
		return err
	}
	now := h.CreatedAt
	idem := key("IDEMP#"+h.UserID+"#"+idemKey, "IDEMP")
	idem["entity"], idem["requestHash"], idem["hold"] = str("IDEMP"), str(rec.RequestHash), str(string(snapshot))
	idem["ttl"] = num(now.Add(idemTTL).Unix())

	ctx, cancel := s.ctx(ctx)
	defer cancel()
	err = s.transact(ctx, &ddb.TransactWriteItemsInput{
		ClientRequestToken: aws.String(h.ID),
		TransactItems: []types.TransactWriteItem{
			{Update: &types.Update{TableName: &s.Table, Key: invKey(h.EventID, h.TicketTypeID),
				UpdateExpression:          aws.String("SET available = available - :q, version = version + :one"),
				ConditionExpression:       aws.String("attribute_exists(PK) AND available >= :q"),
				ExpressionAttributeValues: item{":q": numI(h.Quantity), ":one": numI(1)}}},
			{Put: &types.Put{TableName: &s.Table, Item: holdToItem(h, h.ExpiresAt), ConditionExpression: aws.String("attribute_not_exists(PK)")}},
			{Put: &types.Put{TableName: &s.Table, Item: idem, ConditionExpression: aws.String("attribute_not_exists(PK)")}},
		}})
	if err == nil {
		return nil
	}
	awscfg.LogThrottle(ctx, "dynamodb.TransactWriteItems", err)
	codes := cancelCodes(err)
	switch {
	case at(codes, 2, "ConditionalCheckFailed"):
		return domain.ErrIdempotencyInUse
	case at(codes, 0, "ConditionalCheckFailed"):
		return domain.ErrSoldOut
	}
	return err
}

// GetHold reads a hold row (consistent).
func (s *Store) GetHold(ctx context.Context, holdID string) (domain.Hold, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	out, err := s.Client.GetItem(ctx, &ddb.GetItemInput{TableName: &s.Table, Key: holdKey(holdID), ConsistentRead: aws.Bool(true)})
	if err != nil {
		return domain.Hold{}, err
	}
	if out.Item == nil {
		return domain.Hold{}, domain.ErrNotFound
	}
	return HoldFromItem(out.Item), nil
}

// GetIdempotency loads the remembered outcome for (user, key).
func (s *Store) GetIdempotency(ctx context.Context, userID, idemKey string) (ports.IdempotencyRecord, bool, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	out, err := s.Client.GetItem(ctx, &ddb.GetItemInput{TableName: &s.Table, Key: key("IDEMP#"+userID+"#"+idemKey, "IDEMP"), ConsistentRead: aws.Bool(true)})
	if err != nil || out.Item == nil {
		return ports.IdempotencyRecord{}, false, err
	}
	// DynamoDB TTL deletion is lazy: an expired record may still be readable. Treat
	// it as absent so the 24 h contract holds.
	if gnum(out.Item, "ttl") != 0 && time.Now().Unix() >= gnum(out.Item, "ttl") {
		return ports.IdempotencyRecord{}, false, nil
	}
	var h domain.Hold
	if err := json.Unmarshal([]byte(gstr(out.Item, "hold")), &h); err != nil {
		return ports.IdempotencyRecord{}, false, fmt.Errorf("corrupt idempotency record: %w", err)
	}
	h.UserID = userID // json:"-" on UserID, restore it
	return ports.IdempotencyRecord{RequestHash: gstr(out.Item, "requestHash"), Hold: h}, true, nil
}

// ListUserHolds pages through GSI1 (newest first).
//
// A GSI query is eventually consistent (the index trails the table by
// milliseconds): a hold created a moment ago may be missing from this list.
// Fine for a "my holds" screen; the hold id returned by POST /holds is the
// authoritative handle.
//
// Pagination: we ask for limit+1 items so we know whether another page exists
// without returning an empty last page (LastEvaluatedKey alone is present even
// when nothing is left). The cursor is built from the last item we return.
// The cursor is untrusted client input: we require it to point inside THIS
// user's partition before using it as ExclusiveStartKey.
func (s *Store) ListUserHolds(ctx context.Context, userID string, after *ports.HoldCursor, limit int) ([]domain.Hold, *ports.HoldCursor, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	in := &ddb.QueryInput{TableName: &s.Table, IndexName: aws.String("GSI1"), ScanIndexForward: aws.Bool(false),
		Limit:                     aws.Int32(int32(limit + 1)),
		KeyConditionExpression:    aws.String("GSI1PK = :u"),
		ExpressionAttributeValues: item{":u": str("USER#" + userID)}}
	if after != nil {
		if after.GSI1PK != "USER#"+userID || !strings.HasPrefix(after.PK, "HOLD#") || after.SK != "HOLD" {
			return nil, nil, errors.New("cursor does not belong to this user")
		}
		in.ExclusiveStartKey = item{"PK": str(after.PK), "SK": str(after.SK), "GSI1PK": str(after.GSI1PK), "GSI1SK": str(after.GSI1SK)}
	}
	out, err := s.Client.Query(ctx, in)
	if err != nil {
		return nil, nil, err
	}
	items := out.Items
	var next *ports.HoldCursor
	if len(items) > limit {
		items = items[:limit]
		last := items[limit-1]
		next = &ports.HoldCursor{PK: gstr(last, "PK"), SK: gstr(last, "SK"), GSI1PK: gstr(last, "GSI1PK"), GSI1SK: gstr(last, "GSI1SK")}
	}
	holds := make([]domain.Hold, len(items))
	for i, it := range items {
		holds[i] = HoldFromItem(it)
	}
	return holds, next, nil
}

// ConsumeHold is a single conditional UpdateItem (compare-and-set on `status`).
//
// The condition lists everything that must be true AT WRITE TIME: the row exists,
// is active, belongs to the caller and has not expired. If two orders race for
// the same hold, DynamoDB lets exactly one UPDATE through.
// On a failed condition we read the row to say WHY (domain.ConsumeDecision), which
// also implements idempotency: a retry with the same orderId finds the hold
// already consumed by that order and succeeds.
// IAM: dynamodb:UpdateItem, dynamodb:GetItem.
func (s *Store) ConsumeHold(ctx context.Context, holdID, orderID, userID string, now time.Time) (domain.Hold, error) {
	cctx, cancel := s.ctx(ctx)
	defer cancel()
	out, err := s.Client.UpdateItem(cctx, &ddb.UpdateItemInput{TableName: &s.Table, Key: holdKey(holdID),
		UpdateExpression:         aws.String("SET #st = :consumed, orderId = :o, #ttl = :ttl"),
		ConditionExpression:      aws.String("attribute_exists(PK) AND #st = :active AND userId = :u AND expiresAt > :now"),
		ExpressionAttributeNames: map[string]string{"#st": "status", "#ttl": "ttl"},
		ExpressionAttributeValues: item{":consumed": str(string(domain.StatusConsumed)), ":active": str(string(domain.StatusActive)),
			":o": str(orderID), ":u": str(userID), ":now": num(now.Unix()), ":ttl": num(now.Add(RetainConsumed).Unix())},
		ReturnValues: types.ReturnValueAllNew})
	if err == nil {
		return HoldFromItem(out.Attributes), nil
	}
	if !isCondFail(err) {
		awscfg.LogThrottle(ctx, "dynamodb.UpdateItem", err)
		return domain.Hold{}, err
	}
	h, gerr := s.GetHold(ctx, holdID)
	if gerr != nil {
		return domain.Hold{}, gerr // includes ErrNotFound
	}
	outcome, derr := domain.ConsumeDecision(h, orderID, userID, now)
	if derr != nil {
		return domain.Hold{}, derr
	}
	if outcome == domain.AlreadyConsumedBySame {
		return h, nil
	}
	return domain.Hold{}, errors.New("consume condition failed but hold looks consumable (concurrent change); retry")
}

// ReleaseHold flips the hold to released and returns the tickets, atomically.
//
//  0. Update hold:      status = released   IF status IN (from...)
//  1. Update inventory: available += qty    IF available <= capacity - qty
//
// The first condition is the idempotency marker: only the one transaction that
// moves the hold OUT of `from` can ever apply the increment, so duplicate
// triggers (user DELETE + Scheduler + OrderCancelled + TTL stream) release once.
// The second condition is a safety net: never let `available` exceed capacity.
func (s *Store) ReleaseHold(ctx context.Context, holdID string, from []domain.HoldStatus, now time.Time) (bool, error) {
	h, err := s.GetHold(ctx, holdID)
	if err != nil {
		return false, err
	}
	allowed := false
	for _, f := range from {
		allowed = allowed || h.Status == f
	}
	if !allowed {
		return false, nil
	}
	inv, err := s.GetInventory(ctx, h.EventID, h.TicketTypeID)
	if err != nil {
		return false, err
	}

	names := map[string]string{"#st": "status", "#ttl": "ttl"}
	vals := item{":released": str(string(domain.StatusReleased)), ":ttl": num(now.Add(RetainReleased).Unix())}
	var placeholders []string
	for i, f := range from {
		p := ":f" + strconv.Itoa(i)
		vals[p] = str(string(f))
		placeholders = append(placeholders, p)
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	err = s.transact(ctx, &ddb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Update: &types.Update{TableName: &s.Table, Key: holdKey(holdID),
			UpdateExpression:          aws.String("SET #st = :released, #ttl = :ttl"),
			ConditionExpression:       aws.String("attribute_exists(PK) AND #st IN (" + strings.Join(placeholders, ", ") + ")"),
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: vals}},
		{Update: &types.Update{TableName: &s.Table, Key: invKey(h.EventID, h.TicketTypeID),
			UpdateExpression:          aws.String("SET available = available + :q, version = version + :one"),
			ConditionExpression:       aws.String("attribute_exists(PK) AND available <= :maxBefore"),
			ExpressionAttributeValues: item{":q": numI(h.Quantity), ":one": numI(1), ":maxBefore": numI(inv.Capacity - h.Quantity)}}},
	}})
	if err == nil {
		return true, nil
	}
	codes := cancelCodes(err)
	switch {
	case at(codes, 0, "ConditionalCheckFailed"):
		return false, nil // someone else already moved the hold: nothing to do
	case at(codes, 1, "ConditionalCheckFailed"):
		return false, fmt.Errorf("refusing to release hold %s: inventory would exceed capacity (data inconsistency)", holdID)
	}
	return false, err
}

// ReleaseDeletedHold returns tickets for a hold whose row is already gone (TTL
// removed it; we only know its last image from the DynamoDB stream).
//
//  0. Put marker REL#<holdId>:  IF not exists      (the idempotency guard)
//  1. Update inventory:         available += qty   IF available <= capacity - qty
func (s *Store) ReleaseDeletedHold(ctx context.Context, h domain.Hold, now time.Time) (bool, error) {
	inv, err := s.GetInventory(ctx, h.EventID, h.TicketTypeID)
	if err != nil {
		return false, err
	}
	marker := key("REL#"+h.ID, "REL")
	marker["entity"], marker["ttl"] = str("REL"), num(now.Add(RetainReleased).Unix())

	ctx, cancel := s.ctx(ctx)
	defer cancel()
	err = s.transact(ctx, &ddb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{TableName: &s.Table, Item: marker, ConditionExpression: aws.String("attribute_not_exists(PK)")}},
		{Update: &types.Update{TableName: &s.Table, Key: invKey(h.EventID, h.TicketTypeID),
			UpdateExpression:          aws.String("SET available = available + :q, version = version + :one"),
			ConditionExpression:       aws.String("attribute_exists(PK) AND available <= :maxBefore"),
			ExpressionAttributeValues: item{":q": numI(h.Quantity), ":one": numI(1), ":maxBefore": numI(inv.Capacity - h.Quantity)}}},
	}})
	if err == nil {
		return true, nil
	}
	codes := cancelCodes(err)
	switch {
	case at(codes, 0, "ConditionalCheckFailed"):
		return false, nil
	case at(codes, 1, "ConditionalCheckFailed"):
		return false, fmt.Errorf("refusing to release deleted hold %s: inventory would exceed capacity", h.ID)
	}
	return false, err
}

// ---- processed events -------------------------------------------------------------

// EventProcessed reports whether the marker for eventId exists.
func (s *Store) EventProcessed(ctx context.Context, eventID string) (bool, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	out, err := s.Client.GetItem(ctx, &ddb.GetItemInput{TableName: &s.Table, Key: key("EVT#"+eventID, "EVT"), ConsistentRead: aws.Bool(true)})
	if err != nil {
		return false, err
	}
	return out.Item != nil, nil
}

// MarkEventProcessed writes the marker (7-day TTL: the longest SQS/EventBridge
// redelivery horizon we care about is far shorter).
func (s *Store) MarkEventProcessed(ctx context.Context, eventID string, ttl time.Duration) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	it := key("EVT#"+eventID, "EVT")
	it["entity"], it["ttl"] = str("EVT"), num(time.Now().Add(ttl).Unix())
	_, err := s.Client.PutItem(ctx, &ddb.PutItemInput{TableName: &s.Table, Item: it})
	return err
}
