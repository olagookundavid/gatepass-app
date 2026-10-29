//go:build integration

// Integration tests for the reservations adapters.
//
// Required environment (each test names what it needs and skips otherwise):
//
//	RES_IT_DYNAMODB=true + AWS_REGION    run against DynamoDB. The tests CREATE a temporary table
//	                                     (and delete it), so the role needs dynamodb:CreateTable,
//	                                     DescribeTable, DeleteTable plus the data actions.
//	                                     For DynamoDB Local set AWS_ENDPOINT_URL_DYNAMODB=http://localhost:8000
//	                                     and dummy credentials.
//	TEST_REDIS_ADDR (+TEST_REDIS_TLS=true) for the rate-limit test
//	RES_IT_SCHEDULER_GROUP, RES_IT_SCHEDULER_ROLE_ARN, RES_IT_TARGET_LAMBDA_ARN, AWS_REGION
//	                                     for the schedule create/delete test (real EventBridge Scheduler)
package integration

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awsscheduler "github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/olagookundavid/gatepass-app/internal/platform/awscfg"
	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
	"github.com/olagookundavid/gatepass-app/internal/platform/testenv"
	rddb "github.com/olagookundavid/gatepass-app/internal/reservations/adapters/dynamodb"
	rlambda "github.com/olagookundavid/gatepass-app/internal/reservations/adapters/lambda"
	rredis "github.com/olagookundavid/gatepass-app/internal/reservations/adapters/redis"
	rsched "github.com/olagookundavid/gatepass-app/internal/reservations/adapters/scheduler"
	"github.com/olagookundavid/gatepass-app/internal/reservations/app"
	"github.com/olagookundavid/gatepass-app/internal/reservations/domain"
	"github.com/olagookundavid/gatepass-app/internal/reservations/ports"
)

var ctx = context.Background()

type noSched struct{}

func (noSched) ScheduleExpiry(context.Context, string, time.Time) error { return nil }
func (noSched) CancelExpiry(context.Context, string) error              { return nil }

type allow struct{}

func (allow) Allow(context.Context, string, int, time.Duration) (bool, error) { return true, nil }

// newTable creates a temporary single table with GSI1 and returns a Store on it.
func newTable(t *testing.T) *rddb.Store {
	t.Helper()
	env := testenv.Need(t, "RES_IT_DYNAMODB", "AWS_REGION")
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg)
	name := "gatepass-" + testenv.Prefix()
	s := func(n string) types.AttributeDefinition {
		return types.AttributeDefinition{AttributeName: aws.String(n), AttributeType: types.ScalarAttributeTypeS}
	}
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(name), BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{s("PK"), s("SK"), s("GSI1PK"), s("GSI1SK")},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange}},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{IndexName: aws.String("GSI1"), Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("GSI1PK"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("GSI1SK"), KeyType: types.KeyTypeRange}}}},
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { _, _ = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(name)}) })
	for i := 0; i < 60; i++ {
		d, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		if err == nil && d.Table.TableStatus == types.TableStatusActive {
			break
		}
		time.Sleep(time.Second)
	}
	return &rddb.Store{Client: client, Table: name}
}

func newService(store ports.Store, sched ports.Scheduler, lim ports.RateLimiter) *app.Service {
	return app.New(app.Service{Store: store, Scheduler: sched, Limiter: lim})
}

func seed(t *testing.T, store ports.Store, capacity int) (eventID, ttID string) {
	t.Helper()
	eventID, ttID = uuid.NewString(), uuid.NewString()
	created, err := store.CreateInventoryIfAbsent(ctx, domain.Inventory{EventID: eventID, TicketTypeID: ttID, Capacity: capacity, Available: capacity, PriceCents: 1234, Currency: "USD", Version: 1})
	if err != nil || !created {
		t.Fatalf("seed: %v %v", created, err)
	}
	return
}

func available(t *testing.T, store ports.Store, e, tt string) int {
	t.Helper()
	inv, err := store.GetInventory(ctx, e, tt)
	if err != nil {
		t.Fatal(err)
	}
	return inv.Available
}

func TestFiftyConcurrentHoldsAgainstTenTickets(t *testing.T) {
	store := newTable(t)
	svc := newService(store, noSched{}, allow{})
	e, tt := seed(t, store, 10)

	var ok, soldOut atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.CreateHold(ctx, fmt.Sprintf("user-%d", i), uuid.NewString(), app.CreateHoldInput{EventID: e, TicketTypeID: tt, Quantity: 1})
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, domain.ErrSoldOut):
				soldOut.Add(1)
			default:
				// A TransactionConflict under heavy contention is allowed to surface as a
				// retryable error; it must never oversell.
				t.Logf("non-oversell error: %v", err)
			}
		}()
	}
	wg.Wait()
	if ok.Load() > 10 {
		t.Fatalf("OVERSOLD: %d holds succeeded for 10 tickets", ok.Load())
	}
	if int(ok.Load()) != 10-available(t, store, e, tt) {
		t.Fatalf("counter (%d) does not match successful holds (%d)", available(t, store, e, tt), ok.Load())
	}
	// With on-demand tables and retries, all 10 should be sold.
	if ok.Load() != 10 {
		t.Fatalf("expected exactly 10 successes, got %d (sold out: %d)", ok.Load(), soldOut.Load())
	}
}

func TestIdempotentCreateAndConsume(t *testing.T) {
	store := newTable(t)
	svc := newService(store, noSched{}, allow{})
	e, tt := seed(t, store, 10)
	in := app.CreateHoldInput{EventID: e, TicketTypeID: tt, Quantity: 2}
	key := uuid.NewString()

	first, err := svc.CreateHold(ctx, "u1", key, in)
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.CreateHold(ctx, "u1", key, in)
	if err != nil || !again.Replayed || again.Hold.ID != first.Hold.ID {
		t.Fatalf("replay: %+v %v", again, err)
	}
	if got := available(t, store, e, tt); got != 8 {
		t.Fatalf("available = %d, want 8 (replay must not decrement again)", got)
	}
	in.Quantity = 3
	if _, err := svc.CreateHold(ctx, "u1", key, in); !errors.Is(err, app.ErrIdempotencyConflict) {
		t.Fatalf("different body, same key: %v", err)
	}

	order1, order2 := uuid.NewString(), uuid.NewString()
	h, err := svc.Consume(ctx, first.Hold.ID, order1, "u1")
	if err != nil || h.UnitPriceCents != 1234 {
		t.Fatalf("consume: %+v %v", h, err)
	}
	if _, err := svc.Consume(ctx, first.Hold.ID, order1, "u1"); err != nil {
		t.Fatalf("same orderId again must succeed: %v", err)
	}
	if _, err := svc.Consume(ctx, first.Hold.ID, order2, "u1"); !errors.Is(err, domain.ErrAlreadyConsumed) {
		t.Fatalf("different orderId must conflict: %v", err)
	}
	if _, err := svc.Consume(ctx, first.Hold.ID, order1, "intruder"); !errors.Is(err, domain.ErrWrongOwner) {
		t.Fatalf("other user: %v", err)
	}
}

func TestExpiredHoldCannotBeConsumedAndExpiryReleasesOnce(t *testing.T) {
	store := newTable(t)
	e, tt := seed(t, store, 5)
	past := time.Now().UTC().Add(-time.Hour)
	svc := app.New(app.Service{Store: store, Scheduler: noSched{}, Limiter: allow{}, Now: func() time.Time { return past }})
	r, err := svc.CreateHold(ctx, "u1", uuid.NewString(), app.CreateHoldInput{EventID: e, TicketTypeID: tt, Quantity: 3})
	if err != nil {
		t.Fatal(err)
	}
	// the hold was created "an hour ago" with a 10 minute TTL, so it is expired NOW
	now := app.New(app.Service{Store: store, Scheduler: noSched{}, Limiter: allow{}})
	if _, err := now.Consume(ctx, r.Hold.ID, uuid.NewString(), "u1"); !errors.Is(err, domain.ErrHoldExpired) {
		t.Fatalf("consume expired: %v", err)
	}

	// Lambda handler, scheduler shape, twice
	h := &rlambda.Handler{Svc: now, Emit: func(context.Context, string, int) {}}
	payload, _ := json.Marshal(rlambda.SchedulerInput{HoldID: r.Hold.ID})
	for i := 0; i < 2; i++ {
		if _, err := h.Handle(ctx, payload); err != nil {
			t.Fatal(err)
		}
	}
	if got := available(t, store, e, tt); got != 5 {
		t.Fatalf("available = %d, want 5 (released exactly once)", got)
	}
}

func TestStreamShapeAgainstRealTable(t *testing.T) {
	store := newTable(t)
	e, tt := seed(t, store, 5)
	svc := newService(store, noSched{}, allow{})
	r, err := svc.CreateHold(ctx, "u1", uuid.NewString(), app.CreateHoldInput{EventID: e, TicketTypeID: tt, Quantity: 2})
	if err != nil {
		t.Fatal(err)
	}
	// Synthesise the stream record DynamoDB would deliver for a TTL deletion of this hold.
	rec := fmt.Sprintf(`{"Records":[{"eventID":"1","eventName":"REMOVE","eventSource":"aws:dynamodb",
	 "userIdentity":{"type":"Service","principalId":"dynamodb.amazonaws.com"},
	 "dynamodb":{"SequenceNumber":"1","OldImage":{"PK":{"S":"HOLD#%s"},"SK":{"S":"HOLD"},"holdId":{"S":"%s"},"userId":{"S":"u1"},
	 "eventId":{"S":"%s"},"ticketTypeId":{"S":"%s"},"quantity":{"N":"2"},"status":{"S":"active"},"expiresAt":{"N":"%d"},"createdAt":{"N":"%d"}}}}]}`,
		r.Hold.ID, r.Hold.ID, e, tt, r.Hold.ExpiresAt.Unix(), r.Hold.CreatedAt.Unix())
	h := &rlambda.Handler{Svc: svc, Emit: func(context.Context, string, int) {}}
	for i := 0; i < 3; i++ { // duplicate stream delivery must release once (marker item)
		if _, err := h.Handle(ctx, json.RawMessage(rec)); err != nil {
			t.Fatal(err)
		}
	}
	// the table row still exists in this synthetic test, so also run the scheduler path: still once overall
	if got := available(t, store, e, tt); got != 5 {
		t.Fatalf("available = %d, want 5", got)
	}
}

func TestConsumerHandlesSameEventPublishedTwice(t *testing.T) {
	store := newTable(t)
	svc := newService(store, noSched{}, allow{})
	e, tt := uuid.NewString(), uuid.NewString()
	data, _ := json.Marshal(map[string]any{"eventId": e, "title": "x", "venueId": uuid.NewString(),
		"ticketTypes": []map[string]any{{"id": tt, "name": "GA", "priceCents": 500, "currency": "USD", "capacity": 20}}})
	ev := ebpub.Received{Detail: ebpub.Detail{EventID: uuid.NewString(), EventType: "EventPublished", Data: data}}

	if err := svc.HandleEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateHold(ctx, "u1", uuid.NewString(), app.CreateHoldInput{EventID: e, TicketTypeID: tt, Quantity: 4}); err != nil {
		t.Fatal(err)
	}
	if err := svc.HandleEvent(ctx, ev); err != nil { // exact redelivery
		t.Fatal(err)
	}
	ev.EventID = uuid.NewString() // re-announce with a new eventId
	if err := svc.HandleEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	if got := available(t, store, e, tt); got != 16 {
		t.Fatalf("available = %d, want 16: redelivery must not reset the counter", got)
	}
}

func TestCancelEventReleasesOnce(t *testing.T) {
	store := newTable(t)
	svc := newService(store, noSched{}, allow{})
	e, tt := seed(t, store, 10)
	r, _ := svc.CreateHold(ctx, "u1", uuid.NewString(), app.CreateHoldInput{EventID: e, TicketTypeID: tt, Quantity: 3})
	order := uuid.NewString()
	if _, err := svc.Consume(ctx, r.Hold.ID, order, "u1"); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(domain.OrderEventData{OrderID: order, HoldID: r.Hold.ID, UserID: "u1"})
	for _, typ := range []string{"OrderCancelled", "PaymentFailed", "OrderCancelled"} {
		ev := ebpub.Received{Detail: ebpub.Detail{EventID: uuid.NewString(), EventType: typ, Data: data}}
		if err := svc.HandleEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	if got := available(t, store, e, tt); got != 10 {
		t.Fatalf("available = %d, want 10 (released exactly once)", got)
	}
}

func TestMyHoldsPaginationUsesGSI(t *testing.T) {
	store := newTable(t)
	svc := newService(store, noSched{}, allow{})
	e, tt := seed(t, store, 50)
	for i := 0; i < 5; i++ {
		if _, err := svc.CreateHold(ctx, "u1", uuid.NewString(), app.CreateHoldInput{EventID: e, TicketTypeID: tt, Quantity: 1}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1100 * time.Millisecond) // createdAt has second resolution
	}
	if _, err := svc.CreateHold(ctx, "u2", uuid.NewString(), app.CreateHoldInput{EventID: e, TicketTypeID: tt, Quantity: 1}); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	var cur *ports.HoldCursor
	var order []time.Time
	for page := 0; page < 10; page++ {
		var holds []domain.Hold
		var err error
		// GSIs are eventually consistent: allow a short settle time on the first page
		for try := 0; try < 10; try++ {
			holds, cur, err = svc.ListMyHolds(ctx, "u1", cur, 2)
			if err != nil || len(holds) > 0 || page > 0 {
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range holds {
			if h.UserID != "u1" || seen[h.ID] {
				t.Fatalf("foreign or duplicate hold %+v", h)
			}
			seen[h.ID] = true
			order = append(order, h.CreatedAt)
		}
		if cur == nil {
			break
		}
	}
	if len(seen) != 5 {
		t.Fatalf("saw %d holds, want 5", len(seen))
	}
	for i := 1; i < len(order); i++ {
		if order[i].After(order[i-1]) {
			t.Fatalf("not newest-first: %v", order)
		}
	}
	// a cursor from another user's partition is refused
	if _, _, err := svc.ListMyHolds(ctx, "u2", &ports.HoldCursor{PK: "HOLD#x", SK: "HOLD", GSI1PK: "USER#u1", GSI1SK: "z"}, 2); err == nil {
		t.Fatal("cursor of another user accepted")
	}
}

func TestRateLimitAgainstRealRedis(t *testing.T) {
	env := testenv.Need(t, "TEST_REDIS_ADDR")
	opt := &goredis.Options{Addr: env["TEST_REDIS_ADDR"]}
	if os.Getenv("TEST_REDIS_TLS") == "true" {
		host, _, _ := net.SplitHostPort(env["TEST_REDIS_ADDR"])
		opt.TLSConfig = &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	}
	c := goredis.NewClient(opt)
	defer c.Close()
	l := &rredis.Limiter{C: c}
	user := "it-" + uuid.NewString()
	t.Cleanup(func() { c.Del(ctx, "rl:holds:"+user) })
	for i := 0; i < 10; i++ {
		ok, err := l.Allow(ctx, "holds:"+user, 10, time.Minute)
		if err != nil || !ok {
			t.Fatalf("request %d should be allowed: %v %v", i+1, ok, err)
		}
	}
	if ok, err := l.Allow(ctx, "holds:"+user, 10, time.Minute); err != nil || ok {
		t.Fatalf("11th request must be limited: %v %v", ok, err)
	}
	// a different user is unaffected
	if ok, _ := l.Allow(ctx, "holds:other-"+user, 10, time.Minute); !ok {
		t.Fatal("limit leaked across users")
	}
}

func TestScheduleCreatedAndDeleted(t *testing.T) {
	env := testenv.Need(t, "RES_IT_SCHEDULER_GROUP", "RES_IT_SCHEDULER_ROLE_ARN", "RES_IT_TARGET_LAMBDA_ARN", "AWS_REGION")
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	client := awsscheduler.NewFromConfig(cfg)
	s := &rsched.Scheduler{Client: client, Group: env["RES_IT_SCHEDULER_GROUP"], RoleARN: env["RES_IT_SCHEDULER_ROLE_ARN"], TargetARN: env["RES_IT_TARGET_LAMBDA_ARN"]}
	holdID := uuid.NewString()
	t.Cleanup(func() { _ = s.CancelExpiry(ctx, holdID) })

	if err := s.ScheduleExpiry(ctx, holdID, time.Now().UTC().Add(30*time.Minute)); err != nil {
		t.Fatalf("create schedule (check iam:PassRole and the group): %v", err)
	}
	if err := s.ScheduleExpiry(ctx, holdID, time.Now().UTC().Add(30*time.Minute)); err != nil {
		t.Fatalf("creating the same schedule twice must be idempotent: %v", err)
	}
	got, err := client.GetSchedule(ctx, &awsscheduler.GetScheduleInput{Name: aws.String(rsched.Name(holdID)), GroupName: aws.String(s.Group)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(got.Target.Arn) != s.TargetARN || got.ActionAfterCompletion != "DELETE" {
		t.Fatalf("schedule misconfigured: %+v", got)
	}
	if err := s.CancelExpiry(ctx, holdID); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetSchedule(ctx, &awsscheduler.GetScheduleInput{Name: aws.String(rsched.Name(holdID)), GroupName: aws.String(s.Group)}); err == nil {
		t.Fatal("schedule still exists after CancelExpiry")
	}
	if err := s.CancelExpiry(ctx, holdID); err != nil {
		t.Fatalf("deleting a missing schedule must not fail: %v", err)
	}
}
