//go:build integration

// Integration tests for the notifications adapters.
//
// Required environment (each test names what it needs and skips otherwise):
//
//	NOTIF_IT_DYNAMODB=true + AWS_REGION   creates and deletes a temporary table
//	                                      (DynamoDB Local: AWS_ENDPOINT_URL_DYNAMODB + dummy credentials)
//	TEST_TICKETS_BUCKET                   an S3 bucket the role may Put/Get/Delete objects in
//	TEST_NOTIFICATIONS_QUEUE_URL, SES_FROM_ADDRESS (a verified SES identity)
//	                                      worker end-to-end; mail goes to the SES mailbox simulator
//	                                      (success@simulator.amazonses.com), so nothing real is sent
//	TEST_COGNITO_USER_POOL_ID, TEST_COGNITO_USER_ID   real AdminGetUser lookup
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"

	ncognito "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/cognito"
	nddb "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/dynamodb"
	ns3 "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/s3"
	nses "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/ses"
	nsqs "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/sqs"
	"github.com/olagookundavid/gatepass-app/internal/notifications/adapters/ticketpng"
	"github.com/olagookundavid/gatepass-app/internal/notifications/app"
	"github.com/olagookundavid/gatepass-app/internal/notifications/domain"
	"github.com/olagookundavid/gatepass-app/internal/notifications/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/awscfg"
	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
	"github.com/olagookundavid/gatepass-app/internal/platform/sqsconsumer"
	"github.com/olagookundavid/gatepass-app/internal/platform/testenv"
)

var ctx = context.Background()

func newTable(t *testing.T) (*nddb.Store, *dynamodb.Client) {
	t.Helper()
	env := testenv.Need(t, "NOTIF_IT_DYNAMODB", "AWS_REGION")
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg)
	name := "gatepass-notif-" + testenv.Prefix()
	def := func(n string) types.AttributeDefinition {
		return types.AttributeDefinition{AttributeName: aws.String(n), AttributeType: types.ScalarAttributeTypeS}
	}
	if _, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String(name), BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{def("PK"), def("SK")},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange}}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(name)}) })
	for i := 0; i < 60; i++ {
		d, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		if err == nil && d.Table.TableStatus == types.TableStatusActive {
			break
		}
		time.Sleep(time.Second)
	}
	return &nddb.Store{Client: client, Table: name}, client
}

func TestStoreNotificationLogPreferencesTicketsAndDedupe(t *testing.T) {
	st, _ := newTable(t)
	base := time.Now().UTC()
	for i := 0; i < 5; i++ {
		n := domain.Notification{ID: uuid.NewString(), UserID: "u1", Type: domain.TypeOrderPaid, OrderID: "o", Channel: "email", Status: "sent", CreatedAt: base.Add(time.Duration(i) * time.Millisecond)}
		if err := st.PutNotification(ctx, n); err != nil {
			t.Fatal(err)
		}
	}
	_ = st.PutNotification(ctx, domain.Notification{ID: uuid.NewString(), UserID: "u2", Type: domain.TypeOrderPaid, OrderID: "o", Channel: "email", Status: "sent", CreatedAt: base})

	var seen []time.Time
	var cur *ports.NotificationCursor
	for page := 0; page < 10; page++ {
		items, next, err := st.ListNotifications(ctx, "u1", cur, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range items {
			if n.UserID != "u1" {
				t.Fatal("another user's notification returned")
			}
			seen = append(seen, n.CreatedAt)
		}
		if next == nil {
			break
		}
		cur = next
	}
	if len(seen) != 5 {
		t.Fatalf("saw %d notifications, want 5", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if !seen[i].Before(seen[i-1]) {
			t.Fatalf("not newest first: %v", seen)
		}
	}

	if p, _ := st.GetPreferences(ctx, "u1"); !p.EmailEnabled {
		t.Fatal("default must be e-mail enabled")
	}
	_ = st.PutPreferences(ctx, "u1", domain.Preferences{EmailEnabled: false, UpdatedAt: base})
	if p, _ := st.GetPreferences(ctx, "u1"); p.EmailEnabled {
		t.Fatal("preference not stored")
	}
	if p, _ := st.GetPreferences(ctx, "u2"); !p.EmailEnabled {
		t.Fatal("preference leaked across users")
	}

	_ = st.PutTicketRecord(ctx, "u1", "o1", "tickets/u1/o1.png")
	if k, ok, _ := st.GetTicketRecord(ctx, "u1", "o1"); !ok || k != "tickets/u1/o1.png" {
		t.Fatal("ticket record lost")
	}
	if _, ok, _ := st.GetTicketRecord(ctx, "u2", "o1"); ok {
		t.Fatal("ticket record visible to another user")
	}

	if done, _ := st.EventProcessed(ctx, "evt-1"); done {
		t.Fatal("unprocessed event reported as processed")
	}
	_ = st.MarkEventProcessed(ctx, "evt-1", time.Hour)
	if done, _ := st.EventProcessed(ctx, "evt-1"); !done {
		t.Fatal("marker not stored")
	}
}

func TestPresignedURLDownloadsThenExpires(t *testing.T) {
	env := testenv.Need(t, "TEST_TICKETS_BUCKET", "AWS_REGION")
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	client := awss3.NewFromConfig(cfg)
	store := ns3.New(client, env["TEST_TICKETS_BUCKET"])
	key := "tickets/" + testenv.Prefix() + "/order.png"
	png, _ := ticketpng.Renderer{}.Render(domain.Ticket{OrderID: "o", EventID: "e", Quantity: 1, Token: "GP1.o.sig"})
	if err := store.Put(ctx, key, png); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(env["TEST_TICKETS_BUCKET"]), Key: aws.String(key)})
	})

	url, err := store.PresignGet(ctx, key, "ticket.png", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	get := func() (int, int) {
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, len(b)
	}
	if code, n := get(); code != 200 || n != len(png) {
		t.Fatalf("download: %d, %d bytes (want %d)", code, n, len(png))
	}
	time.Sleep(5 * time.Second)
	if code, _ := get(); code != 403 {
		t.Fatalf("expired URL returned %d, want 403", code)
	}
}

func TestWorkerEndToEndAndDuplicateMessageSendsOneEmail(t *testing.T) {
	env := testenv.Need(t, "TEST_NOTIFICATIONS_QUEUE_URL", "TEST_TICKETS_BUCKET", "SES_FROM_ADDRESS", "NOTIF_IT_DYNAMODB", "AWS_REGION")
	st, _ := newTable(t)
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	s3c := awss3.NewFromConfig(cfg)
	sqsc := sqs.NewFromConfig(cfg)

	userID, orderID := "it-user-"+testenv.Prefix(), "it-order-"+testenv.Prefix()
	key, _ := domain.TicketKey(userID, orderID)
	t.Cleanup(func() {
		_, _ = s3c.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(env["TEST_TICKETS_BUCKET"]), Key: aws.String(key)})
	})

	worker := app.NewWorker(app.Worker{
		Store: st, Tickets: ns3.New(s3c, env["TEST_TICKETS_BUCKET"]), Renderer: ticketpng.Renderer{},
		Directory:  fixedDirectory("success@simulator.amazonses.com"), // SES mailbox simulator: always succeeds
		Mailer:     &nses.Mailer{Client: sesv2.NewFromConfig(cfg), From: env["SES_FROM_ADDRESS"]},
		SigningKey: []byte("integration-test-signing-key-0123456789"),
	})
	consumer, err := sqsconsumer.New(sqsconsumer.Config{Name: "it-notifications", QueueURL: env["TEST_NOTIFICATIONS_QUEUE_URL"], Workers: 2,
		Visibility: 30 * time.Second, Wait: 2 * time.Second, Client: sqsc, Handler: nsqs.Handler(worker)})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = consumer.Run(runCtx); close(done) }()
	defer func() { stop(); <-done }()

	data, _ := json.Marshal(domain.OrderEventData{OrderID: orderID, UserID: userID, EventID: uuid.NewString(), Quantity: 1, AmountCents: 1000, Currency: "USD"})
	eventID := ebpub.NewEventID()
	envelope, _ := json.Marshal(map[string]any{"version": "0", "id": uuid.NewString(), "detail-type": "OrderPaid", "source": "gatepass.orders",
		"detail": ebpub.Detail{EventID: eventID, EventType: "OrderPaid", EventVersion: 1, OccurredAt: time.Now().UTC(), Data: data}})
	send := func() {
		if _, err := sqsc.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(env["TEST_NOTIFICATIONS_QUEUE_URL"]), MessageBody: aws.String(string(envelope))}); err != nil {
			t.Fatal(err)
		}
	}
	send()
	send() // duplicate delivery of the same event

	waitFor := func(want int) []domain.Notification {
		var items []domain.Notification
		for i := 0; i < 60; i++ {
			items, _, _ = st.ListNotifications(ctx, userID, nil, 10)
			if len(items) >= want {
				return items
			}
			time.Sleep(time.Second)
		}
		return items
	}
	items := waitFor(1)
	if len(items) != 1 || items[0].Status != domain.StatusSent || items[0].MessageID == "" {
		t.Fatalf("notification log: %+v", items)
	}
	if _, err := s3c.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(env["TEST_TICKETS_BUCKET"]), Key: aws.String(key)}); err != nil {
		t.Fatalf("ticket object missing: %v", err)
	}
	time.Sleep(8 * time.Second) // give the duplicate time to be processed (and dropped)
	if items, _, _ := st.ListNotifications(ctx, userID, nil, 10); len(items) != 1 {
		t.Fatalf("duplicate message produced %d notifications, want 1", len(items))
	}
}

type fixedDirectory string

func (f fixedDirectory) EmailOf(context.Context, string) (string, error) { return string(f), nil }

func TestRealCognitoLookup(t *testing.T) {
	env := testenv.Need(t, "TEST_COGNITO_USER_POOL_ID", "TEST_COGNITO_USER_ID", "AWS_REGION")
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	d := &ncognito.Directory{Client: cognitoidentityprovider.NewFromConfig(cfg), UserPoolID: env["TEST_COGNITO_USER_POOL_ID"]}
	email, err := d.EmailOf(ctx, env["TEST_COGNITO_USER_ID"])
	if err != nil || !strings.Contains(email, "@") {
		t.Fatalf("lookup by sub failed (UNVERIFIED assumption): %v", err)
	}
	if _, err := d.EmailOf(ctx, uuid.NewString()); !errors.Is(err, domain.ErrUserNotFound) {
		t.Fatalf("unknown user: %v", err)
	}
}
