//go:build integration

// Integration tests for the orders adapters.
//
// Required environment (each test names what it needs and skips otherwise):
//
//	TEST_DATABASE_URL     PostgreSQL DSN with password auth (a local container or an RDS
//	                      test database). Tests create and drop their own schema.
//	TEST_PAYMENTS_QUEUE_URL, TEST_PAYMENTS_DLQ_URL, AWS_REGION
//	                      a payments queue whose redrive policy has maxReceiveCount=5 and
//	                      its DLQ (poison message test)
//	ORDERS_IT_SECRETS=true + TEST_DATABASE_URL + TEST_DB_ADMIN_PASSWORD_ROLE
//	                      real Secrets Manager rotation test (creates a temporary secret)
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	opg "github.com/olagookundavid/gatepass-app/internal/orders/adapters/postgres"
	osqs "github.com/olagookundavid/gatepass-app/internal/orders/adapters/sqs"
	"github.com/olagookundavid/gatepass-app/internal/orders/app"
	"github.com/olagookundavid/gatepass-app/internal/orders/domain"
	"github.com/olagookundavid/gatepass-app/internal/orders/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/awscfg"
	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
	"github.com/olagookundavid/gatepass-app/internal/platform/pgdb"
	"github.com/olagookundavid/gatepass-app/internal/platform/sqsconsumer"
	"github.com/olagookundavid/gatepass-app/internal/platform/testenv"
	"github.com/olagookundavid/gatepass-app/migrations"
)

var ctx = context.Background()

type countingHolds struct {
	mu    sync.Mutex
	calls int
}

func (h *countingHolds) Consume(_ context.Context, holdID, _, _ string) (ports.ConsumedHold, error) {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	return ports.ConsumedHold{HoldID: holdID, EventID: uuid.NewString(), TicketTypeID: uuid.NewString(), Quantity: 2, UnitPriceCents: 1000, Currency: "USD"}, nil
}

type recordingPayments struct {
	mu   sync.Mutex
	sent []domain.PaymentRequest
}

func (p *recordingPayments) Enqueue(_ context.Context, r domain.PaymentRequest) error {
	p.mu.Lock()
	p.sent = append(p.sent, r)
	p.mu.Unlock()
	return nil
}

func count(t *testing.T, pool *pgdb.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestIdempotentCreateConsumesOnceAndConflictsOnDifferentBody(t *testing.T) {
	env := testenv.Need(t, "TEST_DATABASE_URL")
	pool := testenv.FreshSchema(t, env["TEST_DATABASE_URL"], migrations.Orders())
	holds, pay := &countingHolds{}, &recordingPayments{}
	svc := app.New(app.Service{Orders: &opg.Repo{DB: pool}, Holds: holds, Payments: pay})
	hold, key := uuid.NewString(), uuid.NewString()

	first, err := svc.CreateOrder(ctx, "u1", key, hold)
	if err != nil || first.Code != 201 {
		t.Fatalf("first: %+v %v", first, err)
	}
	again, err := svc.CreateOrder(ctx, "u1", key, hold)
	if err != nil || !again.Replayed {
		t.Fatalf("replay: %+v %v", again, err)
	}
	var a, b domain.Order
	_ = json.Unmarshal(first.Body, &a)
	_ = json.Unmarshal(again.Body, &b)
	if a.ID != b.ID || holds.calls != 1 || count(t, pool, "orders") != 1 || count(t, pool, "outbox") != 1 {
		t.Fatalf("replay changed state: ids %s/%s consumes=%d orders=%d outbox=%d", a.ID, b.ID, holds.calls, count(t, pool, "orders"), count(t, pool, "outbox"))
	}
	if _, err := svc.CreateOrder(ctx, "u1", key, uuid.NewString()); !errors.Is(err, app.ErrIdempotencyConflict) {
		t.Fatalf("different body: %v", err)
	}
	// 20 concurrent identical requests -> one order
	key2, hold2 := uuid.NewString(), uuid.NewString()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = svc.CreateOrder(ctx, "u1", key2, hold2) }()
	}
	wg.Wait()
	if n := count(t, pool, "orders"); n != 2 {
		t.Fatalf("concurrent duplicates created %d orders, want 2 in total", n)
	}
}

func TestFailedTransactionLeavesNoRows(t *testing.T) {
	env := testenv.Need(t, "TEST_DATABASE_URL")
	pool := testenv.FreshSchema(t, env["TEST_DATABASE_URL"], migrations.Orders())
	repo := &opg.Repo{DB: pool}
	now := time.Now().UTC()
	mk := func(hold, key string) ports.NewOrder {
		o, _ := domain.NewOrder(uuid.NewString(), "u1", hold, uuid.NewString(), uuid.NewString(), 1, 100, "USD", now)
		return ports.NewOrder{Order: o, IdemKey: key, IdemExpires: now.Add(time.Hour), Idempotency: ports.IdempotencyRecord{RequestHash: "h", ResponseCode: 201, ResponseBody: []byte(`{}`)}}
	}
	hold := uuid.NewString()
	if err := repo.CreateOrder(ctx, mk(hold, "key-A-00000001")); err != nil {
		t.Fatal(err)
	}
	// Second order for the SAME hold: fails at the orders insert, AFTER the
	// idempotency row was inserted in the same transaction.
	err := repo.CreateOrder(ctx, mk(hold, "key-B-00000002"))
	if !errors.Is(err, domain.ErrOrderExists) {
		t.Fatalf("want ErrOrderExists, got %v", err)
	}
	if count(t, pool, "orders") != 1 || count(t, pool, "idempotency_keys") != 1 || count(t, pool, "outbox") != 1 || count(t, pool, "order_status_history") != 1 {
		t.Fatalf("rollback left rows: orders=%d idem=%d outbox=%d history=%d", count(t, pool, "orders"), count(t, pool, "idempotency_keys"), count(t, pool, "outbox"), count(t, pool, "order_status_history"))
	}
	// An application error after an outbox insert rolls the outbox row back too.
	o := mk(uuid.NewString(), "key-C-00000003").Order
	boom := errors.New("boom")
	err = pgdb.InTx(ctx, pool, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, `INSERT INTO outbox (id, aggregate_id, event_type, payload) VALUES ($1,$2,'X','{}')`, uuid.NewString(), o.ID); e != nil {
			return e
		}
		return boom
	})
	if !errors.Is(err, boom) || count(t, pool, "outbox") != 1 {
		t.Fatalf("outbox row survived a rolled-back transaction: err=%v outbox=%d", err, count(t, pool, "outbox"))
	}
}

type countingPublisher struct {
	mu     sync.Mutex
	counts map[string]int
}

func (p *countingPublisher) Publish(_ context.Context, rows []ports.OutboxRow) []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range rows {
		p.counts[r.ID]++
	}
	time.Sleep(5 * time.Millisecond) // widen the window in which a second relay could collide
	return make([]error, len(rows))
}

func TestTwoRelaysPublishEachRowExactlyOnce(t *testing.T) {
	env := testenv.Need(t, "TEST_DATABASE_URL")
	pool := testenv.FreshSchema(t, env["TEST_DATABASE_URL"], migrations.Orders())
	const rows = 300
	for i := 0; i < rows; i++ {
		if _, err := pool.Exec(ctx, `INSERT INTO outbox (id, aggregate_id, event_type, payload) VALUES ($1,$2,'OrderPaid','{}')`, ebpub.NewEventID(), uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}
	repo := &opg.Repo{DB: pool}
	pub := &countingPublisher{counts: map[string]int{}}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		relay := &app.Relay{Outbox: repo, Publisher: pub, Batch: 10}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := relay.RunOnce(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()
	if len(pub.counts) != rows {
		t.Fatalf("published %d distinct rows, want %d", len(pub.counts), rows)
	}
	for id, n := range pub.counts {
		if n != 1 {
			t.Fatalf("row %s published %d times", id, n)
		}
	}
	if n, _ := repo.CountUnpublished(ctx); n != 0 {
		t.Fatalf("%d rows still unpublished", n)
	}
}

func TestConcurrentCancelAndPaymentProduceOneTerminalEvent(t *testing.T) {
	env := testenv.Need(t, "TEST_DATABASE_URL")
	pool := testenv.FreshSchema(t, env["TEST_DATABASE_URL"], migrations.Orders())
	repo := &opg.Repo{DB: pool}
	svc := app.New(app.Service{Orders: repo, Holds: &countingHolds{}, Payments: &recordingPayments{}})
	worker := app.NewPaymentWorker(app.PaymentWorker{Orders: repo, SuccessRate: 1, Latency: func() time.Duration { return 0 }})

	for i := 0; i < 15; i++ {
		resp, err := svc.CreateOrder(ctx, "u1", uuid.NewString(), uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		var o domain.Order
		_ = json.Unmarshal(resp.Body, &o)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = svc.CancelOrder(ctx, "u1", o.ID, "x") }()
		go func() {
			defer wg.Done()
			_ = worker.Process(ctx, domain.PaymentRequest{OrderID: o.ID, AmountCents: o.AmountCents, Attempt: 1})
		}()
		wg.Wait()

		final, _ := repo.Get(ctx, o.ID)
		var events []string
		rows, _ := pool.Query(ctx, `SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY created_at, id`, o.ID)
		for rows.Next() {
			var s string
			_ = rows.Scan(&s)
			events = append(events, s)
		}
		rows.Close()
		hist, _ := repo.History(ctx, o.ID)
		switch final.Status {
		case domain.StatusCancelled:
			// either cancelled directly, or paid then cancelled
			if events[len(events)-1] != domain.EventOrderCancelled || len(events) > 3 {
				t.Fatalf("cancelled order events: %v", events)
			}
		case domain.StatusPaid:
			if strings.Join(events, ",") != "OrderPlaced,OrderPaid" {
				t.Fatalf("paid order events: %v", events)
			}
		default:
			t.Fatalf("illegal final status %s", final.Status)
		}
		if len(hist) != len(events) {
			t.Fatalf("history (%d) and outbox (%d) disagree", len(hist), len(events))
		}
	}
}

func TestReconcilerQueuesPaymentsThatWereNeverEnqueued(t *testing.T) {
	env := testenv.Need(t, "TEST_DATABASE_URL")
	pool := testenv.FreshSchema(t, env["TEST_DATABASE_URL"], migrations.Orders())
	repo := &opg.Repo{DB: pool}
	pay := &recordingPayments{}
	// Payments that always fail simulate the lost enqueue after commit.
	svc := app.New(app.Service{Orders: repo, Holds: &countingHolds{}, Payments: failingPayments{}})
	resp, err := svc.CreateOrder(ctx, "u1", uuid.NewString(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	var o domain.Order
	_ = json.Unmarshal(resp.Body, &o)

	relay := &app.Relay{Outbox: repo, Payments: pay, ReconcileAfter: time.Hour}
	if n, _ := relay.ReconcilePayments(ctx); n != 0 {
		t.Fatalf("reconciler touched a brand-new order (grace period ignored): %d", n)
	}
	relay.ReconcileAfter = time.Millisecond
	time.Sleep(20 * time.Millisecond)
	if n, err := relay.ReconcilePayments(ctx); err != nil || n != 1 || len(pay.sent) != 1 || pay.sent[0].OrderID != o.ID {
		t.Fatalf("reconcile: n=%d err=%v sent=%v", n, err, pay.sent)
	}
	if n, _ := relay.ReconcilePayments(ctx); n != 0 {
		t.Fatalf("order reconciled twice: %d", n)
	}
}

type failingPayments struct{}

func (failingPayments) Enqueue(context.Context, domain.PaymentRequest) error {
	return errors.New("sqs down")
}

// ---- secret rotation -----------------------------------------------------------------

// memSecrets is our own fake of the SecretsAPI port, used for the local variant.
type memSecrets struct {
	mu    sync.Mutex
	value string
	calls atomic.Int32
}

func (m *memSecrets) GetSecretValue(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	m.calls.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	return &secretsmanager.GetSecretValueOutput{SecretString: aws.String(m.value)}, nil
}
func (m *memSecrets) set(v string) { m.mu.Lock(); m.value = v; m.mu.Unlock() }

func rotationScenario(t *testing.T, adminDSN string, secrets pgdb.SecretsAPI, setSecret func(user, password string)) {
	t.Helper()
	admin, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	role := "it_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:10]
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'first-password'", role)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(ctx, "DROP ROLE "+role) })
	setSecret(role, "first-password")

	// DSN with host/db only; user and password are injected per connection.
	cfg := admin.Config().ConnConfig
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s sslmode=disable", cfg.Host, cfg.Port, cfg.Database)
	creds := &pgdb.SecretCredentials{Client: secrets, SecretID: "orders-it"}
	pool, err := pgdb.ConnectDSNWithCreds(ctx, dsn, creds)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("initial connection: %v", err)
	}

	// ROTATE: change the password in the database and in the secret, and drop the
	// pool's open connections so the next query must authenticate again.
	if _, err := admin.Exec(ctx, fmt.Sprintf("ALTER ROLE %s PASSWORD 'second-password'", role)); err != nil {
		t.Fatal(err)
	}
	setSecret(role, "second-password")
	pool.Raw().Reset()

	// The cache still holds the OLD password: the first dial fails with 28P01, the
	// pool invalidates the cache, re-reads the secret and retries exactly once.
	var one int
	if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("query after rotation should succeed after one refresh: %v", err)
	}
}

func TestSecretRotationReconnectsAfterOneAuthFailure(t *testing.T) {
	env := testenv.Need(t, "TEST_DATABASE_URL")
	secrets := &memSecrets{}
	rotationScenario(t, env["TEST_DATABASE_URL"], secrets, func(user, password string) {
		b, _ := json.Marshal(map[string]string{"username": user, "password": password})
		secrets.set(string(b))
	})
	// 1 fetch at the first connection + at least 1 refresh after the auth failure.
	if secrets.calls.Load() < 2 {
		t.Fatalf("secret was fetched %d times; a refresh after the auth failure is required", secrets.calls.Load())
	}
}

func TestSecretRotationAgainstRealSecretsManager(t *testing.T) {
	env := testenv.Need(t, "TEST_DATABASE_URL", "ORDERS_IT_SECRETS", "AWS_REGION")
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	sm := secretsmanager.NewFromConfig(cfg)
	name := "gatepass-" + testenv.Prefix()
	created := false
	rotationScenario(t, env["TEST_DATABASE_URL"], sm, func(user, password string) {
		b, _ := json.Marshal(map[string]string{"username": user, "password": password})
		if !created {
			if _, err := sm.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: aws.String(name), SecretString: aws.String(string(b))}); err != nil {
				t.Fatalf("create secret: %v", err)
			}
			created = true
			return
		}
		if _, err := sm.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{SecretId: aws.String(name), SecretString: aws.String(string(b))}); err != nil {
			t.Fatalf("put secret: %v", err)
		}
	})
	_, _ = sm.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{SecretId: aws.String(name), ForceDeleteWithoutRecovery: aws.Bool(true)})
}

// ---- poison message -> DLQ ---------------------------------------------------------------

func TestPoisonMessageReachesDLQAfterMaxReceives(t *testing.T) {
	env := testenv.Need(t, "TEST_PAYMENTS_QUEUE_URL", "TEST_PAYMENTS_DLQ_URL", "AWS_REGION")
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	client := sqs.NewFromConfig(cfg)
	marker := uuid.NewString()
	body, _ := json.Marshal(domain.PaymentRequest{OrderID: marker, Attempt: 1, Poison: true})
	if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(env["TEST_PAYMENTS_QUEUE_URL"]), MessageBody: aws.String(string(body))}); err != nil {
		t.Fatal(err)
	}

	worker := app.NewPaymentWorker(app.PaymentWorker{SuccessRate: 1})
	consumer, err := sqsconsumer.New(sqsconsumer.Config{Name: "it-payments", QueueURL: env["TEST_PAYMENTS_QUEUE_URL"], Workers: 2,
		Visibility: 3 * time.Second, Wait: 2 * time.Second, Client: client, Handler: osqs.Handler(worker)})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = consumer.Run(runCtx); close(done) }()
	defer func() { stop(); <-done }()

	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(env["TEST_PAYMENTS_DLQ_URL"]), WaitTimeSeconds: 5, MaxNumberOfMessages: 10})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Messages {
			if strings.Contains(aws.ToString(m.Body), marker) {
				_, _ = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(env["TEST_PAYMENTS_DLQ_URL"]), ReceiptHandle: m.ReceiptHandle})
				return
			}
		}
	}
	t.Fatal("poison message did not reach the DLQ in 3 minutes (check maxReceiveCount=5 on the redrive policy)")
}
