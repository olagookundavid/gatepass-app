//go:build integration

// Integration tests for the catalog adapters. They talk to REAL services and are
// skipped unless the environment variables named in each test are set.
//
//	TEST_DATABASE_URL           any PostgreSQL DSN (local container or RDS test DB, password auth).
//	                            Tests create and drop their own schema.
//	CATALOG_IT_RDS_HOST, CATALOG_IT_RDS_PORT, CATALOG_IT_RDS_DB, CATALOG_IT_RDS_USER
//	                            an RDS instance with IAM auth enabled and a user granted rds_iam.
//	TEST_REDIS_ADDR (+ TEST_REDIS_TLS=true for ElastiCache)
//	TEST_IMAGES_BUCKET          an S3 bucket the test role may PutObject/DeleteObject in
//	TEST_EVENT_BUS_NAME, TEST_EVENTS_QUEUE_URL
//	                            a bus with a rule sending source=gatepass.catalog-it events to the queue
//
// All AWS-touching tests use the default credential chain.
package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

	cbridge "github.com/olagookundavid/gatepass-app/internal/catalog/adapters/eventbridge"
	cpg "github.com/olagookundavid/gatepass-app/internal/catalog/adapters/postgres"
	credis "github.com/olagookundavid/gatepass-app/internal/catalog/adapters/redis"
	cs3 "github.com/olagookundavid/gatepass-app/internal/catalog/adapters/s3"
	"github.com/olagookundavid/gatepass-app/internal/catalog/app"
	"github.com/olagookundavid/gatepass-app/internal/catalog/domain"
	"github.com/olagookundavid/gatepass-app/internal/catalog/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/awscfg"
	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
	"github.com/olagookundavid/gatepass-app/internal/platform/pgdb"
	"github.com/olagookundavid/gatepass-app/internal/platform/testenv"
	"github.com/olagookundavid/gatepass-app/migrations"
)

var ctx = context.Background()

func mustEvent(t *testing.T, id, venueID, title string, startsIn time.Duration) domain.Event {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	return domain.Event{ID: id, VenueID: venueID, Title: title, Status: domain.StatusPublished,
		StartsAt: now.Add(startsIn), EndsAt: now.Add(startsIn + time.Hour), CreatedBy: "it", CreatedAt: now, UpdatedAt: now, Version: 1,
		TicketTypes: []domain.TicketType{{ID: strings.Replace(id, "0000", "1111", 1), Name: "GA", PriceCents: 1000, Currency: "USD", Capacity: 10}}}
}

// freshSchema creates a private schema, migrates it, and returns a pool bound to it.
func freshSchema(t *testing.T, dsn string) *pgdb.Pool {
	t.Helper()
	schema := strings.ReplaceAll(testenv.Prefix(), "-", "_")
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); admin.Close() })

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	pool, err := pgdb.ConnectDSN(ctx, dsn+sep+"search_path="+schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pgdb.Migrate(ctx, pool, migrations.Catalog()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func TestPostgresRepositories(t *testing.T) {
	env := testenv.Need(t, "TEST_DATABASE_URL")
	pool := freshSchema(t, env["TEST_DATABASE_URL"])
	repo := &cpg.Repo{DB: pool}
	venues := cpg.VenueRepo{R: repo}

	v := domain.Venue{ID: "00000000-0000-7000-8000-00000000000a", Name: "Arena", City: "Lagos", Address: "1 Rd", Capacity: 1000, CreatedAt: time.Now().UTC()}
	v2 := domain.Venue{ID: "00000000-0000-7000-8000-00000000000b", Name: "Hall", City: "Abuja", Address: "2 Rd", Capacity: 500, CreatedAt: time.Now().UTC()}
	for _, x := range []domain.Venue{v, v2} {
		if err := venues.Create(ctx, x); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := venues.Get(ctx, "not-a-uuid"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("bad uuid should be ErrNotFound, got %v", err)
	}

	// 5 published events in Lagos at increasing times, 1 in Abuja
	for i := 0; i < 5; i++ {
		e := mustEvent(t, fmt.Sprintf("00000000-0000-7000-8000-0000000001%02d", i), v.ID, fmt.Sprintf("Show %d", i), time.Duration(i+1)*time.Hour)
		if err := repo.Create(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.Create(ctx, mustEvent(t, "00000000-0000-7000-8000-000000000200", v2.ID, "Abuja show", time.Hour)); err != nil {
		t.Fatal(err)
	}

	// keyset pagination: pages of 2 over Lagos (limit+1 trick) must cover all 5 exactly once, in order
	var seen []string
	var cur *ports.EventCursor
	for page := 0; page < 10; page++ {
		rows, err := repo.ListPublished(ctx, ports.EventFilter{City: "LAGOS"}, cur, 3) // 2 + 1 lookahead
		if err != nil {
			t.Fatal(err)
		}
		keep := rows
		if len(rows) > 2 {
			keep = rows[:2]
		}
		for _, e := range keep {
			seen = append(seen, e.Title)
			if len(e.TicketTypes) != 1 {
				t.Fatalf("ticket types not loaded for %s", e.Title)
			}
		}
		if len(rows) <= 2 {
			break
		}
		cur = &ports.EventCursor{StartsAt: keep[1].StartsAt, ID: keep[1].ID}
	}
	if strings.Join(seen, ",") != "Show 0,Show 1,Show 2,Show 3,Show 4" {
		t.Fatalf("pagination order/coverage wrong: %v", seen)
	}

	// optimistic locking
	e, _ := repo.Get(ctx, "00000000-0000-7000-8000-000000000100")
	e.Title = "Renamed"
	saved, err := repo.Save(ctx, e, 1, false)
	if err != nil || saved.Version != 2 {
		t.Fatalf("save: %v v=%d", err, saved.Version)
	}
	if _, err := repo.Save(ctx, e, 1, false); !errors.Is(err, domain.ErrVersionConflict) {
		t.Fatalf("stale save: %v", err)
	}
	e.ID = "00000000-0000-7000-8000-0000000fffff"
	if _, err := repo.Save(ctx, e, 1, false); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

// ---- RDS IAM authentication ----

func TestRDSIAMAuth(t *testing.T) {
	env := testenv.Need(t, "CATALOG_IT_RDS_HOST", "CATALOG_IT_RDS_PORT", "CATALOG_IT_RDS_DB", "CATALOG_IT_RDS_USER", "AWS_REGION")
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	var port int
	if _, err := fmt.Sscanf(env["CATALOG_IT_RDS_PORT"], "%d", &port); err != nil {
		t.Fatal(err)
	}
	base := pgdb.Config{Host: env["CATALOG_IT_RDS_HOST"], Port: port, Name: env["CATALOG_IT_RDS_DB"], MaxConns: 2, StatementTimeout: 5 * time.Second}

	// 1. a valid IAM token connects
	good, err := pgdb.Connect(ctx, base, pgdb.IAMAuth{Host: base.Host, Port: port, User: env["CATALOG_IT_RDS_USER"], Region: env["AWS_REGION"], Provider: cfg.Credentials})
	if err != nil {
		t.Fatal(err)
	}
	defer good.Close()
	if err := good.Ping(ctx); err != nil {
		t.Fatalf("IAM-authenticated connection failed: %v", err)
	}

	// 2. a connection WITHOUT a valid token must fail (no password fallback exists)
	bad, err := pgdb.Connect(ctx, base, staticCreds{user: env["CATALOG_IT_RDS_USER"], password: "not-a-token"})
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	if err := bad.Ping(ctx); err == nil {
		t.Fatal("connection with a bogus password succeeded: IAM authentication is not enforced")
	}
}

type staticCreds struct{ user, password string }

func (s staticCreds) Credentials(context.Context) (string, string, error) {
	return s.user, s.password, nil
}

// ---- cache ----

func redisClient(t *testing.T, addr string) *goredis.Client {
	t.Helper()
	opt := &goredis.Options{Addr: addr, DialTimeout: 500 * time.Millisecond, ReadTimeout: 500 * time.Millisecond, MaxRetries: -1}
	if os.Getenv("TEST_REDIS_TLS") == "true" {
		host, _, _ := net.SplitHostPort(addr)
		opt.TLSConfig = &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	}
	c := goredis.NewClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestCacheAsideAgainstRealRedisAndOutageFallback(t *testing.T) {
	env := testenv.Need(t, "TEST_DATABASE_URL", "TEST_REDIS_ADDR")
	pool := freshSchema(t, env["TEST_DATABASE_URL"])
	repo := &cpg.Repo{DB: pool}
	venues := cpg.VenueRepo{R: repo}
	cache := &credis.Cache{C: redisClient(t, env["TEST_REDIS_ADDR"])}

	svc := app.New(app.Service{Events: repo, Venues: venues, Cache: cache, Announcer: noopAnnouncer{}, CDNBase: "https://cdn.example.com"})
	v, err := svc.CreateVenue(ctx, app.CreateVenueInput{Name: "A", City: "Lagos", Address: "x", Capacity: 100})
	if err != nil {
		t.Fatal(err)
	}
	e, err := svc.CreateEvent(ctx, app.CreateEventInput{VenueID: v.ID, Title: "Cached", StartsAt: time.Now().Add(48 * time.Hour), EndsAt: time.Now().Add(50 * time.Hour),
		TicketTypes: []app.TicketTypeInput{{Name: "GA", PriceCents: 1, Currency: "USD", Capacity: 10}}}, "it")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PublishEvent(ctx, e.ID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.DeleteEvent(ctx, e.ID); _ = cache.InvalidateLists(ctx) })

	_ = cache.DeleteEvent(ctx, e.ID)
	// miss then hit
	if _, hit, _ := cache.GetEvent(ctx, e.ID); hit {
		t.Fatal("expected a miss before the first read")
	}
	if _, err := svc.GetEvent(ctx, e.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, hit, _ := cache.GetEvent(ctx, e.ID); !hit {
		t.Fatal("expected a hit after the first read")
	}
	// invalidation on update
	title := "Updated"
	if _, err := svc.UpdateEvent(ctx, e.ID, app.UpdateEventInput{Title: &title}, 2); err != nil {
		t.Fatal(err)
	}
	if _, hit, _ := cache.GetEvent(ctx, e.ID); hit {
		t.Fatal("update did not invalidate the event key")
	}
	got, _ := svc.GetEvent(ctx, e.ID, false)
	if got.Title != "Updated" {
		t.Fatalf("stale read after update: %s", got.Title)
	}

	// outage: a cache pointing at a dead port must not fail requests
	dead := &credis.Cache{C: redisClient(t, "127.0.0.1:1")}
	svc2 := app.New(app.Service{Events: repo, Venues: venues, Cache: dead, Announcer: noopAnnouncer{}, CDNBase: "https://cdn.example.com"})
	if g, err := svc2.GetEvent(ctx, e.ID, false); err != nil || g.ID != e.ID {
		t.Fatalf("cache outage failed the request: %v", err)
	}
	if page, err := svc2.ListEvents(ctx, ports.EventFilter{}, nil, 10); err != nil || len(page.Items) == 0 {
		t.Fatalf("list during cache outage: %v", err)
	}
}

type noopAnnouncer struct{}

func (noopAnnouncer) AnnouncePublished(context.Context, domain.EventPublishedData) error { return nil }

// ---- S3 presigned PUT ----

func TestPresignedPutAcceptsRightContentTypeRejectsWrong(t *testing.T) {
	env := testenv.Need(t, "TEST_IMAGES_BUCKET", "AWS_REGION")
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	client := awss3.NewFromConfig(cfg)
	signer := cs3.New(client, env["TEST_IMAGES_BUCKET"])
	key := "images/events/" + testenv.Prefix() + "/test.png"
	body := []byte("\x89PNG not really a png but the bucket does not care")

	url, err := signer.PresignPut(ctx, key, "image/png", int64(len(body)), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(env["TEST_IMAGES_BUCKET"]), Key: aws.String(key)})
	})

	put := func(contentType string) int {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", contentType)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := put("text/html"); code != http.StatusForbidden {
		t.Fatalf("wrong content type must be rejected with 403, got %d", code)
	}
	if code := put("image/png"); code != http.StatusOK {
		t.Fatalf("correct upload failed with %d", code)
	}
}

// ---- EventBridge PutEvents ----

func TestPutEventsReachesQueue(t *testing.T) {
	env := testenv.Need(t, "TEST_EVENT_BUS_NAME", "TEST_EVENTS_QUEUE_URL", "AWS_REGION")
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	pub := &ebpub.Publisher{Client: eventbridge.NewFromConfig(cfg), Bus: env["TEST_EVENT_BUS_NAME"], Source: "gatepass.catalog-it"}
	announcer := &cbridge.Announcer{Pub: pub}
	eventID := "00000000-0000-7000-8000-" + fmt.Sprintf("%012d", time.Now().UnixNano()%1_000_000_000_000)
	if err := announcer.AnnouncePublished(ctx, domain.EventPublishedData{EventID: eventID, Title: "IT", VenueID: "v", StartsAt: time.Now()}); err != nil {
		t.Fatal(err)
	}

	sqsClient := sqs.NewFromConfig(cfg)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(env["TEST_EVENTS_QUEUE_URL"]), WaitTimeSeconds: 5, MaxNumberOfMessages: 10})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Messages {
			_, _ = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(env["TEST_EVENTS_QUEUE_URL"]), ReceiptHandle: m.ReceiptHandle})
			if strings.Contains(aws.ToString(m.Body), eventID) {
				r, err := ebpub.ParseSQSBody(aws.ToString(m.Body))
				if err != nil || r.DetailType != "EventPublished" || r.Source != "gatepass.catalog-it" {
					t.Fatalf("unexpected envelope: %+v %v", r, err)
				}
				return
			}
		}
	}
	t.Fatal("EventPublished did not arrive in the test queue within 30s (check the rule and queue policy)")
}
