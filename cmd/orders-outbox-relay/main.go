// Command orders-outbox-relay publishes the transactional outbox to EventBridge
// and reconciles payment requests that were lost after an order commit.
//
// It is safe to run 2+ replicas: rows are claimed with FOR UPDATE SKIP LOCKED, so
// replicas share the backlog (see internal/orders/app/relay.go).
//
// AWS services touched: RDS PostgreSQL, Secrets Manager, EventBridge (PutEvents),
// SQS (SendMessage, reconciler only).
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	obridge "github.com/olagookundavid/gatepass-app/internal/orders/adapters/eventbridge"
	opg "github.com/olagookundavid/gatepass-app/internal/orders/adapters/postgres"
	osqs "github.com/olagookundavid/gatepass-app/internal/orders/adapters/sqs"
	"github.com/olagookundavid/gatepass-app/internal/orders/app"
	"github.com/olagookundavid/gatepass-app/internal/platform/bootstrap"
	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
	"github.com/olagookundavid/gatepass-app/internal/platform/lifecycle"
)

type relayConfig struct {
	Common         config.Common
	DB             opg.DBConfig
	Bus            string
	PaymentsQueue  string
	Batch          int
	Poll           time.Duration
	ReconcileAfter time.Duration
	Retention      time.Duration
}

func loadConfig(l *config.Loader) relayConfig {
	c := relayConfig{
		Common:         config.LoadCommon(l, ""),
		DB:             opg.LoadDB(l),
		Bus:            l.Required("EVENT_BUS_NAME"),
		PaymentsQueue:  l.Required("PAYMENTS_QUEUE_URL"),
		Batch:          l.Int("OUTBOX_BATCH_SIZE", 25),
		Poll:           l.Duration("OUTBOX_POLL_INTERVAL", time.Second),
		ReconcileAfter: l.Duration("PAYMENT_RECONCILE_AFTER", 30*time.Second),
		Retention:      l.Duration("OUTBOX_RETENTION", 7*24*time.Hour),
	}
	l.Check(c.Batch >= 1 && c.Batch <= 100, "OUTBOX_BATCH_SIZE must be between 1 and 100, got %d", c.Batch)
	l.Check(c.Poll >= 100*time.Millisecond, "OUTBOX_POLL_INTERVAL must be at least 100ms")
	return c
}

func main() {
	if err := run(context.Background()); err != nil {
		bootstrap.Fatal(err)
	}
}

func run(ctx context.Context) error {
	l := config.FromEnv()
	cfg := loadConfig(l)
	if err := l.Err(); err != nil {
		return err
	}
	env, err := bootstrap.Init(ctx, "orders-outbox-relay", cfg.Common)
	if err != nil {
		return err
	}
	pool, err := opg.Connect(ctx, cfg.DB, env.AWS, "orders-outbox-relay")
	if err != nil {
		return err
	}
	repo := &opg.Repo{DB: pool}
	relay := &app.Relay{
		Outbox:         repo,
		Orders:         repo,
		Publisher:      &obridge.Publisher{Pub: &ebpub.Publisher{Client: eventbridge.NewFromConfig(env.AWS), Bus: cfg.Bus, Source: "gatepass.orders"}},
		Payments:       &osqs.Queue{Client: sqs.NewFromConfig(env.AWS), QueueURL: cfg.PaymentsQueue},
		Batch:          cfg.Batch,
		Poll:           cfg.Poll,
		ReconcileAfter: cfg.ReconcileAfter,
		Retention:      cfg.Retention,
	}

	health := httpserver.NewHealth("orders-outbox-relay", httpserver.Check{Name: "postgres", Fn: pool.Ping})
	srv := httpserver.New(httpserver.Options{Service: "orders-outbox-relay", Addr: cfg.Common.HTTPAddr, Handler: http.NewServeMux(), Health: health})
	return lifecycle.Run(lifecycle.Options{
		SetNotReady: health.SetShuttingDown, ShutdownDelay: cfg.Common.ShutdownDelay,
		Closers: []func(context.Context) error{func(context.Context) error { pool.Close(); return nil }, env.FlushTelemetry},
	}, httpserver.Component{Name_: "probes-http", Srv: srv}, relay)
}
