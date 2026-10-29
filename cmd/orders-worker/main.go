// Command orders-worker consumes the payments queue and simulates a payment
// provider (see internal/orders/app/payments.go for the retry/DLQ design).
//
// Runs two components: the SQS consumer and a tiny HTTP server that only serves
// /livez, /readyz, /version for Kubernetes probes.
//
// AWS services touched: SQS (receive/delete/extend), RDS PostgreSQL, Secrets Manager.
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	opg "github.com/olagookundavid/gatepass-app/internal/orders/adapters/postgres"
	osqs "github.com/olagookundavid/gatepass-app/internal/orders/adapters/sqs"
	"github.com/olagookundavid/gatepass-app/internal/orders/app"
	"github.com/olagookundavid/gatepass-app/internal/platform/bootstrap"
	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
	"github.com/olagookundavid/gatepass-app/internal/platform/lifecycle"
	"github.com/olagookundavid/gatepass-app/internal/platform/sqsconsumer"
)

type workerConfig struct {
	Common      config.Common
	DB          opg.DBConfig
	Queue       string
	Concurrency int
	Visibility  time.Duration
	SuccessRate float64
}

func loadConfig(l *config.Loader) workerConfig {
	c := workerConfig{
		Common:      config.LoadCommon(l, ""),
		DB:          opg.LoadDB(l),
		Queue:       l.Required("PAYMENTS_QUEUE_URL"),
		Concurrency: l.Int("WORKER_CONCURRENCY", 4),
		Visibility:  l.Duration("CONSUMER_VISIBILITY_TIMEOUT", 60*time.Second),
		SuccessRate: l.Float("PAYMENT_SUCCESS_RATE", 0.8),
	}
	l.Check(c.SuccessRate >= 0 && c.SuccessRate <= 1, "PAYMENT_SUCCESS_RATE must be between 0 and 1, got %v", c.SuccessRate)
	l.Check(c.Concurrency >= 1 && c.Concurrency <= 64, "WORKER_CONCURRENCY must be between 1 and 64, got %d", c.Concurrency)
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
	env, err := bootstrap.Init(ctx, "orders-worker", cfg.Common)
	if err != nil {
		return err
	}
	pool, err := opg.Connect(ctx, cfg.DB, env.AWS, "orders-worker")
	if err != nil {
		return err
	}
	worker := app.NewPaymentWorker(app.PaymentWorker{Orders: &opg.Repo{DB: pool}, SuccessRate: cfg.SuccessRate})
	consumer, err := sqsconsumer.New(sqsconsumer.Config{Name: "payments", QueueURL: cfg.Queue, Workers: cfg.Concurrency,
		Visibility: cfg.Visibility, Client: sqs.NewFromConfig(env.AWS), Handler: osqs.Handler(worker)})
	if err != nil {
		return err
	}

	health := httpserver.NewHealth("orders-worker", httpserver.Check{Name: "postgres", Fn: pool.Ping})
	srv := httpserver.New(httpserver.Options{Service: "orders-worker", Addr: cfg.Common.HTTPAddr, Handler: http.NewServeMux(), Health: health})
	return lifecycle.Run(lifecycle.Options{
		SetNotReady: health.SetShuttingDown, ShutdownDelay: cfg.Common.ShutdownDelay,
		Closers: []func(context.Context) error{func(context.Context) error { pool.Close(); return nil }, env.FlushTelemetry},
	}, httpserver.Component{Name_: "probes-http", Srv: srv}, consumer)
}
