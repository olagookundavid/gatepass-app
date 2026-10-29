// Command orders-api is the public orders service, and with the subcommand
// `migrate` the database migration job.
//
//	orders-api            serve HTTP on HTTP_ADDR
//	orders-api migrate    apply SQL migrations, then exit (Kubernetes Job; use the
//	                      migration user's secret in DB_SECRET_ID)
//
// Composition root: wires Postgres (credentials from Secrets Manager), the
// reservations HTTP client and the payments SQS queue to the use cases by hand.
//
// AWS services touched: RDS PostgreSQL, Secrets Manager, SQS (send), Cognito (JWKS).
package main

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	ohttp "github.com/olagookundavid/gatepass-app/internal/orders/adapters/http"
	opg "github.com/olagookundavid/gatepass-app/internal/orders/adapters/postgres"
	oclient "github.com/olagookundavid/gatepass-app/internal/orders/adapters/reservationsclient"
	osqs "github.com/olagookundavid/gatepass-app/internal/orders/adapters/sqs"
	"github.com/olagookundavid/gatepass-app/internal/orders/app"
	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/bootstrap"
	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
	"github.com/olagookundavid/gatepass-app/internal/platform/lifecycle"
	"github.com/olagookundavid/gatepass-app/internal/platform/pgdb"
	"github.com/olagookundavid/gatepass-app/migrations"

	"os"
)

type serveConfig struct {
	Common          config.Common
	Auth            config.Auth
	DB              opg.DBConfig
	ReservationsURL string
	PaymentsQueue   string
}

func loadServe(l *config.Loader) serveConfig {
	return serveConfig{
		Common:          config.LoadCommon(l, "/orders"),
		Auth:            config.LoadAuth(l),
		DB:              opg.LoadDB(l),
		ReservationsURL: l.Required("RESERVATIONS_INTERNAL_URL"),
		PaymentsQueue:   l.Required("PAYMENTS_QUEUE_URL"),
	}
}

func main() {
	ctx := context.Background()
	var err error
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		err = runMigrate(ctx)
	} else {
		err = runServe(ctx)
	}
	if err != nil {
		bootstrap.Fatal(err)
	}
}

func runMigrate(ctx context.Context) error {
	l := config.FromEnv()
	common := config.LoadCommon(l, "")
	db := opg.LoadDB(l)
	if err := l.Err(); err != nil {
		return err
	}
	env, err := bootstrap.Init(ctx, "orders-migrate", common)
	if err != nil {
		return err
	}
	defer env.FlushTelemetry(ctx) //nolint:errcheck
	pool, err := opg.Connect(ctx, db, env.AWS, "orders-migrate")
	if err != nil {
		return err
	}
	defer pool.Close()
	return pgdb.Migrate(ctx, pool, migrations.Orders())
}

func runServe(ctx context.Context) error {
	l := config.FromEnv()
	cfg := loadServe(l)
	if err := l.Err(); err != nil {
		return err
	}
	env, err := bootstrap.Init(ctx, "orders-api", cfg.Common)
	if err != nil {
		return err
	}
	pool, err := opg.Connect(ctx, cfg.DB, env.AWS, "orders-api")
	if err != nil {
		return err
	}

	svc := app.New(app.Service{
		Orders:   &opg.Repo{DB: pool},
		Holds:    oclient.New(cfg.ReservationsURL, "orders-api"),
		Payments: &osqs.Queue{Client: sqs.NewFromConfig(env.AWS), QueueURL: cfg.PaymentsQueue},
	})
	verifier := auth.NewJWKSVerifier(auth.Config{Issuer: cfg.Auth.Issuer, ClientID: cfg.Auth.ClientID})
	mux := http.NewServeMux()
	(&ohttp.Handler{Svc: svc, Verifier: verifier, BasePath: cfg.Common.BasePath}).Routes(mux)

	// Readiness depends only on Postgres. Reservations being down degrades order
	// creation (503) but must not remove every pod from rotation: reads keep working.
	health := httpserver.NewHealth("orders-api", httpserver.Check{Name: "postgres", Fn: pool.Ping})
	srv := httpserver.New(httpserver.Options{Service: "orders-api", Addr: cfg.Common.HTTPAddr, Handler: mux, Health: health})
	return lifecycle.Run(lifecycle.Options{
		SetNotReady: health.SetShuttingDown, ShutdownDelay: cfg.Common.ShutdownDelay,
		Closers: []func(context.Context) error{func(context.Context) error { pool.Close(); return nil }, env.FlushTelemetry},
	}, httpserver.Component{Name_: "public-http", Srv: srv})
}
