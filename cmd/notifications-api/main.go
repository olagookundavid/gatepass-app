// Command notifications-api serves the notification log, preferences and ticket
// download links.
//
// Composition root: DynamoDB store + S3 presigner wired to the use cases by hand.
//
// AWS services touched: DynamoDB, S3 (presign only), Cognito (JWKS).
package main

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	nddb "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/dynamodb"
	nhttp "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/http"
	ns3 "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/s3"
	"github.com/olagookundavid/gatepass-app/internal/notifications/app"
	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/bootstrap"
	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
	"github.com/olagookundavid/gatepass-app/internal/platform/lifecycle"
)

type cfg struct {
	Common config.Common
	Auth   config.Auth
	Table  string
	Bucket string
}

func loadConfig(l *config.Loader) cfg {
	return cfg{Common: config.LoadCommon(l, "/notifications"), Auth: config.LoadAuth(l),
		Table: l.Required("TABLE_NAME"), Bucket: l.Required("TICKETS_BUCKET")}
}

func main() {
	if err := run(context.Background()); err != nil {
		bootstrap.Fatal(err)
	}
}

func run(ctx context.Context) error {
	l := config.FromEnv()
	c := loadConfig(l)
	if err := l.Err(); err != nil {
		return err
	}
	env, err := bootstrap.Init(ctx, "notifications-api", c.Common)
	if err != nil {
		return err
	}
	store := &nddb.Store{Client: dynamodb.NewFromConfig(env.AWS), Table: c.Table}
	api := &app.API{Store: store, Tickets: ns3.New(awss3.NewFromConfig(env.AWS), c.Bucket)}
	verifier := auth.NewJWKSVerifier(auth.Config{Issuer: c.Auth.Issuer, ClientID: c.Auth.ClientID})
	mux := http.NewServeMux()
	(&nhttp.Handler{API: api, Verifier: verifier, BasePath: c.Common.BasePath}).Routes(mux)

	// Readiness: a GetItem proves DynamoDB access without needing DescribeTable.
	health := httpserver.NewHealth("notifications-api", httpserver.Check{Name: "dynamodb", Fn: func(ctx context.Context) error {
		_, err := store.EventProcessed(ctx, "readyz-probe")
		return err
	}})
	srv := httpserver.New(httpserver.Options{Service: "notifications-api", Addr: c.Common.HTTPAddr, Handler: mux, Health: health})
	return lifecycle.Run(lifecycle.Options{SetNotReady: health.SetShuttingDown, ShutdownDelay: c.Common.ShutdownDelay,
		Closers: []func(context.Context) error{env.FlushTelemetry}},
		httpserver.Component{Name_: "public-http", Srv: srv})
}
