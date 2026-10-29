// Command lambda-nightly-report is the AWS Lambda that writes yesterday's sales
// per event as CSV to S3. Runtime provided.al2023 (arm64), binary "bootstrap".
//
// Trigger: an EventBridge Scheduler schedule `cron(5 0 * * ? *)` (UTC), payload {}.
// Optional payload {"date":"YYYY-MM-DD"} regenerates one finished UTC day.
//
// The function runs INSIDE the VPC (it must reach the private RDS instance). It
// authenticates to PostgreSQL with an RDS IAM token as the read-only user
// `orders_report`, so no password exists anywhere.
//
// Env: DB_HOST, DB_NAME, DB_USER, REPORTS_BUCKET (required); DB_PORT (5432);
// AWS_REGION is set by the Lambda runtime.
//
// AWS services touched: RDS PostgreSQL (IAM auth), S3.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/olagookundavid/gatepass-app/internal/platform/awscfg"
	"github.com/olagookundavid/gatepass-app/internal/platform/buildinfo"
	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/logging"
	"github.com/olagookundavid/gatepass-app/internal/platform/pgdb"
	rpg "github.com/olagookundavid/gatepass-app/internal/reports/adapters/postgres"
	rs3 "github.com/olagookundavid/gatepass-app/internal/reports/adapters/s3"
	rapp "github.com/olagookundavid/gatepass-app/internal/reports/app"
)

func main() {
	l := config.FromEnv()
	region := l.Required("AWS_REGION")
	host, name, user, bucket := l.Required("DB_HOST"), l.Required("DB_NAME"), l.Required("DB_USER"), l.Required("REPORTS_BUCKET")
	port := l.Int("DB_PORT", 5432)
	level := l.String("LOG_LEVEL", "info")
	if err := l.Err(); err != nil {
		slog.Error("invalid configuration", "error", err.Error())
		os.Exit(1)
	}
	slog.SetDefault(logging.New(os.Stdout, "nightly-report", buildinfo.Version, level))

	ctx := context.Background()
	awsCfg, err := awscfg.Load(ctx, region)
	if err != nil {
		slog.Error("aws config", "error", err.Error())
		os.Exit(1)
	}
	// One connection is enough: the function runs one query per invocation. The
	// pool connects lazily, so a cold start does not wait for the database.
	pool, err := pgdb.Connect(ctx, pgdb.Config{Host: host, Port: port, Name: name, MaxConns: 1,
		StatementTimeout: 30 * time.Second, AppName: "nightly-report"},
		pgdb.IAMAuth{Host: host, Port: port, User: user, Region: region, Provider: awsCfg.Credentials})
	if err != nil {
		slog.Error("database", "error", err.Error())
		os.Exit(1)
	}
	svc := &rapp.Service{Source: &rpg.Source{DB: pool}, Store: &rs3.Store{Client: awss3.NewFromConfig(awsCfg), Bucket: bucket}}
	lambda.Start(svc.Run)
}
