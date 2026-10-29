// Command lambda-hold-expirer is the AWS Lambda that returns the tickets of an
// expired hold. Runtime provided.al2023 (arm64), binary name "bootstrap".
//
// Triggers (one handler, two payload shapes, see adapters/lambda):
//  1. EventBridge Scheduler one-time schedule created per hold: {"holdId":"..."}
//  2. DynamoDB Streams of the reservations table (TTL deletions): safety net
//
// Initialisation happens here, outside the handler, so a warm invocation reuses
// the DynamoDB client (cold start cost is paid once per execution environment).
//
// Env: TABLE_NAME (required). AWS_REGION and credentials are provided by the
// Lambda runtime (execution role).
//
// AWS services touched: DynamoDB, CloudWatch Logs (EMF metric line).
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	rddb "github.com/olagookundavid/gatepass-app/internal/reservations/adapters/dynamodb"
	rlambda "github.com/olagookundavid/gatepass-app/internal/reservations/adapters/lambda"
	"github.com/olagookundavid/gatepass-app/internal/reservations/app"

	"github.com/olagookundavid/gatepass-app/internal/platform/awscfg"
	"github.com/olagookundavid/gatepass-app/internal/platform/buildinfo"
	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/logging"
)

func main() {
	l := config.FromEnv()
	table := l.Required("TABLE_NAME")
	region := l.Required("AWS_REGION")
	level := l.String("LOG_LEVEL", "info")
	if err := l.Err(); err != nil {
		slog.Error("invalid configuration", "error", err.Error())
		os.Exit(1)
	}
	slog.SetDefault(logging.New(os.Stdout, "hold-expirer", buildinfo.Version, level))

	ctx := context.Background()
	awsCfg, err := awscfg.Load(ctx, region)
	if err != nil {
		slog.Error("aws config", "error", err.Error())
		os.Exit(1)
	}
	store := &rddb.Store{Client: dynamodb.NewFromConfig(awsCfg), Table: table}
	h := &rlambda.Handler{Svc: app.New(app.Service{Store: store})}
	lambda.Start(h.Handle)
}
