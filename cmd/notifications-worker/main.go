// Command notifications-worker consumes the notifications queue: for each
// OrderPaid it renders a ticket PNG with a signed QR code, stores it in S3, looks
// up the buyer's e-mail in Cognito and sends it through SES; PaymentFailed and
// OrderCancelled get short e-mails.
//
// Runs the SQS consumer plus a tiny HTTP server for the Kubernetes probes.
//
// AWS services touched: SQS, DynamoDB, S3, Cognito (AdminGetUser), SES v2.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	ncognito "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/cognito"
	nddb "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/dynamodb"
	ns3 "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/s3"
	nses "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/ses"
	nsqs "github.com/olagookundavid/gatepass-app/internal/notifications/adapters/sqs"
	"github.com/olagookundavid/gatepass-app/internal/notifications/adapters/ticketpng"
	"github.com/olagookundavid/gatepass-app/internal/notifications/app"
	"github.com/olagookundavid/gatepass-app/internal/platform/bootstrap"
	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
	"github.com/olagookundavid/gatepass-app/internal/platform/lifecycle"
	"github.com/olagookundavid/gatepass-app/internal/platform/sqsconsumer"
)

type cfg struct {
	Common      config.Common
	Table       string
	Bucket      string
	Queue       string
	UserPoolID  string
	From        string
	ConfigSet   string
	Override    string
	SigningFile string
	LinkTTL     time.Duration
	Concurrency int
	Visibility  time.Duration
}

func loadConfig(l *config.Loader) cfg {
	c := cfg{
		Common:      config.LoadCommon(l, ""),
		Table:       l.Required("TABLE_NAME"),
		Bucket:      l.Required("TICKETS_BUCKET"),
		Queue:       l.Required("NOTIFICATIONS_QUEUE_URL"),
		UserPoolID:  l.Required("COGNITO_USER_POOL_ID"),
		From:        l.Required("SES_FROM_ADDRESS"),
		ConfigSet:   l.String("SES_CONFIGURATION_SET", ""),
		Override:    l.String("EMAIL_RECIPIENT_OVERRIDE", ""),
		SigningFile: l.Required("TICKET_SIGNING_KEY_FILE"),
		LinkTTL:     l.Duration("TICKET_LINK_TTL", 24*time.Hour),
		Concurrency: l.Int("WORKER_CONCURRENCY", 4),
		Visibility:  l.Duration("CONSUMER_VISIBILITY_TIMEOUT", 60*time.Second),
	}
	l.Check(c.LinkTTL >= time.Minute && c.LinkTTL <= 7*24*time.Hour, "TICKET_LINK_TTL must be between 1m and 168h")
	l.Check(c.Concurrency >= 1 && c.Concurrency <= 64, "WORKER_CONCURRENCY must be between 1 and 64, got %d", c.Concurrency)
	return c
}

// readSigningKey loads the HMAC key from a mounted secret file. At least 32 bytes.
func readSigningKey(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read TICKET_SIGNING_KEY_FILE: %w", err)
	}
	key := []byte(strings.TrimSpace(string(b)))
	if len(key) < 32 {
		return nil, errors.New("TICKET_SIGNING_KEY_FILE must contain at least 32 characters")
	}
	return key, nil
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
	key, err := readSigningKey(c.SigningFile)
	if err != nil {
		return err
	}
	env, err := bootstrap.Init(ctx, "notifications-worker", c.Common)
	if err != nil {
		return err
	}

	store := &nddb.Store{Client: dynamodb.NewFromConfig(env.AWS), Table: c.Table}
	worker := app.NewWorker(app.Worker{
		Store:             store,
		Tickets:           ns3.New(awss3.NewFromConfig(env.AWS), c.Bucket),
		Renderer:          ticketpng.Renderer{},
		Directory:         &ncognito.Directory{Client: cognitoidentityprovider.NewFromConfig(env.AWS), UserPoolID: c.UserPoolID},
		Mailer:            &nses.Mailer{Client: sesv2.NewFromConfig(env.AWS), From: c.From, ConfigurationSet: c.ConfigSet},
		SigningKey:        key,
		RecipientOverride: c.Override,
		LinkTTL:           c.LinkTTL,
	})
	consumer, err := sqsconsumer.New(sqsconsumer.Config{Name: "notifications", QueueURL: c.Queue, Workers: c.Concurrency,
		Visibility: c.Visibility, Client: sqs.NewFromConfig(env.AWS), Handler: nsqs.Handler(worker)})
	if err != nil {
		return err
	}

	health := httpserver.NewHealth("notifications-worker", httpserver.Check{Name: "dynamodb", Fn: func(ctx context.Context) error {
		_, err := store.EventProcessed(ctx, "readyz-probe")
		return err
	}})
	srv := httpserver.New(httpserver.Options{Service: "notifications-worker", Addr: c.Common.HTTPAddr, Handler: http.NewServeMux(), Health: health})
	return lifecycle.Run(lifecycle.Options{SetNotReady: health.SetShuttingDown, ShutdownDelay: c.Common.ShutdownDelay,
		Closers: []func(context.Context) error{env.FlushTelemetry}},
		httpserver.Component{Name_: "probes-http", Srv: srv}, consumer)
}
