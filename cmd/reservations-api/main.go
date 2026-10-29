// Command reservations-api serves holds and inventory, and (when
// CONSUMER_ENABLED=true) also consumes the inventory SQS queue.
//
// Composition root of the reservations hexagon: wires DynamoDB, EventBridge
// Scheduler, ElastiCache and SQS adapters to the use cases by hand.
//
// Runs up to three components, all drained on SIGTERM:
//
//	public HTTP    HTTP_ADDR (:8080)           routes under BASE_PATH + /v1 (needs API_ENABLED=true)
//	internal HTTP  INTERNAL_HTTP_ADDR (:8081)  /internal/v1/... consume (never exposed by the Ingress)
//	SQS consumer   INVENTORY_QUEUE_URL         EventPublished / PaymentFailed / OrderCancelled
//
// The same image can run as two Deployments: API_ENABLED=true,CONSUMER_ENABLED=false
// (scales on CPU/requests) and API_ENABLED=false,CONSUMER_ENABLED=true (scales on
// queue depth). In consumer-only mode the public listener still serves /livez,
// /readyz and /version for the probes.
//
// AWS services touched: DynamoDB, EventBridge Scheduler, SQS, ElastiCache, Cognito (JWKS).
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awsscheduler "github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	rddb "github.com/olagookundavid/gatepass-app/internal/reservations/adapters/dynamodb"
	rhttp "github.com/olagookundavid/gatepass-app/internal/reservations/adapters/http"
	rredis "github.com/olagookundavid/gatepass-app/internal/reservations/adapters/redis"
	rsched "github.com/olagookundavid/gatepass-app/internal/reservations/adapters/scheduler"
	rsqs "github.com/olagookundavid/gatepass-app/internal/reservations/adapters/sqs"
	"github.com/olagookundavid/gatepass-app/internal/reservations/app"

	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/bootstrap"
	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
	"github.com/olagookundavid/gatepass-app/internal/platform/lifecycle"
	"github.com/olagookundavid/gatepass-app/internal/platform/redisx"
	"github.com/olagookundavid/gatepass-app/internal/platform/sqsconsumer"
)

type cfg struct {
	Common       config.Common
	Auth         config.Auth
	Table        string
	APIEnabled   bool
	InternalAddr string
	HoldTTL      time.Duration
	RateLimit    int

	SchedulerGroup, SchedulerRole, ExpirerARN string
	Redis                                     redisx.Config

	ConsumerEnabled bool
	QueueURL        string
	Workers         int
	Visibility      time.Duration
}

func loadConfig(l *config.Loader) cfg {
	c := cfg{
		Common:          config.LoadCommon(l, "/reservations"),
		Table:           l.Required("TABLE_NAME"),
		APIEnabled:      l.Bool("API_ENABLED", true),
		InternalAddr:    l.String("INTERNAL_HTTP_ADDR", ":8081"),
		HoldTTL:         l.Duration("HOLD_TTL", 10*time.Minute),
		RateLimit:       l.Int("RATE_LIMIT_PER_MINUTE", 10),
		ConsumerEnabled: l.Bool("CONSUMER_ENABLED", false),
		Workers:         l.Int("CONSUMER_WORKERS", 4),
		Visibility:      l.Duration("CONSUMER_VISIBILITY_TIMEOUT", 60*time.Second),
	}
	l.Check(c.APIEnabled || c.ConsumerEnabled, "at least one of API_ENABLED and CONSUMER_ENABLED must be true")
	if c.ConsumerEnabled {
		c.QueueURL = l.Required("INVENTORY_QUEUE_URL")
	}
	if c.APIEnabled {
		c.Auth = config.LoadAuth(l)
		c.SchedulerGroup = l.Required("SCHEDULER_GROUP_NAME")
		c.SchedulerRole = l.Required("SCHEDULER_ROLE_ARN")
		c.ExpirerARN = l.Required("EXPIRER_LAMBDA_ARN")
		c.Redis = redisx.Config{
			Addr: l.Required("REDIS_ADDR"), User: l.String("REDIS_USER", ""), IAMAuth: l.Bool("REDIS_IAM_AUTH", false),
			AuthFile: l.String("REDIS_AUTH_FILE", ""), CacheName: l.String("CACHE_NAME", ""), Serverless: l.Bool("REDIS_SERVERLESS", false),
		}
		l.Check(!c.Redis.IAMAuth || (c.Redis.User != "" && c.Redis.CacheName != ""), "REDIS_IAM_AUTH=true requires REDIS_USER and CACHE_NAME")
		l.Check(c.HoldTTL >= time.Minute && c.HoldTTL <= time.Hour, "HOLD_TTL must be between 1m and 1h")
	}
	return c
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
	env, err := bootstrap.Init(ctx, "reservations-api", c.Common)
	if err != nil {
		return err
	}

	store := &rddb.Store{Client: dynamodb.NewFromConfig(env.AWS), Table: c.Table}
	var svc *app.Service
	closers := []func(context.Context) error{}

	// Readiness: a cheap GetItem proves DynamoDB is reachable AND our role may read
	// the table (no extra IAM action such as dynamodb:DescribeTable is needed).
	health := httpserver.NewHealth("reservations-api", httpserver.Check{Name: "dynamodb", Fn: func(ctx context.Context) error {
		_, err := store.EventProcessed(ctx, "readyz-probe")
		return err
	}})
	publicMux := http.NewServeMux()
	var comps []lifecycle.Component

	if c.APIEnabled {
		rdb, err := redisx.New(withAWS(c.Redis, c.Common.AWSRegion, env))
		if err != nil {
			return err
		}
		closers = append(closers, func(context.Context) error { return rdb.Close() })
		svc = app.New(app.Service{
			Store: store,
			Scheduler: &rsched.Scheduler{Client: awsscheduler.NewFromConfig(env.AWS), Group: c.SchedulerGroup,
				RoleARN: c.SchedulerRole, TargetARN: c.ExpirerARN},
			Limiter:   &rredis.Limiter{C: rdb},
			HoldTTL:   c.HoldTTL,
			RateLimit: c.RateLimit,
		})
		verifier := auth.NewJWKSVerifier(auth.Config{Issuer: c.Auth.Issuer, ClientID: c.Auth.ClientID})
		h := &rhttp.Handler{Svc: svc, Verifier: verifier, BasePath: c.Common.BasePath}
		h.PublicRoutes(publicMux)

		internalMux := http.NewServeMux()
		h.InternalRoutes(internalMux)
		// No Health on the internal listener: probes use the public port.
		comps = append(comps, httpserver.Component{Name_: "internal-http",
			Srv: httpserver.New(httpserver.Options{Service: "reservations-internal", Addr: c.InternalAddr, Handler: internalMux})})
	} else {
		svc = app.New(app.Service{Store: store})
	}
	comps = append(comps, httpserver.Component{Name_: "public-http",
		Srv: httpserver.New(httpserver.Options{Service: "reservations-api", Addr: c.Common.HTTPAddr, Handler: publicMux, Health: health})})

	if c.ConsumerEnabled {
		consumer, err := sqsconsumer.New(sqsconsumer.Config{Name: "inventory", QueueURL: c.QueueURL, Workers: c.Workers,
			Visibility: c.Visibility, Client: sqs.NewFromConfig(env.AWS), Handler: rsqs.Handler(svc)})
		if err != nil {
			return err
		}
		comps = append(comps, consumer)
	}

	closers = append(closers, env.FlushTelemetry)
	return lifecycle.Run(lifecycle.Options{SetNotReady: health.SetShuttingDown, ShutdownDelay: c.Common.ShutdownDelay, Closers: closers}, comps...)
}

func withAWS(r redisx.Config, region string, env *bootstrap.Env) redisx.Config {
	r.Region, r.Creds = region, env.AWS.Credentials
	return r
}
