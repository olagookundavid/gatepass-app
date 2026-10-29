// Command catalog-api is the public catalog service (venues, events, ticket
// types), and with the subcommand `migrate` the database migration job.
//
// This file is the COMPOSITION ROOT of the hexagon: the only place that knows
// which adapter implements which port. Wiring is done by hand (no DI framework)
// so a reader can follow it top to bottom.
//
//	catalog-api            serve HTTP on HTTP_ADDR
//	catalog-api migrate    apply SQL migrations, then exit (Kubernetes Job)
//
// AWS services touched: RDS PostgreSQL (IAM auth), ElastiCache, S3 (presign),
// EventBridge, Cognito (JWKS).
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	cbridge "github.com/olagookundavid/gatepass-app/internal/catalog/adapters/eventbridge"
	chttp "github.com/olagookundavid/gatepass-app/internal/catalog/adapters/http"
	cpg "github.com/olagookundavid/gatepass-app/internal/catalog/adapters/postgres"
	credis "github.com/olagookundavid/gatepass-app/internal/catalog/adapters/redis"
	cs3 "github.com/olagookundavid/gatepass-app/internal/catalog/adapters/s3"
	"github.com/olagookundavid/gatepass-app/internal/catalog/app"
	"github.com/olagookundavid/gatepass-app/internal/platform/auth"
	"github.com/olagookundavid/gatepass-app/internal/platform/bootstrap"
	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
	"github.com/olagookundavid/gatepass-app/internal/platform/lifecycle"
	"github.com/olagookundavid/gatepass-app/internal/platform/pgdb"
	"github.com/olagookundavid/gatepass-app/internal/platform/redisx"
	"github.com/olagookundavid/gatepass-app/migrations"
)

// dbConfig is shared by serve and migrate.
type dbConfig struct {
	Host, Name, User, RootCert string
	Port                       int
	MaxConns                   int
	StatementTimeout           time.Duration
}

func loadDB(l *config.Loader) dbConfig {
	d := dbConfig{
		Host: l.Required("DB_HOST"), Port: l.Int("DB_PORT", 5432), Name: l.Required("DB_NAME"), User: l.Required("DB_USER"),
		MaxConns:         l.Int("DB_MAX_CONNS", 10),
		StatementTimeout: l.Duration("DB_STATEMENT_TIMEOUT", 5*time.Second),
		RootCert:         l.String("DB_SSL_ROOT_CERT", ""),
	}
	// The catalog authenticates to RDS ONLY with IAM tokens (no password exists).
	l.Check(l.Bool("DB_IAM_AUTH", false), "DB_IAM_AUTH must be true (catalog supports only RDS IAM authentication)")
	return d
}

type serveConfig struct {
	Common  config.Common
	Auth    config.Auth
	DB      dbConfig
	Redis   redisx.Config
	Images  string
	CDNBase string
	Bus     string
}

func loadServe(l *config.Loader) serveConfig {
	c := serveConfig{
		Common: config.LoadCommon(l, "/catalog"),
		Auth:   config.LoadAuth(l),
		DB:     loadDB(l),
		Redis: redisx.Config{
			Addr: l.Required("REDIS_ADDR"), User: l.String("REDIS_USER", ""),
			IAMAuth: l.Bool("REDIS_IAM_AUTH", false), AuthFile: l.String("REDIS_AUTH_FILE", ""),
			CacheName: l.String("CACHE_NAME", ""), Serverless: l.Bool("REDIS_SERVERLESS", false),
		},
		Images:  l.Required("IMAGES_BUCKET"),
		CDNBase: l.Required("CDN_BASE_URL"),
		Bus:     l.Required("EVENT_BUS_NAME"),
	}
	l.Check(!c.Redis.IAMAuth || (c.Redis.User != "" && c.Redis.CacheName != ""), "REDIS_IAM_AUTH=true requires REDIS_USER and CACHE_NAME")
	return c
}

func main() {
	ctx := context.Background()
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if err := runMigrate(ctx); err != nil {
			bootstrap.Fatal(err)
		}
		return
	}
	if err := runServe(ctx); err != nil {
		bootstrap.Fatal(err)
	}
}

func connectDB(ctx context.Context, d dbConfig, env *bootstrap.Env, region string) (*pgdb.Pool, error) {
	return pgdb.Connect(ctx, pgdb.Config{Host: d.Host, Port: d.Port, Name: d.Name, MaxConns: int32(d.MaxConns),
		StatementTimeout: d.StatementTimeout, RootCertPath: d.RootCert, AppName: "catalog-api"},
		pgdb.IAMAuth{Host: d.Host, Port: d.Port, User: d.User, Region: region, Provider: env.AWS.Credentials})
}

func runMigrate(ctx context.Context) error {
	l := config.FromEnv()
	common := config.LoadCommon(l, "")
	db := loadDB(l)
	if err := l.Err(); err != nil {
		return err
	}
	env, err := bootstrap.Init(ctx, "catalog-migrate", common)
	if err != nil {
		return err
	}
	defer env.FlushTelemetry(ctx) //nolint:errcheck
	// Migrations need DDL rights, so DB_USER here is the migration user
	// (catalog_migrator), not catalog_app.
	pool, err := connectDB(ctx, db, env, common.AWSRegion)
	if err != nil {
		return err
	}
	defer pool.Close()
	return pgdb.Migrate(ctx, pool, migrations.Catalog())
}

func runServe(ctx context.Context) error {
	l := config.FromEnv()
	cfg := loadServe(l)
	if err := l.Err(); err != nil {
		return err
	}
	env, err := bootstrap.Init(ctx, "catalog-api", cfg.Common)
	if err != nil {
		return err
	}

	pool, err := connectDB(ctx, cfg.DB, env, cfg.Common.AWSRegion)
	if err != nil {
		return err
	}

	rc := cfg.Redis
	rc.Region, rc.Creds = cfg.Common.AWSRegion, env.AWS.Credentials
	rdb, err := redisx.New(rc)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	pub := &ebpub.Publisher{Client: eventbridge.NewFromConfig(env.AWS), Bus: cfg.Bus, Source: "gatepass.catalog"}
	repo := &cpg.Repo{DB: pool}
	svc := app.New(app.Service{
		Events:    repo,
		Venues:    cpg.VenueRepo{R: repo},
		Cache:     &credis.Cache{C: rdb},
		Announcer: &cbridge.Announcer{Pub: pub},
		Images:    cs3.New(s3.NewFromConfig(env.AWS), cfg.Images),
		CDNBase:   cfg.CDNBase,
	})

	verifier := auth.NewJWKSVerifier(auth.Config{Issuer: cfg.Auth.Issuer, ClientID: cfg.Auth.ClientID})
	mux := http.NewServeMux()
	(&chttp.Handler{Svc: svc, Verifier: verifier, BasePath: cfg.Common.BasePath}).Routes(mux)

	// Readiness: Postgres is critical (without it every endpoint fails). Redis is
	// deliberately NOT a readiness check: the cache is optional, and taking every
	// pod out of rotation because the cache is down would turn a degraded
	// service into an outage.
	health := httpserver.NewHealth("catalog-api", httpserver.Check{Name: "postgres", Fn: pool.Ping})
	srv := httpserver.New(httpserver.Options{Service: "catalog-api", Addr: cfg.Common.HTTPAddr, Handler: mux, Health: health})

	return lifecycle.Run(lifecycle.Options{
		SetNotReady:   health.SetShuttingDown,
		ShutdownDelay: cfg.Common.ShutdownDelay,
		Closers: []func(context.Context) error{
			func(context.Context) error { return rdb.Close() },
			func(context.Context) error { pool.Close(); return nil },
			env.FlushTelemetry,
		},
	}, httpserver.Component{Name_: "public-http", Srv: srv})
}
