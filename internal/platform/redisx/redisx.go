// Package redisx builds the Redis/Valkey client for Amazon ElastiCache.
//
// Used by catalog (cache-aside) and reservations (rate limiting). Both treat the
// cache as OPTIONAL: any Redis error is logged, counted and the request carries
// on (see the callers). This package only builds the client.
//
// Connection security (required by the Gatepass spec):
//   - TLS always on (ElastiCache "in-transit encryption").
//   - Authentication, in order of preference:
//     (a) IAM authentication (REDIS_IAM_AUTH=true): the password is a SigV4
//     PRESIGNED REQUEST used as a token, valid 15 minutes, for a Redis user
//     (REDIS_USER) that was created with authentication mode "iam". The pod
//     role needs elasticache:Connect on the cache and the user.
//     (b) a static AUTH password read from the file REDIS_AUTH_FILE (a mounted
//     Kubernetes secret). Fallback when IAM auth is not set up.
//
// UNVERIFIED: the exact token format below follows the AWS docs "Authenticating
// with IAM for Valkey and Redis OSS" (a presigned GET to
// http://<cache-name>/?Action=connect&User=<user>[&ResourceType=ServerlessCache],
// service "elasticache", 900 s, scheme prefix removed). It must be verified
// against the real service; see HANDOFF.md.
package redisx

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

// Config for New.
type Config struct {
	Addr       string // host:port
	User       string // Redis ACL user (IAM-enabled user when IAMAuth)
	IAMAuth    bool
	AuthFile   string // fallback password file
	CacheName  string // ElastiCache replication group / serverless cache name (IAM token subject)
	Serverless bool   // true for ElastiCache Serverless (adds ResourceType=ServerlessCache)
	Region     string
	Creds      aws.CredentialsProvider
	// Timeouts keep a sick cache from stalling requests: a cache must be FASTER than the database.
	DialTimeout, ReadTimeout, WriteTimeout time.Duration
}

// New creates the client. It does not dial until first use.
func New(cfg Config) (*redis.Client, error) {
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("REDIS_ADDR must be host:port: %w", err)
	}
	opts := &redis.Options{
		Addr:         cfg.Addr,
		TLSConfig:    &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12},
		DialTimeout:  def(cfg.DialTimeout, 500*time.Millisecond),
		ReadTimeout:  def(cfg.ReadTimeout, 300*time.Millisecond),
		WriteTimeout: def(cfg.WriteTimeout, 300*time.Millisecond),
		MaxRetries:   1,
	}
	switch {
	case cfg.IAMAuth:
		if cfg.User == "" || cfg.CacheName == "" || cfg.Creds == nil {
			return nil, fmt.Errorf("REDIS_IAM_AUTH needs REDIS_USER, CACHE_NAME and AWS credentials")
		}
		ts := &tokenSource{cfg: cfg}
		// Called by go-redis for EVERY new connection (also after reconnects).
		opts.CredentialsProviderContext = func(ctx context.Context) (string, string, error) {
			tok, err := ts.token(ctx)
			return cfg.User, tok, err
		}
	case cfg.AuthFile != "":
		opts.CredentialsProviderContext = func(context.Context) (string, string, error) {
			b, err := os.ReadFile(cfg.AuthFile)
			if err != nil {
				return "", "", fmt.Errorf("read REDIS_AUTH_FILE: %w", err)
			}
			return cfg.User, strings.TrimSpace(string(b)), nil
		}
	}
	c := redis.NewClient(opts)
	c.AddHook(tracingHook{})
	return c, nil
}

func def(v, d time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return d
}

// tokenSource caches the IAM token for 10 of its 15 minutes of validity so it is
// refreshed BEFORE expiry (a connection attempt with an expired token fails
// with WRONGPASS).
type tokenSource struct {
	cfg Config
	mu  sync.Mutex
	tok string
	at  time.Time
}

func (t *tokenSource) token(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tok != "" && time.Since(t.at) < 10*time.Minute {
		return t.tok, nil
	}
	tok, err := BuildIAMToken(ctx, t.cfg.Creds, t.cfg.Region, t.cfg.CacheName, t.cfg.User, t.cfg.Serverless, time.Now())
	if err != nil {
		return "", err
	}
	t.tok, t.at = tok, time.Now()
	return tok, nil
}

// BuildIAMToken creates the ElastiCache IAM auth token (see the package comment
// about UNVERIFIED). Exported for the unit test and for integration tests.
//
// IAM action: elasticache:Connect on arn:aws:elasticache:<region>:<account-id>:replicationgroup:<name>
// (or :serverlesscache:<name>) and on arn:aws:elasticache:<region>:<account-id>:user:<user>.
// Failure looks like: "WRONGPASS invalid username-password pair or user is disabled".
func BuildIAMToken(ctx context.Context, creds aws.CredentialsProvider, region, cacheName, user string, serverless bool, now time.Time) (string, error) {
	q := url.Values{"Action": {"connect"}, "User": {user}}
	if serverless {
		q.Set("ResourceType", "ServerlessCache")
	}
	q.Set("X-Amz-Expires", "900")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+cacheName+"/?"+q.Encode(), nil)
	if err != nil {
		return "", err
	}
	c, err := creds.Retrieve(ctx)
	if err != nil {
		return "", fmt.Errorf("retrieve aws credentials: %w", err)
	}
	emptyHash := sha256.Sum256(nil)
	signed, _, err := v4.NewSigner().PresignHTTP(ctx, c, req, hex.EncodeToString(emptyHash[:]), "elasticache", region, now)
	if err != nil {
		return "", fmt.Errorf("presign: %w", err)
	}
	return strings.TrimPrefix(signed, "http://"), nil
}

// tracingHook creates one span per Redis command (go-redis v9 Hook interface).
type tracingHook struct{}

func (tracingHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (tracingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		ctx, span := telemetry.Tracer().Start(ctx, "redis "+cmd.Name(), trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attribute.String("db.system", "redis"), attribute.String("db.operation", cmd.Name())))
		defer span.End()
		err := next(ctx, cmd)
		if err != nil && err != redis.Nil { //nolint:errorlint // redis.Nil is a sentinel, not a failure
			span.RecordError(err)
			span.SetStatus(codes.Error, "redis command failed")
		}
		return err
	}
}

func (tracingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		ctx, span := telemetry.Tracer().Start(ctx, "redis pipeline", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attribute.String("db.system", "redis"), attribute.Int("db.pipeline.length", len(cmds))))
		defer span.End()
		return next(ctx, cmds)
	}
}
