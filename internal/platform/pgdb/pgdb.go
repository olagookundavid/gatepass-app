// Package pgdb builds the PostgreSQL connection pool (pgx v5 + pgxpool) for
// Amazon RDS, with the three RDS-specific concerns solved in one place:
//
//  1. TLS verify-full against the RDS CA bundle (embedded in the binary, see
//     certs/global-bundle.pem, downloaded from the RDS "truststore" URL named in
//     the AWS docs "Using SSL/TLS to encrypt a connection to a DB instance").
//     verify-full = the certificate chains to the RDS CA AND its name matches the
//     host we dialled, so a man-in-the-middle inside the VPC cannot impersonate RDS.
//  2. Credentials that CHANGE over time. Two sources, both implementing Credentials:
//     - IAMAuth (catalog, nightly-report): a 15-minute IAM database
//     authentication token generated per NEW connection. No password exists
//     anywhere; the pod's IAM role needs rds-db:connect.
//     - SecretCredentials (orders): username/password from AWS Secrets
//     Manager, cached, refreshed once when authentication fails (the secret
//     may have been rotated).
//  3. Observability: a pgx tracer that creates one OpenTelemetry span per query
//     and gauges for pool usage.
//
// Why credentials are applied in BeforeConnect and not at pool creation: a pool
// opens new connections over its lifetime (growth, idle expiry, failover). A
// password captured once at startup would be stale after 15 minutes (IAM) or
// after a rotation (Secrets Manager). BeforeConnect runs for EVERY new connection.
package pgdb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed certs/global-bundle.pem
var embeddedRDSBundle []byte

// Credentials supplies the user and password for a NEW connection.
type Credentials interface {
	Credentials(ctx context.Context) (user, password string, err error)
}

// Invalidator is implemented by credential sources that cache (Secrets Manager).
type Invalidator interface{ Invalidate() }

// Config is the connection configuration.
type Config struct {
	Host             string
	Port             int
	Name             string
	MaxConns         int32
	StatementTimeout time.Duration // 0 = server default (no limit)
	// RootCertPath overrides the embedded RDS bundle (env DB_SSL_ROOT_CERT).
	RootCertPath string
	AppName      string
}

// Pool wraps *pgxpool.Pool. It adds one behaviour: if a call fails because
// authentication failed (SQLSTATE 28P01 invalid_password / 28000
// invalid_authorization_specification) AND the credential source can be
// invalidated, it invalidates and retries ONCE. That is the "secret rotated
// while we were running" recovery path for orders.
type Pool struct {
	p     *pgxpool.Pool
	creds Credentials
}

// Querier is satisfied by *Pool and pgx.Tx, so repositories work inside or outside a transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Connect creates the pool and verifies one connection can be made.
func Connect(ctx context.Context, cfg Config, creds Credentials) (*Pool, error) {
	tlsCfg, err := tlsConfig(cfg.Host, cfg.RootCertPath)
	if err != nil {
		return nil, err
	}
	// sslmode=require here only means "use TLS"; we replace TLSConfig below with
	// a strict verify-full configuration. No credentials are in this string.
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s sslmode=require", cfg.Host, cfg.Port, cfg.Name)
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse db config: %w", err)
	}
	pc.ConnConfig.TLSConfig = tlsCfg
	pc.MaxConns = max(cfg.MaxConns, 1)
	pc.MaxConnLifetime = 30 * time.Minute // recycle so rotated credentials take effect
	pc.MaxConnLifetimeJitter = 5 * time.Minute
	pc.HealthCheckPeriod = time.Minute
	if cfg.AppName != "" {
		pc.ConnConfig.RuntimeParams["application_name"] = cfg.AppName
	}
	if cfg.StatementTimeout > 0 {
		pc.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(cfg.StatementTimeout.Milliseconds(), 10)
	}
	pc.ConnConfig.Tracer = queryTracer{}
	pc.BeforeConnect = func(ctx context.Context, cc *pgx.ConnConfig) error {
		user, pass, err := creds.Credentials(ctx)
		if err != nil {
			return fmt.Errorf("get db credentials: %w", err)
		}
		cc.User, cc.Password = user, pass
		return nil
	}

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	p := &Pool{p: pool, creds: creds}
	registerPoolMetrics(cfg.Name, pool)
	return p, nil
}

func tlsConfig(host, rootCertPath string) (*tls.Config, error) {
	pem := embeddedRDSBundle
	if rootCertPath != "" {
		b, err := os.ReadFile(rootCertPath)
		if err != nil {
			return nil, fmt.Errorf("read DB_SSL_ROOT_CERT: %w", err)
		}
		pem = b
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("no certificates found in RDS CA bundle")
	}
	// ServerName makes Go verify that the certificate is valid FOR THIS HOST
	// (= libpq sslmode=verify-full). RootCAs restricts trust to the RDS CAs.
	return &tls.Config{RootCAs: pool, ServerName: host, MinVersion: tls.VersionTLS12}, nil
}

func isAuthErr(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && (pe.Code == "28P01" || pe.Code == "28000")
}

// retryAuth runs f; on an authentication failure with an invalidatable source
// it invalidates the cache and runs f once more.
func retryAuth[T any](p *Pool, f func() (T, error)) (T, error) {
	v, err := f()
	if err != nil && isAuthErr(err) {
		if inv, ok := p.creds.(Invalidator); ok {
			inv.Invalidate()
			return f()
		}
	}
	return v, err
}

// Exec implements Querier.
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return retryAuth(p, func() (pgconn.CommandTag, error) { return p.p.Exec(ctx, sql, args...) })
}

// Query implements Querier.
func (p *Pool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return retryAuth(p, func() (pgx.Rows, error) { return p.p.Query(ctx, sql, args...) })
}

// QueryRow implements Querier. It is built on Query so an authentication
// failure (which happens when a connection is acquired) can be retried.
func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	rows, err := p.Query(ctx, sql, args...)
	return &row{rows: rows, err: err}
}

type row struct {
	rows pgx.Rows
	err  error
}

func (r *row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return pgx.ErrNoRows
	}
	if err := r.rows.Scan(dest...); err != nil {
		return err
	}
	r.rows.Close()
	return r.rows.Err()
}

// Begin starts a transaction (read committed).
func (p *Pool) Begin(ctx context.Context) (pgx.Tx, error) {
	return retryAuth(p, func() (pgx.Tx, error) { return p.p.Begin(ctx) })
}

// BeginTx starts a transaction with options.
func (p *Pool) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	return retryAuth(p, func() (pgx.Tx, error) { return p.p.BeginTx(ctx, o) })
}

// Ping verifies a connection can be acquired and the server answers.
func (p *Pool) Ping(ctx context.Context) error {
	_, err := retryAuth(p, func() (struct{}, error) { return struct{}{}, p.p.Ping(ctx) })
	return err
}

// Close closes all connections.
func (p *Pool) Close() { p.p.Close() }

// Raw exposes the underlying pool (used by the migration runner).
func (p *Pool) Raw() *pgxpool.Pool { return p.p }

// InTx runs fn inside a transaction: commit if fn returns nil, otherwise
// rollback. Why a helper: forgetting Rollback on an early return leaks a
// connection and holds row locks until the pool recycles it.
func InTx(ctx context.Context, p *Pool, fn func(tx pgx.Tx) error) error {
	tx, err := p.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// IsUniqueViolation reports SQLSTATE 23505.
func IsUniqueViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}
