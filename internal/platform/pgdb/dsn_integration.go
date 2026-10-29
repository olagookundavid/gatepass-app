//go:build integration

package pgdb

// Test-only constructor: a pool from a plain DSN (no TLS verification, no IAM,
// no Secrets Manager). It exists so integration tests can run the SQL of the
// repositories against any PostgreSQL (a throwaway local one, or RDS reached
// with a password-based test user). It is excluded from normal builds by the
// `integration` build tag, so no production binary can contain it.

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ConnectDSN opens a pool using a standard libpq/pgx connection string.
func ConnectDSN(ctx context.Context, dsn string) (*Pool, error) {
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pc.ConnConfig.Tracer = queryTracer{}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, err
	}
	return &Pool{p: pool, creds: nil}, nil
}
