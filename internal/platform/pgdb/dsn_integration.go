//go:build integration

package pgdb

// Test-only constructors. They exist so integration tests can run the SQL of the
// repositories (and the credential-rotation logic) against any PostgreSQL: a
// throwaway local one, or an RDS test database reached with a password-based
// test user. They skip TLS verification and are excluded from normal builds by
// the `integration` build tag, so no production binary can contain them.

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ConnectDSN opens a pool using a standard libpq/pgx connection string
// (which carries user and password).
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

// ConnectDSNWithCreds is like Connect but takes a DSN WITHOUT user/password and no
// TLS verification: the user and password come from creds for every new
// connection, exactly as in production. Used to test secret rotation.
func ConnectDSNWithCreds(ctx context.Context, dsn string, creds Credentials) (*Pool, error) {
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pc.BeforeConnect = func(ctx context.Context, cc *pgx.ConnConfig) error {
		u, p, err := creds.Credentials(ctx)
		if err != nil {
			return fmt.Errorf("get db credentials: %w", err)
		}
		cc.User, cc.Password = u, p
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, err
	}
	return &Pool{p: pool, creds: creds}, nil
}
