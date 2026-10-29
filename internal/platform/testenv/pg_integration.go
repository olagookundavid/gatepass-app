//go:build integration

package testenv

import (
	"context"
	"io/fs"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/olagookundavid/gatepass-app/internal/platform/pgdb"
)

// FreshSchema creates a private schema in the database behind dsn, runs the given
// migrations in it and returns a pool whose search_path points at it. The schema
// is dropped when the test ends. dsn must allow CREATE SCHEMA.
func FreshSchema(t *testing.T, dsn string, migrations fs.FS) *pgdb.Pool {
	t.Helper()
	ctx := context.Background()
	schema := strings.ReplaceAll(Prefix(), "-", "_")
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); admin.Close() })

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	pool, err := pgdb.ConnectDSN(ctx, dsn+sep+"search_path="+schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pgdb.Migrate(ctx, pool, migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}
