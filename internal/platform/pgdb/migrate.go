package pgdb

// This file: running embedded goose migrations.
//
// Migrations run ONLY from `<service>-api migrate` (a Kubernetes Job), never at
// API start-up. Why: with several replicas starting at once, N pods would race to
// alter the schema; and an API pod should not need DDL privileges.
//
// MIGRATION RULE (expand/contract): a release may only ADD columns/tables/indexes.
// Dropping or renaming something happens in a LATER release, after no running
// code uses it. Reason: rolling back an image does not roll back the schema, so
// the old image must still work against the new schema.

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// Migrate applies all pending migrations found in fsys (directory "." of the fs).
func Migrate(ctx context.Context, p *Pool, fsys fs.FS) error {
	db := stdlib.OpenDBFromPool(p.Raw())
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, fsys)
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}
	results, err := provider.Up(ctx)
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	for _, r := range results {
		slog.InfoContext(ctx, "migration applied", "version", r.Source.Version, "path", r.Source.Path, "duration", r.Duration.String())
	}
	if len(results) == 0 {
		slog.InfoContext(ctx, "schema already up to date")
	}
	return nil
}
