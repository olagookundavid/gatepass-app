// Package migrations embeds the SQL migration files into the binaries so a
// Kubernetes Job needs nothing but the image (no volume, no git checkout).
//
// Files are goose migrations (github.com/pressly/goose): "-- +goose Up" sections,
// numbered 0001_, 0002_ ... Only `catalog-api migrate` and `orders-api migrate`
// ever run them. Expand/contract rule: add only; never drop/rename in the same
// release that stops using a column.
package migrations

import (
	"embed"
	"io/fs"
)

//go:embed catalog/*.sql
var catalogFS embed.FS

//go:embed orders/*.sql
var ordersFS embed.FS

// Catalog returns the catalog database migrations.
func Catalog() fs.FS { return must(fs.Sub(catalogFS, "catalog")) }

// Orders returns the orders database migrations.
func Orders() fs.FS { return must(fs.Sub(ordersFS, "orders")) }

func must(f fs.FS, err error) fs.FS {
	if err != nil {
		panic(err) // impossible: the directories exist at compile time
	}
	return f
}
