// Package ports (reports) declares what the report use case needs.
//
// HEXAGONAL BOUNDARY: owned by the application; implemented by the Postgres and
// S3 adapters.
package ports

import (
	"context"

	"github.com/olagookundavid/gatepass-app/internal/reports/domain"
)

// SalesSource reads aggregated sales for a window.
type SalesSource interface {
	SalesBetween(ctx context.Context, w domain.Window) ([]domain.SalesRow, error)
}

// ReportStore writes the finished report.
type ReportStore interface {
	// Put overwrites any existing object with the same key.
	Put(ctx context.Context, key string, csv []byte) error
}
