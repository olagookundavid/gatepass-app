// Package app (reports) is the nightly report use case.
//
// HEXAGONAL BOUNDARY: depends only on domain and ports.
//
// Idempotency by construction: the output key is derived from the REPORT DAY
// (reports/YYYY/MM/DD.csv), not from the time the job ran, and the file is
// overwritten. Scheduler may retry the invocation, or an operator may re-run a
// day: the result is the same single file, never a duplicate. (The data of a past
// day can still change slightly, e.g. a later cancellation changes
// `orders_still_paid`; re-running refreshes it.)
//
// AWS services touched (through ports): RDS PostgreSQL (IAM auth), S3.
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/reports/domain"
	"github.com/olagookundavid/gatepass-app/internal/reports/ports"
)

// Service builds and stores reports.
type Service struct {
	Source ports.SalesSource
	Store  ports.ReportStore
	Now    func() time.Time
}

// Input is the optional Lambda payload. EventBridge Scheduler sends {} for the daily
// run; {"date":"2026-02-03"} regenerates a specific UTC day.
type Input struct {
	Date string `json:"date"`
}

// Output is returned to the invoker (visible in logs / test console).
type Output struct {
	Key  string `json:"key"`
	Rows int    `json:"rows"`
}

// Run generates the report for the day in the input (default: yesterday, UTC).
func (s *Service) Run(ctx context.Context, raw json.RawMessage) (Output, error) {
	var in Input
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &in); err != nil {
			return Output{}, fmt.Errorf("payload is not JSON: %w", err)
		}
	}
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now()
	}
	w := domain.Yesterday(now)
	if in.Date != "" {
		var err error
		if w, err = domain.ParseDay(in.Date); err != nil {
			return Output{}, err
		}
		if w.End.After(now) { // the day is not over yet
			return Output{}, fmt.Errorf("date %s is not finished yet", in.Date)
		}
	}

	rows, err := s.Source.SalesBetween(ctx, w)
	if err != nil {
		return Output{}, fmt.Errorf("query sales: %w", err)
	}
	body, err := domain.CSV(w, rows)
	if err != nil {
		return Output{}, err
	}
	key := w.ObjectKey()
	if err := s.Store.Put(ctx, key, body); err != nil {
		return Output{}, fmt.Errorf("store report: %w", err)
	}
	slog.InfoContext(ctx, "nightly report written", "key", key, "rows", len(rows), "day", w.Start.Format("2006-01-02"))
	return Output{Key: key, Rows: len(rows)}, nil
}
