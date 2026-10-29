// Package domain (reports) holds the nightly sales report model: which day is
// "yesterday", what a row is, and how the CSV is written.
//
// HEXAGONAL BOUNDARY: pure Go. The only subtle rule is the day window, so it is
// isolated and unit tested: report days are UTC calendar days, the window is
// half-open [start, end) so a payment at exactly midnight belongs to exactly one day.
//
// AWS services touched: none.
package domain

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strconv"
	"time"
)

// SalesRow is one line of the report: paid orders of one event and currency.
type SalesRow struct {
	EventID        string
	Currency       string
	Orders         int64 // orders that became paid on the report day
	Tickets        int64
	GrossCents     int64
	StillPaidCount int64 // of those orders, how many are still paid now (not cancelled later)
}

// Window is a half-open UTC interval [Start, End).
type Window struct{ Start, End time.Time }

// DayWindow returns the window of the UTC calendar day containing t.
func DayWindow(t time.Time) Window {
	u := t.UTC()
	start := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
	return Window{Start: start, End: start.AddDate(0, 0, 1)}
}

// Yesterday returns the UTC day before now.
func Yesterday(now time.Time) Window {
	return DayWindow(DayWindow(now).Start.Add(-time.Nanosecond))
}

// ParseDay parses "YYYY-MM-DD" (used for backfills).
func ParseDay(s string) (Window, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return Window{}, fmt.Errorf("date must be YYYY-MM-DD: %w", err)
	}
	return DayWindow(t), nil
}

// ObjectKey is the S3 key: reports/YYYY/MM/DD.csv. Same day => same key, so a
// re-run overwrites the previous file instead of creating a duplicate.
func (w Window) ObjectKey() string {
	return fmt.Sprintf("reports/%04d/%02d/%02d.csv", w.Start.Year(), int(w.Start.Month()), w.Start.Day())
}

// CSV renders the report. Only ids, currency codes and numbers appear, so there is
// nothing to escape or to treat as spreadsheet formula injection.
func CSV(w Window, rows []SalesRow) ([]byte, error) {
	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	if err := cw.Write([]string{"date", "event_id", "currency", "orders_paid", "tickets", "gross_cents", "orders_still_paid"}); err != nil {
		return nil, err
	}
	day := w.Start.Format("2006-01-02")
	for _, r := range rows {
		rec := []string{day, r.EventID, r.Currency, strconv.FormatInt(r.Orders, 10), strconv.FormatInt(r.Tickets, 10),
			strconv.FormatInt(r.GrossCents, 10), strconv.FormatInt(r.StillPaidCount, 10)}
		if err := cw.Write(rec); err != nil {
			return nil, err
		}
	}
	cw.Flush()
	return buf.Bytes(), cw.Error()
}
