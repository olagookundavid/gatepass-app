package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/reports/domain"
)

type src struct {
	w    domain.Window
	rows []domain.SalesRow
	err  error
}

func (s *src) SalesBetween(_ context.Context, w domain.Window) ([]domain.SalesRow, error) {
	s.w = w
	return s.rows, s.err
}

type store struct{ puts map[string]string }

func (s *store) Put(_ context.Context, k string, b []byte) error { s.puts[k] = string(b); return nil }

var now = time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC)

func TestDefaultsToYesterdayAndOverwritesTheSameKey(t *testing.T) {
	s, st := &src{rows: []domain.SalesRow{{EventID: "e", Currency: "USD", Orders: 1, Tickets: 2, GrossCents: 300, StillPaidCount: 1}}}, &store{puts: map[string]string{}}
	svc := &Service{Source: s, Store: st, Now: func() time.Time { return now }}
	for i := 0; i < 2; i++ { // Scheduler retry / manual re-run
		out, err := svc.Run(context.Background(), json.RawMessage(`{}`))
		if err != nil || out.Key != "reports/2026/02/28.csv" || out.Rows != 1 {
			t.Fatalf("%+v %v", out, err)
		}
	}
	if len(st.puts) != 1 || !strings.Contains(st.puts["reports/2026/02/28.csv"], "2026-02-28,e,USD,1,2,300,1") {
		t.Fatalf("re-run must overwrite one file: %v", st.puts)
	}
	if !s.w.Start.Equal(time.Date(2026, 2, 28, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("window %v", s.w)
	}
}

func TestBackfillAndValidation(t *testing.T) {
	s, st := &src{}, &store{puts: map[string]string{}}
	svc := &Service{Source: s, Store: st, Now: func() time.Time { return now }}
	if out, err := svc.Run(context.Background(), json.RawMessage(`{"date":"2026-01-15"}`)); err != nil || out.Key != "reports/2026/01/15.csv" {
		t.Fatalf("%+v %v", out, err)
	}
	if _, err := svc.Run(context.Background(), json.RawMessage(`{"date":"2026-03-01"}`)); err == nil {
		t.Fatal("today's report requested before the day is over")
	}
	if _, err := svc.Run(context.Background(), json.RawMessage(`{"date":"yesterday"}`)); err == nil {
		t.Fatal("bad date accepted")
	}
	if _, err := svc.Run(context.Background(), nil); err != nil {
		t.Fatalf("empty payload must mean yesterday: %v", err)
	}
	// an empty day still produces a header-only file
	if !strings.HasPrefix(st.puts["reports/2026/02/28.csv"], "date,event_id") {
		t.Fatalf("%v", st.puts)
	}
}

func TestDatabaseFailureIsReturnedSoSchedulerRetries(t *testing.T) {
	svc := &Service{Source: &src{err: errors.New("db down")}, Store: &store{puts: map[string]string{}}, Now: func() time.Time { return now }}
	if _, err := svc.Run(context.Background(), json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected error")
	}
}
