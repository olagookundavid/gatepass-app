package domain

import (
	"strings"
	"testing"
	"time"
)

func TestYesterdayWindowIsUTCAndHalfOpen(t *testing.T) {
	// 00:30 local in UTC+14 is still the previous UTC day: the Lambda must reason in UTC only.
	loc := time.FixedZone("far", 14*3600)
	now := time.Date(2026, 3, 1, 0, 30, 0, 0, loc) // = 2026-02-28 10:30 UTC
	w := Yesterday(now)
	if !w.Start.Equal(time.Date(2026, 2, 27, 0, 0, 0, 0, time.UTC)) || !w.End.Equal(time.Date(2026, 2, 28, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("%v", w)
	}
	if w.ObjectKey() != "reports/2026/02/27.csv" {
		t.Fatalf("key %s", w.ObjectKey())
	}
	// year boundary
	if k := Yesterday(time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)).ObjectKey(); k != "reports/2025/12/31.csv" {
		t.Fatalf("year boundary key %s", k)
	}
}

func TestParseDay(t *testing.T) {
	w, err := ParseDay("2026-02-03")
	if err != nil || w.ObjectKey() != "reports/2026/02/03.csv" {
		t.Fatalf("%v %v", w, err)
	}
	if _, err := ParseDay("03/02/2026"); err == nil {
		t.Fatal("bad date accepted")
	}
}

func TestCSV(t *testing.T) {
	w := DayWindow(time.Date(2026, 2, 3, 12, 0, 0, 0, time.UTC))
	b, err := CSV(w, []SalesRow{{EventID: "e1", Currency: "USD", Orders: 2, Tickets: 5, GrossCents: 12500, StillPaidCount: 1}})
	if err != nil {
		t.Fatal(err)
	}
	want := "date,event_id,currency,orders_paid,tickets,gross_cents,orders_still_paid\n2026-02-03,e1,USD,2,5,12500,1\n"
	if string(b) != want {
		t.Fatalf("got:\n%s", b)
	}
	empty, _ := CSV(w, nil)
	if strings.Count(string(empty), "\n") != 1 {
		t.Fatalf("empty report must still have a header: %q", empty)
	}
}
