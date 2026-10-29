package domain

import (
	"errors"
	"testing"
	"time"
)

var now = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func goodEvent() Event {
	return Event{
		ID: "e1", VenueID: "v1", Title: "Concert", Status: StatusDraft,
		StartsAt: now.Add(48 * time.Hour), EndsAt: now.Add(50 * time.Hour),
		TicketTypes: []TicketType{{ID: "t1", Name: "GA", PriceCents: 5000, Currency: "USD", Capacity: 100}},
	}
}

func TestPublishIsIdempotentAndGuarded(t *testing.T) {
	e := goodEvent()
	changed, err := e.Publish(now)
	if err != nil || !changed || e.Status != StatusPublished {
		t.Fatalf("first publish: %v %v %s", changed, err, e.Status)
	}
	changed, err = e.Publish(now)
	if err != nil || changed {
		t.Fatalf("second publish must be a no-op: %v %v", changed, err)
	}

	c := goodEvent()
	c.Status = StatusCancelled
	if _, err := c.Publish(now); !errors.Is(err, ErrCancelled) {
		t.Fatalf("cancelled publish: %v", err)
	}

	empty := goodEvent()
	empty.TicketTypes = nil
	if _, err := empty.Publish(now); err == nil {
		t.Fatal("publish without ticket types accepted")
	}

	past := goodEvent()
	past.StartsAt = now.Add(-time.Hour)
	past.EndsAt = now.Add(time.Hour)
	if _, err := past.Publish(now); err == nil {
		t.Fatal("publish of a started event accepted")
	}
}

func TestTicketTypeValidation(t *testing.T) {
	bad := []TicketType{
		{Name: "", PriceCents: -1, Currency: "usd", Capacity: 0},
		{Name: "GA", PriceCents: 1, Currency: "USD", Capacity: 1},
		{Name: "ga ", PriceCents: 1, Currency: "USD", Capacity: 1}, // duplicate name, case/space-insensitive
	}
	err := ValidateTicketTypes(bad)
	var ve *ValidationError
	if !errors.As(err, &ve) || len(ve.Fields) < 5 {
		t.Fatalf("expected many field errors, got %v", err)
	}
	if ValidateTicketTypes(goodEvent().TicketTypes) != nil {
		t.Fatal("good ticket types rejected")
	}
	if ValidateTicketTypes(nil) == nil {
		t.Fatal("empty list accepted")
	}
}

func TestEventFieldsAndVenueCapacity(t *testing.T) {
	e := goodEvent()
	e.EndsAt = e.StartsAt.Add(-time.Minute)
	if ValidateEventFields(e, now, true) == nil {
		t.Fatal("end before start accepted")
	}
	if err := ValidateEventFields(goodEvent(), now, true); err != nil {
		t.Fatal(err)
	}
	if FitsVenue(goodEvent().TicketTypes, Venue{Capacity: 99}) == nil {
		t.Fatal("over-capacity accepted")
	}
	if FitsVenue(goodEvent().TicketTypes, Venue{Capacity: 100}) != nil {
		t.Fatal("exact capacity rejected")
	}
}

func TestVisibility(t *testing.T) {
	d := goodEvent()
	if d.Visible(false) || !d.Visible(true) {
		t.Fatal("drafts must be admin-only")
	}
	p := goodEvent()
	p.Status = StatusPublished
	if !p.Visible(false) {
		t.Fatal("published must be public")
	}
}

func TestPublishedDataHasNoPersonalData(t *testing.T) {
	e := goodEvent()
	e.CreatedBy = "admin-sub"
	d := e.PublishedData()
	if d.EventID != "e1" || len(d.TicketTypes) != 1 || d.TicketTypes[0].PriceCents != 5000 {
		t.Fatalf("%+v", d)
	}
}
