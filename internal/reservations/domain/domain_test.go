package domain

import (
	"errors"
	"testing"
	"time"
)

var t0 = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func inv() Inventory {
	return Inventory{EventID: "e", TicketTypeID: "t", Capacity: 10, Available: 10, PriceCents: 2500, Currency: "USD"}
}

func TestNewHoldExpiresInTenMinutesAndSnapshotsPrice(t *testing.T) {
	h, err := NewHold("h1", "u1", inv(), 2, t0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !h.ExpiresAt.Equal(t0.Add(10*time.Minute)) || h.UnitPriceCents != 2500 || h.Status != StatusActive {
		t.Fatalf("%+v", h)
	}
	for _, q := range []int{0, 9, -1} {
		if _, err := NewHold("h", "u", inv(), q, t0, 0); !errors.Is(err, ErrInvalidQuantity) {
			t.Errorf("quantity %d accepted", q)
		}
	}
}

func TestEffectiveStatus(t *testing.T) {
	h, _ := NewHold("h1", "u1", inv(), 1, t0, 0)
	if h.EffectiveStatus(t0.Add(9*time.Minute)) != StatusActive {
		t.Error("should still be active")
	}
	if h.EffectiveStatus(t0.Add(10*time.Minute)) != StatusExpired {
		t.Error("exactly at expiry must read as expired")
	}
	h.Status = StatusConsumed
	if h.EffectiveStatus(t0.Add(time.Hour)) != StatusConsumed {
		t.Error("a consumed hold never reads as expired")
	}
}

func TestConsumeDecision(t *testing.T) {
	h, _ := NewHold("h1", "u1", inv(), 1, t0, 0)
	now := t0.Add(time.Minute)

	if o, err := ConsumeDecision(h, "o1", "u1", now); err != nil || o != ConsumeNow {
		t.Fatalf("fresh hold: %v %v", o, err)
	}
	if _, err := ConsumeDecision(h, "o1", "other", now); !errors.Is(err, ErrWrongOwner) {
		t.Errorf("wrong owner: %v", err)
	}
	if _, err := ConsumeDecision(h, "o1", "u1", t0.Add(11*time.Minute)); !errors.Is(err, ErrHoldExpired) {
		t.Errorf("expired: %v", err)
	}
	h.Status, h.OrderID = StatusConsumed, "o1"
	if o, err := ConsumeDecision(h, "o1", "u1", now); err != nil || o != AlreadyConsumedBySame {
		t.Errorf("same order must be idempotent success: %v %v", o, err)
	}
	if _, err := ConsumeDecision(h, "o2", "u1", now); !errors.Is(err, ErrAlreadyConsumed) {
		t.Errorf("different order: %v", err)
	}
	h.Status = StatusReleased
	if _, err := ConsumeDecision(h, "o1", "u1", now); !errors.Is(err, ErrHoldReleased) {
		t.Errorf("released: %v", err)
	}
}

func TestInventoryMath(t *testing.T) {
	if n, err := AfterReserve(3, 3); err != nil || n != 0 {
		t.Errorf("exact: %d %v", n, err)
	}
	if n, err := AfterReserve(2, 3); !errors.Is(err, ErrSoldOut) || n != 2 {
		t.Errorf("oversell must fail and leave the counter alone: %d %v", n, err)
	}
	if n, ok := AfterRelease(8, 2, 10); !ok || n != 10 {
		t.Errorf("release to capacity: %d %v", n, ok)
	}
	if n, ok := AfterRelease(9, 2, 10); ok || n != 9 {
		t.Errorf("release above capacity must be refused: %d %v", n, ok)
	}
	i := inv()
	i.Available = 1
	if CanReserve(i, 2) || !CanReserve(i, 1) || CanReserve(i, 0) {
		t.Error("CanReserve wrong")
	}
}
