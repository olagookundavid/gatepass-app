package domain

import (
	"errors"
	"testing"
	"time"
)

func order(t *testing.T, st Status) Order {
	t.Helper()
	o, err := NewOrder("o1", "u1", "h1", "e1", "tt1", 2, 1500, "USD", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	o.Status = st
	return o
}

func TestAmountIsQuantityTimesSnapshotPrice(t *testing.T) {
	o := order(t, StatusPendingPayment)
	if o.AmountCents != 3000 {
		t.Fatalf("amount = %d", o.AmountCents)
	}
	if _, err := NewOrder("o", "u", "h", "e", "t", 0, 1, "USD", time.Now()); err == nil {
		t.Error("quantity 0 accepted")
	}
	if _, err := NewOrder("o", "u", "h", "e", "t", 8, 1<<62, "USD", time.Now()); err == nil {
		t.Error("overflow accepted")
	}
}

func TestStateMachine(t *testing.T) {
	type tc struct {
		from    Status
		do      func(Order) (Change, error)
		changed bool
		to      Status
		event   string
		err     error
	}
	pay := func(o Order) (Change, error) { return o.Pay() }
	fail := func(o Order) (Change, error) { return o.FailPayment("card_declined") }
	cancel := func(o Order) (Change, error) { return o.Cancel("changed my mind") }
	cases := map[string]tc{
		"pay pending":            {StatusPendingPayment, pay, true, StatusPaid, EventOrderPaid, nil},
		"pay paid is idempotent": {StatusPaid, pay, false, "", "", nil},
		"pay cancelled ignored":  {StatusCancelled, pay, false, "", "", nil},
		"fail pending":           {StatusPendingPayment, fail, true, StatusPaymentFailed, EventPaymentFailed, nil},
		"fail paid ignored":      {StatusPaid, fail, false, "", "", nil},
		"cancel pending":         {StatusPendingPayment, cancel, true, StatusCancelled, EventOrderCancelled, nil},
		"cancel paid":            {StatusPaid, cancel, true, StatusCancelled, EventOrderCancelled, nil},
		"cancel cancelled no-op": {StatusCancelled, cancel, false, "", "", nil},
		"cancel failed refused":  {StatusPaymentFailed, cancel, false, "", "", ErrInvalidTransition},
	}
	for name, c := range cases {
		got, err := c.do(order(t, c.from))
		if !errors.Is(err, c.err) || got.Changed != c.changed || got.NewStatus != c.to || got.EventType != c.event {
			t.Errorf("%s: got %+v err=%v", name, got, err)
		}
	}
}

func TestDeterministicOrderID(t *testing.T) {
	a := DeterministicOrderID("u1", "key-1", "h1")
	if a != DeterministicOrderID("u1", "key-1", "h1") {
		t.Error("not deterministic")
	}
	for _, other := range []string{DeterministicOrderID("u2", "key-1", "h1"), DeterministicOrderID("u1", "key-2", "h1"), DeterministicOrderID("u1", "key-1", "h2")} {
		if other == a {
			t.Error("collision between different inputs")
		}
	}
}

func TestSimulatePaymentDistribution(t *testing.T) {
	counts := map[PaymentOutcome]int{}
	for i := 0; i < 1000; i++ {
		counts[SimulatePayment(0.8, float64(i)/1000)]++
	}
	if counts[PaymentApproved] != 800 || counts[PaymentDeclined] != 100 || counts[PaymentTransient] != 100 {
		t.Fatalf("%v", counts)
	}
}

func TestEventDataHasNoPersonalFields(t *testing.T) {
	d := EventDataFor(order(t, StatusPaid), "")
	if d.OrderID != "o1" || d.UserID != "u1" || d.AmountCents != 3000 {
		t.Fatalf("%+v", d)
	}
}
