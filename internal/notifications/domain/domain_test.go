package domain

import (
	"strings"
	"testing"
)

func TestTokenSignAndVerify(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	tok := SignToken(key, "order-1")
	if id, ok := VerifyToken(key, tok); !ok || id != "order-1" {
		t.Fatalf("valid token rejected: %v %s", ok, id)
	}
	if _, ok := VerifyToken([]byte("another-key-another-key-another!"), tok); ok {
		t.Error("token verified with the wrong key")
	}
	forged := strings.Replace(tok, "order-1", "order-2", 1)
	if _, ok := VerifyToken(key, forged); ok {
		t.Error("token for another order accepted (signature must bind the order id)")
	}
	for _, bad := range []string{"", "GP1.x", "XX1.order-1.sig", tok + "x"} {
		if _, ok := VerifyToken(key, bad); ok {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestTicketKeyRejectsPathTricks(t *testing.T) {
	if k, err := TicketKey("user-1", "order-1"); err != nil || k != "tickets/user-1/order-1.png" {
		t.Fatalf("%s %v", k, err)
	}
	for _, bad := range [][2]string{{"../x", "o"}, {"u", "a/b"}, {"", "o"}, {"u", "o.png"}} {
		if _, err := TicketKey(bad[0], bad[1]); err == nil {
			t.Errorf("accepted %v", bad)
		}
	}
}

func TestEmailsContainNoAddressAndTheLink(t *testing.T) {
	d := OrderEventData{OrderID: "11111111-aaaa", Quantity: 2, AmountCents: 2550, Currency: "USD"}
	e := BuildEmail(TypeOrderPaid, d, "https://s3.example.com/t.png?sig=1")
	if !strings.Contains(e.Text, "https://s3.example.com/t.png?sig=1") || !strings.Contains(e.HTML, "href=") || !strings.Contains(e.Text, "25.50 USD") {
		t.Fatalf("%+v", e)
	}
	for _, typ := range []string{TypePaymentFailed, TypeOrderCancelled} {
		m := BuildEmail(typ, d, "ignored-link")
		if strings.Contains(m.Text, "ignored-link") || m.Subject == "" {
			t.Errorf("%s: %+v", typ, m)
		}
	}
	if TypeForEvent("OrderPaid") != TypeOrderPaid || TypeForEvent("OrderPlaced") != "" {
		t.Error("event mapping wrong")
	}
}
