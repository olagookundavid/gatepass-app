package idempotency

import "testing"

func TestDecide(t *testing.T) {
	h1, h2 := Hash([]byte(`{"a":1}`)), Hash([]byte(`{"a":2}`))
	cases := []struct {
		found      bool
		stored, in string
		want       Outcome
	}{
		{false, "", h1, Proceed},
		{true, h1, h1, Replay},
		{true, h1, h2, Conflict},
	}
	for i, c := range cases {
		if got := Decide(c.found, c.stored, c.in); got != c.want {
			t.Errorf("case %d: got %v want %v", i, got, c.want)
		}
	}
}

func TestValidateKey(t *testing.T) {
	if ValidateKey("0190a2b4-7c6d-7000-8000-000000000001") != nil {
		t.Error("uuid rejected")
	}
	for _, bad := range []string{"", "short", "has space in it 123", string(make([]byte, 200))} {
		if ValidateKey(bad) == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
