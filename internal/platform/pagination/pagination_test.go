package pagination

import (
	"net/http/httptest"
	"testing"
)

func TestLimit(t *testing.T) {
	for url, want := range map[string]int{"/x": 20, "/x?limit=100": 100, "/x?limit=1": 1} {
		got, err := Limit(httptest.NewRequest("GET", url, nil))
		if err != nil || got != want {
			t.Errorf("%s: %d %v", url, got, err)
		}
	}
	for _, url := range []string{"/x?limit=0", "/x?limit=101", "/x?limit=abc"} {
		if _, err := Limit(httptest.NewRequest("GET", url, nil)); err == nil {
			t.Errorf("%s accepted", url)
		}
	}
}

func TestCursorRoundTrip(t *testing.T) {
	type k struct{ A string }
	var out k
	ok, err := Decode(Encode(k{"x"}), &out)
	if !ok || err != nil || out.A != "x" {
		t.Fatalf("%v %v %+v", ok, err, out)
	}
	if ok, err := Decode("", &out); ok || err != nil {
		t.Fatal("empty cursor must be a no-op")
	}
	if _, err := Decode("!!!", &out); err == nil {
		t.Fatal("garbage cursor accepted")
	}
}
