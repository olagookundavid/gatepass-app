package problem

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleRendersProblemAndHidesUnknownErrors(t *testing.T) {
	h := Handle(func(w http.ResponseWriter, r *http.Request) error {
		if r.URL.Query().Get("boom") != "" {
			return errors.New("pq: password authentication failed for user secret")
		}
		return Validation(FieldError{Field: "quantity", Reason: "must be 1-8"})
	})

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 422 || rec.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("got %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
	var p Problem
	_ = json.Unmarshal(rec.Body.Bytes(), &p)
	if p.Code != "validation_failed" || len(p.Errors) != 1 || p.Type != TypePrefix+"validation_failed" {
		t.Fatalf("problem = %+v", p)
	}

	rec = httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/?boom=1", nil))
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "password") {
		t.Fatalf("internal error leaked: %d %s", rec.Code, rec.Body.String())
	}
}

func TestDecodeJSONRejectsUnknownFields(t *testing.T) {
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{"quantiy": 2}`))
	var dst struct {
		Quantity int `json:"quantity"`
	}
	err := DecodeJSON(r, &dst)
	var p *Problem
	if !errors.As(err, &p) || p.Status != 400 {
		t.Fatalf("err = %v", err)
	}
}
