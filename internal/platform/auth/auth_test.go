package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	issuer   = "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_example"
	clientID = "client-123"
)

// jwksServer serves a JWKS containing the given key under kid and counts fetches.
func jwksServer(t *testing.T, kid string, pub *rsa.PublicKey, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		doc := map[string]any{"keys": []map[string]string{{
			"kid": kid, "kty": "RSA", "alg": "RS256", "use": "sig",
			"n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
		}}}
		_ = json.NewEncoder(w).Encode(doc)
	}))
}

func sign(t *testing.T, key *rsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = kid
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func goodClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"iss": issuer, "sub": "user-1", "client_id": clientID, "token_use": "access",
		"exp": time.Now().Add(time.Hour).Unix(), "cognito:groups": []string{"admin"},
	}
}

func TestVerify(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	var hits atomic.Int32
	srv := jwksServer(t, "k1", &key.PublicKey, &hits)
	defer srv.Close()
	v := NewJWKSVerifier(Config{Issuer: issuer, ClientID: clientID, JWKSURL: srv.URL})

	mut := func(f func(jwt.MapClaims)) jwt.MapClaims { c := goodClaims(); f(c); return c }
	cases := []struct {
		name  string
		token string
		ok    bool
	}{
		{"valid", sign(t, key, "k1", goodClaims()), true},
		{"expired beyond leeway", sign(t, key, "k1", mut(func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-2 * time.Minute).Unix() })), false},
		{"expired within leeway", sign(t, key, "k1", mut(func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-10 * time.Second).Unix() })), true},
		{"wrong issuer", sign(t, key, "k1", mut(func(c jwt.MapClaims) { c["iss"] = "https://evil.example.com" })), false},
		{"id token", sign(t, key, "k1", mut(func(c jwt.MapClaims) { c["token_use"] = "id" })), false},
		{"other app client", sign(t, key, "k1", mut(func(c jwt.MapClaims) { c["client_id"] = "other" })), false},
		{"signed by other key", sign(t, other, "k1", goodClaims()), false},
		{"unknown kid", sign(t, key, "nope", goodClaims()), false},
		{"garbage", "not.a.jwt", false},
	}
	for _, c := range cases {
		got, err := v.Verify(context.Background(), c.token)
		if (err == nil) != c.ok {
			t.Errorf("%s: err=%v want ok=%v", c.name, err, c.ok)
		}
		if c.ok && (got.Subject != "user-1" || !got.IsAdmin()) {
			t.Errorf("%s: claims %+v", c.name, got)
		}
	}
	// Unknown kids must not cause a fetch storm: the rate limit allows one refresh.
	if hits.Load() > 2 {
		t.Errorf("jwks fetched %d times; refresh rate limit not working", hits.Load())
	}
}

func TestMiddleware(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	var hits atomic.Int32
	srv := jwksServer(t, "k1", &key.PublicKey, &hits)
	defer srv.Close()
	v := NewJWKSVerifier(Config{Issuer: issuer, ClientID: clientID, JWKSURL: srv.URL})

	h := RequireAdmin(v, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	do := func(authz string) int {
		req := httptest.NewRequest("GET", "/", nil)
		if authz != "" {
			req.Header.Set("Authorization", authz)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	if got := do(""); got != 401 {
		t.Errorf("no token: %d", got)
	}
	if got := do("Bearer " + sign(t, key, "k1", goodClaims())); got != 204 {
		t.Errorf("admin token: %d", got)
	}
	nonAdmin := goodClaims()
	nonAdmin["cognito:groups"] = []string{"customers"}
	if got := do("Bearer " + sign(t, key, "k1", nonAdmin)); got != 403 {
		t.Errorf("non-admin token: %d", got)
	}
}

func TestRequireCaller(t *testing.T) {
	h := RequireCaller(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("POST", "/internal/v1/x", nil))
	if rec.Code != 400 {
		t.Fatalf("missing header accepted: %d", rec.Code)
	}
	req := httptest.NewRequest("POST", "/internal/v1/x", nil)
	req.Header.Set(CallerHeader, "orders-api")
	rec = httptest.NewRecorder()
	h(rec, req)
	if rec.Code != 204 {
		t.Fatalf("got %d", rec.Code)
	}
}
