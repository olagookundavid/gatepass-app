// Package auth validates Amazon Cognito access tokens (JWTs) on public endpoints.
//
// Purpose: turn "Authorization: Bearer <access token>" into trusted Claims
// (user id = `sub`, admin = membership of the `admin` group) without calling
// Cognito on every request.
//
// How validation works (Amazon Cognito Developer Guide, "Verifying a JSON web
// token"):
//  1. The user pool publishes its public signing keys as a JWKS document at
//     <issuer>/.well-known/jwks.json. We download it, cache the keys by `kid`,
//     and re-download when a token names an unknown `kid` (Cognito rotates keys)
//     - but at most once per MinRefresh, so an attacker sending random `kid`s
//     cannot make us hammer Cognito.
//  2. The token must be RS256 signed by one of those keys, `iss` must equal the
//     configured issuer, `exp`/`nbf` are checked with 30 s clock leeway.
//  3. It must be an ACCESS token (`token_use == "access"`) for OUR app client
//     (`client_id`). An ID token or a token for another app client of the same
//     pool is rejected.
//
// Access tokens do NOT contain the user's email. Never expect, log or store one
// from a token; notifications looks the address up with AdminGetUser instead.
//
// Internal service-to-service endpoints use no JWT (they are reachable only
// inside the cluster network); they must carry X-Caller-Service, see RequireCaller.
//
// AWS services touched: Cognito (public JWKS URL over HTTPS, no IAM needed).
package auth

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/olagookundavid/gatepass-app/internal/platform/logging"
	"github.com/olagookundavid/gatepass-app/internal/platform/problem"
)

// AdminGroup is the Cognito group that grants admin rights.
const AdminGroup = "admin"

// Claims are the validated facts we use from a token.
type Claims struct {
	Subject string // Cognito `sub`: the stable user id
	Groups  []string
}

// IsAdmin reports membership of the admin group.
func (c *Claims) IsAdmin() bool {
	for _, g := range c.Groups {
		if g == AdminGroup {
			return true
		}
	}
	return false
}

// Verifier validates raw tokens. The interface lets HTTP-level tests plug in a
// fake without running a JWKS server.
type Verifier interface {
	Verify(ctx context.Context, rawToken string) (*Claims, error)
}

// Config configures the JWKS verifier.
type Config struct {
	Issuer     string // exact value of the `iss` claim
	ClientID   string // the app client id expected in `client_id`
	JWKSURL    string // default: Issuer + "/.well-known/jwks.json"
	HTTPClient *http.Client
	Leeway     time.Duration // default 30s
	MinRefresh time.Duration // minimum gap between JWKS downloads, default 30s
	TTL        time.Duration // proactive refresh interval, default 1h
}

// JWKSVerifier implements Verifier.
type JWKSVerifier struct {
	cfg Config

	mu        sync.RWMutex
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
	lastTry   time.Time
}

// NewJWKSVerifier applies defaults and returns a verifier. It does not fetch
// keys yet; the first request does (so startup does not depend on Cognito).
func NewJWKSVerifier(cfg Config) *JWKSVerifier {
	if cfg.JWKSURL == "" {
		cfg.JWKSURL = strings.TrimRight(cfg.Issuer, "/") + "/.well-known/jwks.json"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	if cfg.Leeway == 0 {
		cfg.Leeway = 30 * time.Second
	}
	if cfg.MinRefresh == 0 {
		cfg.MinRefresh = 30 * time.Second
	}
	if cfg.TTL == 0 {
		cfg.TTL = time.Hour
	}
	return &JWKSVerifier{cfg: cfg, keys: map[string]*rsa.PublicKey{}}
}

// ErrInvalidToken is returned for every validation failure. The specific reason
// is logged at debug level only; clients just see 401.
var ErrInvalidToken = errors.New("invalid token")

// Verify validates rawToken and returns its claims.
func (v *JWKSVerifier) Verify(ctx context.Context, rawToken string) (*Claims, error) {
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{"RS256"}), // never accept "none" or HMAC (algorithm-confusion attacks)
		jwt.WithIssuer(v.cfg.Issuer),
		jwt.WithLeeway(v.cfg.Leeway),
		jwt.WithExpirationRequired(),
	)
	var mc jwt.MapClaims
	tok, err := parser.ParseWithClaims(rawToken, &mc, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		return v.key(ctx, kid)
	})
	if err != nil || !tok.Valid {
		slog.DebugContext(ctx, "token rejected", "reason", err)
		return nil, ErrInvalidToken
	}
	if use, _ := mc["token_use"].(string); use != "access" {
		slog.DebugContext(ctx, "token rejected", "reason", "token_use is not access")
		return nil, ErrInvalidToken
	}
	if cid, _ := mc["client_id"].(string); cid != v.cfg.ClientID {
		slog.DebugContext(ctx, "token rejected", "reason", "client_id mismatch")
		return nil, ErrInvalidToken
	}
	sub, _ := mc["sub"].(string)
	if sub == "" {
		return nil, ErrInvalidToken
	}
	c := &Claims{Subject: sub}
	if raw, ok := mc["cognito:groups"].([]any); ok {
		for _, g := range raw {
			if s, ok := g.(string); ok {
				c.Groups = append(c.Groups, s)
			}
		}
	}
	return c, nil
}

// key returns the public key for kid, refreshing the JWKS when the kid is
// unknown or the cache is older than TTL.
func (v *JWKSVerifier) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.mu.RLock()
	k, ok := v.keys[kid]
	fresh := time.Since(v.fetchedAt) < v.cfg.TTL
	v.mu.RUnlock()
	if ok && fresh {
		return k, nil
	}
	if err := v.refresh(ctx); err != nil && !ok {
		return nil, err
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if k, ok := v.keys[kid]; ok {
		return k, nil
	}
	return nil, fmt.Errorf("unknown kid %q", kid)
}

type jwks struct {
	Keys []struct {
		Kid string `json:"kid"`
		Kty string `json:"kty"`
		N   string `json:"n"`
		E   string `json:"e"`
	} `json:"keys"`
}

func (v *JWKSVerifier) refresh(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	// Rate limit: many concurrent requests with an unknown kid trigger ONE fetch.
	if time.Since(v.lastTry) < v.cfg.MinRefresh {
		return nil
	}
	v.lastTry = time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.cfg.JWKSURL, nil)
	if err != nil {
		return err
	}
	resp, err := v.cfg.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch jwks: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch jwks: status %d", resp.StatusCode)
	}
	var doc jwks
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&doc); err != nil {
		return fmt.Errorf("decode jwks: %w", err)
	}
	keys := map[string]*rsa.PublicKey{}
	for _, k := range doc.Keys {
		if k.Kty != "RSA" {
			continue
		}
		nb, err1 := base64.RawURLEncoding.DecodeString(k.N)
		eb, err2 := base64.RawURLEncoding.DecodeString(k.E)
		if err1 != nil || err2 != nil {
			continue
		}
		keys[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: int(new(big.Int).SetBytes(eb).Int64())}
	}
	if len(keys) == 0 {
		return errors.New("jwks contained no usable RSA keys")
	}
	v.keys, v.fetchedAt = keys, time.Now()
	return nil
}

type claimsKey struct{}

// FromContext returns the authenticated claims set by Require.
func FromContext(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(claimsKey{}).(*Claims)
	return c, ok
}

// WithClaims is used by tests to simulate an authenticated request.
func WithClaims(ctx context.Context, c *Claims) context.Context {
	return context.WithValue(ctx, claimsKey{}, c)
}

// Require wraps a handler so it only runs for a valid Bearer access token.
func Require(v Verifier, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		raw, found := strings.CutPrefix(h, "Bearer ")
		if !found || raw == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="gatepass"`)
			problem.Write(w, r, problem.Unauthorized("A Bearer access token is required."))
			return
		}
		c, err := v.Verify(r.Context(), raw)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="gatepass", error="invalid_token"`)
			problem.Write(w, r, problem.Unauthorized("The access token is invalid or expired."))
			return
		}
		logging.SetUserID(r.Context(), c.Subject)
		next.ServeHTTP(w, r.WithContext(WithClaims(r.Context(), c)))
	})
}

// RequireFunc is Require for a handler function.
func RequireFunc(v Verifier, f http.HandlerFunc) http.Handler { return Require(v, f) }

// RequireAdmin is Require plus an admin-group check (403 otherwise).
func RequireAdmin(v Verifier, f http.HandlerFunc) http.Handler {
	return Require(v, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c, ok := FromContext(r.Context()); !ok || !c.IsAdmin() {
			problem.Write(w, r, problem.Forbidden("This endpoint requires the admin group."))
			return
		}
		f(w, r)
	}))
}

// Optional wraps a handler for endpoints that are public but behave
// differently for admins (e.g. GET event shows drafts to admins). A missing
// token is fine; a PRESENT but invalid token is still rejected so a client with
// an expired token learns about it instead of silently seeing the public view.
func Optional(v Verifier, f http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			f(w, r)
			return
		}
		Require(v, f).ServeHTTP(w, r)
	})
}

// CallerHeader identifies the calling service on internal endpoints.
const CallerHeader = "X-Caller-Service"

// RequireCaller guards internal endpoints: no JWT (network-level restriction is
// the real protection) but the caller must identify itself, and we log who.
func RequireCaller(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller := r.Header.Get(CallerHeader)
		if caller == "" {
			problem.Write(w, r, problem.BadRequest("Header "+CallerHeader+" is required on internal endpoints."))
			return
		}
		slog.InfoContext(r.Context(), "internal call", "caller_service", caller, "path", r.URL.Path)
		next(w, r)
	}
}
