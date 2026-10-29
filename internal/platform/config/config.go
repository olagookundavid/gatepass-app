// Package config reads configuration from environment variables only.
//
// Purpose: every binary must fail FAST at startup with ONE message that lists
// every missing or invalid variable, instead of crashing on the first one and
// making the operator redeploy five times. A Loader collects problems as you
// call its getters and reports them all in Err().
//
// Why env vars only: in Kubernetes the Deployment (written by the GitOps repo)
// is the single place that says how a container is configured. There are no
// config files to forget to mount and no defaults that silently point at a real
// AWS resource.
//
// AWS services touched: none.
package config

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Loader accumulates configuration problems. Use NewLoader(os.LookupEnv) in
// main and a map-backed lookup in tests.
type Loader struct {
	lookup   func(string) (string, bool)
	problems []string
}

// NewLoader builds a Loader over any lookup function.
func NewLoader(lookup func(string) (string, bool)) *Loader {
	return &Loader{lookup: lookup}
}

// FromEnv is the production constructor.
func FromEnv() *Loader { return NewLoader(os.LookupEnv) }

// FromMap is a test helper.
func FromMap(m map[string]string) *Loader {
	return NewLoader(func(k string) (string, bool) { v, ok := m[k]; return v, ok })
}

func (l *Loader) get(name string) (string, bool) {
	v, ok := l.lookup(name)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

func (l *Loader) fail(format string, a ...any) {
	l.problems = append(l.problems, fmt.Sprintf(format, a...))
}

// Required returns the variable or records a "missing" problem.
func (l *Loader) Required(name string) string {
	v, ok := l.get(name)
	if !ok {
		l.fail("%s is required but not set", name)
	}
	return v
}

// String returns the variable or def when unset. def must never be a real AWS
// resource name.
func (l *Loader) String(name, def string) string {
	if v, ok := l.get(name); ok {
		return v
	}
	return def
}

// Int returns an integer variable (def when unset).
func (l *Loader) Int(name string, def int) int {
	v, ok := l.get(name)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.fail("%s must be an integer, got %q", name, v)
		return def
	}
	return n
}

// RequiredInt is Int without a default.
func (l *Loader) RequiredInt(name string) int {
	if _, ok := l.get(name); !ok {
		l.fail("%s is required but not set", name)
		return 0
	}
	return l.Int(name, 0)
}

// Float returns a float variable (def when unset).
func (l *Loader) Float(name string, def float64) float64 {
	v, ok := l.get(name)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		l.fail("%s must be a number, got %q", name, v)
		return def
	}
	return f
}

// Bool returns a boolean variable (def when unset). Accepts true/false/1/0.
func (l *Loader) Bool(name string, def bool) bool {
	v, ok := l.get(name)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.fail("%s must be true or false, got %q", name, v)
		return def
	}
	return b
}

// Duration returns a Go duration ("5s", "2m") variable (def when unset).
func (l *Loader) Duration(name string, def time.Duration) time.Duration {
	v, ok := l.get(name)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.fail("%s must be a duration such as 5s, got %q", name, v)
		return def
	}
	return d
}

// Check records a custom validation problem, e.g. "X is required when Y=true".
func (l *Loader) Check(ok bool, format string, a ...any) {
	if !ok {
		l.fail(format, a...)
	}
}

// Err returns nil, or one error listing every problem found, sorted.
func (l *Loader) Err() error {
	if len(l.problems) == 0 {
		return nil
	}
	p := append([]string(nil), l.problems...)
	sort.Strings(p)
	return fmt.Errorf("invalid configuration:\n  - %s", strings.Join(p, "\n  - "))
}

// Common is the configuration every binary shares.
type Common struct {
	LogLevel      string
	AWSRegion     string
	HTTPAddr      string
	BasePath      string // public route prefix, e.g. "/catalog"
	ShutdownDelay time.Duration
}

// LoadCommon reads the shared variables. defaultBasePath is e.g. "/catalog";
// pass "" for binaries without public routes.
func LoadCommon(l *Loader, defaultBasePath string) Common {
	c := Common{
		LogLevel:      l.String("LOG_LEVEL", "info"),
		AWSRegion:     l.Required("AWS_REGION"),
		HTTPAddr:      l.String("HTTP_ADDR", ":8080"),
		BasePath:      strings.TrimRight(l.String("BASE_PATH", defaultBasePath), "/"),
		ShutdownDelay: l.Duration("SHUTDOWN_DELAY", 5*time.Second),
	}
	// AWS_MAX_ATTEMPTS is read by the AWS SDK itself; we only validate it so a
	// typo is reported at startup rather than silently ignored.
	if _, ok := l.get("AWS_MAX_ATTEMPTS"); ok {
		n := l.Int("AWS_MAX_ATTEMPTS", 3)
		l.Check(n >= 1 && n <= 10, "AWS_MAX_ATTEMPTS must be between 1 and 10, got %d", n)
	}
	return c
}

// Auth is the Cognito JWT validation configuration for public endpoints.
type Auth struct {
	Issuer   string // https://cognito-idp.<region>.amazonaws.com/<pool-id>
	ClientID string
}

// LoadAuth reads COGNITO_ISSUER and COGNITO_CLIENT_ID (both required).
func LoadAuth(l *Loader) Auth {
	return Auth{Issuer: l.Required("COGNITO_ISSUER"), ClientID: l.Required("COGNITO_CLIENT_ID")}
}
