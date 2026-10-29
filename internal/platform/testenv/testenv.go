// Package testenv helps integration tests (build tag `integration`) find the
// real AWS resources they need. A test names the environment variables it
// requires; if any is missing the test is SKIPPED with a message listing them,
// so `go test -tags integration ./...` is safe to run anywhere.
//
// This package is imported only by tests; no binary links it.
package testenv

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"os"
)

// Need returns the values of the named variables, or skips the test.
func Need(t *testing.T, names ...string) map[string]string {
	t.Helper()
	out := map[string]string{}
	var missing []string
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			out[n] = v
		} else {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		t.Skipf("integration test skipped: set %s (see the README of the service)", strings.Join(missing, ", "))
	}
	return out
}

// Prefix returns a unique, short name prefix so parallel runs and leftovers never collide.
func Prefix() string { return "it-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:10] }
