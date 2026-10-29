// Package idempotency holds the pure decision logic for Idempotency-Key.
//
// Contract (same for reservations POST /holds and orders POST /orders):
//   - same key + same request body  -> return the ORIGINAL response (replay)
//   - same key + different body     -> 409 (client bug: reused a key)
//   - unknown key                   -> process the request and store the result
//
// Why it exists: clients on flaky mobile networks retry a POST after a timeout.
// Without idempotency a retry would reserve tickets twice or create two orders.
//
// Storage differs per service (DynamoDB item with TTL in reservations; a
// Postgres row in the same transaction as the order in orders), but the
// decision is identical, so it lives here and is unit tested once.
//
// AWS services touched: none.
package idempotency

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/olagookundavid/gatepass-app/internal/platform/problem"
)

// Header is the HTTP header carrying the key.
const Header = "Idempotency-Key"

// Outcome is what to do with a request.
type Outcome int

const (
	// Proceed: the key is new, do the work.
	Proceed Outcome = iota
	// Replay: the key was seen with the same body, return the stored response.
	Replay
	// Conflict: the key was seen with a different body.
	Conflict
)

// Decide compares the stored request hash (if any) with the new one.
func Decide(found bool, storedHash, newHash string) Outcome {
	switch {
	case !found:
		return Proceed
	case storedHash == newHash:
		return Replay
	default:
		return Conflict
	}
}

// Hash fingerprints a request body. Callers should hash a canonical form (the
// decoded and re-encoded struct) so whitespace differences do not matter.
func Hash(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// ValidateKey checks the header value: 8..128 visible ASCII characters. A UUID
// is the recommended value.
func ValidateKey(k string) error {
	if len(k) < 8 || len(k) > 128 {
		return problem.Validation(problem.FieldError{Field: Header, Reason: "must be 8 to 128 characters"})
	}
	for _, c := range k {
		if c < 0x21 || c > 0x7e {
			return problem.Validation(problem.FieldError{Field: Header, Reason: "must contain only visible ASCII characters"})
		}
	}
	return nil
}

// ConflictProblem is the 409 returned for Conflict.
func ConflictProblem() *problem.Problem {
	return problem.Conflict("idempotency_key_reused", "This Idempotency-Key was already used with a different request body.")
}
