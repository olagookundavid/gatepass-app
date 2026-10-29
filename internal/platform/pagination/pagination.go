// Package pagination implements limit + opaque cursor paging.
//
// Why an opaque cursor: clients must not build cursors themselves, so we are
// free to change what is inside (a Postgres keyset, a DynamoDB
// LastEvaluatedKey) without breaking the API. The cursor is base64url(JSON).
//
// SECURITY NOTE: the cursor is NOT signed. Adapters must therefore treat the
// decoded content as untrusted input: Postgres adapters use it only as bound
// query parameters (keyset), DynamoDB adapters re-check that the partition key
// inside it belongs to the caller before using it as ExclusiveStartKey.
//
// AWS services touched: none.
package pagination

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/olagookundavid/gatepass-app/internal/platform/problem"
)

const (
	DefaultLimit = 20
	MaxLimit     = 100
)

// Limit parses ?limit= (default 20, max 100).
func Limit(r *http.Request) (int, error) {
	s := r.URL.Query().Get("limit")
	if s == "" {
		return DefaultLimit, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > MaxLimit {
		return 0, problem.Validation(problem.FieldError{Field: "limit", Reason: "must be an integer between 1 and 100"})
	}
	return n, nil
}

// Encode turns v into an opaque cursor string.
func Encode(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// Decode fills dst from a cursor. An empty cursor leaves dst untouched and
// returns (false, nil). A malformed cursor is a client error.
func Decode(cursor string, dst any) (bool, error) {
	if cursor == "" {
		return false, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || json.Unmarshal(b, dst) != nil {
		return false, problem.Validation(problem.FieldError{Field: "cursor", Reason: "is not a valid cursor"})
	}
	return true, nil
}

// Page is the standard list response envelope.
type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"nextCursor,omitempty"`
}
