// Package buildinfo holds the values stamped into every binary at build time.
//
// Purpose: GET /version and the "version" log field must tell you exactly which
// commit is running. Go has no build-time constants, so the Makefile and the
// Dockerfile pass them with the linker:
//
//	-ldflags "-X github.com/olagookundavid/gatepass-app/internal/platform/buildinfo.Version=<git sha>"
//
// AWS services touched: none.
package buildinfo

// Version is the git commit SHA (short or full). "dev" for a plain `go build`.
var Version = "dev"

// BuildTime is the UTC build timestamp in RFC 3339 form.
var BuildTime = "unknown"
