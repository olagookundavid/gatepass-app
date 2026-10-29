# HANDOFF: state of the Gatepass Go code

Written incrementally while building; read this first. Placeholders (`<account-id>`, `example.com`) are used on purpose: this repository is public.

## 1. Status

| Binary | Status | Notes |
|---|---|---|
| `internal/platform/*` | done | unit tested |
| `build/Dockerfile` | done | amd64 and arm64 build verified for the skeleton `catalog-api` |
| `catalog-api` (+ `migrate`) | missing | |
| `reservations-api` | missing | |
| `lambda-hold-expirer` | missing | |
| `orders-api` (+ `migrate`) | missing | |
| `orders-worker` | missing | |
| `orders-outbox-relay` | missing | |
| `notifications-api` | missing | |
| `notifications-worker` | missing | |
| `lambda-nightly-report` | missing | |

### Commands run (latest result)

Checks run after every step: `go build ./...`, `go vet ./...`, `go vet -tags integration ./...`, `golangci-lint run`, `go test ./...`.

- After platform: all five pass (0 lint issues). `docker build -f build/Dockerfile --build-arg BINARY=catalog-api --platform linux/amd64` and `linux/arm64` succeed; the arm64 image ran with `--read-only` and answered `/livez` and `/version`.

## 2. Environment variables per binary

(Filled per service below. Variables shared by all binaries.)

| Variable | Required | Meaning |
|---|---|---|
| `AWS_REGION` | yes | region for every SDK client |
| `AWS_MAX_ATTEMPTS` | no | SDK retry attempts, 1..10, default 3 |
| `LOG_LEVEL` | no | debug/info/warn/error, default info |
| `HTTP_ADDR` | no | public listener, default `:8080` |
| `BASE_PATH` | no | public route prefix (per service default) |
| `SHUTDOWN_DELAY` | no | wait after SIGTERM before draining, default 5s |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | no | telemetry; no-op when the endpoint is unset |
| `COGNITO_ISSUER`, `COGNITO_CLIENT_ID` | yes (public APIs) | JWT validation |

## 3. IAM actions per service

(Appended per service.)

## 4. AWS resources the code expects

(Appended per service.)

## 5. Event rules

(Appended per service.)

## 6. Assumptions and decisions

- Module path `github.com/olagookundavid/gatepass-app`, `go 1.26`, `toolchain go1.26.5`.
- Problem `type` values are URNs (`urn:gatepass:problem:<code>`); no public docs site exists to dereference.
- Hexagonal packages: `internal/<service>/{domain,ports,app,adapters/...}`.
- Base images pinned by digest in `build/Dockerfile` (`golang:1.26` index digest, `distroless/static-debian12:nonroot`). The `golang:1.26` image currently ships go 1.26.10; `go.mod` says `toolchain go1.26.5`, a lower version, so no toolchain download happens.
- `.dockerignore` excludes `*.md`, `docs`, `.git`.
- Git commit author/email are whatever the machine's `git config` has (not changed by the agent).
- The RDS CA bundle (`internal/platform/pgdb/certs/global-bundle.pem`) was downloaded from `https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem` on the authoring day and embedded; refresh it when AWS rotates CAs, or override with `DB_SSL_ROOT_CERT`.

## 7. UNVERIFIED items

- Platform: ElastiCache IAM auth token format (`internal/platform/redisx`), RDS IAM token against a real instance, Secrets Manager credential flow, EventBridge partial-failure handling against the real API, SQS consumer against a real queue.

## 8. Dependencies beyond the approved list

- None so far. Transitive: `golang.org/x/*`, grpc/protobuf (pulled in by the OTLP exporters). `otelaws` is approved but upstream marks it deprecated (kept, `//nolint:staticcheck` in `internal/platform/awscfg`).

## 9. Known gaps and suggestions

- OpenAPI validity is not machine-checked (spectral not installed).

## 10. How to read the code

Start with `internal/platform/lifecycle`, `httpserver`, `auth`, `problem`, then `sqsconsumer`, `ebpub`, `pgdb`.
