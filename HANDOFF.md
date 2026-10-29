# HANDOFF: state of the Gatepass Go code

Written incrementally while building; read this first. Placeholders (`<account-id>`, `example.com`) are used on purpose: this repository is public.

## 1. Status

| Binary | Status | Notes |
|---|---|---|
| `internal/platform/*` | done | unit tested |
| `build/Dockerfile` | done | amd64 and arm64 build verified for the skeleton `catalog-api` |
| `catalog-api` (+ `migrate`) | done | unit + local-container integration verified, see section 1 commands |
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

- After catalog: `go build ./...`, `go vet ./...`, `go vet -tags integration ./...`, `golangci-lint run`, `go test ./...` all pass. Catalog integration tests `TestPostgresRepositories` and `TestCacheAsideAgainstRealRedisAndOutageFallback` were run against **throwaway local containers** (postgres:17 and redis:7, started with `docker run`, nothing committed) and pass; this verified the SQL (keyset pagination, optimistic locking, migrations, bootstrap grants) and cache-aside/outage fallback. The AWS-dependent catalog integration tests (RDS IAM auth, S3 presign, PutEvents to SQS) compile but were not run.

<!-- END-S1 -->

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

Per-binary lists are in `contracts/<binary>.yaml` (name, required, secret, source, example, description) and in `services/<service>/README.md`. The final handoff regenerates the complete tables from the contracts. Shared variables above, plus:

- **catalog-api** (`migrate` subcommand uses only `AWS_REGION, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_IAM_AUTH`): `DB_HOST*, DB_PORT, DB_NAME*, DB_USER*, DB_IAM_AUTH*(=true), DB_MAX_CONNS, DB_STATEMENT_TIMEOUT, DB_SSL_ROOT_CERT, REDIS_ADDR*, REDIS_USER, REDIS_IAM_AUTH, CACHE_NAME, REDIS_SERVERLESS, REDIS_AUTH_FILE, IMAGES_BUCKET*, CDN_BASE_URL*, EVENT_BUS_NAME*` (`*` = required).

<!-- END-S2 -->

## 3. IAM actions per service

(Appended per service.)

### catalog-api

| Action | Resource | Why |
|---|---|---|
| `rds-db:connect` | `arn:aws:rds-db:<region>:<account-id>:dbuser:<db-resource-id>/catalog_app` | IAM database authentication token |
| `elasticache:Connect` | cache and user ARNs (`:replicationgroup:` or `:serverlesscache:`, and `:user:`) | Redis IAM auth, only if `REDIS_IAM_AUTH=true` |
| `s3:PutObject` | `arn:aws:s3:::<images-bucket>/images/events/*` | the signer's role must itself be allowed to write what it presigns |
| `events:PutEvents` | the bus ARN | publish `EventPublished` |

`catalog-api migrate` runs as the DB user `catalog_migrator`: `rds-db:connect` on `.../catalog_migrator` only (give the Job its own role).

<!-- END-S3 -->

## 4. AWS resources the code expects

(Appended per service.)

### catalog

- **RDS PostgreSQL** database `catalog`; users `catalog_app` (runtime) and `catalog_migrator` (Job). Bootstrap SQL, run once as the master user while connected to database `catalog` (verified against a local postgres:17 with a stand-in `rds_iam` role; on RDS `rds_iam` already exists):

```sql
CREATE ROLE catalog_migrator LOGIN;  GRANT rds_iam TO catalog_migrator;
CREATE ROLE catalog_app LOGIN;       GRANT rds_iam TO catalog_app;
GRANT CONNECT ON DATABASE catalog TO catalog_migrator, catalog_app;
GRANT USAGE, CREATE ON SCHEMA public TO catalog_migrator;
GRANT USAGE ON SCHEMA public TO catalog_app;
ALTER DEFAULT PRIVILEGES FOR ROLE catalog_migrator IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO catalog_app;
ALTER DEFAULT PRIVILEGES FOR ROLE catalog_migrator IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO catalog_app;
```
- RDS instance must have IAM database authentication enabled; clients connect with TLS verify-full to the **endpoint host name** (not an IP or a CNAME that is not in the certificate).
- **ElastiCache** (Valkey/Redis) with in-transit encryption; for IAM auth a Redis user with `authentication-mode iam` named `REDIS_USER`, attached to a user group on the cache.
- **S3** images bucket (`IMAGES_BUCKET`), objects under `images/events/<eventId>/<uuid>.<ext>`; a CORS rule allowing `PUT` from the web origin with headers `Content-Type, Content-Length`; **CloudFront** distribution at `CDN_BASE_URL` serving that bucket.
- **EventBridge** bus `EVENT_BUS_NAME` (convention `gatepass-<env>`).

<!-- END-S4 -->

## 5. Event rules

(Appended per service.)

- Published by catalog: `source=gatepass.catalog`, `detail-type=EventPublished`. Rule for reservations (see the reservations section): pattern `{"source":["gatepass.catalog"],"detail-type":["EventPublished"]}`.

<!-- END-S5 -->

## 6. Assumptions and decisions

- Module path `github.com/olagookundavid/gatepass-app`, `go 1.26`, `toolchain go1.26.5`.
- Problem `type` values are URNs (`urn:gatepass:problem:<code>`); no public docs site exists to dereference.
- Hexagonal packages: `internal/<service>/{domain,ports,app,adapters/...}`.
- Base images pinned by digest in `build/Dockerfile` (`golang:1.26` index digest, `distroless/static-debian12:nonroot`). The `golang:1.26` image currently ships go 1.26.10; `go.mod` says `toolchain go1.26.5`, a lower version, so no toolchain download happens.
- `.dockerignore` excludes `*.md`, `docs`, `.git`.
- Git commit author/email are whatever the machine's `git config` has (not changed by the agent).
- The RDS CA bundle (`internal/platform/pgdb/certs/global-bundle.pem`) was downloaded from `https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem` on the authoring day and embedded; refresh it when AWS rotates CAs, or override with `DB_SSL_ROOT_CERT`.

- catalog: `DB_IAM_AUTH=true` is mandatory (no password path). Redis is not part of readiness (cache is optional); Postgres is.
- catalog: ids are UUIDv7 generated in the app. `PATCH` ticket types replaces them (new ids), allowed in draft only. `imageUrl` must start with `CDN_BASE_URL/images/events/<eventId>/`.
- catalog: image upload request needs `contentType` and `contentLength` (<= 5 MiB) because a presigned PUT can only pin the length if the client declares it.
- catalog: `publish` is idempotent and re-announces; announcement failure returns 503 while the DB state stays published.
- catalog: only the first page of event lists is cached (key contains a generation counter; invalidation = `INCR`).
- A small circuit breaker in `platform/redisx` opens for 5 s after a transport error so a black-holed cache costs one timeout, not one per request.
- Throwaway local containers (postgres:17, redis:7) were used only to run integration tests by hand; no compose/run files are committed.

<!-- END-S6 -->

## 7. UNVERIFIED items

- Platform: ElastiCache IAM auth token format (`internal/platform/redisx`), RDS IAM token against a real instance, Secrets Manager credential flow, EventBridge partial-failure handling against the real API, SQS consumer against a real queue.

- catalog: RDS IAM auth end to end (`TestRDSIAMAuth`), ElastiCache IAM token acceptance (see platform item), S3 presigned PUT enforcing Content-Type/Content-Length (`TestPresignedPutAcceptsRightContentTypeRejectsWrong`; whether S3 enforces a signed Content-Length for presigned PUT in your region/endpoint), EventBridge-to-SQS delivery (`TestPutEventsReachesQueue`), CORS for browser uploads.

<!-- END-S7 -->

## 8. Dependencies beyond the approved list

- None so far. Transitive: `golang.org/x/*`, grpc/protobuf (pulled in by the OTLP exporters). `otelaws` is approved but upstream marks it deprecated (kept, `//nolint:staticcheck` in `internal/platform/awscfg`).

<!-- END-S8 -->

## 9. Known gaps and suggestions

- OpenAPI validity is not machine-checked (spectral not installed).

<!-- END-S9 -->

## 10. How to read the code

Start with `internal/platform/lifecycle`, `httpserver`, `auth`, `problem`, then `sqsconsumer`, `ebpub`, `pgdb`.

<!-- END-S10 -->

