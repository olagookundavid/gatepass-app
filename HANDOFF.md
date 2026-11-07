# HANDOFF: state of the Gatepass Go code

Written incrementally while building; read this first. Placeholders (`<account-id>`, `example.com`) are used on purpose: this repository is public.

## 1. Status

Everything the prompt asked for is built; nothing was cut. AWS behaviour is **unverified** (no AWS calls were made, by design): see section 7.

| Binary | Status | Verified how |
|---|---|---|
| `internal/platform/*` | done | unit tests |
| `build/Dockerfile` | done | image builds for linux/amd64 and linux/arm64 (arm64 image ran read-only) |
| `catalog-api` (+ `migrate`) | done | unit tests; repositories and cache run against local PostgreSQL 17 and Redis 7 |
| `reservations-api` | done | unit tests; DynamoDB transactions run against DynamoDB Local and Redis 7 |
| `lambda-hold-expirer` | done | handler unit tests (both payload shapes); run against DynamoDB Local |
| `orders-api` (+ `migrate`) | done | unit tests; transactions, idempotency, rotation run against local PostgreSQL 17 |
| `orders-worker` | done | unit tests; DLQ integration test written (needs real SQS) |
| `orders-outbox-relay` | done | unit tests; two relays publish each row exactly once on local PostgreSQL 17 |
| `notifications-api` | done | unit tests; store verified on DynamoDB Local |
| `notifications-worker` | done | unit tests; end-to-end test written (needs real SQS/S3/SES) |
| `lambda-nightly-report` | done | unit tests; report query and window edge cases on local PostgreSQL 17 |
| `skeleton-api` | done (2026-11-07) | unit tests (routes, `FORCE_NOT_READY`, `BASE_PATH`, span and log with the same `trace_id`, duration metric); image built for linux/amd64 and linux/arm64, both ran `--read-only` as `nonroot`; `/readyz` 200, 503 with `FORCE_NOT_READY=true`; SIGTERM drains and exits 0 |

### Commands run and results (final state)

| Command | Result |
|---|---|
| `gofmt -l .` | no output |
| `go build ./...` | ok |
| `go vet ./...` | ok |
| `go vet -tags integration ./...` | ok (integration tests compile) |
| `golangci-lint run` (v2.1.6, config in `.golangci.yml`, includes the `integration` tag) | `0 issues.` |
| `go test -count=1 ./...` | 37 packages ok, 123 tests pass, 0 fail |
| `go test -tags integration -count=1 ./...` with no AWS variables | all packages ok; every integration test skips and names the variables it needs |
| `make build` | builds all 9 binaries |
| `make lint` | go vet (both tag sets) + golangci-lint: 0 issues |
| `make lambda-zip` | `dist/lambda-hold-expirer.zip`, `dist/lambda-nightly-report.zip` (linux/arm64 `bootstrap`, statically linked) |
| `docker build -f build/Dockerfile --build-arg BINARY=<b> --platform linux/amd64 .` | succeeded for `catalog-api`, `reservations-api`, `orders-api`, `notifications-worker` (amd64) and `reservations-api` (arm64; the earlier `catalog-api` skeleton also on both). Images are 10-44 MB, run as `nonroot`. `reservations-api` started with no environment exits with a message listing all 8 missing variables |
| `ruby` script over `api/*.yaml` and `contracts/*.yaml` | all parse; 4 OpenAPI 3.1 files and 9 contracts, operationIds unique (no spectral installed) |
| `bash -n scripts/golden-path.sh` and a run against a throwaway mock server | syntax ok; walkthrough completes and never prints a token |

### Integration tests run against throwaway local services

These were run by hand with containers started only for the checks (nothing for this is committed; no compose files):

| Tests | Against | Result |
|---|---|---|
| catalog: `TestPostgresRepositories`, `TestCacheAsideAgainstRealRedisAndOutageFallback` | postgres:17, redis:7 | pass |
| reservations: 8 of 9 tests in `internal/reservations/integration` (50 concurrent holds vs 10 tickets -> exactly 10; idempotent create/consume; expiry through both Lambda shapes; consumer idempotency; GSI pagination; Lua rate limiter) | `amazon/dynamodb-local` via `AWS_ENDPOINT_URL_DYNAMODB`, redis:7 | pass (`TestScheduleCreatedAndDeleted` skipped: needs Scheduler) |
| orders: idempotency, rollback, two relays exactly once, cancel-vs-payment race, reconciler, secret rotation with a fake secret source | postgres:17 | pass (3 consecutive runs) |
| notifications: `TestStoreNotificationLogPreferencesTicketsAndDedupe` | DynamoDB Local | pass |
| reports: `TestReportQueryWindowAndCSV` | postgres:17 | pass |
| bootstrap SQL of catalog and orders (roles, default privileges, read-only user) | postgres:17 | verified: app user can DML but not DDL; report user cannot write |

Not run (need real AWS): RDS IAM auth, ElastiCache IAM auth and TLS, S3 presigned URL behaviour, EventBridge/SQS delivery, Scheduler, SES, Cognito, real Secrets Manager rotation, DynamoDB TransactionConflict behaviour under real contention.

<!-- END-S1 -->

## 2. Environment variables per binary

Shared by every binary (also in each contract):

| Variable | Required | Meaning |
|---|---|---|
| `AWS_REGION` | yes | region for every SDK client |
| `AWS_MAX_ATTEMPTS` | no | SDK retry attempts, 1..10, default 3 |
| `LOG_LEVEL` | no | debug/info/warn/error, default info |
| `HTTP_ADDR` | no | public (or probe) listener, default `:8080` |
| `BASE_PATH` | no | public route prefix (service default: `/catalog`, `/reservations`, `/orders`, `/notifications`) |
| `SHUTDOWN_DELAY` | no | wait after SIGTERM before draining, default 5s |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | no | telemetry; no-op when the endpoint is unset |
| `COGNITO_ISSUER`, `COGNITO_CLIENT_ID` | public APIs | JWT validation |

Complete per-binary tables, generated from `contracts/*.yaml` (the contracts are the source of truth; the same tables are in each `services/*/README.md`, and `services/*/<binary>.env.example` holds placeholder env files). "Source" in the contracts says where the coach gets the value: `terraform-output`, `static`, `mounted-secret`, `lambda-runtime`.

#### `catalog-api`

| Variable | Required | Secret | Example | Description |
|---|---|---|---|---|
| `AWS_REGION` | yes | no | `us-east-1` | Region for every SDK client |
| `AWS_MAX_ATTEMPTS` | no | no | `3` | SDK retry attempts (1-10), default 3 |
| `LOG_LEVEL` | no | no | `info` | debug/info/warn/error |
| `HTTP_ADDR` | no | no | `:8080` | Public listener, default :8080 |
| `BASE_PATH` | no | no | `/catalog` | Public route prefix, default /catalog; the Ingress does not strip it |
| `SHUTDOWN_DELAY` | no | no | `5s` | Wait after SIGTERM before draining, default 5s |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | no | `http://otel-collector.observability:4318` | OTLP/HTTP collector; telemetry is a no-op when unset |
| `OTEL_SERVICE_NAME` | no | no | `catalog-api` | Service name in traces/metrics |
| `OTEL_RESOURCE_ATTRIBUTES` | no | no | `deployment.environment=dev` | Extra resource attributes |
| `COGNITO_ISSUER` | yes | no | `https://cognito-idp.us-east-1.amazonaws.com/<user-pool-id>` | Expected iss claim; JWKS is fetched from <issuer>/.well-known/jwks.json |
| `COGNITO_CLIENT_ID` | yes | no | `<app-client-id>` | Expected client_id claim of access tokens |
| `DB_HOST` | yes | no | `<rds-endpoint>` | RDS PostgreSQL endpoint host name (must match the TLS certificate) |
| `DB_PORT` | no | no | `5432` | Default 5432 |
| `DB_NAME` | yes | no | `catalog` | Database name |
| `DB_USER` | yes | no | `catalog_app` | Database user created WITH rds_iam; the migrate Job uses catalog_migrator |
| `DB_IAM_AUTH` | yes | no | `true` | Must be true; the service has no password path |
| `DB_MAX_CONNS` | no | no | `10` | pgxpool max connections, default 10. replicas x this must stay below the RDS max_connections |
| `DB_STATEMENT_TIMEOUT` | no | no | `5s` | Server-side statement_timeout, default 5s |
| `DB_SSL_ROOT_CERT` | no | no | `/etc/ssl/rds/global-bundle.pem` | Override the embedded RDS CA bundle (normally unset) |
| `REDIS_ADDR` | yes | no | `<elasticache-endpoint>:6379` | ElastiCache primary endpoint host:port (TLS) |
| `REDIS_USER` | no | no | `catalog` | Redis ACL user (IAM-enabled user when REDIS_IAM_AUTH=true) |
| `REDIS_IAM_AUTH` | no | no | `true` | Use an IAM token as the Redis password |
| `CACHE_NAME` | no | no | `<cache-name>` | ElastiCache replication group / serverless cache name; required when REDIS_IAM_AUTH=true |
| `REDIS_SERVERLESS` | no | no | `false` | true for ElastiCache Serverless (changes the IAM token request) |
| `REDIS_AUTH_FILE` | no | yes | `/var/run/secrets/redis/password` | Fallback: file holding a Redis AUTH password (when IAM auth is off) |
| `IMAGES_BUCKET` | yes | no | `<images-bucket>` | S3 bucket receiving event images |
| `CDN_BASE_URL` | yes | no | `https://cdn.example.com` | CloudFront base URL serving the images bucket |
| `EVENT_BUS_NAME` | yes | no | `gatepass-dev` | EventBridge bus for EventPublished |

#### `lambda-hold-expirer`

| Variable | Required | Secret | Example | Description |
|---|---|---|---|---|
| `TABLE_NAME` | yes | no | `<reservations-table>` | Same DynamoDB table as reservations-api |
| `AWS_REGION` | yes | no | `us-east-1` | Set by the Lambda runtime |
| `LOG_LEVEL` | no | no | `info` | debug/info/warn/error |
| `AWS_MAX_ATTEMPTS` | no | no | `3` | SDK retry attempts |

#### `lambda-nightly-report`

| Variable | Required | Secret | Example | Description |
|---|---|---|---|---|
| `DB_HOST` | yes | no | `<rds-endpoint>` | RDS endpoint host name of the orders database (must match the TLS certificate) |
| `DB_PORT` | no | no | `5432` | Default 5432 |
| `DB_NAME` | yes | no | `orders` | Orders database name |
| `DB_USER` | yes | no | `orders_report` | Read-only database user created WITH rds_iam |
| `REPORTS_BUCKET` | yes | no | `<reports-bucket>` | S3 bucket for reports/YYYY/MM/DD.csv |
| `AWS_REGION` | yes | no | `us-east-1` | Set by the Lambda runtime |
| `LOG_LEVEL` | no | no | `info` | debug/info/warn/error |

#### `notifications-api`

| Variable | Required | Secret | Example | Description |
|---|---|---|---|---|
| `AWS_REGION` | yes | no | `us-east-1` | Region for every SDK client |
| `AWS_MAX_ATTEMPTS` | no | no | `3` | SDK retry attempts (1-10), default 3 |
| `LOG_LEVEL` | no | no | `info` | debug/info/warn/error |
| `HTTP_ADDR` | no | no | `:8080` | Public listener, default :8080 |
| `BASE_PATH` | no | no | `/notifications` | Public route prefix, default /notifications; the Ingress does not strip it |
| `SHUTDOWN_DELAY` | no | no | `5s` | Wait after SIGTERM before draining, default 5s |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | no | `http://otel-collector.observability:4318` | OTLP/HTTP collector; no-op when unset |
| `OTEL_SERVICE_NAME` | no | no | `notifications-api` | Service name in telemetry |
| `OTEL_RESOURCE_ATTRIBUTES` | no | no | `deployment.environment=dev` | Extra resource attributes |
| `TABLE_NAME` | yes | no | `<notifications-table>` | DynamoDB table (PK, SK; TTL attribute ttl; no GSI) |
| `TICKETS_BUCKET` | yes | no | `<tickets-bucket>` | Private S3 bucket for tickets/<userId>/<orderId>.png |
| `COGNITO_ISSUER` | yes | no | `https://cognito-idp.us-east-1.amazonaws.com/<user-pool-id>` | Expected iss claim |
| `COGNITO_CLIENT_ID` | yes | no | `<app-client-id>` | Expected client_id claim of access tokens |

#### `notifications-worker`

| Variable | Required | Secret | Example | Description |
|---|---|---|---|---|
| `AWS_REGION` | yes | no | `us-east-1` | Region for every SDK client |
| `AWS_MAX_ATTEMPTS` | no | no | `3` | SDK retry attempts (1-10), default 3 |
| `LOG_LEVEL` | no | no | `info` | debug/info/warn/error |
| `HTTP_ADDR` | no | no | `:8080` | Probe listener, default :8080 |
| `SHUTDOWN_DELAY` | no | no | `5s` | Wait after SIGTERM before draining, default 5s |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | no | `http://otel-collector.observability:4318` | OTLP/HTTP collector; no-op when unset |
| `OTEL_SERVICE_NAME` | no | no | `notifications-worker` | Service name in telemetry |
| `OTEL_RESOURCE_ATTRIBUTES` | no | no | `deployment.environment=dev` | Extra resource attributes |
| `TABLE_NAME` | yes | no | `<notifications-table>` | DynamoDB table (PK, SK; TTL attribute ttl; no GSI) |
| `TICKETS_BUCKET` | yes | no | `<tickets-bucket>` | Private S3 bucket for tickets/<userId>/<orderId>.png |
| `NOTIFICATIONS_QUEUE_URL` | yes | no | `https://sqs.us-east-1.amazonaws.com/<account-id>/notifications` | Queue fed by EventBridge rules; DLQ with maxReceiveCount 5; visibility timeout >= 60s |
| `COGNITO_USER_POOL_ID` | yes | no | `<user-pool-id>` | Pool used for AdminGetUser (username = user id / sub) |
| `SES_FROM_ADDRESS` | yes | no | `tickets@example.com` | Verified SES sender identity |
| `SES_CONFIGURATION_SET` | no | no | `gatepass` | SES configuration set (bounce/complaint events) |
| `EMAIL_RECIPIENT_OVERRIDE` | no | no | `success@simulator.amazonses.com` | Send EVERY e-mail to this one address (sandbox helper) |
| `TICKET_SIGNING_KEY_FILE` | yes | yes | `/var/run/secrets/ticket/signing-key` | File with the HMAC key for QR tokens (>= 32 chars) |
| `TICKET_LINK_TTL` | no | no | `24h` | Requested validity of the e-mailed link, 1m-168h, default 24h (effective validity is capped by the signing credentials' lifetime) |
| `WORKER_CONCURRENCY` | no | no | `4` | Concurrent messages per pod, 1-64, default 4 |
| `CONSUMER_VISIBILITY_TIMEOUT` | no | no | `60s` | Visibility timeout set per receive (extended by a heartbeat), default 60s |

#### `orders-api`

| Variable | Required | Secret | Example | Description |
|---|---|---|---|---|
| `AWS_REGION` | yes | no | `us-east-1` | Region for every SDK client |
| `AWS_MAX_ATTEMPTS` | no | no | `3` | SDK retry attempts (1-10), default 3 |
| `LOG_LEVEL` | no | no | `info` | debug/info/warn/error |
| `HTTP_ADDR` | no | no | `:8080` | Public listener, default :8080 |
| `BASE_PATH` | no | no | `/orders` | Public route prefix, default /orders; the Ingress does not strip it |
| `SHUTDOWN_DELAY` | no | no | `5s` | Wait after SIGTERM before draining, default 5s |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | no | `http://otel-collector.observability:4318` | OTLP/HTTP collector; no-op when unset |
| `OTEL_SERVICE_NAME` | no | no | `orders-api` | Service name in telemetry |
| `OTEL_RESOURCE_ATTRIBUTES` | no | no | `deployment.environment=dev` | Extra resource attributes |
| `COGNITO_ISSUER` | yes | no | `https://cognito-idp.us-east-1.amazonaws.com/<user-pool-id>` | Expected iss claim |
| `COGNITO_CLIENT_ID` | yes | no | `<app-client-id>` | Expected client_id claim of access tokens |
| `DB_HOST` | yes | no | `<rds-endpoint>` | RDS PostgreSQL endpoint host name (must match the TLS certificate) |
| `DB_PORT` | no | no | `5432` | Default 5432 |
| `DB_NAME` | yes | no | `orders` | Database name |
| `DB_SECRET_ID` | yes | no | `<secret-name-or-arn>` | Secrets Manager secret holding JSON {username,password} (orders_app; the migrate Job uses the orders_migrator secret). The VALUE is never in Git or env |
| `DB_MAX_CONNS` | no | no | `10` | pgxpool max connections, default 10 |
| `DB_STATEMENT_TIMEOUT` | no | no | `5s` | Server-side statement_timeout, default 5s |
| `DB_SSL_ROOT_CERT` | no | no | `/etc/ssl/rds/global-bundle.pem` | Override the embedded RDS CA bundle (normally unset) |
| `RESERVATIONS_INTERNAL_URL` | yes | no | `http://reservations-internal.gatepass.svc.cluster.local:8081` | Cluster-internal URL of the reservations INTERNAL listener (no trailing path) |
| `PAYMENTS_QUEUE_URL` | yes | no | `https://sqs.us-east-1.amazonaws.com/<account-id>/orders-payments` | Payments queue (with DLQ, maxReceiveCount 5) |

#### `orders-outbox-relay`

| Variable | Required | Secret | Example | Description |
|---|---|---|---|---|
| `AWS_REGION` | yes | no | `us-east-1` | Region for every SDK client |
| `AWS_MAX_ATTEMPTS` | no | no | `3` | SDK retry attempts (1-10), default 3 |
| `LOG_LEVEL` | no | no | `info` | debug/info/warn/error |
| `HTTP_ADDR` | no | no | `:8080` | Probe listener, default :8080 |
| `SHUTDOWN_DELAY` | no | no | `5s` | Wait after SIGTERM before draining, default 5s |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | no | `http://otel-collector.observability:4318` | OTLP/HTTP collector; no-op when unset |
| `OTEL_SERVICE_NAME` | no | no | `orders-outbox-relay` | Service name in telemetry |
| `OTEL_RESOURCE_ATTRIBUTES` | no | no | `deployment.environment=dev` | Extra resource attributes |
| `DB_HOST` | yes | no | `<rds-endpoint>` | RDS PostgreSQL endpoint host name (must match the TLS certificate) |
| `DB_PORT` | no | no | `5432` | Default 5432 |
| `DB_NAME` | yes | no | `orders` | Database name |
| `DB_SECRET_ID` | yes | no | `<secret-name-or-arn>` | Secrets Manager secret holding JSON {username,password} (orders_app; the migrate Job uses the orders_migrator secret). The VALUE is never in Git or env |
| `DB_MAX_CONNS` | no | no | `10` | pgxpool max connections, default 10 |
| `DB_STATEMENT_TIMEOUT` | no | no | `5s` | Server-side statement_timeout, default 5s |
| `DB_SSL_ROOT_CERT` | no | no | `/etc/ssl/rds/global-bundle.pem` | Override the embedded RDS CA bundle (normally unset) |
| `EVENT_BUS_NAME` | yes | no | `gatepass-dev` | EventBridge bus |
| `PAYMENTS_QUEUE_URL` | yes | no | `https://sqs.us-east-1.amazonaws.com/<account-id>/orders-payments` | Used only by the payment reconciler |
| `OUTBOX_BATCH_SIZE` | no | no | `25` | Rows per cycle, 1-100, default 25 |
| `OUTBOX_POLL_INTERVAL` | no | no | `1s` | Idle sleep between cycles (min 100ms), default 1s |
| `PAYMENT_RECONCILE_AFTER` | no | no | `30s` | Grace period before an unqueued payment is reconciled, default 30s |
| `OUTBOX_RETENTION` | no | no | `168h` | How long published outbox rows are kept, default 168h (7 days) |

#### `orders-worker`

| Variable | Required | Secret | Example | Description |
|---|---|---|---|---|
| `AWS_REGION` | yes | no | `us-east-1` | Region for every SDK client |
| `AWS_MAX_ATTEMPTS` | no | no | `3` | SDK retry attempts (1-10), default 3 |
| `LOG_LEVEL` | no | no | `info` | debug/info/warn/error |
| `HTTP_ADDR` | no | no | `:8080` | Probe listener, default :8080 |
| `SHUTDOWN_DELAY` | no | no | `5s` | Wait after SIGTERM before draining, default 5s |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | no | `http://otel-collector.observability:4318` | OTLP/HTTP collector; no-op when unset |
| `OTEL_SERVICE_NAME` | no | no | `orders-worker` | Service name in telemetry |
| `OTEL_RESOURCE_ATTRIBUTES` | no | no | `deployment.environment=dev` | Extra resource attributes |
| `DB_HOST` | yes | no | `<rds-endpoint>` | RDS PostgreSQL endpoint host name (must match the TLS certificate) |
| `DB_PORT` | no | no | `5432` | Default 5432 |
| `DB_NAME` | yes | no | `orders` | Database name |
| `DB_SECRET_ID` | yes | no | `<secret-name-or-arn>` | Secrets Manager secret holding JSON {username,password} (orders_app; the migrate Job uses the orders_migrator secret). The VALUE is never in Git or env |
| `DB_MAX_CONNS` | no | no | `10` | pgxpool max connections, default 10 |
| `DB_STATEMENT_TIMEOUT` | no | no | `5s` | Server-side statement_timeout, default 5s |
| `DB_SSL_ROOT_CERT` | no | no | `/etc/ssl/rds/global-bundle.pem` | Override the embedded RDS CA bundle (normally unset) |
| `PAYMENTS_QUEUE_URL` | yes | no | `https://sqs.us-east-1.amazonaws.com/<account-id>/orders-payments` | Queue to consume. Redrive policy to the DLQ: maxReceiveCount 5; queue visibility timeout >= 60s |
| `WORKER_CONCURRENCY` | no | no | `4` | Concurrent payments per pod, 1-64, default 4 |
| `CONSUMER_VISIBILITY_TIMEOUT` | no | no | `60s` | Visibility timeout set per receive (extended by a heartbeat), default 60s |
| `PAYMENT_SUCCESS_RATE` | no | no | `0.8` | Probability a simulated payment is approved (0-1), default 0.8; the rest splits evenly into declines and transient errors |

#### `reservations-api`

| Variable | Required | Secret | Example | Description |
|---|---|---|---|---|
| `AWS_REGION` | yes | no | `us-east-1` | Region for every SDK client |
| `AWS_MAX_ATTEMPTS` | no | no | `3` | SDK retry attempts (1-10), default 3 |
| `LOG_LEVEL` | no | no | `info` | debug/info/warn/error |
| `HTTP_ADDR` | no | no | `:8080` | Public listener, default :8080 |
| `INTERNAL_HTTP_ADDR` | no | no | `:8081` | Internal listener (consume endpoint), default :8081; only started when API_ENABLED=true |
| `BASE_PATH` | no | no | `/reservations` | Public route prefix, default /reservations |
| `SHUTDOWN_DELAY` | no | no | `5s` | Wait after SIGTERM before draining, default 5s |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | no | `http://otel-collector.observability:4318` | OTLP/HTTP collector; no-op when unset |
| `OTEL_SERVICE_NAME` | no | no | `reservations-api` | Service name in telemetry |
| `OTEL_RESOURCE_ATTRIBUTES` | no | no | `deployment.environment=dev` | Extra resource attributes |
| `TABLE_NAME` | yes | no | `<reservations-table>` | DynamoDB table (PK, SK, GSI1 on GSI1PK/GSI1SK, TTL attribute ttl) |
| `API_ENABLED` | no | no | `true` | Serve the public and internal APIs, default true |
| `CONSUMER_ENABLED` | no | no | `false` | Run the inventory SQS consumer, default false |
| `INVENTORY_QUEUE_URL` | no | no | `https://sqs.us-east-1.amazonaws.com/<account-id>/reservations-inventory` | Required when CONSUMER_ENABLED=true |
| `CONSUMER_WORKERS` | no | no | `4` | Concurrent message handlers, default 4 |
| `CONSUMER_VISIBILITY_TIMEOUT` | no | no | `60s` | SQS visibility timeout set while handling (extended by a heartbeat), default 60s |
| `COGNITO_ISSUER` | no | no | `https://cognito-idp.us-east-1.amazonaws.com/<user-pool-id>` | Required when API_ENABLED=true |
| `COGNITO_CLIENT_ID` | no | no | `<app-client-id>` | Required when API_ENABLED=true |
| `SCHEDULER_GROUP_NAME` | no | no | `gatepass-holds` | EventBridge Scheduler group holding hold-<id> schedules; required when API_ENABLED=true |
| `SCHEDULER_ROLE_ARN` | no | no | `arn:aws:iam::<account-id>:role/gatepass-scheduler-invoke-expirer` | Role Scheduler assumes to invoke the Lambda; reservations-api needs iam:PassRole on it; required when API_ENABLED=true |
| `EXPIRER_LAMBDA_ARN` | no | no | `arn:aws:lambda:us-east-1:<account-id>:function:gatepass-hold-expirer` | Schedule target; required when API_ENABLED=true |
| `HOLD_TTL` | no | no | `10m` | How long a hold lasts, 1m-1h, default 10m |
| `RATE_LIMIT_PER_MINUTE` | no | no | `10` | POST /holds per user per minute, default 10 |
| `REDIS_ADDR` | no | no | `<elasticache-endpoint>:6379` | ElastiCache host:port (TLS); required when API_ENABLED=true |
| `REDIS_USER` | no | no | `reservations` | Redis ACL user |
| `REDIS_IAM_AUTH` | no | no | `true` | Use an IAM token as the Redis password |
| `CACHE_NAME` | no | no | `<cache-name>` | ElastiCache name for the IAM token; required when REDIS_IAM_AUTH=true |
| `REDIS_SERVERLESS` | no | no | `false` | true for ElastiCache Serverless |
| `REDIS_AUTH_FILE` | no | yes | `/var/run/secrets/redis/password` | Fallback AUTH password file |


#### `skeleton-api`

Walking-skeleton binary (no AWS, no database): every variable is optional.

| Variable | Required | Secret | Example | Description |
|---|---|---|---|---|
| `LOG_LEVEL` | no | no | `info` | debug\|info\|warn\|error |
| `HTTP_ADDR` | no | no | `:8080` | Public listener, default :8080 |
| `BASE_PATH` | no | no | `/skeleton` | Public route prefix, default /skeleton. The three routes are also served at the root, which is what the kubelet probes |
| `SHUTDOWN_DELAY` | no | no | `5s` | Wait after SIGTERM before draining, default 5s |
| `FORCE_NOT_READY` | no | no | `false` | true makes /readyz answer 503 (the pipeline's "a broken image is not promoted" proof). /livez stays 200 |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | no | `http://otel-collector.observability:4318` | OTLP/HTTP collector; telemetry is a no-op when unset |
| `OTEL_SERVICE_NAME` | no | no | `skeleton-api` | Service name in traces/metrics |
| `OTEL_RESOURCE_ATTRIBUTES` | no | no | `deployment.environment=dev` | Extra resource attributes |

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

### reservations-api

| Action | Resource | Why |
|---|---|---|
| `dynamodb:GetItem` | table ARN | inventory, holds, idempotency, processed-event markers (consistent reads) |
| `dynamodb:Query` | table ARN and `.../index/GSI1` | inventory of an event; "my holds" through GSI1 |
| `dynamodb:PutItem` | table ARN | create inventory if absent, markers, and puts inside transactions |
| `dynamodb:UpdateItem` | table ARN | counters and hold status, single and inside transactions |
| `scheduler:CreateSchedule` | `arn:aws:scheduler:<region>:<account-id>:schedule/<SCHEDULER_GROUP_NAME>/hold-*` | one-time expiry schedule per hold |
| `scheduler:DeleteSchedule` | same | remove it when the hold is consumed or deleted |
| **`iam:PassRole`** | `SCHEDULER_ROLE_ARN`, condition `iam:PassedToService = scheduler.amazonaws.com` | **CreateSchedule passes the execution role to EventBridge Scheduler; without this, CreateSchedule fails with AccessDenied naming iam:PassRole** |
| `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility` | inventory queue ARN | consumer (only if `CONSUMER_ENABLED=true`) |
| `elasticache:Connect` | cache + user ARNs | only if `REDIS_IAM_AUTH=true` |

UNVERIFIED: DynamoDB transactions are authorised by the item-level actions above; there is (to my knowledge) no separate `TransactWriteItems` IAM action. If the first real call is denied, check the error message for the missing action.

### lambda-hold-expirer (execution role)

| Action | Resource | Why |
|---|---|---|
| `dynamodb:GetItem`, `UpdateItem`, `PutItem` | table ARN | load/release the hold, return tickets, release marker |
| `dynamodb:DescribeStream`, `GetRecords`, `GetShardIterator`, `ListStreams` | stream ARN (`*` for ListStreams) | stream event source mapping |
| `logs:CreateLogStream`, `logs:PutLogEvents` (+ `CreateLogGroup`) | log group | logs and EMF metric line |

### EventBridge Scheduler execution role (`SCHEDULER_ROLE_ARN`)

Trust policy: `scheduler.amazonaws.com` (add `aws:SourceAccount` condition). Permission: `lambda:InvokeFunction` on `EXPIRER_LAMBDA_ARN`.

### orders-api

| Action | Resource | Why |
|---|---|---|
| `secretsmanager:GetSecretValue` | the `orders_app` secret ARN | database credentials; re-read after an authentication failure (rotation) |
| `kms:Decrypt` | the secret's CMK (only if customer-managed) | decrypt the secret |
| `sqs:SendMessage` | `orders-payments` queue ARN | queue the payment request |

Network (not IAM): HTTP to the reservations **internal** Service (port 8081), RDS 5432, and Cognito JWKS (HTTPS egress).

### orders-worker

`secretsmanager:GetSecretValue` (+ `kms:Decrypt`); `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility` on `orders-payments`.

### orders-outbox-relay

`secretsmanager:GetSecretValue` (+ `kms:Decrypt`); `events:PutEvents` on the bus ARN; `sqs:SendMessage` on `orders-payments` (payment reconciler).

### orders migrate Job

`secretsmanager:GetSecretValue` on the `orders_migrator` secret only.

### lambda-nightly-report (execution role)

| Action | Resource | Why |
|---|---|---|
| `rds-db:connect` | `arn:aws:rds-db:<region>:<account-id>:dbuser:<db-resource-id>/orders_report` | IAM database authentication as the read-only user |
| `s3:PutObject` | `arn:aws:s3:::<reports-bucket>/reports/*` | write (overwrite) the daily CSV |
| managed `AWSLambdaVPCAccessExecutionRole` | | the function runs in the VPC (ENI actions + logs) |

The Scheduler execution role for the daily schedule needs `lambda:InvokeFunction` on this function.

### notifications-api

`dynamodb:GetItem`, `dynamodb:PutItem`, `dynamodb:Query` on the notifications table ARN; `s3:GetObject` on `arn:aws:s3:::<tickets-bucket>/tickets/*` (the signing role must itself be allowed to read what it presigns).

### notifications-worker

| Action | Resource | Why |
|---|---|---|
| `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility` | notifications queue ARN | consumer |
| `dynamodb:GetItem`, `dynamodb:PutItem` | notifications table ARN | preferences, log, ticket record, dedupe marker |
| `s3:PutObject`, `s3:GetObject` | `arn:aws:s3:::<tickets-bucket>/tickets/*` | store the ticket; make the presigned link work |
| `cognito-idp:AdminGetUser` | `arn:aws:cognito-idp:<region>:<account-id>:userpool/<pool-id>` | look up the address at send time |
| `ses:SendEmail` | the identity ARN (`arn:aws:ses:<region>:<account-id>:identity/<address-or-domain>`) and the configuration set ARN | send mail (SES v2) |

### skeleton-api

None. No AWS client; the contract has `iam: []`.

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

### reservations

- **DynamoDB table** (`TABLE_NAME`), on-demand. Keys: `PK` (S) hash, `SK` (S) range. **GSI1**: `GSI1PK` (S) hash, `GSI1SK` (S) range, projection ALL. **TTL attribute: `ttl`** (epoch seconds; set on hold, idempotency, processed-event and release-marker items). **Streams enabled** (view type `NEW_AND_OLD_IMAGES`; `OLD_IMAGE` is the minimum used) for the Lambda safety net.
- **SQS inventory queue** (`INVENTORY_QUEUE_URL`) + DLQ: `maxReceiveCount` 5, visibility timeout >= 60 s (the consumer sets 60 s per receive and extends), long polling is done by the client. Queue policy: allow `events.amazonaws.com` `sqs:SendMessage` with `aws:SourceArn` = the three rule ARNs.
- **EventBridge Scheduler**: schedule group `SCHEDULER_GROUP_NAME` (e.g. `gatepass-holds`); execution role `SCHEDULER_ROLE_ARN` (trust `scheduler.amazonaws.com`, `lambda:InvokeFunction` on the Lambda); the schedules themselves (`hold-<holdId>`) are created at runtime with `ActionAfterCompletion=DELETE`, retry policy 5 attempts / 1 h.
- **Lambda `hold-expirer`**: `provided.al2023`, arm64, handler `bootstrap`, env `TABLE_NAME`, timeout ~15 s. **Invoke permission** for the Scheduler role (identity-based, on the role) is enough; no resource policy is needed for Scheduler invocations through a role. Triggers: (1) Scheduler (no mapping needed), (2) DynamoDB stream event source mapping with `FunctionResponseTypes=["ReportBatchItemFailures"]`, bounded retries and an on-failure SQS destination.
- **ElastiCache** user for reservations (same mechanism as catalog), key prefix `rl:`.
- Recommended CloudWatch alarms: DLQ depth > 0; Lambda errors; `holds_expired_released_total` flat while holds are created (expiry broken).

### orders

- **RDS PostgreSQL** database `orders` (can be the same instance as `catalog`, different database). Users `orders_migrator`, `orders_app` (passwords in Secrets Manager, **no password in Git**), `orders_report` (IAM auth, read-only, used by the nightly report Lambda). Bootstrap SQL run once as the master user (verified locally; replace the placeholders from Secrets Manager values):

```sql
CREATE DATABASE orders;
\c orders
CREATE ROLE orders_migrator LOGIN PASSWORD '<value from the migrator secret>';
CREATE ROLE orders_app      LOGIN PASSWORD '<value from the app secret>';
CREATE ROLE orders_report   LOGIN;  GRANT rds_iam TO orders_report;
GRANT CONNECT ON DATABASE orders TO orders_migrator, orders_app, orders_report;
GRANT USAGE, CREATE ON SCHEMA public TO orders_migrator;
GRANT USAGE ON SCHEMA public TO orders_app, orders_report;
ALTER DEFAULT PRIVILEGES FOR ROLE orders_migrator IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO orders_app;
ALTER DEFAULT PRIVILEGES FOR ROLE orders_migrator IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO orders_app;
ALTER DEFAULT PRIVILEGES FOR ROLE orders_migrator IN SCHEMA public
  GRANT SELECT ON TABLES TO orders_report;
```
- **Secrets Manager** secrets (JSON `{"username":"...","password":"..."}`): `orders_app` (`DB_SECRET_ID` of api/worker/relay) and `orders_migrator` (`DB_SECRET_ID` of the migrate Job). Rotation is supported by the code (re-read after one authentication failure, connections recycled every 30 min); a rotation that alternates two users (RDS "alternating users" strategy) keeps `username` in the secret JSON current, which the code honours.
- **SQS** `orders-payments` + DLQ: `maxReceiveCount` 5, visibility timeout >= 60 s. The payments queue is NOT fed by EventBridge; the API and the reconciler send to it directly.
- **EventBridge** bus: same as catalog. Source used by orders: `gatepass.orders`.
- Recommended alarms: DLQ not empty; `outbox_unpublished` > 0 for 5 min; `orders_payment_enqueue_failures_total` rate; relay pod count < 1.

### notifications

- **DynamoDB table** (own table, `TABLE_NAME`), on-demand. Keys `PK` (S) hash, `SK` (S) range, **no GSI**, **TTL attribute `ttl`** (used only by `EVT#` markers). Item layout: `USER#<id>/NOTIF#<time>#<id>`, `USER#<id>/PREFS`, `USER#<id>/TICKET#<orderId>`, `EVT#<eventId>/EVT`.
- **S3 tickets bucket** (`TICKETS_BUCKET`): private, block public access, default encryption; keys `tickets/<userId>/<orderId>.png`; add a lifecycle expiry rule.
- **SQS `notifications`** + DLQ (`maxReceiveCount` 5, visibility timeout >= 60 s), queue policy allowing `events.amazonaws.com` from the three rules.
- **SES**: a verified identity for `SES_FROM_ADDRESS` (domain identity with DKIM is the production choice), optionally a configuration set. The account is in the **SES sandbox** until production access is granted: set `EMAIL_RECIPIENT_OVERRIDE` to a verified address (or `success@simulator.amazonses.com`).
- **Cognito user pool** (`COGNITO_USER_POOL_ID`): users must have an `email` attribute. The worker role needs `AdminGetUser`.
- **Secret for the QR signing key**: a random string of at least 32 characters (for example `openssl rand -base64 48`), delivered to the pod as a file (`TICKET_SIGNING_KEY_FILE`), never in Git or env.

### reports (lambda-nightly-report)

- **Lambda** `nightly-report`: `provided.al2023`, arm64, handler `bootstrap`, memory 256 MB, timeout 60 s, reserved concurrency 1, **inside the VPC** (private subnets that can reach RDS; its security group must be allowed by the RDS security group on 5432). Env: `DB_HOST, DB_NAME, DB_USER=orders_report, REPORTS_BUCKET` (+ optional `DB_PORT`).
- **Network path to S3**: an S3 gateway VPC endpoint (free) or a NAT gateway, otherwise PutObject times out.
- **S3 reports bucket** (`REPORTS_BUCKET`): private, versioning recommended, keys `reports/YYYY/MM/DD.csv`.
- **EventBridge Scheduler schedule** (daily): `cron(5 0 * * ? *)` (UTC), input `{}`, flexible window off, its own invoke role (`lambda:InvokeFunction`), a small retry policy (the function is idempotent). Backfill: invoke with `{"date":"YYYY-MM-DD"}`.
- **DB user** `orders_report` (IAM, SELECT only): created by the orders bootstrap SQL above; migration `0002_status_history_at_index.sql` adds the index the report query uses.
- Alarms: Lambda `Errors`; absence of `reports/<yesterday>.csv` by 01:00 UTC.

<!-- END-S4 -->

## 5. Event rules

(Appended per service.)

- Published by catalog: `source=gatepass.catalog`, `detail-type=EventPublished`. Rule for reservations (see the reservations section): pattern `{"source":["gatepass.catalog"],"detail-type":["EventPublished"]}`.

Three rules on bus `gatepass-<env>`, all with the inventory queue as target (SQS target, no input transformer; the consumer unwraps the full EventBridge envelope):

| Rule | Event pattern |
|---|---|
| inventory-event-published | `{"source":["gatepass.catalog"],"detail-type":["EventPublished"]}` |
| inventory-payment-failed | `{"source":["gatepass.orders"],"detail-type":["PaymentFailed"]}` |
| inventory-order-cancelled | `{"source":["gatepass.orders"],"detail-type":["OrderCancelled"]}` |

- Published by orders (source `gatepass.orders`, via the outbox relay): `OrderPlaced`, `OrderPaid`, `PaymentFailed`, `OrderCancelled`. Consumer rules: reservations (PaymentFailed, OrderCancelled, see above); notifications rules are listed in the notifications section. Optionally an archive of the whole bus for `OrderPlaced` audit.

Three more rules on the same bus, target = the notifications queue (SQS target, no input transformer):

| Rule | Event pattern |
|---|---|
| notifications-order-paid | `{"source":["gatepass.orders"],"detail-type":["OrderPaid"]}` |
| notifications-payment-failed | `{"source":["gatepass.orders"],"detail-type":["PaymentFailed"]}` |
| notifications-order-cancelled | `{"source":["gatepass.orders"],"detail-type":["OrderCancelled"]}` |

Summary of all rules: catalog `EventPublished` -> inventory queue; orders `PaymentFailed`, `OrderCancelled` -> inventory queue AND notifications queue; orders `OrderPaid` -> notifications queue; `OrderPlaced` has no consumer (archive only).

<!-- END-S5 -->

## 6. Assumptions and decisions

- Module path `github.com/olagookundavid/gatepass-app`, `go 1.26`, `toolchain go1.26.5`.
- Problem `type` values are URNs (`urn:gatepass:problem:<code>`); no public docs site exists to dereference.
- Hexagonal packages: `internal/<service>/{domain,ports,app,adapters/...}`.
- Base images pinned by digest in `build/Dockerfile` (`golang:1.26` index digest, `distroless/static-debian12:nonroot`). The pinned `golang:1.26` digest ships go 1.26.10 (refreshed 2026-11-07 to `sha256:cfa27959...`, same Go version); `go.mod` says `toolchain go1.26.10` (raised from go1.26.5 after `govulncheck` reported ten standard-library vulnerabilities fixed in 1.26.9/1.26.10), so no toolchain download happens in the image build. CI uses the `go.mod` toolchain (`go-version-file`), which is why that line, not the image digest, was the vulnerable one. `golang.org/x/net` (indirect, pulled in by `google.golang.org/grpc` through the OTLP metric exporter) is v0.62.0, fixing CVE-2026-78661 in `x/net/html`.
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

- reservations: **TTL attribute is `ttl`, not `expiresAt`** (deviation from the plan). Reason: with `expiresAt` as TTL, DynamoDB deletes a consumed hold ~10 minutes after creation, and a later `OrderCancelled`/`PaymentFailed` could not find the hold to give the tickets back. `ttl` = hold expiry while active; moved to +30 days once consumed and +7 days once released.
- reservations: idempotency item key is `IDEMP#<userId>#<key>` (scoped per user) instead of `IDEMP#<key>`, so one user's key can never replay another user's hold.
- reservations: the price snapshot is copied onto the hold at creation (read from inventory first), so the consume endpoint answers from the hold alone.
- reservations: if the Scheduler call fails after the hold was written, the hold is rolled back (released) and the API returns 503; the idempotency record then replays a `released` hold, so the client must use a new key.
- reservations: a replayed `POST /holds` returns 201 with header `Idempotent-Replayed: true` and the hold's CURRENT status.
- reservations: `GET /me/holds` uses GSI1 (eventually consistent), requests `limit+1` to avoid empty last pages, and refuses cursors that point outside the caller's partition.
- reservations: real DynamoDB cancels concurrent transactions on one item with `TransactionConflict`; the store retries 8 times with jittered backoff (same `ClientRequestToken`), then returns 503 + `Retry-After: 1`.
- reservations: `HOLD_TTL` is configurable (default 10m, 1m-1h) so end-to-end tests do not wait 10 minutes; the product rule remains 10 minutes.
- reservations: `API_ENABLED=false` runs a consumer-only Deployment that still serves `/livez`, `/readyz`, `/version`.
- reservations: readiness = a `GetItem` on the table (needs no `DescribeTable`). Redis is not a readiness dependency.
- reservations: unknown/irrelevant event types and releases for holds that no longer exist are acknowledged (logged), never sent to the DLQ.
- hold-expirer: an early Scheduler firing (`now < expiresAt`) returns an error so Scheduler's retry policy re-invokes it.

- orders: **order ids are deterministic** (UUID v5 of user + Idempotency-Key + hold id), not UUIDv7. This makes "crash after reservations consumed the hold, before the order committed" recoverable by a client retry. Outbox ids / event ids are UUIDv7.
- orders: the payment enqueue after commit is not atomic with the commit. A `payment_enqueued_at` column plus a **reconciler inside `orders-outbox-relay`** closes the gap (extra, not in the plan). The relay therefore needs `sqs:SendMessage`.
- orders: transition timestamps (`updated_at`, history `at`, outbox `created_at`) come from `clock_timestamp()` inside the locked transaction, so they follow commit order (found by an integration test: using the caller's clock re-ordered `OrderCancelled` before `OrderPaid`).
- orders: a payment approval that arrives for an already **cancelled** order is ignored (logged); refunds are out of scope. `Cancel` of a `payment_failed` order is refused (409): its tickets were already released.
- orders: simulated payment outcomes: success `PAYMENT_SUCCESS_RATE`; the remainder splits 50/50 into permanent decline (`card_declined`) and transient error (retry via SQS).
- orders: idempotency key retention 24 h (rows cleaned by the relay every 10 min); keys are scoped per user. Replay returns the stored JSON (jsonb re-serialised, semantically identical).
- orders: readiness checks only Postgres. A reservations outage returns 503 on `POST /orders` but reads still work.
- orders: outbox published rows are deleted after `OUTBOX_RETENTION` (default 7 days) by the relay; this needs DELETE on `outbox`/`idempotency_keys` for `orders_app` (granted by the default privileges above).
- orders: cancel body is optional (`{"reason": "..."}`), default reason `cancelled by user`; only the owner may cancel (admins can read, not cancel).

- notifications: the ticket pointer is stored as an extra item `USER#<id>/TICKET#<orderId>` so ownership needs no scan; "ownership via the notification log" is therefore a record in the same partition and the same table.
- notifications: preferences are checked before the Cognito lookup; the ticket is still generated when e-mail is disabled (so `ticket-url` works).
- notifications: marker-after-work means a crash between SES send and marker can produce a duplicate e-mail (at-least-once). Accepted and documented in code.
- notifications: **a presigned URL cannot outlive the credentials that signed it.** With IRSA/Pod Identity (about 1 h session credentials) the "valid 24 h" e-mailed link may expire earlier. The fresh-link endpoint always works. For durable links use CloudFront signed URLs or signing with a long-lived principal (not allowed here).
- notifications: ticket PNG text uses a built-in 3x5 pixel font because no text-rendering library is on the approved list; only `go-qrcode` is added for the QR matrix.
- notifications: a PaymentFailed/OrderCancelled e-mail never contains a link. A cancelled paid order keeps its ticket object (no revoke); gate verification should also check the order status.
- notifications: `userId` and `orderId` from events must match `[A-Za-z0-9_-]{1,128}` (they become S3 keys); otherwise the message errors into the DLQ.
- sqsconsumer: every message now gets a log correlation context (`request_id` = SQS message id) so `user_id` can be attached by handlers.

- platform: problem `type` values are URNs; `X-Request-Id` is only reused when it matches `[A-Za-z0-9._-]{8,64}` (otherwise a new UUID), so arbitrary client text never reaches logs or response headers.
- platform: `BASE_PATH` prefixes only the public API; health routes are always at the root (the skeleton additionally serves them under `BASE_PATH`, see above). The internal listener has no health routes (probes use the public port).
- platform: readiness results are cached 5 s and failures are reported as `"fail"` without error text (hostnames could leak).
- platform: `pgdb` has test-only constructors (`ConnectDSN`, `ConnectDSNWithCreds`) behind the `integration` build tag; no production binary contains them. The same tag guards `testenv.FreshSchema`.
- reports: "sales of a day" = orders whose transition to `paid` happened in the UTC day (from `order_status_history`), with a column showing how many are still paid; a report for today is refused (`{"date":...}` must be a finished day).
- A `.dockerignore` excludes docs, markdown and `.git` from the build context. `services/<service>/<binary>.env.example` files are generated from the contracts and contain placeholders only.
- Git: local commits only, author = the machine's `git config` (user.name/user.email); no remote, no push, no trailers or AI attribution in messages.
- The golden-path script needs `curl` and `jq`, reads `TOKEN`/`ADMIN_TOKEN` from the environment and never prints them. `WAIT_EXPIRY=true` additionally waits for a hold to expire.

- skeleton: `cmd/skeleton-api` is the walking-skeleton binary for the delivery pipeline (request from the coach, `docs/GO_AGENT_REQUEST_skeleton-api.md`). It does **not** use `config.LoadCommon` or `bootstrap.Init`, because both require `AWS_REGION` and load an AWS config; it reads its five variables with `config.Loader` and wires the logger and telemetry itself. Nothing is required.
- skeleton: the three routes are served **both** under `BASE_PATH` (what the Ingress routes, and what the request's check `curl localhost:8080/skeleton/readyz` uses) **and** at the root (what the kubelet probes, as for every other service). The platform `Health` is not used, because its bodies differ from the requested ones (`alive` / `not_ready` / a `checks` object versus `ok` / `not ready`); the skeleton has its own three handlers with the requested bodies. `/readyz` is also 503 while shutting down.
- platform (changed for every service, found while doing the skeleton): (1) `httpserver` access log: a 5xx on a non-probe route is now logged at `error` (was `info`), and the probe exemption (debug) now matches `/livez` and `/readyz` under any prefix, so `/skeleton/readyz` is quiet too. A failing probe stays at debug on purpose (a forced not-ready or a shutdown would write an error line every probe period); the request said "any 5xx", flip it in `accessLog` if probes should count. (2) new innermost middleware `routeTag`: `http.route` was **missing** from `http.server.request.duration` and from spans for every service (the `requestID` middleware copies the request, so the pattern the mux sets never reached `otelhttp`); it is now set from the matched pattern, method stripped (`/catalog/v1/events/{id}`), low cardinality, none for unmatched paths. Without it Phase 5 could not show RED per endpoint.
- skeleton: `OTEL_SERVICE_NAME` is documented as the service name, but `telemetry.Setup` applies the code-supplied name after the environment, so the code name wins when they differ. For the skeleton both are `skeleton-api`; not changed.

<!-- END-S6 -->

## 7. UNVERIFIED items

- Platform: ElastiCache IAM auth token format (`internal/platform/redisx`), RDS IAM token against a real instance, Secrets Manager credential flow, EventBridge partial-failure handling against the real API, SQS consumer against a real queue.

- catalog: RDS IAM auth end to end (`TestRDSIAMAuth`), ElastiCache IAM token acceptance (see platform item), S3 presigned PUT enforcing Content-Type/Content-Length (`TestPresignedPutAcceptsRightContentTypeRejectsWrong`; whether S3 enforces a signed Content-Length for presigned PUT in your region/endpoint), EventBridge-to-SQS delivery (`TestPutEventsReachesQueue`), CORS for browser uploads.

- reservations: real DynamoDB behaviour of the 3-item `TransactWriteItems` under 50-way contention (retry budget of 8 may need tuning); IAM authorisation of transactions through item actions; `ClientRequestToken` behaviour on SDK retries.
- reservations: EventBridge Scheduler: `at()` schedule creation with `ActionAfterCompletion=DELETE`, `ClientToken`, `iam:PassRole` requirement and the exact error when it is missing, retry policy honoured when the Lambda errors, schedule actually invokes the Lambda with `{"holdId": ...}` (`TestScheduleCreatedAndDeleted` covers create/get/delete only).
- hold-expirer: DynamoDB Streams TTL-deletion record shape (`userIdentity.type=Service`, `principalId=dynamodb.amazonaws.com`) was modelled from the AWS documentation, not observed; `BatchItemFailures` identifier is the record `SequenceNumber`; EMF metric line appears as a CloudWatch metric.
- reservations: Lua rate limiter on ElastiCache/Valkey (uses `TIME` inside the script; fine on Redis >= 5 / Valkey, verified only on redis:7 locally).
- reservations: SQS consumer against a real queue fed by EventBridge rules (envelope parsing is unit tested with a representative document).

- orders: RDS password authentication with TLS verify-full using the embedded RDS CA bundle against a real instance; real Secrets Manager rotation (`TestSecretRotationAgainstRealSecretsManager`, the 28P01 detection through pgx's connect error); behaviour when the secret JSON `username` changes on alternating-user rotation.
- orders: poison message really lands in the DLQ after 5 receives with the visibility/heartbeat settings (`TestPoisonMessageReachesDLQAfterMaxReceives`); heartbeat `ChangeMessageVisibility` against real SQS.
- orders: EventBridge `PutEvents` from the relay with the real bus; entry size limits (events are small).
- orders: the `Transition` row lock and `clock_timestamp()` ordering under the production RDS parameter group (verified on postgres:17 only).

- notifications: `AdminGetUser` with the `sub` as `Username` for the real user pool (`TestRealCognitoLookup`); SES v2 `SendEmail` permissions/ARNs for identity and configuration set, sandbox behaviour, `MessageRejected` classification of real rejections; worker end to end (`TestWorkerEndToEndAndDuplicateMessageSendsOneEmail`).
- notifications: presigned GET behaviour when signed with role credentials (effective expiry) and the download/expiry test (`TestPresignedURLDownloadsThenExpires`); S3 `ResponseContentDisposition` override accepted on presigned GET.
- notifications: whether `cognito-idp:AdminGetUser` is throttled at the worker's concurrency (default quota is low); raise `WORKER_CONCURRENCY` carefully.

- reports: RDS IAM authentication for the read-only user from a VPC Lambda (token generation is local; the TLS handshake against the embedded CA bundle is only verified offline), S3 reachability from the VPC, `PutObject` overwrite (`TestS3OverwritesTheSameKey`, `TestReadOnlyUserOverRDSIAMAuth`).
- platform, all binaries: OTLP/HTTP export to a real collector; `otelaws` middleware (deprecated upstream) still produces spans; JWKS fetch from the real Cognito pool (unit tested with a local JWKS server only); `COGNITO_ISSUER` must equal the `iss` claim exactly (no trailing slash).
- platform: Docker images were built and started on the build host only; nothing was run on EKS (read-only root filesystem with `/tmp` writable is assumed; the code does not write to disk except what the AWS SDK may cache under `/tmp` when configured).
- OpenAPI files were hand-written for OpenAPI 3.1 and parse as YAML with unique operationIds, but were not validated by a linter or compared with the real responses by a tool.
- Rate-limit/consumer/relay behaviour at scale (many pods, throttling) is reasoned about in comments but not load-tested.

<!-- END-S7 -->

## 8. Dependencies beyond the approved list

Final answer: **none**. Everything in `go.mod` is on the approved list or is a transitive dependency of it (`golang.org/x/*`, grpc/protobuf from the OTLP exporters, `smithy-go`, `go-retry` from goose, the AWS SDK internal modules). Direct requirements, with the reason:

| Module | Why |
|---|---|
| `aws-sdk-go-v2` + `config`, `feature/rds/auth`, `service/{dynamodb,eventbridge,s3,scheduler,secretsmanager,sesv2,sqs,cognitoidentityprovider}` | AWS access (default credential chain only) |
| `aws-lambda-go` | Lambda runtime and event types (hold-expirer, nightly-report) |
| `jackc/pgx/v5`, `redis/go-redis/v9` | PostgreSQL and Valkey/Redis |
| `golang-jwt/jwt/v5` | Cognito token validation (JWKS cache is hand-written) |
| `google/uuid` | UUIDv7 and deterministic v5 ids |
| `pressly/goose/v3` | embedded migrations |
| `skip2/go-qrcode` | QR matrix for the ticket |
| `go.opentelemetry.io/otel/*`, `contrib/.../otelhttp`, `contrib/.../otelaws` | traces and metrics. `otelaws` is deprecated upstream (staticcheck SA1019, suppressed with a comment in `internal/platform/awscfg`); the supported replacement is the SDK's native smithy tracing |

<!-- END-S8 -->

## 9. Known gaps and suggestions

- OpenAPI validity is not machine-checked (spectral not installed).

Known gaps (deliberate, not forgotten):

- No refund flow: a payment approved after a cancellation is ignored and logged, nothing is refunded.
- No ticket revocation: cancelling a paid order keeps the PNG in S3; a gate service should check the order status as well as the QR signature. There is no gate/scan service.
- Presigned e-mail links are capped by the signing credentials' lifetime (see section 6). CloudFront signed URLs would fix it.
- No admin endpoints to cancel an event or to list orders; no event cancellation flow (status `cancelled` exists but nothing sets it).
- `GET /me/holds` and the notification log use eventually consistent / simple pagination; fine for UI lists.
- Redis rate limiting is per user only (no per-IP), and the circuit breaker is a minimal one in `platform/redisx`.
- The orders worker's payment simulator is random; there is no seed for reproducible runs (inject `Roll`/`Latency` in tests).
- OpenAPI files are not generated from or checked against the code; the handlers are the truth.
- Terraform/Helm/CI/GitOps are the coach's job; the contracts carry the facts (env, IAM, ports, queues, scaling hints).

Suggestions for the coach, in the order that will bite first:

1. Create DynamoDB tables with exactly the keys/GSI/TTL/streams of section 4 (reservations needs **GSI1** and **streams**; the TTL attribute is **`ttl`**).
2. Remember `iam:PassRole` for reservations-api on the Scheduler role (section 3) and the Scheduler group; without them every `POST /holds` returns 503 after rolling the hold back.
3. The internal listener of reservations must have its own ClusterIP Service and a NetworkPolicy; never route it through the Ingress.
4. Run `catalog-api migrate` and `orders-api migrate` as PreSync Jobs with the migrator credentials; run the bootstrap SQL once first.
5. Give the relay 2 replicas and alarm on `outbox_unpublished`; alarm on both DLQs.
6. Start with `EMAIL_RECIPIENT_OVERRIDE` while SES is in the sandbox.
7. First end-to-end check: `scripts/golden-path.sh`.

<!-- END-S9 -->

## 10. How to read the code

Start with `internal/platform/lifecycle`, `httpserver`, `auth`, `problem`, then `sqsconsumer`, `ebpub`, `pgdb`.

1. `README.md` (repo level), then `services/<service>/README.md` for the service you are reading.
2. Platform, in this order: `internal/platform/lifecycle` (graceful shutdown) -> `httpserver` (health, middleware) -> `problem` and `pagination` -> `auth` (JWT/JWKS) -> `config` -> `sqsconsumer` (at-least-once consumer) -> `ebpub` (EventBridge, partial failures) -> `pgdb` (TLS, IAM token, Secrets Manager, rotation) -> `redisx` (IAM token, circuit breaker) -> `telemetry`.
3. `internal/catalog`: domain -> ports -> app (cache-aside, optimistic locking) -> adapters -> `cmd/catalog-api`.
4. `internal/reservations`: domain (inventory math) -> app -> `adapters/dynamodb` (the transactions: the most important file in the repo) -> `scheduler` -> `lambda` -> `cmd/*`.
5. `internal/orders`: domain (state machine, deterministic order id) -> app (`CreateOrder`, then `payments.go`, then `relay.go`) -> `adapters/postgres` -> `reservationsclient` -> `cmd/*`.
6. `internal/notifications`: domain -> app (failure classification) -> adapters -> `cmd/*`; `internal/reports` last.
7. For any service: read `integration/integration_test.go`; each test states a distributed-systems guarantee.

<!-- END-S10 -->

