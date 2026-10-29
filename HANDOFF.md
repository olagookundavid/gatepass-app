# HANDOFF: state of the Gatepass Go code

Written incrementally while building; read this first. Placeholders (`<account-id>`, `example.com`) are used on purpose: this repository is public.

## 1. Status

| Binary | Status | Notes |
|---|---|---|
| `internal/platform/*` | done | unit tested |
| `build/Dockerfile` | done | amd64 and arm64 build verified for the skeleton `catalog-api` |
| `catalog-api` (+ `migrate`) | done | unit + local-container integration verified, see section 1 commands |
| `reservations-api` | done | public + internal HTTP, inventory SQS consumer (`CONSUMER_ENABLED`), unit tests, integration tests run against DynamoDB Local + Redis |
| `lambda-hold-expirer` | done | both payload shapes; handler unit-tested and exercised against DynamoDB Local |
| `orders-api` (+ `migrate`) | done | unit tests; integration against local Postgres |
| `orders-worker` | done | unit tests; poison/DLQ integration test written, needs real SQS |
| `orders-outbox-relay` | done | two-relay exactly-once verified against local Postgres |
| `notifications-api` | done | unit tests; store integration verified on DynamoDB Local |
| `notifications-worker` | done | unit tests; end-to-end test written, needs real SQS/S3/SES |
| `lambda-nightly-report` | missing | |

### Commands run (latest result)

Checks run after every step: `go build ./...`, `go vet ./...`, `go vet -tags integration ./...`, `golangci-lint run`, `go test ./...`.

- After platform: all five pass (0 lint issues). `docker build -f build/Dockerfile --build-arg BINARY=catalog-api --platform linux/amd64` and `linux/arm64` succeed; the arm64 image ran with `--read-only` and answered `/livez` and `/version`.

- After catalog: `go build ./...`, `go vet ./...`, `go vet -tags integration ./...`, `golangci-lint run`, `go test ./...` all pass. Catalog integration tests `TestPostgresRepositories` and `TestCacheAsideAgainstRealRedisAndOutageFallback` were run against **throwaway local containers** (postgres:17 and redis:7, started with `docker run`, nothing committed) and pass; this verified the SQL (keyset pagination, optimistic locking, migrations, bootstrap grants) and cache-aside/outage fallback. The AWS-dependent catalog integration tests (RDS IAM auth, S3 presign, PutEvents to SQS) compile but were not run.

- After reservations (+ hold-expirer): `go build ./...`, `go vet ./...`, `go vet -tags integration ./...`, `golangci-lint run`, `go test ./...` all pass. `go test -tags integration ./internal/reservations/integration/` was run against **DynamoDB Local** (`amazon/dynamodb-local` container, `AWS_ENDPOINT_URL_DYNAMODB`, dummy credentials) and a local redis:7: 50 concurrent holds vs 10 tickets -> exactly 10, idempotent create/consume, expiry released once through both Lambda shapes, consumer idempotency, GSI pagination, Lua rate limiter all PASS. Only `TestScheduleCreatedAndDeleted` was skipped (needs real EventBridge Scheduler). DynamoDB Local does not reproduce real-DynamoDB contention (`TransactionConflict`); that path is covered by a unit test with a scripted client.

- After orders (3 binaries): all five checks pass, `docker build --build-arg BINARY=orders-api --platform linux/amd64` succeeds. `go test -tags integration ./internal/orders/integration/` was run (3 times) against a **local postgres:17 container**: idempotency (replay does not re-consume, different body -> 409, 20 concurrent duplicates -> one order), rollback leaves no rows, two relays publish 300 rows exactly once each (`FOR UPDATE SKIP LOCKED`), cancel-vs-payment races end in a legal state with matching history/outbox, the payment reconciler, and secret rotation (reconnect after one auth failure, using an in-memory fake of the Secrets Manager port) all PASS. Skipped (need AWS): `TestPoisonMessageReachesDLQAfterMaxReceives`, `TestSecretRotationAgainstRealSecretsManager`. The orders bootstrap SQL below was verified locally (app can DML but not DDL, report user is read-only).

- After notifications (2 binaries): all five checks pass; `docker build --build-arg BINARY=notifications-worker --platform linux/amd64` succeeds. `TestStoreNotificationLogPreferencesTicketsAndDedupe` was run against **DynamoDB Local** and passes (log pagination newest-first without empty pages, preferences, owner-only ticket records, dedupe marker). Skipped (need AWS): presigned URL download/expiry, worker end-to-end with SES simulator, real Cognito lookup. The ticket PNG was rendered and inspected by eye.

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

- **reservations-api** (`*` required; the second group is required only when `API_ENABLED=true`): `TABLE_NAME*, API_ENABLED, CONSUMER_ENABLED, INVENTORY_QUEUE_URL (if CONSUMER_ENABLED), CONSUMER_WORKERS, CONSUMER_VISIBILITY_TIMEOUT, INTERNAL_HTTP_ADDR, HOLD_TTL, RATE_LIMIT_PER_MINUTE`; API mode: `COGNITO_ISSUER, COGNITO_CLIENT_ID, SCHEDULER_GROUP_NAME, SCHEDULER_ROLE_ARN, EXPIRER_LAMBDA_ARN, REDIS_ADDR, REDIS_USER, REDIS_IAM_AUTH, CACHE_NAME, REDIS_SERVERLESS, REDIS_AUTH_FILE`.
- **lambda-hold-expirer**: `TABLE_NAME*`, `AWS_REGION` (runtime), `LOG_LEVEL`.

- **orders-api** (`migrate` uses `AWS_REGION, DB_HOST, DB_PORT, DB_NAME, DB_SECRET_ID` with the migrator's secret): `DB_HOST*, DB_PORT, DB_NAME*, DB_SECRET_ID*, DB_MAX_CONNS, DB_STATEMENT_TIMEOUT, DB_SSL_ROOT_CERT, RESERVATIONS_INTERNAL_URL*, PAYMENTS_QUEUE_URL*, COGNITO_ISSUER*, COGNITO_CLIENT_ID*`.
- **orders-worker**: DB vars as above, `PAYMENTS_QUEUE_URL*, WORKER_CONCURRENCY, CONSUMER_VISIBILITY_TIMEOUT, PAYMENT_SUCCESS_RATE`.
- **orders-outbox-relay**: DB vars as above, `EVENT_BUS_NAME*, PAYMENTS_QUEUE_URL*, OUTBOX_BATCH_SIZE, OUTBOX_POLL_INTERVAL, PAYMENT_RECONCILE_AFTER, OUTBOX_RETENTION`.

- **notifications-api**: `TABLE_NAME*, TICKETS_BUCKET*, COGNITO_ISSUER*, COGNITO_CLIENT_ID*`.
- **notifications-worker**: `TABLE_NAME*, TICKETS_BUCKET*, NOTIFICATIONS_QUEUE_URL*, COGNITO_USER_POOL_ID*, SES_FROM_ADDRESS*, TICKET_SIGNING_KEY_FILE*` (a mounted secret file), `SES_CONFIGURATION_SET, EMAIL_RECIPIENT_OVERRIDE, TICKET_LINK_TTL, WORKER_CONCURRENCY, CONSUMER_VISIBILITY_TIMEOUT`.

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

### nightly report Lambda (see its section when present)

`rds-db:connect` on `.../dbuser:<db-resource-id>/orders_report`, `s3:PutObject` on `reports/*`.

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

<!-- END-S7 -->

## 8. Dependencies beyond the approved list

- None so far. Transitive: `golang.org/x/*`, grpc/protobuf (pulled in by the OTLP exporters). `otelaws` is approved but upstream marks it deprecated (kept, `//nolint:staticcheck` in `internal/platform/awscfg`).

- `github.com/aws/aws-lambda-go` (events + runtime): within "lambda events" in the approved list. `service/scheduler`, `service/dynamodb` are on the list. No DynamoDB `attributevalue` helper is used (items are marshalled by hand).

- `github.com/skip2/go-qrcode`: approved list (QR in the ticket PNG). `service/sesv2`, `service/cognitoidentityprovider`, `service/s3` are on the list. No unapproved module was added. (golang.org/x/image was considered for text and rejected in favour of a tiny built-in font.)

<!-- END-S8 -->

## 9. Known gaps and suggestions

- OpenAPI validity is not machine-checked (spectral not installed).

<!-- END-S9 -->

## 10. How to read the code

Start with `internal/platform/lifecycle`, `httpserver`, `auth`, `problem`, then `sqsconsumer`, `ebpub`, `pgdb`.

<!-- END-S10 -->

