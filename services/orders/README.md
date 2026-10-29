# orders (`orders-api`, `orders-worker`, `orders-outbox-relay`)

## Purpose

Turns a ticket hold into an **order**, takes a (simulated) payment asynchronously, and announces every state change as a domain event. Data lives in PostgreSQL (RDS); credentials come from Secrets Manager.

| Binary | Role |
|---|---|
| `orders-api` | public REST API; `orders-api migrate` applies the schema (Kubernetes Job) |
| `orders-worker` | consumes the payments queue, simulates the provider, moves orders to `paid` / `payment_failed` |
| `orders-outbox-relay` | publishes outbox rows to EventBridge; also re-queues payments lost after a commit |

## Request flow (create an order)

```
POST /orders/v1/orders  {holdId}  + Idempotency-Key
 1. replay check on (user, key)               same body => stored response, other body => 409
 2. reservations: POST /internal/v1/holds/{id}/consume  for a DETERMINISTIC order id
    (returns quantity + price snapshot; 5xx/timeouts retried, 4xx never)
 3. ONE Postgres transaction:  order + status history + outbox(OrderPlaced) + idempotency record
 4. after commit: SQS SendMessage {orderId, amountCents, attempt:1}
```

Why the order id is derived from (user, key, hold): if the process dies between steps 2 and 3, the client's retry produces the *same* order id, reservations treats the consume as a repeat, and the order is created. A random id would strand the hold ("already consumed by another order").

Why step 4 can be lost and is still safe: the order keeps `payment_enqueued_at = NULL`; `orders-outbox-relay` queues the payment of any pending order older than 30 s (the *reconciler*). The worker ignores duplicates because it only acts on `pending_payment` orders.

## Event flow (transactional outbox)

Every state change writes order + history + `outbox` row in one transaction. The relay does `SELECT ... FOR UPDATE SKIP LOCKED`, `PutEvents`, then sets `published_at`. Delivery is at-least-once; the outbox row id is the event's `eventId`, so consumers dedupe.

| Event | When | Consumers |
|---|---|---|
| `OrderPlaced` | order created | none (audit) |
| `OrderPaid` | payment approved | notifications |
| `PaymentFailed` | payment declined (`reason`) | notifications, reservations (release) |
| `OrderCancelled` | user cancelled (`reason`) | notifications, reservations (release) |

## Payments and the DLQ

Worker outcomes (`PAYMENT_SUCCESS_RATE`, default 0.8): approved, permanent decline (`payment_failed`, message acknowledged), transient error (handler returns an error: SQS redelivers after the visibility timeout; after 5 receives the message moves to the DLQ). A message containing `"poison": true` fails every time, which is how you exercise the DLQ: `aws sqs send-message --queue-url <payments-queue-url> --message-body '{"orderId":"x","poison":true}'`.

## Concurrency decisions worth reading

- `Transition` locks the order row (`FOR UPDATE`), so a cancel and a payment cannot overwrite each other; a payment arriving for a cancelled order is ignored (a real system would refund).
- Timestamps of transitions come from the database clock *after* the lock is acquired, so history and outbox ordering follow commit order.
- Cursor pagination is keyset-based: `(created_at, id)`.

## How to read the code

1. `internal/orders/domain` - state machine, deterministic order id, payment simulation.
2. `internal/orders/app` - `CreateOrder`, `CancelOrder` (app.go), `PaymentWorker` (payments.go), `Relay` + reconciler (relay.go).
3. `internal/orders/adapters/postgres` - the transactions; `reservationsclient` - resilient HTTP client; `sqs`, `eventbridge`, `http`.
4. `cmd/orders-api`, `cmd/orders-worker`, `cmd/orders-outbox-relay` - wiring.
5. `migrations/orders/0001_init.sql` - schema (expand/contract rule: add only).
6. `internal/orders/integration` - tests against real Postgres / SQS / Secrets Manager.

## Metrics

`orders_created_total`, `orders_paid_total`, `payments_failed_total{reason}`, `payment_processing_duration_seconds`, `outbox_unpublished` (gauge), `outbox_publish_latency_seconds`, `outbox_published_total`, `outbox_publish_failures_total`, `orders_payment_enqueue_failures_total`, `db_pool_connections_*`.

## Configuration

Full list (generated from the contracts):

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

IAM actions:

| Action | Resource | Why |
|---|---|---|
| `secretsmanager:GetSecretValue` | `arn:aws:secretsmanager:<region>:<account-id>:secret:<DB_SECRET_ID>-*` | Read the database credentials; re-read after a rotation (auth failure) |
| `kms:Decrypt` | `the secret's KMS key (only if it is a customer-managed key)` | Decrypt the secret value |
| `sqs:SendMessage` | `arn:aws:sqs:<region>:<account-id>:orders-payments` | Queue the payment request after an order is committed |

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

IAM actions:

| Action | Resource | Why |
|---|---|---|
| `secretsmanager:GetSecretValue` | `arn:aws:secretsmanager:<region>:<account-id>:secret:<DB_SECRET_ID>-*` | Read the database credentials; re-read after a rotation (auth failure) |
| `kms:Decrypt` | `the secret's KMS key (only if it is a customer-managed key)` | Decrypt the secret value |
| `sqs:ReceiveMessage` | `arn:aws:sqs:<region>:<account-id>:orders-payments` | Long-poll the payments queue |
| `sqs:DeleteMessage` | `arn:aws:sqs:<region>:<account-id>:orders-payments` | Acknowledge after the payment was processed |
| `sqs:ChangeMessageVisibility` | `arn:aws:sqs:<region>:<account-id>:orders-payments` | Heartbeat extension while a payment is processed |

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

IAM actions:

| Action | Resource | Why |
|---|---|---|
| `secretsmanager:GetSecretValue` | `arn:aws:secretsmanager:<region>:<account-id>:secret:<DB_SECRET_ID>-*` | Read the database credentials; re-read after a rotation (auth failure) |
| `kms:Decrypt` | `the secret's KMS key (only if it is a customer-managed key)` | Decrypt the secret value |
| `events:PutEvents` | `arn:aws:events:<region>:<account-id>:event-bus/<EVENT_BUS_NAME>` | Publish OrderPlaced / OrderPaid / PaymentFailed / OrderCancelled |
| `sqs:SendMessage` | `arn:aws:sqs:<region>:<account-id>:orders-payments` | Payment reconciler |

## Tests

- `go test ./internal/orders/... ./cmd/orders-api/ ./cmd/orders-worker/` - unit tests with in-memory fakes of the ports.
- `go test -tags integration ./internal/orders/integration/` - `TEST_DATABASE_URL` (any password-auth PostgreSQL) covers idempotency, rollback, two relays, cancel-vs-payment race, reconciler, secret rotation (with a fake secret source). `TEST_PAYMENTS_QUEUE_URL` + `TEST_PAYMENTS_DLQ_URL` cover the poison message. `ORDERS_IT_SECRETS=true` runs the rotation test against real Secrets Manager.

## How the coach will deploy this

- `orders-api`: Deployment behind the Ingress path `/orders`; a PreSync **Job** running `orders-api migrate` with the migrator secret; IRSA role with the IAM actions of the contract.
- `orders-worker`: Deployment scaled by KEDA on the payments queue; `orders-outbox-relay`: Deployment with **2 replicas** (safe by design).
- Queue `orders-payments` with a DLQ (`maxReceiveCount` 5, visibility timeout >= 60 s), an alarm on the DLQ and on `outbox_unpublished`.
- Network: orders pods may reach the reservations **internal** Service on 8081 (NetworkPolicy); `RESERVATIONS_INTERNAL_URL` points there.
