# reservations (`reservations-api`, `lambda-hold-expirer`)

## Purpose

Owns **inventory counters and ticket holds** in one DynamoDB table. A hold takes N tickets for 10 minutes; orders later *consume* the hold; unused holds expire and give the tickets back. The service never oversells and never leaks tickets, even with concurrent buyers, retries and duplicate events.

## Request flow (place a hold)

```
POST /reservations/v1/holds  (Idempotency-Key)
  1. rate limit per user (Redis, sliding window; Redis down => allow)
  2. idempotency lookup (DynamoDB)            same key+body => original hold; other body => 409
  3. read inventory                            404 unknown ticket type, 409 sold_out (fast path)
  4. TransactWriteItems (ONE atomic step):
        a. inventory.available -= q   IF available >= q      <- the oversell guard
        b. put hold                   IF not exists
        c. put idempotency record     IF not exists
  5. EventBridge Scheduler: create one-time schedule hold-<id> at(expiresAt) -> hold-expirer Lambda
     (if this fails the hold is rolled back and the API answers 503)
```

Consume (called by orders on the internal listener) is a single conditional `UpdateItem` (`active -> consumed` if unexpired and owned by the caller); the same `orderId` again succeeds, another `orderId` gets 409. The response carries the hold **and the price snapshot** (`unitPriceCents`, `currency`) so orders never call catalog.

## Expiry (why a schedule and not DynamoDB TTL)

DynamoDB TTL deletes items "typically within a few days", so it cannot release a hold after 10 minutes. Each hold gets an EventBridge Scheduler one-time schedule that invokes `lambda-hold-expirer` with `{"holdId":"..."}`. TTL plus DynamoDB Streams stays as a safety net: the same Lambda handles stream records deleted by the TTL service and releases only holds that were still `active`. Readers also treat an active hold past `expiresAt` as `expired` even before the Lambda has run.

All release paths (user DELETE, Scheduler, TTL stream, `OrderCancelled`, `PaymentFailed`) are idempotent through conditional writes: only the transaction that flips the hold out of its previous status (or writes the `REL#` marker) can add tickets back, and `available` can never exceed `capacity`.

## Event flow

| Event (from bus) | Action |
|---|---|
| `EventPublished` (catalog) | create inventory per ticket type **only if absent**, storing the price snapshot |
| `PaymentFailed`, `OrderCancelled` (orders) | release the consumed hold's tickets once |

Queue messages are deduplicated by `eventId` (marker item, 7 day TTL) and the operations are idempotent anyway (a re-announced event has a new `eventId`).

## DynamoDB layout (single table)

| Entity | PK | SK | Notes |
|---|---|---|---|
| inventory | `EVENT#<eventId>` | `TT#<ticketTypeId>` | capacity, available, priceCents, currency, version |
| hold | `HOLD#<holdId>` | `HOLD` | userId, status, quantity, expiresAt, ttl, GSI1PK=`USER#<userId>`, GSI1SK=`<createdAt>#<holdId>` |
| idempotency | `IDEMP#<userId>#<key>` | `IDEMP` | requestHash, hold snapshot, ttl 24 h |
| processed event | `EVT#<eventId>` | `EVT` | ttl 7 days |
| release marker | `REL#<holdId>` | `REL` | ttl 7 days |

TTL attribute: `ttl`. (The plan said `expiresAt`; a separate attribute is used so consumed holds are not deleted 10 minutes after creation. See HANDOFF.)

## How to read the code

1. `internal/reservations/domain` - hold lifecycle and inventory arithmetic (pure).
2. `internal/reservations/ports` - what the use cases need.
3. `internal/reservations/app` - `CreateHold`, `Consume`, `ExpireHold`, `HandleEvent`.
4. `internal/reservations/adapters/dynamodb` - the transactions and their conditions (read the comments).
5. `adapters/scheduler`, `adapters/redis` (Lua rate limiter), `adapters/sqs`, `adapters/http`, `adapters/lambda`.
6. `cmd/reservations-api`, `cmd/lambda-hold-expirer` - wiring.
7. `internal/reservations/integration` - tests that need DynamoDB, Redis and Scheduler.

## Metrics

`reservations_holds_created_total`, `reservations_holds_rejected_total{reason=sold_out|rate_limited|invalid}`, `reservations_holds_consumed_total`, `reservations_holds_expired_released_total`, `reservations_inventory_events_processed_total`, `reservations_rate_limiter_errors_total`. The Lambda emits `holds_expired_released_total` as a CloudWatch EMF log line.

## IAM that is easy to miss

Creating a schedule also needs **`iam:PassRole`** on `SCHEDULER_ROLE_ARN` (condition `iam:PassedToService = scheduler.amazonaws.com`) for the reservations-api role. The Scheduler role must trust `scheduler.amazonaws.com` and allow `lambda:InvokeFunction` on `EXPIRER_LAMBDA_ARN`. The Scheduler group named in `SCHEDULER_GROUP_NAME` must exist.

## Configuration

Full list (generated from the contracts):

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

IAM actions:

| Action | Resource | Why |
|---|---|---|
| `dynamodb:GetItem` | `arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME>` | Read inventory, holds, idempotency records, processed-event markers (consistent reads) |
| `dynamodb:Query` | `arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME> and .../index/GSI1` | List inventory of an event; list a user's holds via GSI1 |
| `dynamodb:PutItem` | `arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME>` | Create inventory (if absent), processed-event markers, and the hold/idempotency/release-marker puts inside transactions |
| `dynamodb:UpdateItem` | `arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME>` | Decrement/increment inventory, consume/release holds (single and inside transactions). Transactions are authorised through these item actions (UNVERIFIED) |
| `scheduler:CreateSchedule` | `arn:aws:scheduler:<region>:<account-id>:schedule/<SCHEDULER_GROUP_NAME>/hold-*` | One-time expiry schedule per hold |
| `scheduler:DeleteSchedule` | `arn:aws:scheduler:<region>:<account-id>:schedule/<SCHEDULER_GROUP_NAME>/hold-*` | Remove the schedule when a hold is consumed or deleted |
| `iam:PassRole` | `<SCHEDULER_ROLE_ARN> with condition iam:PassedToService=scheduler.amazonaws.com` | CreateSchedule hands the invoke role to EventBridge Scheduler; without PassRole it fails with AccessDenied |
| `sqs:ReceiveMessage` | `arn:aws:sqs:<region>:<account-id>:<inventory-queue>` | Consumer long polling (CONSUMER_ENABLED=true) |
| `sqs:DeleteMessage` | `arn:aws:sqs:<region>:<account-id>:<inventory-queue>` | Acknowledge after successful handling |
| `sqs:ChangeMessageVisibility` | `arn:aws:sqs:<region>:<account-id>:<inventory-queue>` | Heartbeat extension while a handler runs |
| `elasticache:Connect` | `cache and user ARNs` | Redis IAM auth, only when REDIS_IAM_AUTH=true |

#### `lambda-hold-expirer`

| Variable | Required | Secret | Example | Description |
|---|---|---|---|---|
| `TABLE_NAME` | yes | no | `<reservations-table>` | Same DynamoDB table as reservations-api |
| `AWS_REGION` | yes | no | `us-east-1` | Set by the Lambda runtime |
| `LOG_LEVEL` | no | no | `info` | debug/info/warn/error |
| `AWS_MAX_ATTEMPTS` | no | no | `3` | SDK retry attempts |

IAM actions:

| Action | Resource | Why |
|---|---|---|
| `dynamodb:GetItem` | `arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME>` | Load hold and inventory |
| `dynamodb:UpdateItem` | `arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME>` | Release the hold and return tickets (transaction) |
| `dynamodb:PutItem` | `arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME>` | Release marker for holds already deleted by TTL (transaction) |
| `dynamodb:DescribeStream` | `<table stream ARN>` | Stream event source mapping |
| `dynamodb:GetRecords` | `<table stream ARN>` | Stream event source mapping |
| `dynamodb:GetShardIterator` | `<table stream ARN>` | Stream event source mapping |
| `dynamodb:ListStreams` | `*` | Stream event source mapping |
| `logs:CreateLogGroup / logs:CreateLogStream / logs:PutLogEvents` | `the function's log group` | Logs and the EMF metric line |

## Tests

- `go test ./internal/reservations/... ./cmd/reservations-api/` - unit tests with in-memory fakes of the ports.
- `go test -tags integration ./internal/reservations/integration/` - needs `RES_IT_DYNAMODB=true` + `AWS_REGION` (creates and deletes a temporary table), `TEST_REDIS_ADDR`, and for the schedule test `RES_IT_SCHEDULER_GROUP`, `RES_IT_SCHEDULER_ROLE_ARN`, `RES_IT_TARGET_LAMBDA_ARN`.

## How the coach will deploy this

- `reservations-api`: Deployment(s) from `contracts/reservations-api.yaml`. Public port 8080 behind the Ingress path `/reservations`; **internal port 8081 through a separate ClusterIP Service that the Ingress never references**, with a NetworkPolicy allowing only the orders pods. Optionally a second Deployment with `API_ENABLED=false CONSUMER_ENABLED=true` scaled by KEDA on the inventory queue.
- `lambda-hold-expirer`: `make lambda-zip` -> `dist/lambda-hold-expirer.zip`; Terraform creates the function, the Scheduler group + invoke role, the DynamoDB stream event source mapping (`ReportBatchItemFailures`) and the table with GSI1, TTL on `ttl`, streams enabled.
- EventBridge rules to the inventory queue: see HANDOFF section 5.
