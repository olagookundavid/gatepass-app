# notifications (`notifications-api`, `notifications-worker`)

## Purpose

Turns order events into **tickets and e-mails**, and keeps each user's notification log and preferences in DynamoDB. Ticket files live in a private S3 bucket and are handed out only as presigned URLs.

| Binary | Role |
|---|---|
| `notifications-worker` | SQS consumer: `OrderPaid` -> render PNG with signed QR -> S3 -> look up e-mail in Cognito -> SES; `PaymentFailed` / `OrderCancelled` -> short e-mails |
| `notifications-api` | `GET /me/notifications`, `GET`/`PUT /me/preferences`, `GET /orders/{orderId}/ticket-url` |

## Event flow (OrderPaid)

```
EventBridge rule -> SQS notifications queue -> worker
  1. dedupe on eventId (marker item)                          duplicate => drop silently
  2. render PNG (QR = GP1.<orderId>.<HMAC>) -> S3 tickets/<userId>/<orderId>.png
  3. ticket record in DynamoDB (owner's partition)
  4. preferences: e-mail disabled => log "skipped", stop
  5. Cognito AdminGetUser(userId) -> address (kept in memory only)
  6. presigned GET (24 h requested) + SES v2 SendEmail
  7. notification log item (sent) + processed-event marker
```

Failure handling is deliberate (see `internal/notifications/app/app.go`): SES throttling and other temporary errors return an error so SQS redelivers (then DLQ); a rejected address or a user without an address is recorded as `failed` and acknowledged. A crash between sending and writing the marker can cause a second e-mail: accepted, because losing a ticket mail is worse (SES has no idempotency key).

## Privacy

E-mail addresses are never put in events, logs, the notification table or errors. Access tokens do not contain an e-mail either, which is why the worker asks Cognito at send time. The notification log stores ids, type, status and a short reason code only.

## QR token

`GP1.<orderId>.<base64url(HMAC-SHA256(key, "GP1." + orderId))>`, key from `TICKET_SIGNING_KEY_FILE`. A gate scanner holding the key verifies it offline (`domain.VerifyToken`, constant-time compare). The PNG is drawn with the standard library plus `go-qrcode` and a tiny built-in pixel font.

## Known limits

- A presigned URL dies when the credentials that signed it expire. With IRSA/Pod Identity (session credentials, about 1 hour) the "24 h" e-mail link may stop earlier. `GET /orders/{orderId}/ticket-url` always signs a fresh 5-minute URL, so users are never stuck. For durable links use CloudFront signed URLs (not built here).
- SES sandbox: only verified recipients; use `EMAIL_RECIPIENT_OVERRIDE`.

## How to read the code

1. `internal/notifications/domain` - notification model, token signing, e-mail text (pure).
2. `internal/notifications/app` - `Worker.HandleEvent` (failure classification) and `API`.
3. `adapters/dynamodb` (single partition per user), `s3`, `ses` (error classification), `cognito`, `ticketpng`, `sqs`, `http`.
4. `cmd/notifications-worker`, `cmd/notifications-api` - wiring.
5. `internal/notifications/integration` - tests against real DynamoDB, S3, SQS, SES simulator and Cognito.

## Metrics

`notifications_sent_total{type,channel}`, `notifications_failed_total{type,reason}`, `ticket_render_duration_seconds`, `ses_throttled_total`, plus SQS and HTTP metrics.

## Configuration

Full list (generated from the contracts):

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

IAM actions:

| Action | Resource | Why |
|---|---|---|
| `dynamodb:GetItem` | `arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME>` | Preferences, ticket records (consistent reads); readiness probe |
| `dynamodb:PutItem` | `arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME>` | Save preferences |
| `dynamodb:Query` | `arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME>` | List the caller's notifications |
| `s3:GetObject` | `arn:aws:s3:::<TICKETS_BUCKET>/tickets/*` | The role signs presigned GET URLs; a URL signed by a role without GetObject returns 403 |

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

IAM actions:

| Action | Resource | Why |
|---|---|---|
| `sqs:ReceiveMessage` | `arn:aws:sqs:<region>:<account-id>:notifications` | Long-poll the queue |
| `sqs:DeleteMessage` | `arn:aws:sqs:<region>:<account-id>:notifications` | Acknowledge after processing |
| `sqs:ChangeMessageVisibility` | `arn:aws:sqs:<region>:<account-id>:notifications` | Heartbeat extension |
| `dynamodb:GetItem` | `arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME>` | Preferences, processed-event marker |
| `dynamodb:PutItem` | `arn:aws:dynamodb:<region>:<account-id>:table/<TABLE_NAME>` | Notification log, ticket record, processed-event marker |
| `s3:PutObject` | `arn:aws:s3:::<TICKETS_BUCKET>/tickets/*` | Store the rendered ticket |
| `s3:GetObject` | `arn:aws:s3:::<TICKETS_BUCKET>/tickets/*` | Needed so the presigned GET link it creates works |
| `cognito-idp:AdminGetUser` | `arn:aws:cognito-idp:<region>:<account-id>:userpool/<COGNITO_USER_POOL_ID>` | Look up the buyer's e-mail address at send time |
| `ses:SendEmail` | `arn:aws:ses:<region>:<account-id>:identity/<SES_FROM_ADDRESS or its domain> and arn:aws:ses:<region>:<account-id>:configuration-set/<SES_CONFIGURATION_SET>` | Send the e-mail (SES v2 SendEmail) |

## Tests

- `go test ./internal/notifications/... ./cmd/notifications-*/` - unit tests with in-memory fakes of the ports.
- `go test -tags integration ./internal/notifications/integration/` - `NOTIF_IT_DYNAMODB=true` (store; creates a temporary table), `TEST_TICKETS_BUCKET` (presigned URL download then expiry), `TEST_NOTIFICATIONS_QUEUE_URL` + `SES_FROM_ADDRESS` (worker end to end to the SES mailbox simulator, duplicate message gives one notification), `TEST_COGNITO_USER_POOL_ID` + `TEST_COGNITO_USER_ID` (real lookup).

## How the coach will deploy this

- `notifications-api`: Deployment behind the Ingress path `/notifications`.
- `notifications-worker`: Deployment scaled by KEDA on the queue; a Secret with the ticket signing key mounted as a file; IRSA role with the IAM actions of the contract.
- Queue `notifications` + DLQ (`maxReceiveCount` 5, visibility timeout >= 60 s) fed by three EventBridge rules (HANDOFF section 5); SES identity, optional configuration set; a private tickets bucket with a lifecycle rule (for example expire objects after 400 days).
