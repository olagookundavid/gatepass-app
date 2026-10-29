# gatepass-app

The Go code of **Gatepass**, a small event-ticketing platform built the way a production team would: Go microservices and Lambdas on AWS (EKS, RDS, DynamoDB, ElastiCache, SQS, EventBridge, S3, SES, Cognito). One module, one Dockerfile for every service, Kubernetes-agnostic: how it is deployed lives in other repositories and is derived from the **deploy contracts** in `contracts/`.

This repository is public. It contains no secrets, account ids or real host names; examples use placeholders such as `<account-id>` and `example.com`.

## What is in here

| Binary (`cmd/`) | Service | What it does |
|---|---|---|
| `catalog-api` (+ `migrate`) | [catalog](services/catalog/README.md) | venues, events, ticket types (PostgreSQL), cache-aside on ElastiCache, presigned S3 uploads, announces `EventPublished` |
| `reservations-api` | [reservations](services/reservations/README.md) | inventory and 10-minute holds (DynamoDB), rate limit (Redis), expiry through EventBridge Scheduler, inventory SQS consumer |
| `lambda-hold-expirer` | reservations | Lambda that returns the tickets of an expired hold (Scheduler + DynamoDB Streams safety net) |
| `orders-api` (+ `migrate`) | [orders](services/orders/README.md) | orders from holds, idempotent, transactional outbox |
| `orders-worker` | orders | simulated payments from an SQS queue, retries, DLQ |
| `orders-outbox-relay` | orders | publishes the outbox to EventBridge (safe with 2+ replicas), reconciles lost payment requests |
| `notifications-api` | [notifications](services/notifications/README.md) | notification log, preferences, ticket download links |
| `notifications-worker` | notifications | renders a PNG ticket with a signed QR, stores it in S3, sends mail with SES |
| `lambda-nightly-report` | [reports](services/reports/README.md) | daily sales CSV to S3 (RDS IAM authentication, read-only user) |

```
cmd/<binary>/main.go     composition roots: wire adapters to use cases by hand
internal/platform/       shared code: config, logging, telemetry, auth (Cognito JWT), http server and
                         health, problem+json, pagination, idempotency, SQS consumer loop, EventBridge
                         publisher, pgx + IAM/Secrets Manager credentials, Redis + IAM auth
internal/<service>/      hexagonal layout: domain (pure rules), ports (interfaces), app (use cases),
                         adapters/* (Postgres, DynamoDB, Redis, SQS, S3, SES, ...)
migrations/{catalog,orders}  embedded goose SQL, applied only by `<service>-api migrate`
api/*.openapi.yaml       OpenAPI 3.1, one file per service
contracts/*.yaml         one deploy contract per binary: env vars, IAM actions, ports, queues, scaling hints
build/Dockerfile         one multi-stage Dockerfile (ARG BINARY), linux/amd64 and linux/arm64, distroless
scripts/golden-path.sh   curl + jq walkthrough of the whole flow against a deployed environment
HANDOFF.md               status, env tables, IAM, AWS resources to create, UNVERIFIED items, assumptions
```

## How the services talk

```
                          +-----------+   EventPublished    +--------------+
 admin -> catalog-api --> | EventBridge| -----------------> | SQS inventory| -> reservations (consumer)
                          |   bus      |                     +--------------+
 user -> reservations-api --(hold + Scheduler one-time schedule)--> lambda-hold-expirer
 user -> orders-api --consume hold (internal HTTP)--> reservations-api
          |  one transaction: order + outbox            payments SQS -> orders-worker -> order paid/failed
          +--> orders-outbox-relay --OrderPaid / PaymentFailed / OrderCancelled--> EventBridge
                  -> notifications queue -> notifications-worker -> S3 ticket + SES mail
                  -> inventory queue (release tickets on PaymentFailed / OrderCancelled)
```

Delivery is at-least-once everywhere; every consumer is idempotent (dedupe on `eventId`, conditional writes, unique keys).

## Build, test, lint

```
make build               # all binaries into ./bin
make test                # unit tests, no AWS needed
make lint                # go vet (with and without -tags integration) + golangci-lint
make test-integration    # needs real services; each test skips and says which env vars it needs
make image BINARY=orders-api
make lambda-zip          # dist/lambda-*.zip (arm64, provided.al2023, file "bootstrap")
make tidy
```

Unit tests use in-memory fakes of the application's own ports; the AWS SDK is never mocked. Integration tests (`//go:build integration`) are written for every AWS adapter and read resource names from environment variables (listed at the top of each `integration_test.go` and in each service README). Several of them also run against any compatible endpoint (a PostgreSQL DSN in `TEST_DATABASE_URL`, `TEST_REDIS_ADDR`, `AWS_ENDPOINT_URL_DYNAMODB`), which is how the SQL, the DynamoDB transactions and the cache logic were verified without an AWS account.

## Conventions that apply to every service

- Public routes live under `/<service>/v1/...` (`BASE_PATH`); `/livez`, `/readyz`, `/version` are served at the root. The internal listener (reservations only) is never exposed through the Ingress.
- Cognito access tokens (never ID tokens); admin = group `admin`; no e-mail address is ever read from a token, stored in an event or logged.
- Errors are RFC 9457 `application/problem+json` with stable `code`s.
- Configuration is environment variables only, validated at start-up with one message listing every problem.
- Structured JSON logs with `request_id`, `trace_id`, `user_id`; OpenTelemetry traces and metrics (no-op without `OTEL_EXPORTER_OTLP_ENDPOINT`).
- Graceful shutdown on SIGTERM: not ready, wait `SHUTDOWN_DELAY`, drain in-flight work (25 s), flush telemetry, close pools.
- Database migrations are expand/contract only and run by a Job, never at API start-up.

## Reading order for learners

`internal/platform/lifecycle`, `httpserver`, `auth`, `problem` -> `internal/catalog` (a conventional hexagon with a cache) -> `internal/reservations` (DynamoDB transactions, idempotency, Scheduler) -> `internal/orders` (transactional outbox, SQS worker) -> `internal/notifications` (failure classification, S3, SES). Each service README ends with a "how to read the code" list, and `HANDOFF.md` section 10 has the full order.
