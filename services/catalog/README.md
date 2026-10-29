# catalog (`catalog-api`)

## Purpose

Owns **venues, events and ticket types** in PostgreSQL (RDS). Anyone can browse published events; admins (Cognito group `admin`) create, edit and publish them. When an event is published the service announces `EventPublished` on EventBridge so `reservations` can create inventory.

## Request flow (read an event)

```
client -> ALB/Ingress (/catalog/...) -> http adapter -> app.GetEvent
                                                         |-- cache.GetEvent (ElastiCache)  hit -> return
                                                         |-- events.Get (RDS Postgres)     miss -> read
                                                         `-- cache.SetEvent (best effort, TTL 5 min)
```

The cache is **cache-aside** and optional: any Redis error is logged, counted (`catalog_cache_errors_total`) and the request continues against Postgres. A small circuit breaker in `platform/redisx` makes a black-holed cache fail instantly instead of adding its timeout to every request.

## Event flow (publish)

1. `POST /catalog/v1/events/{id}/publish` -> one Postgres transaction moves the event `draft -> published` (optimistic version check).
2. After the commit, `EventPublished` is put on the bus (`source=gatepass.catalog`, `detail-type=EventPublished`, `data` = eventId, title, startsAt, venueId, ticketTypes[{id,name,priceCents,currency,capacity}]).
3. If step 2 fails after retries the API answers **503**; the event *is* published. Repeating the publish call re-announces. This is the deliberate "no outbox" trade-off; `orders` shows the outbox version.

## How to read the code

1. `internal/catalog/domain` - entities and rules (publish, validation, capacity). No I/O.
2. `internal/catalog/ports` - the interfaces the application needs.
3. `internal/catalog/app` - use cases: cache-aside, optimistic locking, publish + announce, presigned upload.
4. `internal/catalog/adapters/*` - `postgres` (keyset pagination, optimistic UPDATE), `redis`, `s3` (presigned PUT), `eventbridge`, `http`.
5. `cmd/catalog-api/main.go` - hand-wired composition root and the `migrate` subcommand.
6. `internal/catalog/integration` - tests against real services (`-tags integration`).

## Endpoints (OpenAPI: `api/catalog.openapi.yaml`)

`GET /catalog/v1/events`, `GET /catalog/v1/events/{eventId}`, `POST /catalog/v1/events`, `PATCH /catalog/v1/events/{eventId}`, `POST /catalog/v1/events/{eventId}/publish`, `POST /catalog/v1/events/{eventId}/image-upload-url`, `GET /catalog/v1/venues`, `POST /catalog/v1/venues`. Health at the root: `/livez`, `/readyz`, `/version`.

## Metrics

`catalog_cache_hits_total`, `catalog_cache_misses_total` (label `key_type`), `catalog_cache_errors_total`, `catalog_events_published_total`, `catalog_event_announce_failures_total`, `db_pool_connections_*`, plus the standard HTTP server metrics.

## Configuration

Environment variables only; the process exits at start-up listing **every** missing or invalid one. Full list (generated from `contracts/catalog-api.yaml`):

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

IAM actions:

| Action | Resource | Why |
|---|---|---|
| `rds-db:connect` | `arn:aws:rds-db:<region>:<account-id>:dbuser:<db-resource-id>/catalog_app` | Generate an IAM auth token used as the PostgreSQL password |
| `elasticache:Connect` | `arn:aws:elasticache:<region>:<account-id>:replicationgroup:<CACHE_NAME> (or :serverlesscache:) and arn:aws:elasticache:<region>:<account-id>:user:<REDIS_USER>` | Redis IAM authentication (only when REDIS_IAM_AUTH=true) |
| `s3:PutObject` | `arn:aws:s3:::<IMAGES_BUCKET>/images/events/*` | The role signs presigned PUT URLs; the URL only works if the signer may write that key |
| `events:PutEvents` | `arn:aws:events:<region>:<account-id>:event-bus/<EVENT_BUS_NAME>` | Publish EventPublished |

`catalog-api migrate` needs only `AWS_REGION`, `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER` (the **migration** user), `DB_IAM_AUTH=true`.

## Tests

- `go test ./internal/catalog/...` - unit tests with in-memory fakes of the ports.
- `go test -tags integration ./internal/catalog/integration/` - needs `TEST_DATABASE_URL`, `TEST_REDIS_ADDR`, `CATALOG_IT_RDS_*`, `TEST_IMAGES_BUCKET`, `TEST_EVENT_BUS_NAME`, `TEST_EVENTS_QUEUE_URL` (each test skips and says which it needs).

## How the coach will deploy this

- One Deployment of `catalog-api` (image arg `BINARY=catalog-api`), port 8080, probes `/livez` and `/readyz`, `terminationGracePeriodSeconds: 30`, read-only root filesystem with an `emptyDir` on `/tmp`, runs as non-root.
- A Job (same image, args `migrate`, `DB_USER=catalog_migrator`) runs before each rollout (Argo CD PreSync hook).
- IRSA / Pod Identity role with the IAM actions above; Ingress path `/catalog` -> Service, no prefix stripping.
- Values come from `contracts/catalog-api.yaml`; Terraform outputs fill the `terraform-output` sources.
