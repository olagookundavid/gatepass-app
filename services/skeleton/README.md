# skeleton (`skeleton-api`)

## Purpose

The **walking skeleton**: a tiny binary with no AWS client, no database and no required configuration. It goes through the same `build/Dockerfile`, release workflow, generic chart and Argo CD path as the real services, so the delivery pipeline (build, sign, push by digest, gitops pull request, Argo CD sync, PostSync smoke test on `/readyz`) and the observability stack can be proven **before** the data layer exists. The real services cannot start without RDS, ElastiCache, S3, EventBridge and Cognito.

## Routes

Served under `BASE_PATH` (default `/skeleton`, what the Ingress routes) **and** at the root (what the kubelet probes).

| Route | Answer |
|---|---|
| `GET /livez` | `200 {"status":"ok"}` while the process runs |
| `GET /readyz` | `200 {"status":"ready"}`; `503 {"status":"not ready"}` when `FORCE_NOT_READY=true` or while shutting down |
| `GET /version` | `200 {"service":"skeleton-api","version":"<git sha>","buildTime":"<RFC 3339>","commit":"<git sha>"}` |

`FORCE_NOT_READY=true` is the "a broken image is not promoted" proof: put it in a values file in Git, no code change, and the smoke test fails.

## Telemetry (all from `internal/platform`, no code in this binary)

- one server span per request (OTLP/HTTP to `OTEL_EXPORTER_OTLP_ENDPOINT`, a no-op when unset);
- `http.server.request.duration` (seconds) with `http.response.status_code` and `http.route`;
- JSON logs on stdout with `time`, `level`, `msg`, `request_id` and, inside a request, `trace_id` and `span_id`;
- an `info` line per request, **except** `/livez` and `/readyz` (debug), and an `error` line for any other 5xx.

## Run it

```
docker build -f build/Dockerfile --build-arg BINARY=skeleton-api -t gatepass/skeleton-api:dev .
docker run --rm --read-only -p 8080:8080 gatepass/skeleton-api:dev
curl -i localhost:8080/skeleton/readyz                        # 200
docker run --rm --read-only -p 8080:8080 -e FORCE_NOT_READY=true gatepass/skeleton-api:dev
curl -i localhost:8080/skeleton/readyz                        # 503
```

## Environment

All optional, see `skeleton-api.env.example` (generated from `contracts/skeleton-api.yaml`).

## How to read the code

`cmd/skeleton-api/main.go` (one file: config, routes, wiring) and `main_test.go` (routes, `FORCE_NOT_READY`, `BASE_PATH`, span + log with the same `trace_id`, the duration metric).
