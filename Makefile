# Gatepass Go repository: common tasks.
# Every target is a thin wrapper so a reader can see the real command.

SHELL := /bin/bash
GO ?= go
VERSION ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG := github.com/olagookundavid/gatepass-app/internal/platform/buildinfo
LDFLAGS := -s -w -X $(PKG).Version=$(VERSION) -X $(PKG).BuildTime=$(BUILD_TIME)

SERVICES := catalog-api reservations-api orders-api orders-worker orders-outbox-relay notifications-api notifications-worker skeleton-api
LAMBDAS := lambda-hold-expirer lambda-nightly-report

.PHONY: build test test-integration lint image lambda-zip openapi-check tidy \
        migrate-catalog migrate-orders

build: ## Compile every binary into ./bin (native OS/arch)
	@mkdir -p bin
	@for b in $(SERVICES) $(LAMBDAS); do \
	  echo "build $$b"; \
	  CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/$$b ./cmd/$$b || exit 1; \
	done

test: ## Unit tests (no AWS needed)
	$(GO) test ./...

test-integration: ## Integration tests: need real AWS resources, see the env vars in each test file. Skipped when unset.
	$(GO) test -tags integration -count=1 -v ./...

lint: ## go vet (with and without the integration tag) + golangci-lint
	$(GO) vet ./...
	$(GO) vet -tags integration ./...
	golangci-lint run

image: ## Build one container image: make image BINARY=catalog-api
	@test -n "$(BINARY)" || (echo "usage: make image BINARY=catalog-api"; exit 1)
	docker build -f build/Dockerfile --build-arg BINARY=$(BINARY) \
	  --build-arg VERSION=$(VERSION) --build-arg BUILD_TIME=$(BUILD_TIME) \
	  -t gatepass/$(BINARY):$(VERSION) .

lambda-zip: ## Build Lambda zips (arm64, provided.al2023, file name "bootstrap") into ./dist
	@mkdir -p dist
	@for b in $(LAMBDAS); do \
	  echo "zip $$b"; \
	  rm -rf dist/$$b && mkdir -p dist/$$b; \
	  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -trimpath -tags lambda.norpc \
	    -ldflags "$(LDFLAGS)" -o dist/$$b/bootstrap ./cmd/$$b || exit 1; \
	  (cd dist/$$b && zip -q -j ../$$b.zip bootstrap) || exit 1; \
	done

openapi-check: ## Lint OpenAPI specs with spectral if installed (not installed on the authoring machine: skipped)
	@if command -v spectral >/dev/null 2>&1; then spectral lint api/*.openapi.yaml; \
	else echo "spectral not installed: skipped (specs were written by hand as OpenAPI 3.1; operationIds are unique)"; fi

migrate-catalog: ## Documents the command (runs in a Kubernetes Job in real life)
	@echo "Run: catalog-api migrate   (env: DB_HOST DB_PORT DB_NAME DB_USER DB_IAM_AUTH=true AWS_REGION)"

migrate-orders: ## Documents the command (runs in a Kubernetes Job in real life)
	@echo "Run: orders-api migrate    (env: DB_HOST DB_PORT DB_NAME DB_SECRET_ID AWS_REGION)"

tidy:
	$(GO) mod tidy
