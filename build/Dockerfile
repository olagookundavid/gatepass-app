# syntax=docker/dockerfile:1
#
# ONE Dockerfile for every Gatepass service binary.
#   docker build -f build/Dockerfile --build-arg BINARY=orders-api -t gatepass/orders-api .
#
# Cross-compiling instead of emulating: the build stage always runs on the
# machine's NATIVE architecture ($BUILDPLATFORM) and Go cross-compiles to
# $TARGETARCH. That is why `linux/amd64` and `linux/arm64` images build without
# QEMU: Go needs no emulated C toolchain because CGO is off.
#
# Base images are pinned by digest (the digest of the multi-arch index, so the
# same line works for amd64 and arm64). Update them deliberately, not by accident:
#   docker buildx imagetools inspect golang:1.26
#
# Lambda binaries are NOT built here; see `make lambda-zip`.

FROM --platform=$BUILDPLATFORM golang:1.26@sha256:670bb5aa5349ae03b8516764718486f4f16399134bdc9acd0c46a23f043ab05c AS build
ARG BINARY
ARG TARGETARCH
ARG VERSION=dev
ARG BUILD_TIME=unknown
WORKDIR /src

# Copy go.mod/go.sum first: this layer (module download) is cached until the
# dependencies change, so code-only edits rebuild in seconds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN test -n "$BINARY" || (echo "build arg BINARY is required, e.g. --build-arg BINARY=catalog-api" && exit 1)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH \
    go build -trimpath \
      -ldflags "-s -w \
        -X github.com/olagookundavid/gatepass-app/internal/platform/buildinfo.Version=${VERSION} \
        -X github.com/olagookundavid/gatepass-app/internal/platform/buildinfo.BuildTime=${BUILD_TIME}" \
      -o /out/app ./cmd/${BINARY}

# distroless/static: no shell, no package manager, just CA certificates, tzdata
# and a non-root user (uid 65532). The RDS CA bundle is embedded in the binary.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/app /app
USER nonroot:nonroot
# The root filesystem is meant to be read-only at runtime (Kubernetes
# readOnlyRootFilesystem); the app writes only to /tmp, which the Pod mounts as emptyDir.
ENTRYPOINT ["/app"]
