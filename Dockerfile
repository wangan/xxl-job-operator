# Build the manager and sidecar binaries
FROM golang:1.24.0-alpine3.20 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/
COPY pkg/ pkg/

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o manager ./cmd/main.go
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o sidecar ./cmd/sidecar/main.go

# Target 1: Sidecar image (includes /bin/sh and wget/curl for PreStop hooks & GLUE_SHELL execution)
FROM alpine:3.20 AS sidecar
RUN apk --no-cache add ca-certificates tzdata curl wget
WORKDIR /
COPY --from=builder /workspace/sidecar /sidecar
USER 65532:65532
ENTRYPOINT ["/sidecar"]

# Target 2 (Default): Operator Manager image
FROM alpine:3.20 AS manager
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /
COPY --from=builder /workspace/manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
