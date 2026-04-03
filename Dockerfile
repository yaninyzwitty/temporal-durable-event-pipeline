# Multi-stage Dockerfile for building both server and client
# Usage:
#   Server: docker build --target server-runtime .
#   Client: docker build --target client-runtime .

# ---- Build Stage ----
FROM golang:1.26-alpine@sha256:2389ebfa5b7f43eeafbd6be0c3700cc46690ef842ad962f6c5bd6be49ed82039 AS builder

WORKDIR /app

# Copy dependency files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG BINARY_NAME=server

# Build the specified binary
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags="-s -w" \
    -o /app/${BINARY_NAME} \
    ./cmd/${BINARY_NAME}

# ---- Runtime Stage: Server ----
FROM gcr.io/distroless/static:nonroot@sha256:47b2d72ff90843eb8a768b5c2f89b40741843b639d065b9b937b07cd59b479c6 AS server-runtime

WORKDIR /app

COPY --chown=nonroot:nonroot --from=builder /app/server ./server
COPY --chown=nonroot:nonroot db/migrations ./db/migrations

EXPOSE 50051

ENTRYPOINT ["./server"]

# ---- Runtime Stage: Client ----
FROM gcr.io/distroless/static:nonroot@sha256:47b2d72ff90843eb8a768b5c2f89b40741843b639d065b9b937b07cd59b479c6 AS client-runtime

WORKDIR /app

COPY --chown=nonroot:nonroot --from=builder /app/client ./client

ENTRYPOINT ["./client"]