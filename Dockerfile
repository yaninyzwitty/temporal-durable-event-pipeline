# ---- Build Stage ----
FROM golang:1.26-alpine@sha256:2389ebfa5b7f43eeafbd6be0c3700cc46690ef842ad962f6c5bd6be49ed82039 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags="-s -w" \
    -o /app/server \
    ./cmd/server

# ---- Runtime Stage ----
FROM gcr.io/distroless/static:nonroot@sha256:47b2d72ff90843eb8a768b5c2f89b40741843b639d065b9b937b07cd59b479c6

WORKDIR /app

COPY --chown=nonroot:nonroot --from=builder /app/server ./server
COPY --chown=nonroot:nonroot db/migrations ./db/migrations

EXPOSE 50051

ENTRYPOINT ["./server"]
