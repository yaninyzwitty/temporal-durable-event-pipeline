# Temporal Durable Event Pipeline

A production-grade event sourcing system implementing the Transactional Outbox Pattern with gRPC APIs, PostgreSQL, and Redpanda (Kafka-compatible message broker).

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌──────────┐     ┌─────────┐
│   gRPC      │────▶│ PostgreSQL │────▶│ NOTIFY   │────▶│ Outbox  │
│   Clients   │     │   (store)  │     │ (listen)│     │Poller   │
└─────────────┘     └─────────────┘     └──────────┘     └────────┘
                                                        │
                                                        ▼
                                               ┌─────────────┐
                                               │  Redpanda  │
                                               │ (publish) │
                                               └─────────────┘
```

## Features

- **Transactional Outbox Pattern**: Guarantees exactly-once event delivery
- **gRPC API**: High-performance APIs for Users, Products, Orders, and Events
- **PostgreSQL LISTEN/NOTIFY**: Real-time event triggering
- **Durable Event Processing**: Automatic retry, backlog scanning, and reconnection
- **Redpanda/Kafka Integration**: Reliable event streaming to topics
- **Graceful Shutdown**: Clean resource handling

## Prerequisites

- Go 1.26+
- PostgreSQL 14+
- Redpanda (or Kafka)
- Docker & Docker Compose (optional)

## Quick Start

### 1. Start Infrastructure

```bash
# Start PostgreSQL and Redpanda
docker compose up -d
```

### 2. Run Migrations

```bash
# Connect to PostgreSQL and create tables (see db/migrations.sql)
psql -h localhost -U dev -d temporal_pipeline -f db/migrations.sql
```

### 3. Build and Run

```bash
# Build server
go build -o bin/server ./cmd/server

# Run with config
./bin/server --config config.yaml
```

### 4. Run Client Demo

```bash
# Build client
go build -o bin/client ./cmd/client

# Run demo (creates entities and triggers event pipeline)
./bin/client
```

## Configuration

```yaml
server:
  port: 50051
  env: "development"
database:
  host: "localhost"
  port: 5432
  user: "dev"
  password: "dev"
  sslMode: "disable"
  name: "temporal_pipeline"
redpanda:
  brokers:
    - "localhost:19092"
  topicPrefix: "temporal-pipeline"
```

## gRPC Services

| Service | Methods |
|--------|---------|
| UserService | CreateUser, GetUser, ListUsers, UpdateUser, DeleteUser, GetUserByEmail |
| ProductService | CreateProduct, GetProduct, ListProducts, UpdateProduct, DeleteProduct |
| OrderService | CreateOrder, GetOrder, ListOrdersByUser, UpdateOrderStatus, CreateOrderItem, GetOrderItems, DeleteOrderItem, DeleteOrder |
| EventService | CreateEvent, GetEvent, PollPendingEvents, UpdateEventStatus, ListEvents |

## Event Flow

1. **Create**: Client calls `CreateEvent` → event stored in `outbox_events` table with `PENDING` status
2. **Trigger**: PostgreSQL triggers `NOTIFY` on insert → outbox poller receives notification
3. **Process**: Poller reads pending events → publishes to Redpanda
4. **Complete**: Event status updated to `COMPLETED`
5. **Monitor**: Client polls `GetEvent` until status = `COMPLETED`

## Project Structure

```
.
├── cmd/
��   ├── server/          # Main server application
│   └── client/         # Demo client
├── internal/
│   ├── handler/       # gRPC service handlers
│   ├── poller/        # Outbox pattern poller
│   ├── publisher/     # Redpanda publisher
│   ├── repository/    # Database queries (sqlc)
│   └── server/       # gRPC server setup
├── db/                 # Database schema
├── gen/                # Generated protobuf code
└── shared/            # Shared utilities
```

## Testing

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...
```

## Guarantees

- **At-least-once delivery**: Events retry on failure
- **Exactly-once processing**: Idempotent operations with status tracking
- **Ordering**: Events processed in FIFO order per topic
- **Recovery**: Backlog scanning handles missed notifications