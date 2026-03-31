# PulseCheck

PulseCheck is a Go service scaffold for asynchronous URL health-check processing.
It is split into two executables:

- Producer: publishes check jobs/events to Kafka.
- Consumer: reads check events from Kafka and persists results to Redis.

This repository currently provides a clean project structure and interfaces with
stub implementations for Kafka and Redis so you can implement your preferred
clients without changing the app shape.

## Features

- Clean, modular package layout (`cmd`, `internal/domain`, `internal/transport`, `internal/storage`)
- Shared domain model for health check messages
- Configurable runtime via environment variables
- Graceful shutdown handling for producer and consumer
- Clear interface boundaries (`Producer`, `Consumer`, `Store`)

## Project Structure

```text
.
├── cmd/
│   ├── producer/main.go   # publishes check messages every 30s
│   └── consumer/main.go   # consumes messages and stores them
├── internal/
│   ├── domain/check.go    # Check model + status enums
│   ├── transport/kafka.go # Kafka producer/consumer interfaces + stubs
│   └── storage/redis.go   # Redis store interface + stub
├── go.mod
└── README.md
```

## Requirements

- Go 1.24+
- Kafka broker
- Redis server

## Configuration

Set these environment variables as needed.

| Variable | Default | Used By | Description |
| --- | --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | producer, consumer | Comma-separated Kafka brokers |
| `KAFKA_TOPIC` | `pulse-checks` | producer, consumer | Topic used for check messages |
| `KAFKA_GROUP_ID` | `pulsecheck-consumer` | consumer | Consumer group ID |
| `REDIS_ADDR` | `localhost:6379` | consumer | Redis server address |

## Run Locally

### 1) Start dependencies

Make sure Kafka and Redis are running and reachable from your machine.

### 2) Run producer

```bash
go run ./cmd/producer
```

The producer emits an example check every 30 seconds:

- `id`: `example-check`
- `url`: `https://example.com`

### 3) Run consumer

```bash
go run ./cmd/consumer
```

The consumer subscribes to Kafka and attempts to save checks to the Redis store.

## Current Implementation Status

The transport and storage layers are intentionally scaffolded:

- `internal/transport/kafka.go`
	- `Publish` currently marshals JSON but does not send to Kafka yet.
	- `Subscribe` currently blocks on context cancellation.
- `internal/storage/redis.go`
	- `Save` currently marshals JSON but does not write to Redis yet.
	- `Get` returns a `not implemented` error.

This means the app wiring, lifecycle, and interfaces are ready, but real broker/
database I/O still needs to be implemented.

## Suggested Next Steps

1. Implement Kafka producer/consumer using a client library such as `franz-go`.
2. Implement Redis persistence using `go-redis`.
3. Add message keys, retries, and dead-letter behavior.
4. Add integration tests for producer -> Kafka -> consumer -> Redis flow.
5. Add `docker-compose.yml` for one-command local infrastructure startup.

## Domain Model

The shared `Check` message includes:

- `id`, `url`
- `status` (`UP`, `DOWN`, `UNKNOWN`)
- `status_code`, `response_time_ms`, `checked_at`, `error`

## License

This project is distributed under the terms in the `LICENSE` file.