# kueue

A persistent message queue server built in Go with Pebble storage.

## Features

- Multiple named queues with configurable max retries
- FIFO message ordering
- Ack/nack with at-least-once delivery
- Visibility timeout with automatic reaper for expired messages
- Dead letter queue after max delivery attempts
- Long polling on receive
- Delivery tokens to prevent duplicate acks

## API

| Endpoint | Method | Description |
| --- | --- | --- |
| `/create` | POST | Create a queue |
| `/get?id=QUEUE_ID` | GET | Get queue info |
| `/publish` | POST | Publish a message |
| `/receive?id=QUEUE_ID` | GET | Receive next ready message |
| `/receive?id=QUEUE_ID&wait=true` | GET | Long-poll for next message (30s timeout) |
| `/ack` | POST | Acknowledge a message |
| `/nack` | POST | Reject a message (returns to queue or dead-letters) |

### Create Queue

```json
POST /create
{ "name": "my-queue", "maxRetries": 3 }
```

### Publish

```json
POST /publish
{ "queueId": "<id>", "message": { "body": "<base64>" } }
```

### Ack / Nack

```json
POST /ack
{ "queueId": "<id>", "messageId": "<id>", "deliveryToken": "<token>" }
```

## Run

```bash
go run main.go
```

Environment variables:
- `KUEUE_DB_PATH` - Pebble data directory (default: `./tmp/pebble`)
- `PORT` - Server port (default: `8080`)
- `KUEUE_WAL_SYNC` - WAL fsync mode: `none`, `batch`, or `always` (default: `none`)
- `KUEUE_SNAPSHOT_EVERY_OPS` - Take a checkpoint after this many committed WAL entries (default: `100000`; `0` disables the ops dimension)
- `KUEUE_SNAPSHOT_EVERY_SECONDS` - Take a checkpoint at least this often (default: `60`; `0` disables the seconds dimension). Both `0` disables snapshots entirely.
- `KUEUE_WAL_COMPACT_BATCH` - Max keys per Pebble Delete batch during WAL compaction / snapshot pruning (default: `1000`; `0` = single unbounded batch)
- `KUEUE_MAX_IN_MEMORY_MESSAGES` / `KUEUE_MAX_IN_MEMORY_BYTES` - Per-queue in-memory limits (`0` = unlimited)
- `KUEUE_CPU_PROFILE` - Write a CPU profile to this path when set

## Benchmark

```bash
go run ./cmd/bench --targets kueue
```

## Performance

Benchmarked with 10k messages, 256-byte payload, 1 producer, 10 consumers, batch receive (prefetch=10):

| Metric | Value |
| --- | ---: |
| Publish throughput | ~28,000 msg/s |
| Consume throughput | ~8,700 msg/s |
| Latency p50 | ~354 ms |
| Latency p95 | ~742 ms |
| Latency p99 | ~774 ms |

Apples-to-apples (1 message per round-trip, 1 consumer):

| Metric | Value |
| --- | ---: |
| Publish throughput | ~35,500 msg/s |
| Consume throughput | ~1,700 msg/s |
| Latency p50 | ~2,580 ms |
| Latency p99 | ~5,590 ms |

## Test

```bash
go test ./...
```