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
{ "queueId": "<id>", "receiptHandle": "<handle>", "deliveryToken": "<token>" }
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

## Durability and Recovery

kueue keeps live queue state in memory and persists every mutating operation to a Pebble-backed WAL. Pebble is not used as the message hot path: publish, receive, ack, nack, metrics, and reaping operate on `queueRuntime`, then recovery rebuilds that runtime from the latest snapshot plus WAL entries after the snapshot LSN.

`KUEUE_WAL_SYNC=none` commits WAL batches with `pebble.NoSync`, which is fastest but can lose the most recent OS-buffered writes on process or machine failure. `batch` and `always` both commit the grouped WAL batch with `pebble.Sync`; concurrent appends are coalesced so many operations can share one fsync while preserving WAL order.

Snapshots are consistent checkpoints of queues, ready/in-flight/dead messages, delivery tokens, visibility deadlines, and durable metrics. Snapshot data and the latest-snapshot pointer are written atomically; if the pointed snapshot is missing or corrupt at startup, recovery walks older snapshots and repairs the pointer. WAL compaction only deletes entries at or below a durable snapshot LSN.

`KUEUE_MAX_IN_MEMORY_MESSAGES` and `KUEUE_MAX_IN_MEMORY_BYTES` are per-queue admission limits enforced on publish. They bound live queued message count or body bytes for ready/in-flight/dead runtime state; `0` means unlimited.

On startup, kueue runs a one-time migration for databases created by the old Pebble message-key hot path. It scans legacy queue/message/index keys once, writes a snapshot at LSN 0 plus the migration marker in one synced batch, then uses normal snapshot + WAL replay. Corrupt or ambiguous legacy data aborts startup without writing the marker.

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
