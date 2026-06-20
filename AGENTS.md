# AGENTS.md

## Project

kueue is a persistent message queue server written in Go. Single binary, no dependencies beyond Pebble and uuid. Stdlib `net/http` for the server — no framework.

## Commands

```bash
go test ./...          # run all tests (always run after changes)
go build ./...          # verify compilation
go run main.go          # run the server (PORT=8080, KUEUE_DB_PATH=./tmp/pebble)
go run ./cmd/bench      # run benchmarks against kueue and/or rabbitmq
```

Air (hot-reload) is always running during development. Config is in `.air.toml` — it builds to `tmp/main.exe`, excludes `_test.go` files, and restarts on Go/template/html changes.

For benchmarks on Windows, use `scripts/run-benchmark.ps1`. It starts both kueue and a RabbitMQ container, runs the bench binary, and cleans up afterward.

CPU profiling: set `KUEUE_CPU_PROFILE=<file>` env var when starting the server.

### Benchmark workloads

```bash
# Default: e2e + apples-to-apples
go run ./cmd/bench -targets=kueue -workload=default

# Competing consumers: 1 producer, 10 consumers, FIFO verification + fairness
go run ./cmd/bench -targets=kueue -workload=competing-consumers

# Backlog drain: pre-fills queue then measures drain rate
go run ./cmd/bench -targets=kueue -workload=backlog-drain

# Size sweep: 64B, 256B, 1KB, 4KB, 16KB payloads
go run ./cmd/bench -targets=kueue -workload=size-sweep

# Full suite: single consumer, competing consumers, size sweep
go run ./cmd/bench -targets=kueue -workload=full -verify-order

# Rate-limited publishing (avoids coordinated omission)
go run ./cmd/bench -targets=kueue -rate=1000 -workload=default

# Slow consumer simulation
go run ./cmd/bench -targets=kueue -consumer-delay=5 -workload=default
```

### Benchmark methodology

Follows OpenMessaging Benchmark methodology: warmup → measured runs → median of runs.

Metrics reported (per OpenMessaging standard):
- **Throughput**: publish msg/s, consume msg/s
- **End-to-end latency**: p50, p95, p99, p99.9, max (in ms)
- **FIFO violations**: count of out-of-order deliveries per consumer (with `-verify-order`)
- **Consumer fairness**: std dev and min/max of per-consumer message counts

Workload presets align with OpenMessaging canonical patterns:
- Max-rate (1p/1c), competing consumers (1p/Nc), backlog drain, message size sweep

## Architecture

Code is in the root package (`package main`) split across focused files (`main.go`, `runtime.go`, `wal.go`, `recovery.go`). There is no sub-package splitting yet.

### HTTP routes

```
/              POST  (health/ping — just returns "Hello Consumer")
/create        POST  (create queue)
/get           GET   (get queue info)
/publish       POST  (publish single message)
/publish-batch POST  (publish multiple messages)
/receive       GET   (receive single message; ?wait=true for long-polling)
/receive-batch GET   (receive batch of messages; ?wait=true for long-polling)
/ack           POST  (ack single message)
/ack-batch     POST  (ack multiple messages)
/nack          POST  (nack message)
/metrics       GET   (queue metrics)
```

### Data model

- **Queue**: `QueueConfig{Name, MaxRetries}` stored in the runtime `queueManager` and persisted via WAL. The ID (UUID) is the key clients pass around.
- **Message**: in-memory `messageRecord` in the per-queue runtime. State machine: `ready -> in_flight -> ready|dead`. Persisted via WAL; Pebble holds the WAL log, not per-message keys.

### Key design decisions

- **FIFO ordering**: Each queue has a monotonically increasing `nextSeq` counter (per-queue, no global lock). Messages are pushed to the back of a doubly-linked `readyList` in seq order. `ClaimBatch` pops from the front in O(1).
- **Atomic claim**: `ClaimBatch` is protected by the per-queue `queueRuntime.mu` mutex. It pops up to `max` messages from the ready list front, flips them to `StateInFlight`, assigns receipt handles + delivery tokens, and appends a single `opClaimBatch` WAL entry. If WAL append fails, all popped messages are rolled back to the ready list front.
- **Visibility timeout**: 30 seconds hardcoded. The reaper goroutine (`reaper()`) runs every 1 second, calls `QueueManager.ReapExpired` which pops from each queue's deadline min-heap, finds in-flight messages past their `VisibilityDeadline`, and transitions them to `ready` or `dead`.
- **Delivery tokens**: Each claim generates a `DeliveryAttemptID` (UUID). Both `/ack` and `/nack` require this token. If a message is re-delivered (after timeout expiry or nack), the old token is rejected with 409. This prevents stale acks.
- **Long polling**: `signalQueueReady` closes the old per-queue channel and creates a new one. `receive?wait=true` blocks on that channel with a 30s timeout. The re-check between subscribing and waiting prevents lost signals.
- **Dead letter**: When `DeliveryCount >= MaxDeliveryCount` and the message is nacked or reaped, state becomes `dead`. Dead messages stay in the runtime `dead` map but are never returned by receive.
- **Receipt handles**: Messages claimed via receive are assigned a `receiptHandle` (base64 of `queueID|seq|messageID`). The receipt handle is immutable across redeliveries (seq is fixed at publish time). It's required for ack/nack alongside `deliveryToken`.

### Metrics

Per-queue in-memory counters in `queueMetrics`, stored in a `sync.Map` (`metricsStore`). On `/metrics?id=X`, `reconcileMetricsFromDB` uses a Pebble snapshot iterator for truth and uses `snapshotMax` (CAS loop) to avoid overwriting live counters with stale snapshots.

`ackWindow` is a sliding window of ack timestamps (60s). `resetAckWindow` is called from the reaper goroutine every second to prevent unbounded growth — not just from `/metrics`.

### Reaper returns transitions

`QueueManager.ReapExpired` returns `[]reapTransition{QueueID, ToState}`. The reaper goroutine updates per-queue metrics per transition and only calls `signalQueueReady` when `ToState == StateReady` — dead-letter transitions must not wake long-polling consumers.

### Runtime model

`runtime.go` holds the in-memory queue manager (`queueManager`), which is the source of truth for all live handler operations. Each queue has:
- A ready list (`readyList`) of messages ordered by monotonic sequence number for FIFO delivery.
- An in-flight map (`inflight`) keyed by receipt handle, plus a deadline heap (`deadlineHeap`) ordered by `VisibilityDeadline`.
- A dead-letter set (`dead`) of messages that exceeded `MaxDeliveryCount`.
- Per-queue `nextSeq` and `bytesInMem` counters.

State transitions follow: `ready -> in_flight -> ready|dead`. The runtime is the source of truth; Pebble holds the WAL log, not per-message keys.

### WAL

`wal.go` implements a write-ahead log on top of Pebble. Every mutating runtime operation is recorded as a `WALEntry` before being applied. Entries are encoded as JSON per `WALEntry` and appended to the Pebble log. Supported operations: `create`, `publish`, `claim`, `ack`, `nack`, `reap`.

The WAL stores:
- `snapshot_lsn`: last snapshot LSN (currently `0`; reserved for Phase 2.7).
- `next_lsn`: next write LSN.
- `entries`: per-LSN entries.

Snapshots truncate the log; replay starts from the latest snapshot LSN.

### Recovery

`recovery.go` rebuilds the runtime model at startup. `main()` calls `initQueueManagerFromEnv`, which:
1. Opens/creates the `walStore` from Pebble.
2. Creates an empty `queueManager`.
3. Replays every WAL entry through `ApplyWALEntry`.
4. Runs one `ReapExpired` pass to drain any in-flight messages that expired while the server was down.

`ApplyWALEntry` validates consistency strictly: duplicate queue creates, missing messages, wrong states, or stale delivery tokens fail loudly and stop startup. During replay, ready channels are not signaled because there are no consumers yet.

## Testing

Tests in `main_test.go` use `httptest.NewRecorder` + direct handler calls against a temp Pebble DB. No HTTP server startup. Pattern: `setupTestDB(t)` opens Pebble in `t.TempDir()` and resets global state including `metricsStore`, `messageKeyCache`, `receiveChannel`, `queueReadyChans`, `QueueManager`, and `WAL`. Tests do NOT test the reaper goroutine directly — they call `QueueManager.ReapExpired` synchronously.

Runtime/WAL/recovery tests typically build state through the runtime API, close the Pebble DB, reopen it, and call `recoverQueueManager` / `ApplyWALEntry` to verify that the in-memory model is rebuilt correctly.

When adding new global state (like `metricsStore`), add cleanup in `setupTestDB`.

There is also `cmd/bench/main_test.go` for benchmark tool tests.

## Known gotchas

- `var queue []int` (line 25) and `var Queues []Queue` / `var DeadLetterQueue []Message` (lines 88-89) are legacy in-memory remnants. They should not be used.
- Legacy Pebble key functions (`messageKey`, `readyKey`, `inflightKey`, `nextMessageSequence`, `findMessageRecord`, key builders) and `store.go` functions remain in the codebase but are off the live handler path. Some are still referenced by metrics reconciliation tests (`TestReconcileMetricsFromDBIfStale*`) and `TestReadyPartsFromKeyUsesKnownPrefix`. Full removal is #35 (Phase 2.10).
- `reapExpiredMessages` scanned ALL keys with no prefix filter — it iterated the entire DB via a snapshot. This was the legacy reaper path; it has been removed. The live reaper uses `QueueManager.ReapExpired` which pops from a per-queue deadline min-heap.
- Pebble `Batch.Get()` returns `(val []byte, closer io.Closer, err error)`. Always call `closer.Close()` after reading the value.
- The `Db` package global is set in `main()` and used everywhere. Tests set it in `setupTestDB`.