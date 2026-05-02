# AGENTS.md

## Project

kueue is a persistent message queue server written in Go. Single binary, no dependencies beyond BadgerDB and uuid. Stdlib `net/http` for the server — no framework.

## Commands

```bash
go test ./...          # run all tests (always run after changes)
go build ./...          # verify compilation
go run main.go          # run the server (PORT=8080, KUEUE_DB_PATH=./tmp/badger)
go run ./cmd/bench      # run benchmarks against kueue and/or rabbitmq
```

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

Everything lives in `main.go`. There is no package splitting.

### Data model

- **Queue**: stored in BadgerDB as key=`<uuid>`, value=`QueueConfig{Name, MaxRetries}`. The ID is the key clients pass around.
- **Message**: stored as key=`<queueID>|<8-byte-big-endian-seq>|<messageID>`, value=JSON `Message`. The sequence number gives FIFO ordering. State machine: `ready -> in_flight -> ready|dead`.

### Key design decisions

- **FIFO ordering**: `messageKey` uses `GetSequence` for monotonically increasing keys. Badger iterators return lexicographic order, so `claimNextReadyMessage` scans in enqueue order.
- **Atomic claim**: `claimNextReadyMessage` runs inside a single `Db.Update` transaction. It finds the first `StateReady` message, flips it to `StateInFlight`, and writes it back — all atomically. This is what makes competing consumers safe.
- **Visibility timeout**: 30 seconds hardcoded. The reaper goroutine (`reaper()`) runs every 1 second, calls `reapExpiredMessages` which scans ALL keys (not per-queue), finds in-flight messages past their `VisibilityDeadline`, and transitions them.
- **Delivery tokens**: Each claim generates a `DeliveryAttemptID` (UUID). Both `/ack` and `/nack` require this token. If a message is re-delivered (after timeout expiry), the old token is rejected with 409. This prevents stale acks.
- **Long polling**: `signalQueueReady` closes the old per-queue channel and creates a new one. `receive?wait=true` blocks on that channel with a 30s timeout. The re-check between subscribing and waiting prevents lost signals.
- **Dead letter**: When `DeliveryCount >= MaxDeliveryCount` and the message is nacked or reaped, state becomes `dead`. Dead messages stay in the store but are never returned by receive.

### Metrics

Per-queue in-memory counters in `queueMetrics`, stored in a `sync.Map` (`metricsStore`). On `/metrics?id=X`, `reconcileMetricsFromDB` scans BadgerDB for truth and uses `snapshotMax` (CAS loop) to avoid overwriting live counters with stale snapshots.

`ackWindow` is a sliding window of ack timestamps (60s). `trimAckWindow` is called from the reaper goroutine every second to prevent unbounded growth — not just from `/metrics`.

### Reaper returns transitions

`reapExpiredMessages` returns `[]reapTransition{QueueID, ToState}`. The reaper goroutine updates per-queue metrics per transition and only calls `signalQueueReady` when `ToState == StateReady` — dead-letter transitions must not wake long-polling consumers.

## Testing

Tests in `main_test.go` use `httptest.NewRecorder` + direct handler calls against a temp BadgerDB. No HTTP server startup. Pattern: `setupTestDB(t)` opens BadgerDB in `t.TempDir()` and resets global state including `metricsStore`. Tests do NOT test the reaper goroutine directly — they call `reapExpiredMessages` synchronously.

When adding new global state (like `metricsStore`), add cleanup in `setupTestDB`.

## Known gotchas

- `var queue []int` (line 19) and `var Queues []Queue` / `var DeadLetterQueue []Message` (lines 63-64) are legacy in-memory remnants. They should not be used.
- `reapExpiredMessages` scans ALL keys with no prefix filter — it iterates the entire DB. This is intentional because it needs to find expired in-flight messages across all queues in one pass.
- BadgerDB `item.Key()` is only valid inside the `item.Value()` callback. Always use `item.KeyCopy(nil)` if you need the key outside that callback.
- The `Db` package global is set in `main()` and used everywhere. Tests set it in `setupTestDB`.
- The `getQueue` handler does not return after writing the empty-id error — it falls through to the DB query with an empty string. This is a known bug, not a feature.