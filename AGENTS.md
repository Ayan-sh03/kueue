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

Per-queue in-memory atomic counters in `queueMetrics`, stored in a `sync.Map` (`metricsStore`). Counters (`readyCount`, `inFlightCount`, `deadCount`, `totalPublished`, `totalReceived`, `totalAcked`, `totalNacked`, `ackCountWindow`) are maintained on every mutating transition in the live runtime (publish, claim, ack, nack, reap-ready, reap-dead) and replayed via `ApplyWALEntry` on recovery (`recovery.go`). `/metrics?id=X` is O(1) w.r.t. queue depth: it only loads these atomics — no Pebble scan.

`ackCountWindow` is a per-second ack counter reset by the reaper goroutine every second (`resetAckWindow` in `reaper.go`) to feed `ackRatePerSec`; uptime fallback is `totalAcked / uptimeSeconds`.

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

**Group commit**: `Append` coalesces concurrent appends into a single Pebble batch + single fsync. Each entry's frame is encoded outside the WAL lock (the frame does not depend on the LSN — the LSN is only the Pebble key), so only LSN assignment and staging into the shared batch happen under the lock. The first appender becomes the leader, commits the accumulated batch, then drains any batches that filled up during the commit; the rest wait on their group's `done` channel. Concurrent appends only ever come from different queues (a queue holds its own `mu` across its Append), so cross-batch ordering is irrelevant. Under `KUEUE_WAL_SYNC=always` this amortizes one fsync across many queues' operations (~12× throughput at 32 concurrent producers).

### Snapshots and WAL compaction (`snapshot.go`)

Consistent snapshots checkpoint the live `queueManager` into a single `snapshot|<LSN>` Pebble value and advance `walmeta|latest_snapshot_lsn` in **one atomic Pebble batch (Sync)**. The snapshot LSN is `walStore.nextLSN - 1` read while all per-queue `mu` are held (so no Append can be mid-flight), making the captured state and the LSN strictly consistent. Snapshot payload (`snapshotData`) covers queue configs, `nextSeq`, ready/in-flight/dead messages with bodies, **in-flight receipt handles + delivery tokens + visibility deadlines**, and the durable metric counters (`totalPublished/Received/Acked/Nacked`, `readyCount/inFlightCount/deadCount`). `ackCountWindow` is intentionally not durable (it is recomputed by the reaper post-recovery). The frame format mirrors the WAL frame: `"KSNA"` magic + version + payload CRC + length, reused `walPayloadWriter/Reader` for the body.

**Triggers**: the reaper goroutine ticks `QueueManager.maybeSnapshot` every second. It checks `KUEUE_SNAPSHOT_EVERY_OPS` (default `100000`) and `KUEUE_SNAPSHOT_EVERY_SECONDS` (default `60`); `0` disables that dimension, both zero disables snapshots entirely. On success it `compactWAL`s through the snapshot LSN and `pruneOldSnapshots` keeps the 2 newest snapshot objects. Defaults are high enough that short CI/benchmark runs never trigger a snapshot (no throughput regression in CI).

**Crash safety**: snapshot data and the meta pointer land atomically. On startup `recoverQueueManager` loads the snapshot at `latest_snapshot_lsn`; if it is missing or corrupt (CRC/magic failure or truncated value), it walks the `snapshot|` prefix descending for the next-newest usable snapshot, applies that, and durably rewrites the pointer. If no usable snapshot exists, the pointer is reset to 0 and full WAL replay runs from LSN 1. So a partial commit of a snapshot never makes startup pick a corrupt/checkpoint.

**Compaction**: `compactWAL(throughLSN)` deletes every `wal|<LSN>` with `LSN ≤ throughLSN` in bounded Pebble Delete batches of `KUEUE_WAL_COMPACT_BATCH` (default `1000`; `0` = single batch) using `NoSync` — safe because the authorizing snapshot batch was already `Sync`'d; a crash before compaction lands just means re-compacting on next start. WAL entries above the snapshot LSN are never deleted. `applySnapshot` reconstructs each queue's `readyList` (in stored order), `inflight` map + `deadlines` heap (fresh `deliveryRecordSeq` for heap tie-breaking), and `dead` set, then `Store`s the snapshot metric counters. WAL entries with LSN > snapshot replay on top via `ApplyWALEntry`, which strictly validates state transitions (e.g. `opAckBatch` requires the message in `StateInFlight`), so snapshot+replay is byte-identical to full replay.

### Legacy-layout migration (`migration.go`)

One-time migration from the pre-WAL Pebble hot-path layout to the snapshot/WAL architecture. gated by the durable marker `migration|pebble_hot_path_imported=true`. Run by `recoverQueueManager` before snapshot loading; idempotent (marker set → no-op scan).

Legacy layout scanned (all written by the pre-WAL handlers):
- `<queueID>` → JSON `QueueConfig{Name, MaxRetries}`
- `seq:<queueID>` → 8-byte uint64 next-sequence counter
- `<queueID>|<8B seq>|<messageID>` → JSON `Message`
- `ready|<queueID>|<8B seq>|<messageID>` → message key bytes (ignored — message key is authoritative)
- `inflight|<8B deadline>|<queueID>|<messageID>` → message key bytes (ignored — message state field is authoritative)

`scanLegacyLayout` walks every key in the DB, ignoring reserved modern prefixes (`wal|`, `walmeta|`, `snapshot|`, `migration|`) and the unwanted legacy `ready|`/`inflight|` indexes. For each queue seen it builds config + message records. `buildSnapshotFromLegacy` constructs a `snapshotData` at **LSN 0**: ready messages sorted by seq for FIFO delivery, in-flight messages keep their visibility deadline + delivery count + delivery token (receipt handle is recomputed via `receiptHandleForMessage`), dead messages migrate as dead, `nextSeq = max(maxObservedSeq, seqCounter) + 1`, and metric counters are derived from observed state.

`commitMigrationSnapshot` writes `snapshot|0` + `walmeta|next_lsn=1` + `walmeta|latest_snapshot_lsn=0` + the marker in **one atomic Pebble batch (Sync)**. Crash safety: the batch either fully lands or doesn't; a crash before commit means the next start re-scans (idempotent). After commit, the load-snapshot path below picks up `snapshot@0` via the descending fallback scan (LSN 0 is a valid found result, not the not-found sentinel) and replays no WAL entries (none exist with LSN > 0).

**Failure modes abort startup loudly, with no partial marker written**: corrupt config JSON, corrupt message JSON, message ID mismatch between key and value, invalid message state, seq value with wrong length, or a queue with messages but no config (ambiguous). The `nextLsn > 1` shortcut writes only the marker (DB is already on the new layout from a prior start) so we never re-scan a populated WAL DB. The `!hasOld` shortcut writes only the marker for a fresh DB so we never re-scan an empty store.

### Recovery

`recovery.go` rebuilds the runtime model at startup. `main()` calls `initQueueManagerFromEnv`, which:
1. Opens/creates the `walStore` from Pebble.
2. Creates an empty `queueManager`.
3. Runs `maybeMigrateLegacyLayout` — no-op if the durable marker exists, otherwise scans once for old-layout keys and (if present) writes `snapshot|0` + WAL meta pointers + the marker in one atomic Sync batch. See "Legacy-layout migration" above.
4. Loads the newest usable snapshot (per the crash-safety protocol above) and applies it. For a freshly-migrated DB this is `snapshot@0` via the descending fallback scan.
5. Replays WAL entries with LSN > snapshot LSN through `ApplyWALEntry` (none for a fresh migration — `next_lsn=1`, `latest_snapshot_lsn=0`).
6. Runs one `ReapExpired` pass to drain any in-flight messages that expired while the server was down.

`ApplyWALEntry` validates consistency strictly: duplicate queue creates, missing messages, wrong states, or stale delivery tokens fail loudly and stop startup. During replay, ready channels are not signaled because there are no consumers yet.

## Testing

Tests in `main_test.go` use `httptest.NewRecorder` + direct handler calls against a temp Pebble DB. No HTTP server startup. Pattern: `setupTestDB(t)` opens Pebble in `t.TempDir()` and resets global state including `metricsStore`, `QueueManager`, and `WAL`. Tests do NOT test the reaper goroutine directly — they call `QueueManager.ReapExpired` synchronously.

Runtime/WAL/recovery tests typically build state through the runtime API, close the Pebble DB, reopen it, and call `recoverQueueManager` / `ApplyWALEntry` to verify that the in-memory model is rebuilt correctly.

When adding new global state (like `metricsStore`), add cleanup in `setupTestDB`.

There is also `cmd/bench/main_test.go` for benchmark tool tests.

## Known gotchas

- The live hot path does not write or scan per-message Pebble keys. Pebble stores WAL/snapshot/migration metadata; message state lives in `queueRuntime` and is recovered from snapshots + WAL replay.
- Legacy Pebble key builders (`messageKey`, `readyKey`, `inflightKey`) remain only to seed old-layout migration tests and document the import format. Do not use them in handlers, metrics, or reaper code.
- `reapExpiredMessages` scanned ALL keys with no prefix filter — it iterated the entire DB via a snapshot. This was the legacy reaper path; it has been removed. The live reaper uses `QueueManager.ReapExpired` which pops from a per-queue deadline min-heap.
- Pebble `Batch.Get()` returns `(val []byte, closer io.Closer, err error)`. Always call `closer.Close()` after reading the value.
- The `Db` package global is set in `main()` and used everywhere. Tests set it in `setupTestDB`.
