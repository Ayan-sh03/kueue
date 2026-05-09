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

## Architecture

Everything lives in `main.go`. There is no package splitting.

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

- **Queue**: stored in Pebble as key=`<uuid>`, value=`QueueConfig{Name, MaxRetries}`. The ID is the key clients pass around.
- **Message**: stored as key=`<queueID>|<8-byte-big-endian-seq>|<messageID>`, value=JSON `Message`. The sequence number gives FIFO ordering. State machine: `ready -> in_flight -> ready|dead`.

### Key design decisions

- **FIFO ordering**: `messageKey` uses a mutex-protected counter (`seqMu`) for monotonically increasing keys. Pebble iterators return lexicographic order, so `claimReadyMessages` scans in enqueue order.
- **Atomic claim**: `claimReadyMessages` is protected by `claimMu` (global mutex) to serialize competing consumer claims. It finds the first `StateReady` message, flips it to `StateInFlight`, and writes it back — all within a single `IndexedBatch` commit. The mutex is needed because Pebble lacks Badger's serializable transaction isolation.
- **Visibility timeout**: 30 seconds hardcoded. The reaper goroutine (`reaper()`) runs every 1 second, calls `reapExpiredMessages` which uses a snapshot iterator over inflight keys, finds in-flight messages past their `VisibilityDeadline`, and transitions them.
- **Delivery tokens**: Each claim generates a `DeliveryAttemptID` (UUID). Both `/ack` and `/nack` require this token. If a message is re-delivered (after timeout expiry), the old token is rejected with 409. This prevents stale acks.
- **Long polling**: `signalQueueReady` closes the old per-queue channel and creates a new one. `receive?wait=true` blocks on that channel with a 30s timeout. The re-check between subscribing and waiting prevents lost signals.
- **Dead letter**: When `DeliveryCount >= MaxDeliveryCount` and the message is nacked or reaped, state becomes `dead`. Dead messages stay in the store but are never returned by receive.
- **Receipt handles**: Messages claimed via receive/get are assigned a `receiptHandle`. This is required for ack/nack alongside `deliveryToken`. The receipt handle maps to the Pebble key via `messageKeyCache` (a `sync.Map`).

### Metrics

Per-queue in-memory counters in `queueMetrics`, stored in a `sync.Map` (`metricsStore`). On `/metrics?id=X`, `reconcileMetricsFromDB` uses a Pebble snapshot iterator for truth and uses `snapshotMax` (CAS loop) to avoid overwriting live counters with stale snapshots.

`ackWindow` is a sliding window of ack timestamps (60s). `resetAckWindow` is called from the reaper goroutine every second to prevent unbounded growth — not just from `/metrics`.

### Reaper returns transitions

`reapExpiredMessages` returns `[]reapTransition{QueueID, ToState}`. The reaper goroutine updates per-queue metrics per transition and only calls `signalQueueReady` when `ToState == StateReady` — dead-letter transitions must not wake long-polling consumers.

## Testing

Tests in `main_test.go` use `httptest.NewRecorder` + direct handler calls against a temp Pebble DB. No HTTP server startup. Pattern: `setupTestDB(t)` opens Pebble in `t.TempDir()` and resets global state including `metricsStore`, `messageKeyCache`, `receiveChannel`, and `queueReadyChans`. Tests do NOT test the reaper goroutine directly — they call `reapExpiredMessages` synchronously.

When adding new global state (like `metricsStore`), add cleanup in `setupTestDB`.

There is also `cmd/bench/main_test.go` for benchmark tool tests.

## Known gotchas

- `var queue []int` (line 25) and `var Queues []Queue` / `var DeadLetterQueue []Message` (lines 88-89) are legacy in-memory remnants. They should not be used.
- `reapExpiredMessages` scans ALL keys with no prefix filter — it iterates the entire DB via a snapshot. This is intentional because it needs to find expired in-flight messages across all queues in one pass.
- Pebble `Batch.Get()` returns `(val []byte, closer io.Closer, err error)`. Always call `closer.Close()` after reading the value.
- `claimMu` serializes `claimReadyMessages` to prevent competing consumers from claiming the same message, since Pebble lacks Badger's transaction isolation.
- The `Db` package global is set in `main()` and used everywhere. Tests set it in `setupTestDB`.
- The `getQueue` handler does not return after writing the empty-id error — it falls through to the DB query with an empty string. This is a known bug, not a feature.