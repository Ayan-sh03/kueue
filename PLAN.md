# Kueue Throughput Plan

## Summary

The merged batch receive/ack branch improved the benchmark harness, but the server still has two algorithmic hot spots:

- `ack`, `batch ack`, and `nack` resolve messages by scanning the queue prefix with `findMessageRecord`.
- `reapExpiredMessages` scans the whole Badger database every second.

The next optimization pass should make ack/nack direct-key operations and make the reaper scan only due in-flight messages. Existing benchmark data can be reset; no old Badger format migration is required.

## Target Model

Current complexity:

- Batch receive claim: `O(batch)`.
- Batch ack/nack: `O(batch * queue_depth)` because each entry calls `findMessageRecord`.
- Reaper: `O(total_db_keys)` every tick.

Target complexity:

- Batch receive claim: `O(batch)`.
- Batch ack/nack: `O(batch)` using direct receipt handles.
- Reaper: `O(expired_inflight_due)` using an in-flight deadline index.

After this change, HTTP, JSON encoding, and Badger write cost should dominate instead of queue-depth scans.

## Server Changes

- Add receipt handles to receive responses.
  - Single receive returns `id`, `body`, `state`, `deliveryToken`, and `receiptHandle`.
  - Batch receive returns the same fields per message under `messages`.
  - `id` remains for logging and metrics, but ack/nack no longer use it for lookup.
- Change ack/nack request bodies to use direct handles.
  - Single ack/nack: `{"queueId","receiptHandle","deliveryToken"}`.
  - Batch ack: `{"queueId","acks":[{"receiptHandle","deliveryToken"}]}`.
  - Decode `receiptHandle` into the Badger message key, verify the queue prefix, verify `StateInFlight`, verify the delivery token, then update/delete directly.
- Add an in-flight deadline index.
  - On claim, write `inflight|<deadline>|<queueID>|<messageID>` with value equal to the message key bytes.
  - Store enough data in the message or index value to delete the exact in-flight key on ack/nack.
  - On ack, delete the message and its in-flight index key.
  - On nack, delete the in-flight index key; if requeued, write a ready pointer.
  - Reaper iterates `inflight|` keys up to `now`, transitions only due entries, and deletes consumed in-flight index keys.
- Keep ready-index FIFO behavior unchanged.
  - Publish writes message key plus ready pointer.
  - Receive claims from the ready prefix.
  - Nacked/reaped messages that become ready receive a new ready sequence and go to the back of the ready queue.

## Benchmark And Docs

- Add `docs/performance-model.md` with the current and target complexity model, expected bottleneck shift, and benchmark commands.
- Add Go microbenchmarks for:
  - Batch receive only at depths `100`, `1_000`, and `10_000`.
  - Batch ack only at depths `100`, `1_000`, and `10_000`.
  - Receive plus ack batch loop at depths `100`, `1_000`, and `10_000`.
  - Reaper with many ready messages and few expired in-flight messages.
- Update `cmd/bench` to parse `receiptHandle`, send the new ack shape, and keep failing on partial batch ack errors.

## Test Plan

- Run `go test ./...`.
- Add unit tests for:
  - Ack deletes by `receiptHandle` without scanning `findMessageRecord`.
  - Ack rejects malformed receipt handles with `400`.
  - Ack rejects a receipt handle for the wrong queue.
  - Ack rejects stale or wrong delivery tokens with `409`.
  - Batch ack reports per-entry success/error.
  - Nack by receipt handle requeues or dead-letters correctly.
  - Reaper restores expired messages using the in-flight index.
  - Reaper wakes long-poll consumers only for ready transitions, not dead-letter transitions.
- Run benchmark checks:
  - `go test -run ^$ -bench BenchmarkBatch -benchmem`
  - `go test -run ^$ -bench BenchmarkReaper -benchmem`
  - `go run ./cmd/bench --targets=kueue --messages=10000 --warmup=500 --runs=3`

## Assumptions

- Breaking receive/ack/nack JSON shapes is acceptable.
- Existing Badger data can be discarded for this optimization branch.
- The first implementation pass covers ack/nack lookup and reaper indexing only.
- Durability settings remain the current Badger defaults.
