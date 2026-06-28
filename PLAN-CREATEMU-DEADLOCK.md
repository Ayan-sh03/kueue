# Plan: Fix createMu RWMutex Deadlock (Phase 2.8 follow-up)

## What is broken

`createMu sync.RWMutex` in `queueManager` (runtime.go) was introduced to prevent
TakeSnapshot from racing with CreateQueue. But Go's `sync.RWMutex` has a
starvation property: once a writer is waiting, new readers also block. With 8+
goroutines alternating between CreateQueue (write lock) and TakeSnapshot (read
lock), the system deadlocks:

1. Goroutine A: TakeSnapshot → `createMu.RLock()` held
2. Goroutine B: CreateQueue → `createMu.Lock()` → **blocked** (A holds RLock)
3. Goroutine C: TakeSnapshot → `createMu.RLock()` → **blocked** (B is a pending writer)
4. Goroutine A finishes and releases RLock → B gets write lock
5. Goroutine B is now inside createMu and calls `wal.Append()` → `walStore.mu.Lock()`
6. Goroutines C (TakeSnapshot, blocked at RLock) and further CreateQueue goroutines
   pile up → all goroutines stuck → deadlock

The failing test (added to snapshot_test.go but not yet committed) is
`TestCreateQueueConcurrentWithSnapshotDoesNotLoseQueue`.

## Root cause

The RWMutex is too coarse. TakeSnapshot holds the RLock for the **entire
duration** of locking all per-queue mutexes + reading nextLSN. In a loaded
system with many queues this is long enough to cause write-lock starvation.

## Recommended fix: replace RWMutex with an atomic flag + careful ordering

The real invariant is narrow: **for any WAL entry at LSN K, if
`opCreateQueue@K` is committed, then the queue is in `qm.queues` before K
can be included in a snapshot's snapshotLSN**.

This can be enforced without an RWMutex:

### Option A — Move queue install before WAL Append (simplest, preferred)

In `CreateQueue`, install the queue into `qm.queues` **before** calling
`wal.Append`. Since the queue object is live but has no messages yet, this is
safe. TakeSnapshot will either see it (and snapshot it as an empty queue) or
miss it (and the subsequent opCreateQueue will be in the tail WAL that gets
replayed). Either way is correct.

```go
func (qm *queueManager) CreateQueue(ctx context.Context, name string, maxRetries int) (string, error) {
    queueID := uuid.NewString()
    metrics := getOrCreateMetrics(queueID)
    config := QueueConfig{Name: name, MaxRetries: maxRetries}
    q := newQueueRuntime(queueID, config, metrics)

    // Install first so TakeSnapshot always sees a consistent view: if
    // opCreateQueue@K commits and snapshotLSN >= K, the queue is already
    // in qm.queues. If the WAL append fails below, the queue stays in
    // memory but has no WAL record — we remove it on error.
    qm.mu.Lock()
    qm.queues[queueID] = q
    qm.mu.Unlock()

    entry := walEntry{Op: opCreateQueue, Payload: walCreateQueuePayload{...}}
    if _, _, err := qm.wal.Append(ctx, []walEntry{entry}); err != nil {
        qm.mu.Lock()
        delete(qm.queues, queueID)
        qm.mu.Unlock()
        return "", fmt.Errorf("wal append create queue: %w", err)
    }
    return queueID, nil
}
```

Then **remove `createMu` entirely** from both `CreateQueue` and `TakeSnapshot`.

**Why this is correct:** TakeSnapshot reads `qm.queues` under `qm.mu.RLock()`
and then reads `walStore.nextLSN` (the committed LSN ceiling). After installing
the queue into qm.queues (step 1), any TakeSnapshot that captures the IDs will
include this queue. Any TakeSnapshot that doesn't capture the IDs has
`snapshotLSN < the LSN assigned to opCreateQueue` (because the Append happens
after the install and LSN allocation is monotone), so the opCreateQueue falls in
the tail WAL that gets replayed. Either branch is correct.

The WAL-append failure path (delete from qm.queues) is safe because the queue
has no messages and no external reference yet.

### Option B — Keep the concept but use a sync.Mutex (not RWMutex)

Replace `createMu sync.RWMutex` with `createMu sync.Mutex`. Make TakeSnapshot
acquire it briefly (just around the nextLSN read, not across all q.mu
acquisitions). This removes the starvation risk because both sides use `Lock()`.

This is more surgical but less elegant than Option A.

## Files to change

- `runtime.go`: Implement Option A (install before Append, rollback on error,
  remove `createMu`)
- `snapshot.go` (`TakeSnapshot`): Remove the `createMu.RLock()/RUnlock()` calls
- `snapshot_test.go`: Commit `TestCreateQueueConcurrentWithSnapshotDoesNotLoseQueue`
  after the fix (it's already written in the working tree, just not committed)

## Tests to run after fix

```bash
go test -run TestCreateQueueConcurrentWithSnapshotDoesNotLoseQueue -count=5 -timeout 60s
go test ./... -count=1 -timeout 120s
```

Run the concurrent test at least 5× with `-count=5` to confirm no flakiness.
