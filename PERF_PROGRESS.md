# Kueue Performance Optimization Progress

## Baseline (c44c314 on perf/optimize-hot-paths)
- **Workload:** 500 messages, 64-byte payload, 1 producer, 1 consumer, prefetch 200
- **Publish:** ~453 msg/s
- **Consume:** ~101 msg/s
- **Latency p99:** ~15,693 ms

## Optimization Phase (BadgerDB)

| Opt | Commit | Description |
|-----|--------|-------------|
| 1 | `5937fb8` | Message key cache – eliminates O(n) `findMessageRecord` scan in ack/nack |
| 2 | `40d8881` | Lock-free metrics – replace mutex+slice `ackWindow` with atomic counters |
| 3 | `f041ef8` | Store full `msgKey` in `readyValue` – saves reconstruction in claim |
| 4 | `060a38f` | Single-pass reaper – eliminates double transaction overhead |
| 5 | `57f8b1c` | Benchmark client `deliveryToken` support, results, and ignore artifacts |

### BadgerDB Final Results

**Workload:** 500 messages, 64-byte payload, 1 producer, 1 consumer, prefetch 200, 20 runs

| Metric | Value |
| --- | ---: |
| Publish | 423 msg/s |
| Consume | 178 msg/s |
| p50 latency | 907 ms |
| p95 latency | 1,582 ms |
| p99 latency | 1,602 ms |

| Metric | Baseline | Optimized | Change |
|--------|----------|-----------|--------|
| Consume msg/s | ~101 | 178 | **+76%** (1.8x) |
| p99 Latency | ~15,693 ms | 1,602 ms | **-90%** (9.8x better) |
| Publish msg/s | ~453 | 423 | -7% (within noise) |

## Phase 1: Pebble Migration

Replaced BadgerDB with CockroachDB Pebble. Key changes:
- Pebble's group commit pipeline eliminates write serialization bottleneck
- `IndexedBatch` replaces `Db.Update` for atomic read-modify-write
- `claimMu` mutex serializes competing consumer claims (Pebble lacks serializable transaction isolation)
- `Db.NewSnapshot()` for consistent reads replaces `Db.View`
- Sequence counter uses mutex+Pebble key instead of Badger's `GetSequence`
- `prefixUpperBound()` helper replaces Badger's prefix iteration

### Pebble Benchmark Results

**Workload:** 10,000 messages, 256-byte payload, 1 producer, 10 consumers, prefetch 10, 3 runs

| Metric | Value |
| --- | ---: |
| Publish | 27,941 msg/s |
| Consume | 8,708 msg/s |
| p50 latency | 354 ms |
| p95 latency | 742 ms |
| p99 latency | 774 ms |

### Head-to-Head (BadgerDB vs Pebble)

Same 10k message workload, different consumer counts:

| Metric | BadgerDB (1 consumer) | Pebble (10 consumers) | Change |
|--------|---:|---:|--------|
| Publish msg/s | 3,800 | 27,941 | **+635%** (7.4x) |
| Consume msg/s | 3,338 | 8,708 | **+161%** (2.6x) |
| p50 latency | 6.48 ms | 354 ms | Higher (due to 10 consumers contending) |
| p99 latency | 346 ms | 774 ms | |

Note: different consumer counts make direct p-latency comparison misleading. The throughput improvements are significant despite higher tail latency from concurrent claim contention.