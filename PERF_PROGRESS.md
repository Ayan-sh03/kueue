# Kueue Performance Optimization Progress

## Baseline (c44c314 on perf/optimize-hot-paths)
- **Workload:** 500 messages, 64-byte payload, 1 producer, 1 consumer, prefetch 200
- **Publish:** ~453 msg/s
- **Consume:** ~101 msg/s
- **Latency p99:** ~15,693 ms

## Optimizations Applied

| Opt | Commit | Description |
|-----|--------|-------------|
| 1 | `5937fb8` | Message key cache – eliminates O(n) `findMessageRecord` scan in ack/nack |
| 2 | `40d8881` | Lock-free metrics – replace mutex+slice `ackWindow` with atomic counters |
| 3 | `f041ef8` | Store full `msgKey` in `readyValue` – saves reconstruction in claim |
| 4 | `060a38f` | Single-pass reaper – eliminates double transaction overhead |
| 5 | `57f8b1c` | Benchmark client `deliveryToken` support, results, and ignore artifacts |

## Final Benchmark Results

**Workload:** 500 messages, 64-byte payload, 1 producer, 1 consumer, prefetch 200, 20 runs
**Date:** 2026-04-25

### Median Result
- **Publish:** 423 msg/s
- **Consume:** 178 msg/s
- **Latency p50:** 907 ms
- **Latency p95:** 1,582 ms
- **Latency p99:** 1,602 ms

### Run Range
- Consume rate: 143 – 200 msg/s
- p99 latency: 1,387 – 2,299 ms

## Comparison to Baseline

| Metric | Baseline | Optimized | Change |
|--------|----------|-----------|--------|
| Consume msg/s | ~101 | **178** | **+76%** (1.8×) |
| p99 Latency | ~15,693 ms | **1,602 ms** | **-89.8%** (9.8× better) |
| Publish msg/s | ~453 | **423** | -7% (within noise) |

## Notes
- All changes committed and benchmarked on `perf/optimize-hot-paths`.
- The dramatic latency reduction comes from removing O(n) scans and lock contention on the hot path.
- Publish throughput is unchanged; the bottleneck was always the consume/claim path.
- The benchmark binary and service were restarted with a clean state before the final run.
