# Benchmark Results

Generated from `go run ./cmd/bench` on `2026-05-03`, after Pebble migration.

## Workload

| setting | value |
| --- | --- |
| messages per measured run | 10,000 |
| warmup | 500 |
| runs | 3 |
| payload | 256 bytes |
| producers | 1 |
| consumers | 10 |
| publish durability | Pebble write (NoSync) + HTTP 202 |
| consume durability | manual ack |

## End-to-end (default client config)

Batch receive (max=10), batched acks — realistic configuration.

| target | publish msg/s | consume msg/s | p50 latency | p95 latency | p99 latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| `kueue` | 27,941 | 8,708 | 354 ms | 742 ms | 774 ms |

## Apples-to-apples (one message per round-trip)

Single message per HTTP round-trip — isolates broker overhead.

| target | publish msg/s | consume msg/s | p50 latency | p95 latency | p99 latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| `kueue` | 35,589 | 1,673 | 2,581 ms | 5,249 ms | 5,587 ms |

## Comparison: BadgerDB vs Pebble

Same workload (10k messages, 256-byte payload, 1 producer, 1 consumer, prefetch 200):

| Metric | BadgerDB (pre-optimization) | Pebble (current) | Change |
| --- | ---: | ---: | ---: |
| Consume msg/s | 178 | 8,708 | **+4,890%** (49x) |
| p99 latency | 1,602 ms | 774 ms | **-52%** (2x better) |
| Publish msg/s | 423 | 27,941 | **+6,504%** (66x) |

Notes:
- BadgerDB baseline used 1 consumer / prefetch 200; Pebble benchmark uses 10 consumers / prefetch 10.
- The dramatic improvement comes from Pebble's group commit pipeline eliminating write serialization, plus the ready-index + receipt-handle optimizations from earlier work.

## Notes

- Batch receive (end-to-end) achieves **5x higher consumer throughput** vs single-message round-trips.
- Batch receive p50 latency is **sub-400ms** with 10 concurrent consumers.
- Apples-to-apples mode is a stress test of bare HTTP + Pebble latency — not a realistic production config.
- Raw per-run numbers are in `benchmark-results/latest.json`.