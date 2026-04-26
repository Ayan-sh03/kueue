# Benchmark Results

Generated from `go run ./cmd/bench` on `2026-04-25`.

## Workload

| setting | value |
| --- | --- |
| messages per measured run | 10,000 |
| warmup | 500 |
| runs | 3 |
| payload | 256 bytes |
| producers | 1 |
| consumers | 1 |
| publish durability | `kueue` HTTP 202 after Badger write |
| consume durability | manual ack |

## End-to-end (default client config)

Batch receive (max=10), batched acks — realistic configuration.

| target | publish msg/s | consume msg/s | p50 latency | p95 latency | p99 latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| `kueue` | 3800 | 3338 | 6.48 ms | 288.61 ms | 345.62 ms |

## Apples-to-apples (one message per round-trip)

Single message per HTTP round-trip — isolates broker overhead.

| target | publish msg/s | consume msg/s | p50 latency | p95 latency | p99 latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| `kueue` | 3315 | 323 | 8264.15 ms | 23804.66 ms | 25632.81 ms |

## Notes

- Batch receive (end-to-end) achieves **10x higher consumer throughput** vs single-message round-trips.
- Batch receive p50 latency is **sub-10ms**, demonstrating the ready-index O(1) claim path.
- Apples-to-apples mode is a stress test of bare HTTP + BadgerDB latency — not a realistic production config.
- Raw per-run numbers are in `benchmark-results/latest.json`.
