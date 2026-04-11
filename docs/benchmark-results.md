# Benchmark Results

Generated from `benchmark-results/latest.json` on `2026-04-11`.

## Workload

| setting | value |
| --- | --- |
| messages per measured run | 10,000 |
| warmup | 500 |
| runs | 3 |
| payload | 256 bytes |
| producers | 1 |
| consumers | 1 |
| publish durability | `kueue` HTTP 202 after Badger write, RabbitMQ publisher confirms |
| consume durability | manual ack |

## Median Result

| target | publish msg/s | consume msg/s | p50 latency | p95 latency | p99 latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| `kueue` | 2864 | 309 | 9150.22 ms | 27754.93 ms | 30344.54 ms |
| `rabbitmq` | 341 | 341 | 2.16 ms | 2.91 ms | 3.62 ms |

## Notes

- `kueue` publishes faster than RabbitMQ under this strict single-message confirm workload.
- `kueue` consumes much slower and its end-to-end latency is orders of magnitude higher on the current implementation.
- Raw per-run numbers are in `benchmark-results/latest.json`.
