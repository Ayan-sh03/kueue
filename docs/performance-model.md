# Performance Model

## Current Hot Path

All handlers route through the in-memory `queueRuntime` (the source of truth) with WAL persistence. Ready messages live in a doubly-linked list; in-flight messages live in a map + deadline min-heap; ack/nack resolve by receipt handle without any key scan.

| operation | current complexity | reason |
| --- | ---: | --- |
| publish | `O(1)` | append to ready-list tail + one WAL entry (batch amortized) |
| batch receive | `O(batch)` | pop from ready-list front + one WAL entry for the batch |
| single/batch ack | `O(batch)` | direct in-flight map lookup per receipt handle + one WAL entry |
| nack | `O(1)` | direct in-flight map lookup + one WAL entry |
| reaper tick | `O(expired * log n)` | pop from per-queue deadline min-heap; does not touch ready messages |

## Benchmark Commands

Run focused microbenchmarks:

```powershell
go test -run ^$ -bench BenchmarkBatch -benchmem
go test -run ^$ -bench BenchmarkReaper -benchmem
```

Run the end-to-end benchmark:

```powershell
go run ./cmd/bench --targets=kueue --messages=10000 --warmup=500 --runs=3
```

## Expected Reading

The batch receive/ack benchmarks should stay roughly flat across queue depths `100`, `1_000`, and `10_000` (in-memory list ops, no scan). The reaper benchmark should scale with the number of expired in-flight messages, not with ready backlog size.

## Legacy vs Runtime Performance

Measured on a 12th Gen Intel i5-1235U (Windows). Legacy path = pre-Phase-2.4 (`d149620`, Pebble hot path). Runtime path = Phase 2.5 (this branch).

| Benchmark | Legacy (ns/op) | Runtime (ns/op) | Speedup |
| --- | ---: | ---: | ---: |
| ReceiveLatencyVsDepth/depth_100 | 176,000 | 22,933 | **7.7x** |
| ReceiveLatencyVsDepth/depth_1000 | 205,000 | 24,265 | **8.4x** |
| ReceiveLatencyVsDepth/depth_10000 | 2,807,000 | 26,052 | **108x** |
| BatchReceiveOnly/depth_100 | 2,316,000 | 37,383 | **62x** |
| BatchReceiveOnly/depth_1000 | 2,344,000 | 45,920 | **51x** |
| BatchReceiveOnly/depth_10000 | 2,382,000 | 44,731 | **53x** |
| BatchAckOnly/depth_100 | 334,000 | 87,161 | **3.8x** |
| ReaperDueInflightIndex/ready_100 | — | 1,965 | — |
| ReaperDueInflightIndex/ready_10000 | — | 2,094 | — |

The legacy path did a Pebble iterator scan + JSON unmarshal + `IndexedBatch` commit per claim. The runtime pops the front of an in-memory linked list in O(1) and appends one WAL entry per batch. At depth 10000, the iterator had to skip 10000 keys (2.8ms vs 26us). The reaper uses a min-heap of deadlines, so it never touches ready messages or scans all keys.