# Performance Model

## Current Hot Path

The ready index makes batch receive cheap, and receipt handles make ack/nack O(1). The reaper only visits due in-flight messages.

| operation | current complexity | reason |
| --- | ---: | --- |
| batch receive | `O(batch)` | scans only the ready index prefix |
| single/batch ack | `O(batch)` | direct Pebble get/delete per ack via receipt handle |
| nack | `O(1)` | direct Pebble get/set plus optional ready pointer |
| reaper tick | `O(expired_inflight_due)` | due message transitions only |

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

The batch ack benchmarks should stay roughly flat across queue depths `100`, `1_000`, and `10_000`. The reaper benchmark should scale with the number of expired in-flight messages, not with ready backlog size.