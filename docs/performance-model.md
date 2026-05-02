# Performance Model

## Current Hot Path

Before receipt handles and the in-flight index, the ready index makes batch receive cheap, but ack/nack and the reaper still scale with stored queue depth.

| operation | current complexity | reason |
| --- | ---: | --- |
| batch receive | `O(batch)` | scans only the ready index prefix |
| single/batch ack | `O(batch * queue_depth)` | each ack calls `findMessageRecord` and scans message keys |
| nack | `O(queue_depth)` | resolves the message by scanning message keys |
| reaper tick | `O(total_db_keys)` | scans every Badger key to find expired in-flight messages |

At 10k messages this makes consume throughput dominated by lookup work, not HTTP or Badger write cost.

## Target Hot Path

The optimized path uses a receipt handle returned by receive. The handle is an encoded message key, so ack/nack can load the message directly. It also writes an `inflight|<deadline>|...` key on claim, so the reaper only visits due in-flight messages.

| operation | target complexity | expected dominant cost |
| --- | ---: | --- |
| batch receive | `O(batch)` | Badger updates and JSON encode |
| single/batch ack | `O(batch)` | direct Badger get/delete per ack |
| nack | `O(1)` | direct Badger get/set plus optional ready pointer |
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
