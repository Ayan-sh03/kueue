# Kueue Performance Optimization Progress

## Baseline (c44c314 on perf/optimize-hot-paths)
- Publish: ~453 msg/s
- Consume: ~101 msg/s  
- Latency p99: ~15,693 ms

## Root Causes Identified
1. `findMessageRecord` does O(n) full prefix scan for every ack/nack
2. `claimNextReadyMessage` does extra txn.Get() + txn.Delete() per claim
3. Metrics ackWindow uses mutex + slice append on every ack
4. Reaper does two-pass scan + allocates new sequences
5. Publish writes 2 keys instead of 1

## Optimization Plan
- [ ] Opt 1: Message key cache (eliminates O(n) findMessageRecord scan)
- [ ] Opt 2: Store full msgKey in readyValue (saves reconstruct in claim)
- [ ] Opt 3: Lock-free metrics (atomic ring buffer instead of mutex+slice)
- [ ] Opt 4: Single-pass reaper with batch sequence allocation
- [ ] Opt 5: Benchmark each change
- [ ] Opt 6: Overnight iterations via cron
