# Benchmarking

## Default workload

| setting | value |
| --- | --- |
| queue shape | single durable queue |
| publish safety | sync publish completion (`kueue` HTTP 202, RabbitMQ publisher confirms) |
| consume safety | manual ack after receipt |
| producers | 1 |
| consumers | 1 |
| warmup | 500 messages |
| measured run | 10,000 messages |
| payload size | 256 bytes |
| runs | 3 |
| reported stats | publish msg/s, consume msg/s, p50/p95/p99 end-to-end latency |

## Why this setup

- RabbitMQ’s docs recommend publisher confirms and consumer acknowledgements when data safety matters: https://www.rabbitmq.com/docs/confirms
- RabbitMQ’s own performance suite keeps a `1 publisher / 1 consumer` test as a standard baseline: https://www.rabbitmq.com/blog/2023/05/17/rabbitmq-3.12-performance-improvements
- RabbitMQ guidance also notes that consumer prefetch materially affects throughput; `100-300` is a practical range, so the runner defaults to `200`: https://www.rabbitmq.com/docs/confirms

## Run

```powershell
pwsh -File .\scripts\run-benchmark.ps1
```

The wrapper starts:

- `kueue` on `127.0.0.1:18080`
- RabbitMQ on `127.0.0.1:5673`

## Output

- Console summary table
- Raw JSON report at `benchmark-results/latest.json`

## Direct runner

If `kueue` is already running and RabbitMQ is already running:

```powershell
go run .\cmd\bench --targets kueue,rabbitmq --kueue-url http://127.0.0.1:8080 --rabbitmq-uri amqp://guest:guest@127.0.0.1:5672/ --messages 10000 --warmup 500 --runs 3 --payload-bytes 256 --producers 1 --consumers 1 --prefetch 200 --json-out benchmark-results/latest.json
```
