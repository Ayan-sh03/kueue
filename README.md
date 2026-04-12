# kueue

A persistent message queue server built in Go with BadgerDB storage.

## Features

- Multiple named queues with configurable max retries
- FIFO message ordering
- Ack/nack with at-least-once delivery
- Visibility timeout with automatic reaper for expired messages
- Dead letter queue after max delivery attempts
- Long polling on receive
- Delivery tokens to prevent duplicate acks

## API

| Endpoint | Method | Description |
| --- | --- | --- |
| `/create` | POST | Create a queue |
| `/get?id=QUEUE_ID` | GET | Get queue info |
| `/publish` | POST | Publish a message |
| `/receive?id=QUEUE_ID` | GET | Receive next ready message |
| `/receive?id=QUEUE_ID&wait=true` | GET | Long-poll for next message (30s timeout) |
| `/ack` | POST | Acknowledge a message |
| `/nack` | POST | Reject a message (returns to queue or dead-letters) |

### Create Queue

```json
POST /create
{ "name": "my-queue", "maxRetries": 3 }
```

### Publish

```json
POST /publish
{ "queueId": "<id>", "message": { "body": "<base64>" } }
```

### Ack / Nack

```json
POST /ack
{ "queueId": "<id>", "messageId": "<id>", "deliveryToken": "<token>" }
```

## Run

```bash
go run main.go
```

Environment variables:
- `KUEUE_DB_PATH` - BadgerDB data directory (default: `./tmp/badger`)
- `PORT` - Server port (default: `8080`)

## Benchmark

```bash
go run ./cmd/bench --targets kueue,rabbitmq
```

## Test

```bash
go test ./...
```