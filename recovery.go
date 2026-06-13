package main

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// Phase 2.3: WAL replay into queueManager.

var (
	// QueueManager is the process-wide in-memory queue runtime.
	QueueManager *queueManager
	// WAL is the durable append-only log backing QueueManager.
	WAL *walStore
)

// recoverQueueManager opens the WAL store, creates an empty queueManager, and
// replays all WAL entries after the latest snapshot LSN. After replay it runs
// one reaper pass for in-flight deliveries that already expired while the
// process was down. Any replay error is returned and startup must not proceed.
func recoverQueueManager(ctx context.Context, db *pebble.DB, syncMode walSyncMode) (*queueManager, *walStore, error) {
	wal, err := newWalStore(db, syncMode)
	if err != nil {
		return nil, nil, fmt.Errorf("open wal store: %w", err)
	}

	qm := newQueueManager(wal)

	if err := wal.Replay(ctx, wal.latestSnapshotLSN, qm.ApplyWALEntry); err != nil {
		return nil, nil, fmt.Errorf("replay wal: %w", err)
	}

	// Drain deliveries whose visibility timeout already passed while we were
	// down. This appends reap entries to the WAL so the transitions are durable.
	qm.ReapExpired(ctx, time.Now())

	return qm, wal, nil
}

// initQueueManagerFromEnv is a convenience wrapper that reads KUEUE_WAL_SYNC
// from the environment and recovers the queue manager from the supplied DB.
func initQueueManagerFromEnv(ctx context.Context, db *pebble.DB) (*queueManager, *walStore, error) {
	syncMode, err := walSyncModeFromEnv()
	if err != nil {
		return nil, nil, err
	}
	return recoverQueueManager(ctx, db, syncMode)
}

// ApplyWALEntry applies a single WAL entry to the in-memory queue manager.
// It is used during recovery and must be deterministic: the same WAL order
// always produces the same final state. Inconsistent records fail loudly.
func (qm *queueManager) ApplyWALEntry(entry walEntry) error {
	switch entry.Op {
	case opCreateQueue:
		p, ok := entry.Payload.(walCreateQueuePayload)
		if !ok {
			return fmt.Errorf("LSN %d: invalid create queue payload type %T", entry.LSN, entry.Payload)
		}
		return qm.applyCreateQueue(p)
	case opPublishBatch:
		p, ok := entry.Payload.(walPublishBatchPayload)
		if !ok {
			return fmt.Errorf("LSN %d: invalid publish batch payload type %T", entry.LSN, entry.Payload)
		}
		return qm.applyPublishBatch(p)
	case opClaimBatch:
		p, ok := entry.Payload.(walClaimBatchPayload)
		if !ok {
			return fmt.Errorf("LSN %d: invalid claim batch payload type %T", entry.LSN, entry.Payload)
		}
		return qm.applyClaimBatch(p)
	case opAckBatch:
		p, ok := entry.Payload.(walAckBatchPayload)
		if !ok {
			return fmt.Errorf("LSN %d: invalid ack batch payload type %T", entry.LSN, entry.Payload)
		}
		return qm.applyAckBatch(p)
	case opNack:
		p, ok := entry.Payload.(walNackPayload)
		if !ok {
			return fmt.Errorf("LSN %d: invalid nack payload type %T", entry.LSN, entry.Payload)
		}
		return qm.applyNack(p)
	case opReapBatch:
		p, ok := entry.Payload.(walReapBatchPayload)
		if !ok {
			return fmt.Errorf("LSN %d: invalid reap batch payload type %T", entry.LSN, entry.Payload)
		}
		return qm.applyReapBatch(p)
	default:
		return fmt.Errorf("LSN %d: unknown WAL op %d", entry.LSN, entry.Op)
	}
}

func (qm *queueManager) applyCreateQueue(p walCreateQueuePayload) error {
	if p.QueueID == "" {
		return errors.New("create queue: empty queue id")
	}

	qm.mu.Lock()
	defer qm.mu.Unlock()

	if _, exists := qm.queues[p.QueueID]; exists {
		return fmt.Errorf("create queue: queue %q already exists", p.QueueID)
	}

	config := QueueConfig{Name: p.Name, MaxRetries: p.MaxRetries}
	qm.queues[p.QueueID] = newQueueRuntime(p.QueueID, config, getOrCreateMetrics(p.QueueID))
	return nil
}

func (qm *queueManager) applyPublishBatch(p walPublishBatchPayload) error {
	q, err := qm.getQueue(p.QueueID)
	if err != nil {
		return fmt.Errorf("publish batch: %w", err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	var totalBytes int64
	for _, wm := range p.Messages {
		if wm.MessageID == "" {
			return errors.New("publish batch: empty message id")
		}
		if _, exists := q.messages[wm.MessageID]; exists {
			return fmt.Errorf("publish batch: message %q already exists", wm.MessageID)
		}

		msg := &messageRecord{
			ID:               wm.MessageID,
			QueueID:          p.QueueID,
			Seq:              wm.Seq,
			Body:             wm.Body,
			State:            StateReady,
			EnqueuedAt:       wm.EnqueuedAt,
			DeliveryCount:    0,
			MaxDeliveryCount: wm.MaxDeliveryCount,
		}
		msg.readyElement = q.ready.PushBack(msg)
		q.messages[msg.ID] = msg

		if msg.Seq >= q.nextSeq {
			q.nextSeq = msg.Seq + 1
		}
		totalBytes += int64(len(msg.Body))
	}

	q.bytesInMem += totalBytes
	q.metrics.totalPublished.Add(int64(len(p.Messages)))
	q.metrics.readyCount.Add(int64(len(p.Messages)))
	return nil
}

func (qm *queueManager) applyClaimBatch(p walClaimBatchPayload) error {
	q, err := qm.getQueue(p.QueueID)
	if err != nil {
		return fmt.Errorf("claim batch: %w", err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, wc := range p.Claims {
		msg, ok := q.messages[wc.MessageID]
		if !ok {
			return fmt.Errorf("claim batch: message %q not found", wc.MessageID)
		}
		if msg.State != StateReady {
			return fmt.Errorf("claim batch: message %q is %q, want ready", wc.MessageID, msg.State)
		}

		// Remove from ready list.
		if msg.readyElement == nil {
			return fmt.Errorf("claim batch: message %q has no ready element", wc.MessageID)
		}
		q.ready.Remove(msg.readyElement)
		msg.readyElement = nil

		msg.State = StateInFlight
		msg.DeliveryCount = wc.DeliveryCount
		msg.CurrentReceiptHandle = wc.ReceiptHandle
		msg.CurrentDeliveryToken = wc.DeliveryToken
		msg.VisibilityDeadline = wc.VisibilityDeadline

		dr := &deliveryRecord{
			MessageID:     msg.ID,
			ReceiptHandle: msg.CurrentReceiptHandle,
			DeliveryToken: msg.CurrentDeliveryToken,
			Deadline:      msg.VisibilityDeadline,
			DeliveryCount: msg.DeliveryCount,
			seq:           deliveryRecordSeq.Add(1),
		}
		q.inflight[dr.ReceiptHandle] = dr
		heap.Push(&q.deadlines, dr)
	}

	q.metrics.readyCount.Add(-int64(len(p.Claims)))
	q.metrics.inFlightCount.Add(int64(len(p.Claims)))
	q.metrics.totalReceived.Add(int64(len(p.Claims)))
	return nil
}

func (qm *queueManager) applyAckBatch(p walAckBatchPayload) error {
	q, err := qm.getQueue(p.QueueID)
	if err != nil {
		return fmt.Errorf("ack batch: %w", err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, wa := range p.Acks {
		dr, ok := q.inflight[wa.ReceiptHandle]
		if !ok {
			return fmt.Errorf("ack batch: receipt handle %q not found", wa.ReceiptHandle)
		}
		if dr.DeliveryToken != wa.DeliveryToken {
			return fmt.Errorf("ack batch: delivery token mismatch for receipt handle %q", wa.ReceiptHandle)
		}
		if dr.MessageID != wa.MessageID {
			return fmt.Errorf("ack batch: message ID mismatch for receipt handle %q (inflight=%q, wal=%q)", wa.ReceiptHandle, dr.MessageID, wa.MessageID)
		}

		if dr.heapIndex >= 0 && dr.heapIndex < len(q.deadlines) {
			heap.Remove(&q.deadlines, dr.heapIndex)
		}
		delete(q.inflight, wa.ReceiptHandle)

		if msg, ok := q.messages[wa.MessageID]; ok {
			q.bytesInMem -= int64(len(msg.Body))
			delete(q.messages, wa.MessageID)
		}
	}

	q.metrics.inFlightCount.Add(-int64(len(p.Acks)))
	q.metrics.totalAcked.Add(int64(len(p.Acks)))
	return nil
}

func (qm *queueManager) applyNack(p walNackPayload) error {
	q, err := qm.getQueue(p.QueueID)
	if err != nil {
		return fmt.Errorf("nack: %w", err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	dr, ok := q.inflight[p.ReceiptHandle]
	if !ok {
		return fmt.Errorf("nack: receipt handle %q not found", p.ReceiptHandle)
	}
	if dr.DeliveryToken != p.DeliveryToken {
		return fmt.Errorf("nack: delivery token mismatch for receipt handle %q", p.ReceiptHandle)
	}
	if dr.MessageID != p.MessageID {
		return fmt.Errorf("nack: message ID mismatch for receipt handle %q (inflight=%q, wal=%q)", p.ReceiptHandle, dr.MessageID, p.MessageID)
	}

	msg, ok := q.messages[p.MessageID]
	if !ok {
		return fmt.Errorf("nack: message %q not found", p.MessageID)
	}

	if dr.heapIndex >= 0 && dr.heapIndex < len(q.deadlines) {
		heap.Remove(&q.deadlines, dr.heapIndex)
	}
	delete(q.inflight, p.ReceiptHandle)

	if err := q.applyNackOrReapToMessage(msg, p.TargetState, p.HasNewReadySeq, p.NewReadySeq); err != nil {
		return err
	}
	q.metrics.totalNacked.Add(1)
	return nil
}

func (qm *queueManager) applyReapBatch(p walReapBatchPayload) error {
	q, err := qm.getQueue(p.QueueID)
	if err != nil {
		return fmt.Errorf("reap batch: %w", err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, wr := range p.Reaps {
		msg, ok := q.messages[wr.MessageID]
		if !ok {
			return fmt.Errorf("reap batch: message %q not found", wr.MessageID)
		}
		if msg.State != StateInFlight {
			return fmt.Errorf("reap batch: message %q is %q, want in_flight", wr.MessageID, msg.State)
		}
		if msg.CurrentDeliveryToken != wr.PreviousDeliveryToken {
			return fmt.Errorf("reap batch: delivery token mismatch for message %q", wr.MessageID)
		}

		// Locate the delivery record via the message's current receipt handle.
		dr, ok := q.inflight[msg.CurrentReceiptHandle]
		if !ok {
			return fmt.Errorf("reap batch: delivery record for message %q not found", wr.MessageID)
		}
		if dr.DeliveryToken != wr.PreviousDeliveryToken {
			return fmt.Errorf("reap batch: delivery record token mismatch for message %q", wr.MessageID)
		}

		if dr.heapIndex >= 0 && dr.heapIndex < len(q.deadlines) {
			heap.Remove(&q.deadlines, dr.heapIndex)
		}
		delete(q.inflight, dr.ReceiptHandle)

		if err := q.applyNackOrReapToMessage(msg, wr.TargetState, wr.HasNewReadySeq, wr.NewReadySeq); err != nil {
			return fmt.Errorf("reap batch: %w", err)
		}
	}

	return nil
}

// applyNackOrReapToMessage transitions a message from in_flight to ready or
// dead. It assumes the caller holds q.mu and has already removed the delivery
// record from q.inflight and the deadline heap. It updates metrics but does
// not signal readyCh (recovery has no waiting consumers).
func (q *queueRuntime) applyNackOrReapToMessage(msg *messageRecord, targetState MessageState, hasNewReadySeq bool, newReadySeq uint64) error {
	switch targetState {
	case StateDead:
		msg.State = StateDead
		q.dead[msg.ID] = msg
		q.metrics.deadCount.Add(1)
	case StateReady:
		msg.State = StateReady
		msg.CurrentReceiptHandle = ""
		msg.CurrentDeliveryToken = ""
		msg.VisibilityDeadline = time.Time{}
		if hasNewReadySeq {
			msg.Seq = newReadySeq
			if newReadySeq >= q.nextSeq {
				q.nextSeq = newReadySeq + 1
			}
		}
		msg.readyElement = q.ready.PushBack(msg)
		q.metrics.readyCount.Add(1)
	default:
		return fmt.Errorf("invalid target state %q", targetState)
	}

	q.metrics.inFlightCount.Add(-1)
	return nil
}
