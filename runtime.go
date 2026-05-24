package main

import (
	"bytes"
	"container/heap"
	"container/list"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Phase 2.2: In-memory queueRuntime state model.

type walAppender interface {
	Append(ctx context.Context, entries []walEntry) (firstLSN, lastLSN uint64, err error)
}

type messageRecord struct {
	ID               string
	QueueID          string
	Seq              uint64
	Body             []byte
	State            MessageState
	EnqueuedAt       time.Time
	DeliveryCount    int
	MaxDeliveryCount int

	CurrentReceiptHandle string
	CurrentDeliveryToken string
	VisibilityDeadline   time.Time

	readyElement *list.Element // list node for O(1) removal from ready list
	heapIndex    int           // index in visibilityHeap; -1 when not in heap
}

type deliveryRecord struct {
	MessageID     string
	ReceiptHandle string
	DeliveryToken string
	Deadline      time.Time
	DeliveryCount int
	seq           uint64
	heapIndex     int // index in visibilityHeap
}

var deliveryRecordSeq atomic.Uint64

type queueRuntime struct {
	mu sync.Mutex

	id     string
	config QueueConfig

	nextSeq uint64

	ready    *list.List                 // []*messageRecord — FIFO order
	messages map[string]*messageRecord  // keyed by message ID
	inflight map[string]*deliveryRecord // keyed by receiptHandle
	dead     map[string]*messageRecord

	deadlines visibilityHeap // min-heap of *deliveryRecord

	readyCh chan struct{}
	metrics *queueMetrics

	maxMessages int64
	maxBytes    int64
	bytesInMem  int64
}

func newQueueRuntime(id string, config QueueConfig, metrics *queueMetrics) *queueRuntime {
	q := &queueRuntime{
		id:          id,
		config:      config,
		ready:       list.New(),
		messages:    make(map[string]*messageRecord),
		inflight:    make(map[string]*deliveryRecord),
		dead:        make(map[string]*messageRecord),
		readyCh:     make(chan struct{}, 1),
		metrics:     metrics,
		maxMessages: parseInt64Env("KUEUE_MAX_IN_MEMORY_MESSAGES", 0),
		maxBytes:    parseInt64Env("KUEUE_MAX_IN_MEMORY_BYTES", 0),
	}
	heap.Init(&q.deadlines)
	return q
}

func parseInt64Env(name string, defaultVal int64) int64 {
	s := os.Getenv(name)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return defaultVal
	}
	return v
}

func receiptHandleForMessage(queueID string, seq uint64, messageID string) string {
	raw := queueID + "|" + strconv.FormatUint(seq, 10) + "|" + messageID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func (q *queueRuntime) signalReady() {
	select {
	case q.readyCh <- struct{}{}:
	default:
	}
}

type queueManager struct {
	mu     sync.RWMutex
	queues map[string]*queueRuntime
	wal    walAppender
}

func newQueueManager(wal walAppender) *queueManager {
	return &queueManager{
		queues: make(map[string]*queueRuntime),
		wal:    wal,
	}
}

func (qm *queueManager) getQueue(queueID string) (*queueRuntime, error) {
	qm.mu.RLock()
	q, ok := qm.queues[queueID]
	qm.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("queue %q not found", queueID)
	}
	return q, nil
}

func (qm *queueManager) CreateQueue(ctx context.Context, name string, maxRetries int) (string, error) {
	queueID := uuid.NewString()
	metrics := getOrCreateMetrics(queueID)

	entry := walEntry{
		Op: opCreateQueue,
		Payload: walCreateQueuePayload{
			QueueID:    queueID,
			Name:       name,
			MaxRetries: maxRetries,
		},
	}
	if _, _, err := qm.wal.Append(ctx, []walEntry{entry}); err != nil {
		return "", fmt.Errorf("wal append create queue: %w", err)
	}

	config := QueueConfig{Name: name, MaxRetries: maxRetries}
	q := newQueueRuntime(queueID, config, metrics)

	qm.mu.Lock()
	qm.queues[queueID] = q
	qm.mu.Unlock()

	return queueID, nil
}

func (qm *queueManager) PublishBatch(ctx context.Context, queueID string, bodies [][]byte) ([]string, error) {
	q, err := qm.getQueue(queueID)
	if err != nil {
		return nil, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	n := len(bodies)
	if n == 0 {
		return nil, nil
	}

	// Enforce memory limits.
	if q.maxMessages > 0 && int64(len(q.messages)+n) > q.maxMessages {
		return nil, errors.New("queue message limit exceeded")
	}
	var totalBytes int64
	for _, b := range bodies {
		totalBytes += int64(len(b))
	}
	if q.maxBytes > 0 && q.bytesInMem+totalBytes > q.maxBytes {
		return nil, errors.New("queue byte limit exceeded")
	}

	// Allocate seq range.
	startSeq := q.nextSeq
	q.nextSeq += uint64(n)

	records := make([]*messageRecord, n)
	now := time.Now()
	walMsgs := make([]walPublishedMessage, n)
	ids := make([]string, n)

	for i, body := range bodies {
		msgID := uuid.NewString()
		seq := startSeq + uint64(i)
		msg := &messageRecord{
			ID:               msgID,
			QueueID:          queueID,
			Seq:              seq,
			Body:             bytes.Clone(body),
			State:            StateReady,
			EnqueuedAt:       now,
			DeliveryCount:    0,
			MaxDeliveryCount: q.config.MaxRetries,
		}
		records[i] = msg
		walMsgs[i] = walPublishedMessage{
			MessageID:        msgID,
			Seq:              seq,
			Body:             msg.Body,
			EnqueuedAt:       now,
			MaxDeliveryCount: msg.MaxDeliveryCount,
		}
		ids[i] = msgID
	}

	entry := walEntry{
		Op: opPublishBatch,
		Payload: walPublishBatchPayload{
			QueueID:  queueID,
			Messages: walMsgs,
		},
	}
	if _, _, err := qm.wal.Append(ctx, []walEntry{entry}); err != nil {
		q.nextSeq = startSeq // rollback seq allocation
		return nil, fmt.Errorf("wal append publish batch: %w", err)
	}

	// WAL succeeded — install into memory.
	for _, msg := range records {
		msg.readyElement = q.ready.PushBack(msg)
		q.messages[msg.ID] = msg
	}
	q.bytesInMem += totalBytes
	q.metrics.totalPublished.Add(int64(n))
	q.metrics.readyCount.Add(int64(n))
	q.signalReady()

	return ids, nil
}

func (qm *queueManager) ClaimBatch(ctx context.Context, queueID string, max int) ([]claimedMessage, error) {
	q, err := qm.getQueue(queueID)
	if err != nil {
		return nil, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Pop up to max from ready list front.
	var popped []*messageRecord
	for i := 0; i < max; i++ {
		front := q.ready.Front()
		if front == nil {
			break
		}
		msg := front.Value.(*messageRecord)
		q.ready.Remove(front)
		msg.readyElement = nil
		popped = append(popped, msg)
	}
	if len(popped) == 0 {
		return nil, ErrNoReadyMessages
	}

	now := time.Now()
	vt := 30 * time.Second
	claims := make([]walClaimedMessage, len(popped))

	for i, msg := range popped {
		msg.State = StateInFlight
		msg.DeliveryCount++
		msg.CurrentReceiptHandle = receiptHandleForMessage(queueID, msg.Seq, msg.ID)
		msg.CurrentDeliveryToken = uuid.NewString()
		msg.VisibilityDeadline = now.Add(vt)

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

		claims[i] = walClaimedMessage{
			MessageID:          msg.ID,
			ReceiptHandle:      dr.ReceiptHandle,
			DeliveryToken:      dr.DeliveryToken,
			VisibilityDeadline: msg.VisibilityDeadline,
			DeliveryCount:      msg.DeliveryCount,
		}
	}

	entry := walEntry{
		Op: opClaimBatch,
		Payload: walClaimBatchPayload{
			QueueID: queueID,
			Claims:  claims,
		},
	}
	if _, _, err := qm.wal.Append(ctx, []walEntry{entry}); err != nil {
		// Rollback: restore popped records to ready front in reverse order
		// and remove the delivery records added to inflight/heap.
		for i := len(popped) - 1; i >= 0; i-- {
			msg := popped[i]
			rh := msg.CurrentReceiptHandle
			msg.State = StateReady
			msg.CurrentReceiptHandle = ""
			msg.CurrentDeliveryToken = ""
			msg.VisibilityDeadline = time.Time{}
			msg.DeliveryCount--
			msg.readyElement = q.ready.PushFront(msg)
			delete(q.inflight, rh)
		}
		q.deadlines = q.deadlines[:0]
		for _, dr := range q.inflight {
			dr.heapIndex = -1
			heap.Push(&q.deadlines, dr)
		}
		return nil, fmt.Errorf("wal append claim batch: %w", err)
	}

	q.metrics.readyCount.Add(-int64(len(popped)))
	q.metrics.inFlightCount.Add(int64(len(popped)))
	q.metrics.totalReceived.Add(int64(len(popped)))

	result := make([]claimedMessage, len(popped))
	for i, msg := range popped {
		result[i] = msg.toClaimedMessage()
	}
	return result, nil
}

func (msg *messageRecord) toClaimedMessage() claimedMessage {
	return claimedMessage{
		Message: Message{
			ID:                 msg.ID,
			Body:               msg.Body,
			State:              msg.State,
			EnqueuedAt:         msg.EnqueuedAt,
			DeliveryCount:      msg.DeliveryCount,
			MaxDeliveryCount:   msg.MaxDeliveryCount,
			VisibilityDeadline: msg.VisibilityDeadline,
			DeliveryAttemptID:  msg.CurrentDeliveryToken,
		},
		ReceiptHandle: msg.CurrentReceiptHandle,
	}
}

type runtimeAckResult struct {
	ReceiptHandle string
	Status        string // "ok" or "error"
	Error         string
}

func (qm *queueManager) AckBatch(ctx context.Context, queueID string, acks []AckEntry) []runtimeAckResult {
	q, err := qm.getQueue(queueID)
	if err != nil {
		results := make([]runtimeAckResult, len(acks))
		for i := range acks {
			results[i] = runtimeAckResult{
				ReceiptHandle: acks[i].ReceiptHandle,
				Status:        "error",
				Error:         err.Error(),
			}
		}
		return results
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Validate each entry and collect valid ones for WAL.
	valid := make([]*deliveryRecord, 0, len(acks))
	seen := make(map[string]bool, len(acks))
	results := make([]runtimeAckResult, len(acks))
	for i, entry := range acks {
		results[i].ReceiptHandle = entry.ReceiptHandle
		if seen[entry.ReceiptHandle] {
			results[i].Status = "error"
			results[i].Error = "duplicate receipt handle"
			continue
		}
		dr, ok := q.inflight[entry.ReceiptHandle]
		if !ok {
			results[i].Status = "error"
			results[i].Error = "receipt handle not found"
			continue
		}
		if dr.DeliveryToken != entry.DeliveryToken {
			results[i].Status = "error"
			results[i].Error = (&ErrDeliveryTokenMismatch{Expected: dr.DeliveryToken, Got: entry.DeliveryToken}).Error()
			continue
		}
		seen[entry.ReceiptHandle] = true
		valid = append(valid, dr)
		results[i].Status = "ok"
	}

	if len(valid) == 0 {
		return results
	}

	walAcks := make([]walAckedMessage, len(valid))
	for i, dr := range valid {
		walAcks[i] = walAckedMessage{
			MessageID:     dr.MessageID,
			ReceiptHandle: dr.ReceiptHandle,
			DeliveryToken: dr.DeliveryToken,
		}
	}
	entry := walEntry{
		Op: opAckBatch,
		Payload: walAckBatchPayload{
			QueueID: queueID,
			Acks:    walAcks,
		},
	}
	if _, _, err := qm.wal.Append(ctx, []walEntry{entry}); err != nil {
		for i := range results {
			results[i].Status = "error"
			results[i].Error = "wal append failed: " + err.Error()
		}
		return results
	}

	// WAL succeeded — remove from memory.
	for _, dr := range valid {
		if dr.heapIndex >= 0 && dr.heapIndex < len(q.deadlines) {
			heap.Remove(&q.deadlines, dr.heapIndex)
		}
		delete(q.inflight, dr.ReceiptHandle)
		if msg, ok := q.messages[dr.MessageID]; ok {
			q.bytesInMem -= int64(len(msg.Body))
			delete(q.messages, dr.MessageID)
		}
	}
	q.metrics.inFlightCount.Add(-int64(len(valid)))
	q.metrics.totalAcked.Add(int64(len(valid)))
	q.metrics.ackCountWindow.Add(int64(len(valid)))

	return results
}

func (qm *queueManager) Nack(ctx context.Context, queueID, receiptHandle, deliveryToken string) (MessageState, error) {
	q, err := qm.getQueue(queueID)
	if err != nil {
		return "", err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	dr, ok := q.inflight[receiptHandle]
	if !ok {
		return "", &ErrInvalidReceiptHandle{Reason: "receipt handle not found"}
	}
	if dr.DeliveryToken != deliveryToken {
		return "", &ErrDeliveryTokenMismatch{Expected: dr.DeliveryToken, Got: deliveryToken}
	}

	msg, ok := q.messages[dr.MessageID]
	if !ok {
		return "", errors.New("message not found")
	}

	// Remove from inflight and deadlines.
	if dr.heapIndex >= 0 && dr.heapIndex < len(q.deadlines) {
		heap.Remove(&q.deadlines, dr.heapIndex)
	}
	delete(q.inflight, receiptHandle)

	// Determine target state.
	var targetState MessageState
	if msg.MaxDeliveryCount > 0 && msg.DeliveryCount >= msg.MaxDeliveryCount {
		targetState = StateDead
		msg.State = StateDead
		q.dead[msg.ID] = msg
	} else {
		targetState = StateReady
		msg.State = StateReady
		msg.CurrentReceiptHandle = ""
		msg.CurrentDeliveryToken = ""
		msg.VisibilityDeadline = time.Time{}
		msg.readyElement = q.ready.PushBack(msg)
	}

	walEntryVal := walEntry{
		Op: opNack,
		Payload: walNackPayload{
			QueueID:        queueID,
			MessageID:      msg.ID,
			ReceiptHandle:  dr.ReceiptHandle,
			DeliveryToken:  dr.DeliveryToken,
			TargetState:    targetState,
			HasNewReadySeq: false,
			NewReadySeq:    0,
		},
	}
	if _, _, err := qm.wal.Append(ctx, []walEntry{walEntryVal}); err != nil {
		// Rollback: restore delivery record.
		q.inflight[receiptHandle] = dr
		dr.heapIndex = -1
		heap.Push(&q.deadlines, dr)
		if targetState == StateReady {
			q.ready.Remove(msg.readyElement)
			msg.readyElement = nil
			msg.State = StateInFlight
			msg.CurrentReceiptHandle = dr.ReceiptHandle
			msg.CurrentDeliveryToken = dr.DeliveryToken
			msg.VisibilityDeadline = dr.Deadline
		} else {
			delete(q.dead, msg.ID)
			msg.State = StateInFlight
		}
		return "", fmt.Errorf("wal append nack: %w", err)
	}

	q.metrics.inFlightCount.Add(-1)
	if targetState == StateReady {
		q.metrics.readyCount.Add(1)
		q.signalReady()
	} else {
		q.metrics.deadCount.Add(1)
	}

	return targetState, nil
}

func (qm *queueManager) ReapExpired(ctx context.Context, now time.Time) []reapTransition {
	qm.mu.RLock()
	ids := make([]string, 0, len(qm.queues))
	for id := range qm.queues {
		ids = append(ids, id)
	}
	qm.mu.RUnlock()

	var allTransitions []reapTransition

	for _, queueID := range ids {
		q, err := qm.getQueue(queueID)
		if err != nil {
			continue
		}

		q.mu.Lock()
		var transitions []reapTransition
		var reaps []walReapedMessage

		// Collect expired deliveries without mutating state yet.
		type pendingReap struct {
			dr          *deliveryRecord
			msg         *messageRecord
			targetState MessageState
		}
		var pending []pendingReap

		for len(q.deadlines) > 0 && !q.deadlines[0].Deadline.After(now) {
			dr := heap.Pop(&q.deadlines).(*deliveryRecord)
			msg, ok := q.messages[dr.MessageID]
			if !ok {
				continue
			}
			if msg.State != StateInFlight {
				continue
			}
			if msg.CurrentDeliveryToken != dr.DeliveryToken {
				continue
			}
			if msg.VisibilityDeadline.After(now) {
				continue
			}

			var targetState MessageState
			if msg.MaxDeliveryCount > 0 && msg.DeliveryCount >= msg.MaxDeliveryCount {
				targetState = StateDead
			} else {
				targetState = StateReady
			}

			pending = append(pending, pendingReap{
				dr:          dr,
				msg:         msg,
				targetState: targetState,
			})

			reaps = append(reaps, walReapedMessage{
				MessageID:             msg.ID,
				PreviousDeliveryToken: dr.DeliveryToken,
				TargetState:           targetState,
				HasNewReadySeq:        false,
				NewReadySeq:           0,
			})
			transitions = append(transitions, reapTransition{QueueID: queueID, ToState: targetState})
		}

		if len(reaps) == 0 {
			// Nothing expired, but we may have popped stale entries from the heap.
			// Push any valid inflight entries back.
			q.deadlines = q.deadlines[:0]
			for _, dr := range q.inflight {
				dr.heapIndex = -1
				heap.Push(&q.deadlines, dr)
			}
			q.mu.Unlock()
			continue
		}

		entry := walEntry{
			Op: opReapBatch,
			Payload: walReapBatchPayload{
				QueueID: queueID,
				Reaps:   reaps,
			},
		}
		if _, _, err := qm.wal.Append(ctx, []walEntry{entry}); err != nil {
			// WAL failed — no mutations were applied. The delivery records
			// are still in q.inflight. Rebuild the heap from the inflight
			// map (which is unchanged).
			q.deadlines = q.deadlines[:0]
			for _, dr := range q.inflight {
				dr.heapIndex = -1
				heap.Push(&q.deadlines, dr)
			}
			q.mu.Unlock()
			continue
		}

		// WAL succeeded — apply all pending mutations.
		for _, p := range pending {
			delete(q.inflight, p.dr.ReceiptHandle)
			msg := p.msg
			if p.targetState == StateDead {
				msg.State = StateDead
				q.dead[msg.ID] = msg
			} else {
				msg.State = StateReady
				msg.CurrentReceiptHandle = ""
				msg.CurrentDeliveryToken = ""
				msg.VisibilityDeadline = time.Time{}
				msg.readyElement = q.ready.PushBack(msg)
			}
		}

		hasReadyTransition := false
		for _, tr := range transitions {
			q.metrics.inFlightCount.Add(-1)
			if tr.ToState == StateReady {
				q.metrics.readyCount.Add(1)
				hasReadyTransition = true
			} else {
				q.metrics.deadCount.Add(1)
			}
		}
		if hasReadyTransition {
			q.signalReady()
		}

		allTransitions = append(allTransitions, transitions...)
		q.mu.Unlock()
	}

	return allTransitions
}
