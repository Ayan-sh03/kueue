package main

import (
	"bytes"
	"container/heap"
	"container/list"
	"context"
	"encoding/base64"
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

func init() {
	// Batch crypto/rand reads for UUID generation. UUIDs are minted on every
	// publish (message ID) and every claim (delivery token), so the default
	// per-call rand syscall is a measurable throughput tax. The pool amortizes
	// it across many UUIDs. Safe for our usage: we never fork.
	uuid.EnableRandPool()
}

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

	// notifyCh wakes long-polling receivers. close-and-replace under notifyMu
	// (a dedicated lock, never held with q.mu) gives lost-wakeup-free signaling
	// without any global, cross-queue contention.
	notifyMu sync.Mutex
	notifyCh chan struct{}

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
		notifyCh:    make(chan struct{}),
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
	// Build the raw handle directly into a byte buffer to avoid the intermediate
	// string concatenation + []byte conversion allocations on the claim hot path.
	// Output is byte-identical to base64(queueID|seq|messageID).
	raw := make([]byte, 0, len(queueID)+1+20+1+len(messageID))
	raw = append(raw, queueID...)
	raw = append(raw, '|')
	raw = strconv.AppendUint(raw, seq, 10)
	raw = append(raw, '|')
	raw = append(raw, messageID...)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func (q *queueRuntime) signalReady() {
	select {
	case q.readyCh <- struct{}{}:
	default:
	}
}

// waitChan returns the current ready-notification channel. Callers select on it
// after re-checking for ready messages; the re-check prevents lost signals.
func (q *queueRuntime) waitChan() <-chan struct{} {
	q.notifyMu.Lock()
	ch := q.notifyCh
	q.notifyMu.Unlock()
	return ch
}

// notify wakes all current waiters by closing the notification channel and
// installing a fresh one for future waiters.
func (q *queueRuntime) notify() {
	q.notifyMu.Lock()
	close(q.notifyCh)
	q.notifyCh = make(chan struct{})
	q.notifyMu.Unlock()
}

type queueManager struct {
	mu     sync.RWMutex
	queues map[string]*queueRuntime
	wal    walAppender

	// walStore is the concrete store when the manager is backed by a real
	// Pebble WAL (nil in tests that use fakeWAL). It is required for snapshots
	// and WAL compaction.
	walStore *walStore

	// snapshot configuration + trigger state. Populated only on the real
	// (walStore-backed) path; fakeWAL-backed managers leave these zero and
	// maybeSnapshot becomes a no-op.
	snapshotCfg   snapshotConfig
	lastSnapshotMu sync.Mutex
	lastSnapshotAt time.Time
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
		return nil, fmt.Errorf("%w: %q", ErrQueueNotFound, queueID)
	}
	return q, nil
}

func (qm *queueManager) CreateQueue(ctx context.Context, name string, maxRetries int) (string, error) {
	queueID := uuid.NewString()
	metrics := getOrCreateMetrics(queueID)

	config := QueueConfig{Name: name, MaxRetries: maxRetries}
	q := newQueueRuntime(queueID, config, metrics)

	// Install the queue into qm.queues *before* the WAL append. This makes
	// the snapshot invariant self-synchronizing: any snapshot that captures
	// this queue ID will have snapshotLSN >= the opCreateQueue LSN (the
	// append is serialized by the per-queue mutex and the WAL lock, and LSNs
	// are monotone), so the create entry is below the replay boundary. Any
	// snapshot that misses the ID has snapshotLSN < the opCreateQueue LSN,
	// so the create entry falls in the tail WAL that gets replayed.
	// applyCreateQueue is idempotent, so the rare case where the queue is
	// snapshotted empty and its create entry is also tail-replayed is safe.
	qm.mu.Lock()
	qm.queues[queueID] = q
	qm.mu.Unlock()

	entry := walEntry{
		Op: opCreateQueue,
		Payload: walCreateQueuePayload{
			QueueID:    queueID,
			Name:       name,
			MaxRetries: maxRetries,
		},
	}
	if _, _, err := qm.wal.Append(ctx, []walEntry{entry}); err != nil {
		// The queue is live but has no WAL record; roll it back so clients
		// don't see a phantom queue that will vanish on the next restart.
		qm.mu.Lock()
		delete(qm.queues, queueID)
		qm.mu.Unlock()
		return "", fmt.Errorf("wal append create queue: %w", err)
	}

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
		return nil, ErrMessageLimitExceeded
	}
	var totalBytes int64
	for _, b := range bodies {
		totalBytes += int64(len(b))
	}
	if q.maxBytes > 0 && q.bytesInMem+totalBytes > q.maxBytes {
		return nil, ErrByteLimitExceeded
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
	MessageID     string
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

	// Validate each entry and collect valid ones for WAL. The dedup map is only
	// needed when there is more than one entry — the common single-ack path
	// skips the allocation entirely.
	valid := make([]*deliveryRecord, 0, len(acks))
	var seen map[string]bool
	if len(acks) > 1 {
		seen = make(map[string]bool, len(acks))
	}
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
		if seen != nil {
			seen[entry.ReceiptHandle] = true
		}
		valid = append(valid, dr)
		results[i].Status = "ok"
		results[i].MessageID = dr.MessageID
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
		return "", ErrMessageNotFound
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
	q.metrics.totalNacked.Add(1)
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

		// Track whether we disturbed the heap. In steady state (visibility
		// timeout >> reaper interval) the front entry is almost always in the
		// future, so the loop never runs and the heap is untouched. Only then
		// can we skip the O(N) rebuild below.
		poppedAny := false

		for len(q.deadlines) > 0 && !q.deadlines[0].Deadline.After(now) {
			poppedAny = true
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
			// Nothing expired. If the loop popped only stale entries it
			// disturbed the heap, so rebuild from the inflight map (the source
			// of truth). If nothing was popped (the common case), the heap is
			// already intact and the rebuild would be pure O(N) waste.
			if poppedAny {
				q.deadlines = q.deadlines[:0]
				for _, dr := range q.inflight {
					dr.heapIndex = -1
					heap.Push(&q.deadlines, dr)
				}
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
