package main

import (
	"container/heap"
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// Phase 2.7: Consistent snapshots and bounded WAL compaction.

const (
	snapshotFrameMagic   = "KSNA"
	snapshotFrameVersion = uint16(1)
	snapshotFrameHeader  = 16

	// Default snapshot thresholds. High enough that short benchmarks/CI runs
	// never trigger a snapshot; production snapshots roughly every minute or
	// every 100k committed WAL entries.
	defaultSnapshotEveryOps     = 100000
	defaultSnapshotEverySeconds = 60

	// Bounded compaction batch size (keys per Pebble Delete batch).
	defaultCompactBatchSize = 1000
)

// snapshotConfig controls when snapshots fire. A threshold of 0 disables that
// dimension. Both zero = snapshots disabled entirely.
type snapshotConfig struct {
	opsThreshold     int64
	secondsThreshold time.Duration
	compactBatchSize int
}

func parseSnapshotConfig() (snapshotConfig, error) {
	cfg := snapshotConfig{
		opsThreshold:     int64(parsePositiveIntEnv("KUEUE_SNAPSHOT_EVERY_OPS", defaultSnapshotEveryOps)),
		secondsThreshold: time.Duration(parsePositiveIntEnv("KUEUE_SNAPSHOT_EVERY_SECONDS", defaultSnapshotEverySeconds)) * time.Second,
		compactBatchSize: parsePositiveIntEnv("KUEUE_WAL_COMPACT_BATCH", defaultCompactBatchSize),
	}
	return cfg, nil
}

func parsePositiveIntEnv(name string, defaultVal int) int {
	s := os.Getenv(name)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return defaultVal
	}
	return v
}

// snapshotData is the top-level serialized form of a full queueManager
// checkpoint at a single LSN.
type snapshotData struct {
	SnapshotLSN uint64
	Queues      []snapshotQueue
}

type snapshotQueue struct {
	QueueID     string
	Name        string
	MaxRetries  int
	NextSeq     uint64
	MaxMessages int64
	MaxBytes    int64

	Ready    []snapshotMessage
	Inflight []snapshotInflight
	Dead     []snapshotMessage

	Metrics snapshotMetrics
}

type snapshotMessage struct {
	ID                string
	Seq               uint64
	Body              []byte
	EnqueuedAt        time.Time
	DeliveryCount     int
	MaxDeliveryCount  int
}

type snapshotInflight struct {
	// message fields
	MessageID        string
	Seq              uint64
	Body             []byte
	EnqueuedAt       time.Time
	DeliveryCount    int
	MaxDeliveryCount int
	// delivery record fields
	ReceiptHandle      string
	DeliveryToken      string
	VisibilityDeadline time.Time
}

type snapshotMetrics struct {
	ReadyCount     int64
	InFlightCount  int64
	DeadCount      int64
	TotalPublished int64
	TotalReceived  int64
	TotalAcked     int64
	TotalNacked    int64
}

// ---- on-disk keys ---------------------------------------------------------

func snapshotPrefix() []byte {
	return []byte("snapshot|")
}

func snapshotKey(lsn uint64) []byte {
	prefix := snapshotPrefix()
	key := make([]byte, len(prefix)+8)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], lsn)
	return key
}

func parseSnapshotKeyLSN(key []byte) (uint64, error) {
	prefix := snapshotPrefix()
	if len(key) != len(prefix)+8 || !hasPrefix(key, prefix) {
		return 0, fmt.Errorf("invalid snapshot key %q", string(key))
	}
	return binary.BigEndian.Uint64(key[len(prefix):]), nil
}

func hasPrefix(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	return string(b[:len(prefix)]) == string(prefix)
}

// ---- frame encode/decode --------------------------------------------------

// encodeSnapshotEntry wraps a serialized snapshotData in a WAL-style frame:
// magic | version | reserved | reserved | payloadCRC | payloadLen | payload.
func encodeSnapshotEntry(data snapshotData) ([]byte, error) {
	w := walPayloadWriter{buf: make([]byte, snapshotFrameHeader, snapshotFrameHeader+256)}
	encodeSnapshotDataInto(&w, data)
	if w.err != nil {
		return nil, w.err
	}

	frame := w.buf
	payload := frame[snapshotFrameHeader:]
	if uint64(len(payload)) > walMaxUint32 {
		return nil, fmt.Errorf("snapshot payload too large: %d bytes", len(payload))
	}

	copy(frame[0:4], snapshotFrameMagic)
	binary.BigEndian.PutUint16(frame[4:6], snapshotFrameVersion)
	frame[6] = 0 // reserved
	frame[7] = 0 // reserved
	binary.BigEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(payload))
	binary.BigEndian.PutUint32(frame[12:16], uint32(len(payload)))
	return frame, nil
}

func decodeSnapshotEntry(frame []byte) (snapshotData, error) {
	if len(frame) < snapshotFrameHeader {
		return snapshotData{}, fmt.Errorf("short snapshot frame: got %d bytes, want at least %d", len(frame), snapshotFrameHeader)
	}
	if string(frame[0:4]) != snapshotFrameMagic {
		return snapshotData{}, fmt.Errorf("invalid snapshot magic %q", string(frame[0:4]))
	}
	version := binary.BigEndian.Uint16(frame[4:6])
	if version != snapshotFrameVersion {
		return snapshotData{}, fmt.Errorf("unknown snapshot version %d", version)
	}
	payloadLen := binary.BigEndian.Uint32(frame[12:16])
	if len(frame)-snapshotFrameHeader != int(payloadLen) {
		return snapshotData{}, fmt.Errorf("malformed snapshot frame: payload length is %d, frame has %d payload bytes", payloadLen, len(frame)-snapshotFrameHeader)
	}
	payload := frame[snapshotFrameHeader:]
	wantCRC := binary.BigEndian.Uint32(frame[8:12])
	gotCRC := crc32.ChecksumIEEE(payload)
	if gotCRC != wantCRC {
		return snapshotData{}, fmt.Errorf("snapshot CRC mismatch: got %08x, want %08x", gotCRC, wantCRC)
	}
	return decodeSnapshotData(payload)
}

// ---- payload (de)serialization --------------------------------------------

func encodeSnapshotDataInto(w *walPayloadWriter, d snapshotData) {
	w.writeUint64(d.SnapshotLSN)
	w.writeCount(len(d.Queues))
	for _, q := range d.Queues {
		encodeSnapshotQueueInto(w, q)
	}
}

func decodeSnapshotData(payload []byte) (snapshotData, error) {
	r := walPayloadReader{data: payload}
	snapLSN, err := r.readUint64()
	if err != nil {
		return snapshotData{}, err
	}
	count, err := r.readCount()
	if err != nil {
		return snapshotData{}, err
	}
	queues := make([]snapshotQueue, count)
	for i := range queues {
		q, err := decodeSnapshotQueue(&r)
		if err != nil {
			return snapshotData{}, err
		}
		queues[i] = q
	}
	if r.remaining() != 0 {
		return snapshotData{}, fmt.Errorf("malformed snapshot: %d trailing bytes", r.remaining())
	}
	return snapshotData{SnapshotLSN: snapLSN, Queues: queues}, nil
}

func encodeSnapshotQueueInto(w *walPayloadWriter, q snapshotQueue) {
	w.writeString(q.QueueID)
	w.writeString(q.Name)
	w.writeInt(q.MaxRetries)
	w.writeUint64(q.NextSeq)
	w.writeInt64(q.MaxMessages)
	w.writeInt64(q.MaxBytes)

	w.writeCount(len(q.Ready))
	for _, m := range q.Ready {
		encodeSnapshotMessageInto(w, m)
	}
	w.writeCount(len(q.Inflight))
	for _, m := range q.Inflight {
		encodeSnapshotInflightInto(w, m)
	}
	w.writeCount(len(q.Dead))
	for _, m := range q.Dead {
		encodeSnapshotMessageInto(w, m)
	}

	encodeSnapshotMetricsInto(w, q.Metrics)
}

func decodeSnapshotQueue(r *walPayloadReader) (snapshotQueue, error) {
	var q snapshotQueue
	var err error
	q.QueueID, err = r.readString()
	if err != nil {
		return q, err
	}
	q.Name, err = r.readString()
	if err != nil {
		return q, err
	}
	q.MaxRetries, err = r.readInt()
	if err != nil {
		return q, err
	}
	q.NextSeq, err = r.readUint64()
	if err != nil {
		return q, err
	}
	q.MaxMessages, err = r.readInt64()
	if err != nil {
		return q, err
	}
	q.MaxBytes, err = r.readInt64()
	if err != nil {
		return q, err
	}

	q.Ready, err = decodeSnapshotMessages(r)
	if err != nil {
		return q, err
	}
	q.Inflight, err = decodeSnapshotInflights(r)
	if err != nil {
		return q, err
	}
	q.Dead, err = decodeSnapshotMessages(r)
	if err != nil {
		return q, err
	}
	q.Metrics, err = decodeSnapshotMetrics(r)
	if err != nil {
		return q, err
	}
	return q, nil
}

func encodeSnapshotMessageInto(w *walPayloadWriter, m snapshotMessage) {
	w.writeString(m.ID)
	w.writeUint64(m.Seq)
	w.writeBytes(m.Body)
	w.writeTime(m.EnqueuedAt)
	w.writeInt(m.DeliveryCount)
	w.writeInt(m.MaxDeliveryCount)
}

func decodeSnapshotMessage(r *walPayloadReader) (snapshotMessage, error) {
	var m snapshotMessage
	var err error
	m.ID, err = r.readString()
	if err != nil {
		return m, err
	}
	m.Seq, err = r.readUint64()
	if err != nil {
		return m, err
	}
	m.Body, err = r.readBytes()
	if err != nil {
		return m, err
	}
	m.EnqueuedAt, err = r.readTime()
	if err != nil {
		return m, err
	}
	m.DeliveryCount, err = r.readInt()
	if err != nil {
		return m, err
	}
	m.MaxDeliveryCount, err = r.readInt()
	if err != nil {
		return m, err
	}
	return m, nil
}

func decodeSnapshotMessages(r *walPayloadReader) ([]snapshotMessage, error) {
	count, err := r.readCount()
	if err != nil {
		return nil, err
	}
	out := make([]snapshotMessage, count)
	for i := range out {
		m, err := decodeSnapshotMessage(r)
		if err != nil {
			return nil, err
		}
		out[i] = m
	}
	return out, nil
}

func encodeSnapshotInflightInto(w *walPayloadWriter, m snapshotInflight) {
	w.writeString(m.MessageID)
	w.writeUint64(m.Seq)
	w.writeBytes(m.Body)
	w.writeTime(m.EnqueuedAt)
	w.writeInt(m.DeliveryCount)
	w.writeInt(m.MaxDeliveryCount)
	w.writeString(m.ReceiptHandle)
	w.writeString(m.DeliveryToken)
	w.writeTime(m.VisibilityDeadline)
}

func decodeSnapshotInflight(r *walPayloadReader) (snapshotInflight, error) {
	var m snapshotInflight
	var err error
	m.MessageID, err = r.readString()
	if err != nil {
		return m, err
	}
	m.Seq, err = r.readUint64()
	if err != nil {
		return m, err
	}
	m.Body, err = r.readBytes()
	if err != nil {
		return m, err
	}
	m.EnqueuedAt, err = r.readTime()
	if err != nil {
		return m, err
	}
	m.DeliveryCount, err = r.readInt()
	if err != nil {
		return m, err
	}
	m.MaxDeliveryCount, err = r.readInt()
	if err != nil {
		return m, err
	}
	m.ReceiptHandle, err = r.readString()
	if err != nil {
		return m, err
	}
	m.DeliveryToken, err = r.readString()
	if err != nil {
		return m, err
	}
	m.VisibilityDeadline, err = r.readTime()
	if err != nil {
		return m, err
	}
	return m, nil
}

func decodeSnapshotInflights(r *walPayloadReader) ([]snapshotInflight, error) {
	count, err := r.readCount()
	if err != nil {
		return nil, err
	}
	out := make([]snapshotInflight, count)
	for i := range out {
		m, err := decodeSnapshotInflight(r)
		if err != nil {
			return nil, err
		}
		out[i] = m
	}
	return out, nil
}

func encodeSnapshotMetricsInto(w *walPayloadWriter, m snapshotMetrics) {
	w.writeInt64(m.ReadyCount)
	w.writeInt64(m.InFlightCount)
	w.writeInt64(m.DeadCount)
	w.writeInt64(m.TotalPublished)
	w.writeInt64(m.TotalReceived)
	w.writeInt64(m.TotalAcked)
	w.writeInt64(m.TotalNacked)
}

func decodeSnapshotMetrics(r *walPayloadReader) (snapshotMetrics, error) {
	var m snapshotMetrics
	var err error
	if m.ReadyCount, err = r.readInt64(); err != nil {
		return m, err
	}
	if m.InFlightCount, err = r.readInt64(); err != nil {
		return m, err
	}
	if m.DeadCount, err = r.readInt64(); err != nil {
		return m, err
	}
	if m.TotalPublished, err = r.readInt64(); err != nil {
		return m, err
	}
	if m.TotalReceived, err = r.readInt64(); err != nil {
		return m, err
	}
	if m.TotalAcked, err = r.readInt64(); err != nil {
		return m, err
	}
	if m.TotalNacked, err = r.readInt64(); err != nil {
		return m, err
	}
	return m, nil
}

// ---- taking a snapshot -----------------------------------------------------

// TakeSnapshot checkpoints the entire live runtime into a single
// snapshot|<LSN> object and advances walmeta|latest_snapshot_lsn atomically.
// Returns the committed snapshot LSN, or 0 if no snapshot was taken (e.g. no
// WAL entries exist yet).
func (qm *queueManager) TakeSnapshot(ctx context.Context) (uint64, error) {
	if qm == nil || qm.walStore == nil {
		return 0, errors.New("snapshot requires a walStore-backed queue manager")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// createMu.RLock serializes with CreateQueue's (Append + install) critical
	// section. We hold it only long enough to (1) capture the queue ID set
	// and (2) read the snapshot LSN, so the two are mutually consistent: any
	// CreateQueue whose opCreateQueue committed at LSN K <= snapshotLSN has
	// already installed its queue when we read the IDs; any CreateQueue that
	// commits afterwards will have LSN K > snapshotLSN and its creation will
	// be replayed post-snapshot. Without this coordination, TakeSnapshot
	// could commit snapshot@K that omits the queue while leaving its
	// opCreateQueue@K below the replay boundary, silently dropping the queue
	// on the next restart.
	qm.createMu.RLock()

	// Snapshot the set of queue IDs under the manager RLock, then take every
	// per-queue mu in a deterministic (sorted) order to avoid deadlock.
	qm.mu.RLock()
	ids := make([]string, 0, len(qm.queues))
	for id := range qm.queues {
		ids = append(ids, id)
	}
	qm.mu.RUnlock()

	// Acquire every per-queue lock. No Append can complete while we hold all
	// of these, since each Append holds its q.mu across its wal.Append call.
	queues := make([]*queueRuntime, len(ids))
	for i, id := range ids {
		q, err := qm.getQueue(id)
		if err != nil {
			// A queue disappearing here is impossible — queues are never
			// deleted in the live runtime. Release what we hold and bail.
			for j := 0; j < i; j++ {
				queues[j].mu.Unlock()
			}
			return 0, err
		}
		q.mu.Lock()
		queues[i] = q
	}

	// Read the snapshot LSN = last committed WAL LSN, under the walStore
	// lock. Because every live Append holds its q.mu, and we hold all q.mu,
	// no Append can be mid-flight: nextLSN-1 is exactly the highest committed
	// LSN whose effects are reflected in the state we're about to serialize.
	// This preserves snapshot+tail-replay ≡ full-replay: every published
	// message whose LSN ≤ snapshotLSN is in q.messages under our lock, every
	// post-snapshot publish has LSN > snapshotLSN and will be tail-replayed.
	qm.walStore.mu.Lock()
	snapshotLSN := qm.walStore.nextLSN - 1
	qm.walStore.mu.Unlock()
	qm.createMu.RUnlock()

	if snapshotLSN == 0 {
		for i := len(queues) - 1; i >= 0; i-- {
			if queues[i] != nil {
				queues[i].mu.Unlock()
			}
		}
		return 0, nil // empty store, nothing to checkpoint
	}

	// Capture sorted IDs now (qm.mu was held only briefly above for ID
	// capture, so the queue order is whatever range the map iteration gave;
	// sort for deterministic on-disk encoding). Per-queue locks are already
	// held, so reading q.id under each lock is safe.
	sort.Strings(ids)

	// Serialize every queue under its own lock. The fresh snapshotQueue
	// slices are owned exclusively by this goroutine; nothing else can mutate
	// them once we drop the lock.
	data := snapshotData{SnapshotLSN: snapshotLSN}
	data.Queues = make([]snapshotQueue, len(queues))
	for i, q := range queues {
		data.Queues[i] = serializeQueue(q, snapshotLSN)
	}

	// Release per-queue locks now that all state has been deep-copied into
	// fresh snapshotQueue allocations. The encode + CRC run on those copies
	// outside any lock so a snapshot interval cannot freeze the live runtime
	// for the duration of a full-payload CRC.
	for i := len(queues) - 1; i >= 0; i-- {
		queues[i].mu.Unlock()
		queues[i] = nil
	}

	// Encode + CRC the captured state with no queue locks held.
	frame, err := encodeSnapshotEntry(data)
	if err != nil {
		return 0, fmt.Errorf("encode snapshot: %w", err)
	}

	// One atomic Pebble batch: snapshot payload + meta pointer. Both land or
	// neither does. Crash-safety: a partial commit leaves the prior
	// latest_snapshot_lsn intact.
	batch := qm.walStore.db.NewBatch()
	if err := batch.Set(snapshotKey(snapshotLSN), frame, nil); err != nil {
		batch.Close()
		return 0, fmt.Errorf("stage snapshot: %w", err)
	}
	if err := batch.Set(walMetaLatestSnapshotLSNKey(), encodeUint64(snapshotLSN), nil); err != nil {
		batch.Close()
		return 0, fmt.Errorf("stage snapshot meta: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, fmt.Errorf("commit snapshot batch: %w", err)
	}

	qm.walStore.mu.Lock()
	qm.walStore.latestSnapshotLSN = snapshotLSN
	qm.walStore.mu.Unlock()

	return snapshotLSN, nil
}

func serializeQueue(q *queueRuntime, snapshotLSN uint64) snapshotQueue {
	sq := snapshotQueue{
		QueueID:     q.id,
		Name:        q.config.Name,
		MaxRetries:  q.config.MaxRetries,
		NextSeq:     q.nextSeq,
		MaxMessages: q.maxMessages,
		MaxBytes:    q.maxBytes,
	}

	// Ready messages in list order (already seq-ordered FIFO).
	sq.Ready = make([]snapshotMessage, 0, q.ready.Len())
	for e := q.ready.Front(); e != nil; e = e.Next() {
		msg := e.Value.(*messageRecord)
		sq.Ready = append(sq.Ready, snapshotMessage{
			ID:                msg.ID,
			Seq:               msg.Seq,
			Body:              msg.Body,
			EnqueuedAt:        msg.EnqueuedAt,
			DeliveryCount:     msg.DeliveryCount,
			MaxDeliveryCount:  msg.MaxDeliveryCount,
		})
	}

	// Inflight deliveries (with their delivery tokens + deadlines).
	sq.Inflight = make([]snapshotInflight, 0, len(q.inflight))
	for _, dr := range q.inflight {
		msg, ok := q.messages[dr.MessageID]
		if !ok {
			continue
		}
		sq.Inflight = append(sq.Inflight, snapshotInflight{
			MessageID:          msg.ID,
			Seq:                msg.Seq,
			Body:               msg.Body,
			EnqueuedAt:         msg.EnqueuedAt,
			DeliveryCount:      msg.DeliveryCount,
			MaxDeliveryCount:   msg.MaxDeliveryCount,
			ReceiptHandle:      dr.ReceiptHandle,
			DeliveryToken:      dr.DeliveryToken,
			VisibilityDeadline: dr.Deadline,
		})
	}

	// Dead messages.
	sq.Dead = make([]snapshotMessage, 0, len(q.dead))
	for _, msg := range q.dead {
		sq.Dead = append(sq.Dead, snapshotMessage{
			ID:                msg.ID,
			Seq:               msg.Seq,
			Body:              msg.Body,
			EnqueuedAt:        msg.EnqueuedAt,
			DeliveryCount:     msg.DeliveryCount,
			MaxDeliveryCount:  msg.MaxDeliveryCount,
		})
	}

	sq.Metrics = snapshotMetrics{
		ReadyCount:     q.metrics.readyCount.Load(),
		InFlightCount:  q.metrics.inFlightCount.Load(),
		DeadCount:      q.metrics.deadCount.Load(),
		TotalPublished: q.metrics.totalPublished.Load(),
		TotalReceived:  q.metrics.totalReceived.Load(),
		TotalAcked:     q.metrics.totalAcked.Load(),
		TotalNacked:    q.metrics.totalNacked.Load(),
	}
	return sq
}

// ---- applying a snapshot --------------------------------------------------

// applySnapshot rebuilds the in-memory queueManager from a snapshot. It is
// called during recovery before WAL replay. It is NOT routed through
// ApplyWALEntry (snapshots are not WAL ops).
func (qm *queueManager) applySnapshot(data snapshotData) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	for _, sq := range data.Queues {
		qm.applySnapshotQueue(sq)
	}
}

func (qm *queueManager) applySnapshotQueue(sq snapshotQueue) {
	config := QueueConfig{Name: sq.Name, MaxRetries: sq.MaxRetries}
	metrics := getOrCreateMetrics(sq.QueueID)
	q := &queueRuntime{
		id:          sq.QueueID,
		config:      config,
		ready:       list.New(),
		messages:    make(map[string]*messageRecord),
		inflight:    make(map[string]*deliveryRecord),
		dead:        make(map[string]*messageRecord),
		readyCh:     make(chan struct{}, 1),
		notifyCh:    make(chan struct{}),
		metrics:     metrics,
		maxMessages: sq.MaxMessages,
		maxBytes:    sq.MaxBytes,
		nextSeq:     sq.NextSeq,
	}
	heap.Init(&q.deadlines)

	var bytesInMem int64

	for _, m := range sq.Ready {
		msg := &messageRecord{
			ID:                m.ID,
			QueueID:           sq.QueueID,
			Seq:               m.Seq,
			Body:              m.Body,
			State:             StateReady,
			EnqueuedAt:        m.EnqueuedAt,
			DeliveryCount:     m.DeliveryCount,
			MaxDeliveryCount:  m.MaxDeliveryCount,
		}
		msg.readyElement = q.ready.PushBack(msg)
		q.messages[msg.ID] = msg
		bytesInMem += int64(len(msg.Body))
	}

	for _, m := range sq.Inflight {
		msg := &messageRecord{
			ID:                 m.MessageID,
			QueueID:            sq.QueueID,
			Seq:                m.Seq,
			Body:               m.Body,
			State:              StateInFlight,
			EnqueuedAt:         m.EnqueuedAt,
			DeliveryCount:      m.DeliveryCount,
			MaxDeliveryCount:   m.MaxDeliveryCount,
			CurrentReceiptHandle: m.ReceiptHandle,
			CurrentDeliveryToken: m.DeliveryToken,
			VisibilityDeadline:   m.VisibilityDeadline,
		}
		q.messages[msg.ID] = msg
		dr := &deliveryRecord{
			MessageID:     msg.ID,
			ReceiptHandle: m.ReceiptHandle,
			DeliveryToken: m.DeliveryToken,
			Deadline:      m.VisibilityDeadline,
			DeliveryCount: m.DeliveryCount,
			seq:           deliveryRecordSeq.Add(1),
			heapIndex:     -1,
		}
		q.inflight[dr.ReceiptHandle] = dr
		heap.Push(&q.deadlines, dr)
		bytesInMem += int64(len(msg.Body))
	}

	for _, m := range sq.Dead {
		msg := &messageRecord{
			ID:                m.ID,
			QueueID:           sq.QueueID,
			Seq:               m.Seq,
			Body:              m.Body,
			State:             StateDead,
			EnqueuedAt:        m.EnqueuedAt,
			DeliveryCount:     m.DeliveryCount,
			MaxDeliveryCount:  m.MaxDeliveryCount,
		}
		q.dead[msg.ID] = msg
		q.messages[msg.ID] = msg
		bytesInMem += int64(len(msg.Body))
	}

	q.bytesInMem = bytesInMem

	// Restore durable metric counters. ackCountWindow is intentionally not
	// durable — it's recomputed by the reaper after recovery.
	metrics.readyCount.Store(sq.Metrics.ReadyCount)
	metrics.inFlightCount.Store(sq.Metrics.InFlightCount)
	metrics.deadCount.Store(sq.Metrics.DeadCount)
	metrics.totalPublished.Store(sq.Metrics.TotalPublished)
	metrics.totalReceived.Store(sq.Metrics.TotalReceived)
	metrics.totalAcked.Store(sq.Metrics.TotalAcked)
	metrics.totalNacked.Store(sq.Metrics.TotalNacked)

	qm.queues[sq.QueueID] = q
}

// ---- loading snapshots on recovery ----------------------------------------

// loadUsableSnapshot attempts to load snapshot@snapLSN; if missing or corrupt
// it walks the snapshot| prefix descending for the next-newest usable one.
// Returns (applied, usedLSN, fellBack, err). On err the caller should abort
// startup. If no usable snapshot exists, applied=false, usedLSN=0.
func (w *walStore) loadUsableSnapshot(ctx context.Context, snapLSN uint64, qm *queueManager) (applied bool, usedLSN uint64, fellBack bool, err error) {
	if w == nil || w.db == nil {
		return false, 0, false, errors.New("wal store is not initialized")
	}

if snapLSN > 0 {
		data, ok, loadErr := w.loadSnapshot(snapLSN)
		if loadErr == nil && ok {
			qm.applySnapshot(data)
			return true, snapLSN, false, nil
		}
		// ok=true + loadErr!=nil: snapshot is corrupt → fall back to an older one.
		// ok=false + loadErr==nil: key not found → fall back (shouldn't normally
		//   happen, but safe to try older snapshots).
		// ok=false + loadErr!=nil: transient I/O error → abort rather than
		//   silently treating a healthy snapshot as if it were corrupt.
		if loadErr != nil && !ok {
			return false, 0, false, fmt.Errorf("read snapshot@%d: %w", snapLSN, loadErr)
		}
		// Fall through to fallback scan.
	}

	// Walk snapshot| descending, skipping snapLSN (already attempted). When
	// snapLSN == 0 there is nothing to exclude — the meta pointer is at the
	// baseline and a snapshot@0 written by legacy-layout migration is still
	// a valid candidate.
	excludeLSN := snapLSN
	if snapLSN == 0 {
		excludeLSN = ^uint64(0) // never matches any real LSN
	}
	usable, found, scanErr := w.findUsableSnapshotLSN(excludeLSN)
	if scanErr != nil {
		return false, 0, false, scanErr
	}
	if !found {
		// No snapshot at all in the store. usable == 0 here is the sentinel
		// from findUsableSnapshotLSN's not-found return; a real snapshot@0
		// written by legacy-layout migration is a valid found=true result.
		return false, 0, false, nil
	}
	data, ok, loadErr := w.loadSnapshot(usable)
	if loadErr != nil {
		return false, 0, false, loadErr
	}
	if !ok {
		return false, 0, false, nil
	}
	qm.applySnapshot(data)
	return true, usable, true, nil
}

// loadSnapshot reads and decodes snapshot@lsn. ok=false means the key does
// not exist (e.g. partial commit truncated the value). err is non-nil for
// decode failures (bad CRC/magic) that indicate corruption.
func (w *walStore) loadSnapshot(lsn uint64) (snapshotData, bool, error) {
	val, closer, err := w.db.Get(snapshotKey(lsn))
	if err == pebble.ErrNotFound {
		return snapshotData{}, false, nil
	}
	if err != nil {
		return snapshotData{}, false, err
	}
	defer closer.Close()
	data, err := decodeSnapshotEntry(val)
	if err != nil {
		return snapshotData{}, true, err
	}
	if data.SnapshotLSN != lsn {
		return snapshotData{}, true, fmt.Errorf("snapshot LSN mismatch: key=%d, payload=%d", lsn, data.SnapshotLSN)
	}
	return data, true, nil
}

// findUsableSnapshotLSN iterates the snapshot| prefix in descending key order
// and returns the highest LSN whose frame decodes cleanly, skipping
// `excludeLSN` (already attempted by the caller). Returns (lsn, found, err).
func (w *walStore) findUsableSnapshotLSN(excludeLSN uint64) (uint64, bool, error) {
	lower := snapshotPrefix()
	upper := prefixUpperBound(snapshotPrefix())
	iter, err := w.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return 0, false, err
	}
	defer iter.Close()

	// Iterate in reverse (descending) so we try the newest first.
	for iter.Last(); iter.Valid(); iter.Prev() {
		lsn, keyErr := parseSnapshotKeyLSN(iter.Key())
		if keyErr != nil {
			return 0, false, keyErr
		}
		if lsn == excludeLSN {
			continue
		}
		val, valErr := iter.ValueAndErr()
		if valErr != nil {
			return 0, false, valErr
		}
		// Defensive copy — iter.Value() reuses internal buffer.
		buf := append([]byte(nil), val...)
		if _, decodeErr := decodeSnapshotEntry(buf); decodeErr != nil {
			// Corrupt: try the next-oldest snapshot.
			continue
		}
		return lsn, true, nil
	}
	if err := iter.Error(); err != nil {
		return 0, false, err
	}
	return 0, false, nil
}

// setLatestSnapshotLSN rewrites the walmeta pointer durably. Used after a
// fallback so the next startup picks the known-good snapshot directly.
func (w *walStore) setLatestSnapshotLSN(lsn uint64) error {
	if err := w.db.Set(walMetaLatestSnapshotLSNKey(), encodeUint64(lsn), pebble.Sync); err != nil {
		return err
	}
	w.mu.Lock()
	w.latestSnapshotLSN = lsn
	w.mu.Unlock()
	return nil
}

// ---- WAL compaction -------------------------------------------------------

// compactWAL deletes every wal|<LSN> entry whose LSN is <= throughLSN using a
// single Pebble DeleteRange. WAL keys are contiguous (wal|1 … wal|throughLSN),
// so one range tombstone replaces iterating every key. Committed NoSync: the
// snapshot that authorized this compaction was already Sync'd, so a crash
// before this lands just means re-compaction on the next start, which is safe.
func (w *walStore) compactWAL(ctx context.Context, throughLSN uint64) error {
	if w == nil || w.db == nil {
		return errors.New("wal store is not initialized")
	}
	if throughLSN == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	batch := w.db.NewBatch()
	defer batch.Close()
	// Delete [wal|1, wal|(throughLSN+1)) — all LSNs 1..throughLSN inclusive.
	// throughLSN+1 cannot overflow: nextLSN is bounded by the overflow guard in
	// Append, and throughLSN = snapshotLSN = nextLSN-1 at snapshot time.
	if err := batch.DeleteRange(walKey(1), walKey(throughLSN+1), nil); err != nil {
		return fmt.Errorf("delete range wal: %w", err)
	}
	return batch.Commit(pebble.NoSync)
}

// pruneOldSnapshots keeps the two newest snapshot objects and deletes the rest
// using a single Pebble DeleteRange. Snapshot keys are snapshot|<8B LSN>, so
// all keys below the second-newest form a contiguous prefix range.
func (w *walStore) pruneOldSnapshots(ctx context.Context) error {
	if w == nil || w.db == nil {
		return errors.New("wal store is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	type snap struct {
		lsn uint64
		key []byte
	}
	var snaps []snap

	lower := snapshotPrefix()
	upper := prefixUpperBound(snapshotPrefix())
	iter, err := w.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.SeekGE(lower); iter.Valid(); iter.Next() {
		lsn, keyErr := parseSnapshotKeyLSN(iter.Key())
		if keyErr != nil {
			continue
		}
		snaps = append(snaps, snap{lsn: lsn, key: append([]byte(nil), iter.Key()...)})
	}
	if err := iter.Error(); err != nil {
		return err
	}

	// snaps is in ascending LSN order. Keep the newest two, prune the rest via
	// a single DeleteRange: [snapshotPrefix, secondNewestKey).
	if len(snaps) <= 2 {
		return nil
	}

	// secondNewestKey is the lower of the two retained snapshots; everything
	// with a smaller key is older and safe to delete.
	secondNewestKey := snaps[len(snaps)-2].key

	batch := w.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(snapshotPrefix(), secondNewestKey, nil); err != nil {
		return fmt.Errorf("delete range snapshots: %w", err)
	}
	return batch.Commit(pebble.NoSync)
}

// ---- trigger wiring -------------------------------------------------------

// maybeSnapshot is invoked from the reaper goroutine tick. It checks the ops
// and seconds thresholds and, if due, takes one snapshot + compacts the WAL +
// prunes old snapshots. Returns the snapshot LSN (0 if none taken) and any
// error.
func (qm *queueManager) maybeSnapshot(ctx context.Context, now time.Time) (uint64, error) {
	if qm == nil || qm.walStore == nil {
		return 0, nil
	}
	cfg := qm.snapshotCfg
	if cfg.opsThreshold == 0 && cfg.secondsThreshold == 0 {
		return 0, nil
	}

	ops := qm.walStore.opsSinceSnapshot.Load()
	dueOps := cfg.opsThreshold > 0 && ops >= cfg.opsThreshold
	qm.lastSnapshotMu.Lock()
	last := qm.lastSnapshotAt
	qm.lastSnapshotMu.Unlock()
	dueTime := cfg.secondsThreshold > 0 && now.Sub(last) >= cfg.secondsThreshold

	if !dueOps && !dueTime {
		return 0, nil
	}

	// Capture the current snapshot LSN before taking the new one. We compact
	// WAL only up to prevLSN (not the new lsn) so that the second-retained
	// snapshot always has its WAL tail available: if snapshot@newLSN is corrupt
	// on disk, recovery can fall back to snapshot@prevLSN and replay from
	// prevLSN+1..newLSN, which we have not deleted.
	qm.walStore.mu.Lock()
	prevLSN := qm.walStore.latestSnapshotLSN
	qm.walStore.mu.Unlock()

	lsn, err := qm.TakeSnapshot(ctx)
	if err != nil {
		return 0, fmt.Errorf("snapshot: %w", err)
	}
	if lsn == 0 {
		// Nothing to snapshot yet (empty store). Don't reset the timer/counter
		// so we retry next tick.
		return 0, nil
	}

	if err := qm.walStore.compactWAL(ctx, prevLSN); err != nil {
		return lsn, fmt.Errorf("compact wal: %w", err)
	}
	if err := qm.walStore.pruneOldSnapshots(ctx); err != nil {
		return lsn, fmt.Errorf("prune snapshots: %w", err)
	}

	qm.walStore.opsSinceSnapshot.Store(0)
	qm.lastSnapshotMu.Lock()
	qm.lastSnapshotAt = now
	qm.lastSnapshotMu.Unlock()
	return lsn, nil
}