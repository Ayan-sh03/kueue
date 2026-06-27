package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

const (
	walFrameMagic   = "KWAL"
	walFrameVersion = uint16(1)
	walFrameHeader  = 16
	walMaxUint32    = uint64(1<<32 - 1)
)

const walZeroTimeUnixNano int64 = -1 << 63

type walOp uint8

const (
	opCreateQueue walOp = iota + 1
	opPublishBatch
	opClaimBatch
	opAckBatch
	opNack
	opReapBatch
)

type walEntry struct {
	LSN     uint64
	Op      walOp
	Flags   uint8
	Payload any
}

type walCreateQueuePayload struct {
	QueueID    string
	Name       string
	MaxRetries int
}

type walPublishBatchPayload struct {
	QueueID  string
	Messages []walPublishedMessage
}

type walPublishedMessage struct {
	MessageID        string
	Seq              uint64
	Body             []byte
	EnqueuedAt       time.Time
	MaxDeliveryCount int
}

type walClaimBatchPayload struct {
	QueueID string
	Claims  []walClaimedMessage
}

type walClaimedMessage struct {
	MessageID          string
	ReceiptHandle      string
	DeliveryToken      string
	VisibilityDeadline time.Time
	DeliveryCount      int
}

type walAckBatchPayload struct {
	QueueID string
	Acks    []walAckedMessage
}

type walAckedMessage struct {
	MessageID     string
	ReceiptHandle string
	DeliveryToken string
}

type walNackPayload struct {
	QueueID        string
	MessageID      string
	ReceiptHandle  string
	DeliveryToken  string
	TargetState    MessageState
	HasNewReadySeq bool
	NewReadySeq    uint64
}

type walReapBatchPayload struct {
	QueueID string
	Reaps   []walReapedMessage
}

type walReapedMessage struct {
	MessageID             string
	PreviousDeliveryToken string
	TargetState           MessageState
	HasNewReadySeq        bool
	NewReadySeq           uint64
}

type walSyncMode int

const (
	walSyncNone walSyncMode = iota
	walSyncBatch
	walSyncAlways
)

func parseWalSyncMode(value string) (walSyncMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "none":
		return walSyncNone, nil
	case "batch":
		return walSyncBatch, nil
	case "always":
		return walSyncAlways, nil
	default:
		return walSyncNone, fmt.Errorf("invalid KUEUE_WAL_SYNC %q: expected none, batch, or always", value)
	}
}

func walSyncModeFromEnv() (walSyncMode, error) {
	return parseWalSyncMode(os.Getenv("KUEUE_WAL_SYNC"))
}

func (m walSyncMode) writeOptions() (*pebble.WriteOptions, error) {
	switch m {
	case walSyncNone:
		return pebble.NoSync, nil
	case walSyncBatch, walSyncAlways:
		return pebble.Sync, nil
	default:
		return nil, fmt.Errorf("invalid WAL sync mode %d", m)
	}
}

type walStore struct {
	mu                sync.Mutex
	db                *pebble.DB
	nextLSN           uint64
	latestSnapshotLSN uint64
	syncMode          walSyncMode

	// opsSinceSnapshot counts successful Append commits since the last
	// snapshot. Bumped by the Append leader after a successful commit. The
	// snapshot trigger polls it on each reaper tick.
	opsSinceSnapshot atomic.Int64

	// compactBatchSize bounds the number of Delete keys per Pebble batch in
	// WAL compaction and snapshot pruning. Set from KUEUE_WAL_COMPACT_BATCH
	// (default 1000). 0 = single unbounded batch.
	compactBatchSize int

	// Group commit state. Concurrent appends (always from different queues —
	// each queue holds its own mutex across its Append call) accumulate into a
	// shared Pebble batch. The first appender becomes the leader and commits
	// the whole batch with a single fsync; the rest wait for that commit.
	// New appenders that arrive mid-flush accumulate into a fresh batch which
	// the leader drains in turn, so one leader can amortize many fsyncs.
	cur      *commitGroup
	flushing bool
}

// commitGroup is one accumulating WAL batch shared by a group of appenders.
type commitGroup struct {
	batch  *pebble.Batch
	done   chan struct{}
	err    error
	poison error // set if encoding/staging an entry into the batch failed
}

func newWalStore(db *pebble.DB, syncMode walSyncMode) (*walStore, error) {
	if db == nil {
		return nil, errors.New("WAL store requires a Pebble DB")
	}
	if _, err := syncMode.writeOptions(); err != nil {
		return nil, err
	}

	nextLSN, haveNext, err := readWalMetaUint64OrDefault(db, walMetaNextLSNKey(), 1)
	if err != nil {
		return nil, err
	}
	if nextLSN == 0 {
		return nil, errors.New("invalid WAL next_lsn metadata: value must be >= 1")
	}

	latestSnapshotLSN, haveLatestSnapshot, err := readWalMetaUint64OrDefault(db, walMetaLatestSnapshotLSNKey(), 0)
	if err != nil {
		return nil, err
	}

	if !haveNext || !haveLatestSnapshot {
		batch := db.NewBatch()
		defer batch.Close()
		if !haveNext {
			if err := batch.Set(walMetaNextLSNKey(), encodeUint64(nextLSN), nil); err != nil {
				return nil, err
			}
		}
		if !haveLatestSnapshot {
			if err := batch.Set(walMetaLatestSnapshotLSNKey(), encodeUint64(latestSnapshotLSN), nil); err != nil {
				return nil, err
			}
		}
		opts, err := syncMode.writeOptions()
		if err != nil {
			return nil, err
		}
		if err := batch.Commit(opts); err != nil {
			return nil, err
		}
	}

	return &walStore{
		db:                db,
		nextLSN:           nextLSN,
		latestSnapshotLSN: latestSnapshotLSN,
		syncMode:          syncMode,
	}, nil
}

func (w *walStore) Append(ctx context.Context, entries []walEntry) (firstLSN, lastLSN uint64, err error) {
	if w == nil || w.db == nil {
		return 0, 0, errors.New("WAL store is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	if len(entries) == 0 {
		return 0, 0, nil
	}

	opts, err := w.syncMode.writeOptions()
	if err != nil {
		return 0, 0, err
	}

	// Encode every entry's frame up front, outside the lock. The encoded frame
	// does not depend on the LSN (the LSN is only the Pebble key), so this — the
	// CPU-heavy part (CRC + serialization) — runs fully concurrently across
	// appenders and keeps the critical section tiny.
	frames := make([][]byte, len(entries))
	for i := range entries {
		encoded, err := encodeWalEntry(entries[i])
		if err != nil {
			return 0, 0, err
		}
		frames[i] = encoded
	}

	w.mu.Lock()

	firstLSN = w.nextLSN
	if uint64(len(entries))-1 > ^uint64(0)-firstLSN {
		w.mu.Unlock()
		return 0, 0, errors.New("WAL LSN overflow")
	}
	lastLSN = firstLSN + uint64(len(entries)) - 1
	nextLSN := lastLSN + 1
	if nextLSN == 0 {
		w.mu.Unlock()
		return 0, 0, errors.New("WAL next LSN overflow")
	}

	if w.cur == nil {
		w.cur = &commitGroup{batch: w.db.NewBatch(), done: make(chan struct{})}
	}
	myGroup := w.cur

	// Stage this append's keys into the shared batch under the lock.
	for i, frame := range frames {
		if err := myGroup.batch.Set(walKey(firstLSN+uint64(i)), frame, nil); err != nil && myGroup.poison == nil {
			myGroup.poison = err
		}
	}
	if err := myGroup.batch.Set(walMetaNextLSNKey(), encodeUint64(nextLSN), nil); err != nil && myGroup.poison == nil {
		myGroup.poison = err
	}
	w.nextLSN = nextLSN

	if w.flushing {
		// A leader is already committing an earlier batch and will drain ours
		// next. Wait for our group's commit to complete. We do not abandon on
		// ctx cancellation here: our keys are already staged, so the only
		// consistent outcomes are "committed" or "commit failed".
		w.mu.Unlock()
		<-myGroup.done
		err := myGroup.err
		if err == nil {
			w.opsSinceSnapshot.Add(int64(len(frames)))
		}
		return firstLSN, lastLSN, err
	}

	// We are the leader. Drain the current group and any groups that accumulate
	// while we commit, so a single goroutine amortizes many fsyncs.
	w.flushing = true
	group := myGroup
	for {
		w.cur = nil
		w.mu.Unlock()

		var commitErr error
		if group.poison != nil {
			commitErr = group.poison
		} else {
			commitErr = group.batch.Commit(opts)
		}
		group.batch.Close()

		w.mu.Lock()
		group.err = commitErr
		close(group.done)

		if w.cur == nil {
			w.flushing = false
			w.mu.Unlock()
			break
		}
		group = w.cur
	}

	retErr := myGroup.err
	if retErr == nil {
		w.opsSinceSnapshot.Add(int64(len(frames)))
	}
	return firstLSN, lastLSN, retErr
}

func (w *walStore) Replay(ctx context.Context, afterLSN uint64, apply func(walEntry) error) error {
	if w == nil || w.db == nil {
		return errors.New("WAL store is not initialized")
	}
	if apply == nil {
		return errors.New("WAL replay requires an apply function")
	}
	if afterLSN == ^uint64(0) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	lower := walPrefix()
	if afterLSN > 0 {
		lower = walKey(afterLSN + 1)
	}
	iter, err := w.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(walPrefix()),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.SeekGE(lower); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		lsn, err := parseWalKeyLSN(iter.Key())
		if err != nil {
			return err
		}
		val, err := iter.ValueAndErr()
		if err != nil {
			return err
		}
		entry, err := decodeWalEntry(lsn, val)
		if err != nil {
			return err
		}
		if err := apply(entry); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return nil
}

func encodeWalEntry(entry walEntry) ([]byte, error) {
	if !isKnownWalOp(entry.Op) {
		return nil, fmt.Errorf("unknown WAL op %d", entry.Op)
	}

	// Reserve the frame header up front and encode the payload directly after
	// it. The whole frame is then a single allocation with no header/payload
	// copy — the header is filled in place once the payload length is known.
	w := walPayloadWriter{buf: make([]byte, walFrameHeader, walFrameHeader+128)}
	encodeWalPayloadInto(&w, entry.Op, entry.Payload)
	if w.err != nil {
		return nil, w.err
	}

	frame := w.buf
	payload := frame[walFrameHeader:]
	if uint64(len(payload)) > walMaxUint32 {
		return nil, fmt.Errorf("WAL payload too large: %d bytes", len(payload))
	}

	copy(frame[0:4], walFrameMagic)
	binary.BigEndian.PutUint16(frame[4:6], walFrameVersion)
	frame[6] = byte(entry.Op)
	frame[7] = entry.Flags
	binary.BigEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(payload))
	binary.BigEndian.PutUint32(frame[12:16], uint32(len(payload)))
	return frame, nil
}

func decodeWalEntry(lsn uint64, frame []byte) (walEntry, error) {
	if len(frame) < walFrameHeader {
		return walEntry{}, fmt.Errorf("short WAL frame: got %d bytes, want at least %d", len(frame), walFrameHeader)
	}
	if string(frame[0:4]) != walFrameMagic {
		return walEntry{}, fmt.Errorf("invalid WAL magic %q", string(frame[0:4]))
	}

	version := binary.BigEndian.Uint16(frame[4:6])
	if version != walFrameVersion {
		return walEntry{}, fmt.Errorf("unknown WAL version %d", version)
	}

	op := walOp(frame[6])
	if !isKnownWalOp(op) {
		return walEntry{}, fmt.Errorf("unknown WAL op %d", op)
	}

	payloadLen := binary.BigEndian.Uint32(frame[12:16])
	if len(frame)-walFrameHeader != int(payloadLen) {
		return walEntry{}, fmt.Errorf("malformed WAL frame: payload length is %d, frame has %d payload bytes", payloadLen, len(frame)-walFrameHeader)
	}
	payload := frame[walFrameHeader:]
	wantCRC := binary.BigEndian.Uint32(frame[8:12])
	gotCRC := crc32.ChecksumIEEE(payload)
	if gotCRC != wantCRC {
		return walEntry{}, fmt.Errorf("WAL CRC mismatch: got %08x, want %08x", gotCRC, wantCRC)
	}

	decodedPayload, err := decodeWalPayload(op, payload)
	if err != nil {
		return walEntry{}, err
	}
	return walEntry{
		LSN:     lsn,
		Op:      op,
		Flags:   frame[7],
		Payload: decodedPayload,
	}, nil
}

func encodeWalPayloadInto(w *walPayloadWriter, op walOp, payload any) {
	switch op {
	case opCreateQueue:
		p, ok := payload.(walCreateQueuePayload)
		if !ok {
			w.err = fmt.Errorf("WAL create queue payload has type %T", payload)
			return
		}
		encodeWalCreateQueuePayload(w, p)
	case opPublishBatch:
		p, ok := payload.(walPublishBatchPayload)
		if !ok {
			w.err = fmt.Errorf("WAL publish batch payload has type %T", payload)
			return
		}
		encodeWalPublishBatchPayload(w, p)
	case opClaimBatch:
		p, ok := payload.(walClaimBatchPayload)
		if !ok {
			w.err = fmt.Errorf("WAL claim batch payload has type %T", payload)
			return
		}
		encodeWalClaimBatchPayload(w, p)
	case opAckBatch:
		p, ok := payload.(walAckBatchPayload)
		if !ok {
			w.err = fmt.Errorf("WAL ack batch payload has type %T", payload)
			return
		}
		encodeWalAckBatchPayload(w, p)
	case opNack:
		p, ok := payload.(walNackPayload)
		if !ok {
			w.err = fmt.Errorf("WAL nack payload has type %T", payload)
			return
		}
		encodeWalNackPayload(w, p)
	case opReapBatch:
		p, ok := payload.(walReapBatchPayload)
		if !ok {
			w.err = fmt.Errorf("WAL reap batch payload has type %T", payload)
			return
		}
		encodeWalReapBatchPayload(w, p)
	default:
		w.err = fmt.Errorf("unknown WAL op %d", op)
	}
}

func decodeWalPayload(op walOp, payload []byte) (any, error) {
	reader := walPayloadReader{data: payload}
	var out any
	var err error

	switch op {
	case opCreateQueue:
		out, err = decodeWalCreateQueuePayload(&reader)
	case opPublishBatch:
		out, err = decodeWalPublishBatchPayload(&reader)
	case opClaimBatch:
		out, err = decodeWalClaimBatchPayload(&reader)
	case opAckBatch:
		out, err = decodeWalAckBatchPayload(&reader)
	case opNack:
		out, err = decodeWalNackPayload(&reader)
	case opReapBatch:
		out, err = decodeWalReapBatchPayload(&reader)
	default:
		return nil, fmt.Errorf("unknown WAL op %d", op)
	}
	if err != nil {
		return nil, err
	}
	if reader.remaining() != 0 {
		return nil, fmt.Errorf("malformed WAL payload: %d trailing bytes", reader.remaining())
	}
	return out, nil
}

func encodeWalCreateQueuePayload(w *walPayloadWriter, p walCreateQueuePayload) {
	w.writeString(p.QueueID)
	w.writeString(p.Name)
	w.writeInt(p.MaxRetries)
}

func decodeWalCreateQueuePayload(reader *walPayloadReader) (walCreateQueuePayload, error) {
	queueID, err := reader.readString()
	if err != nil {
		return walCreateQueuePayload{}, err
	}
	name, err := reader.readString()
	if err != nil {
		return walCreateQueuePayload{}, err
	}
	maxRetries, err := reader.readInt()
	if err != nil {
		return walCreateQueuePayload{}, err
	}
	return walCreateQueuePayload{QueueID: queueID, Name: name, MaxRetries: maxRetries}, nil
}

func encodeWalPublishBatchPayload(w *walPayloadWriter, p walPublishBatchPayload) {
	w.writeString(p.QueueID)
	w.writeCount(len(p.Messages))
	for _, msg := range p.Messages {
		w.writeString(msg.MessageID)
		w.writeUint64(msg.Seq)
		w.writeBytes(msg.Body)
		w.writeTime(msg.EnqueuedAt)
		w.writeInt(msg.MaxDeliveryCount)
	}
}

func decodeWalPublishBatchPayload(reader *walPayloadReader) (walPublishBatchPayload, error) {
	queueID, err := reader.readString()
	if err != nil {
		return walPublishBatchPayload{}, err
	}
	count, err := reader.readCount()
	if err != nil {
		return walPublishBatchPayload{}, err
	}
	messages := make([]walPublishedMessage, count)
	for i := range messages {
		messageID, err := reader.readString()
		if err != nil {
			return walPublishBatchPayload{}, err
		}
		seq, err := reader.readUint64()
		if err != nil {
			return walPublishBatchPayload{}, err
		}
		body, err := reader.readBytes()
		if err != nil {
			return walPublishBatchPayload{}, err
		}
		enqueuedAt, err := reader.readTime()
		if err != nil {
			return walPublishBatchPayload{}, err
		}
		maxDeliveryCount, err := reader.readInt()
		if err != nil {
			return walPublishBatchPayload{}, err
		}
		messages[i] = walPublishedMessage{
			MessageID:        messageID,
			Seq:              seq,
			Body:             body,
			EnqueuedAt:       enqueuedAt,
			MaxDeliveryCount: maxDeliveryCount,
		}
	}
	return walPublishBatchPayload{QueueID: queueID, Messages: messages}, nil
}

func encodeWalClaimBatchPayload(w *walPayloadWriter, p walClaimBatchPayload) {
	w.writeString(p.QueueID)
	w.writeCount(len(p.Claims))
	for _, claim := range p.Claims {
		w.writeString(claim.MessageID)
		w.writeString(claim.ReceiptHandle)
		w.writeString(claim.DeliveryToken)
		w.writeTime(claim.VisibilityDeadline)
		w.writeInt(claim.DeliveryCount)
	}
}

func decodeWalClaimBatchPayload(reader *walPayloadReader) (walClaimBatchPayload, error) {
	queueID, err := reader.readString()
	if err != nil {
		return walClaimBatchPayload{}, err
	}
	count, err := reader.readCount()
	if err != nil {
		return walClaimBatchPayload{}, err
	}
	claims := make([]walClaimedMessage, count)
	for i := range claims {
		messageID, err := reader.readString()
		if err != nil {
			return walClaimBatchPayload{}, err
		}
		receiptHandle, err := reader.readString()
		if err != nil {
			return walClaimBatchPayload{}, err
		}
		deliveryToken, err := reader.readString()
		if err != nil {
			return walClaimBatchPayload{}, err
		}
		visibilityDeadline, err := reader.readTime()
		if err != nil {
			return walClaimBatchPayload{}, err
		}
		deliveryCount, err := reader.readInt()
		if err != nil {
			return walClaimBatchPayload{}, err
		}
		claims[i] = walClaimedMessage{
			MessageID:          messageID,
			ReceiptHandle:      receiptHandle,
			DeliveryToken:      deliveryToken,
			VisibilityDeadline: visibilityDeadline,
			DeliveryCount:      deliveryCount,
		}
	}
	return walClaimBatchPayload{QueueID: queueID, Claims: claims}, nil
}

func encodeWalAckBatchPayload(w *walPayloadWriter, p walAckBatchPayload) {
	w.writeString(p.QueueID)
	w.writeCount(len(p.Acks))
	for _, ack := range p.Acks {
		w.writeString(ack.MessageID)
		w.writeString(ack.ReceiptHandle)
		w.writeString(ack.DeliveryToken)
	}
}

func decodeWalAckBatchPayload(reader *walPayloadReader) (walAckBatchPayload, error) {
	queueID, err := reader.readString()
	if err != nil {
		return walAckBatchPayload{}, err
	}
	count, err := reader.readCount()
	if err != nil {
		return walAckBatchPayload{}, err
	}
	acks := make([]walAckedMessage, count)
	for i := range acks {
		messageID, err := reader.readString()
		if err != nil {
			return walAckBatchPayload{}, err
		}
		receiptHandle, err := reader.readString()
		if err != nil {
			return walAckBatchPayload{}, err
		}
		deliveryToken, err := reader.readString()
		if err != nil {
			return walAckBatchPayload{}, err
		}
		acks[i] = walAckedMessage{MessageID: messageID, ReceiptHandle: receiptHandle, DeliveryToken: deliveryToken}
	}
	return walAckBatchPayload{QueueID: queueID, Acks: acks}, nil
}

func encodeWalNackPayload(w *walPayloadWriter, p walNackPayload) {
	if !isValidWalMessageState(p.TargetState) {
		w.err = fmt.Errorf("invalid WAL nack target state %q", p.TargetState)
		return
	}

	w.writeString(p.QueueID)
	w.writeString(p.MessageID)
	w.writeString(p.ReceiptHandle)
	w.writeString(p.DeliveryToken)
	w.writeString(string(p.TargetState))
	w.writeBool(p.HasNewReadySeq)
	if p.HasNewReadySeq {
		w.writeUint64(p.NewReadySeq)
	}
}

func decodeWalNackPayload(reader *walPayloadReader) (walNackPayload, error) {
	queueID, err := reader.readString()
	if err != nil {
		return walNackPayload{}, err
	}
	messageID, err := reader.readString()
	if err != nil {
		return walNackPayload{}, err
	}
	receiptHandle, err := reader.readString()
	if err != nil {
		return walNackPayload{}, err
	}
	deliveryToken, err := reader.readString()
	if err != nil {
		return walNackPayload{}, err
	}
	targetState, err := reader.readMessageState()
	if err != nil {
		return walNackPayload{}, err
	}
	hasNewReadySeq, err := reader.readBool()
	if err != nil {
		return walNackPayload{}, err
	}
	var newReadySeq uint64
	if hasNewReadySeq {
		newReadySeq, err = reader.readUint64()
		if err != nil {
			return walNackPayload{}, err
		}
	}
	return walNackPayload{
		QueueID:        queueID,
		MessageID:      messageID,
		ReceiptHandle:  receiptHandle,
		DeliveryToken:  deliveryToken,
		TargetState:    targetState,
		HasNewReadySeq: hasNewReadySeq,
		NewReadySeq:    newReadySeq,
	}, nil
}

func encodeWalReapBatchPayload(w *walPayloadWriter, p walReapBatchPayload) {
	w.writeString(p.QueueID)
	w.writeCount(len(p.Reaps))
	for _, reap := range p.Reaps {
		if !isValidWalMessageState(reap.TargetState) {
			w.err = fmt.Errorf("invalid WAL reap target state %q", reap.TargetState)
			return
		}
		w.writeString(reap.MessageID)
		w.writeString(reap.PreviousDeliveryToken)
		w.writeString(string(reap.TargetState))
		w.writeBool(reap.HasNewReadySeq)
		if reap.HasNewReadySeq {
			w.writeUint64(reap.NewReadySeq)
		}
	}
}

func decodeWalReapBatchPayload(reader *walPayloadReader) (walReapBatchPayload, error) {
	queueID, err := reader.readString()
	if err != nil {
		return walReapBatchPayload{}, err
	}
	count, err := reader.readCount()
	if err != nil {
		return walReapBatchPayload{}, err
	}
	reaps := make([]walReapedMessage, count)
	for i := range reaps {
		messageID, err := reader.readString()
		if err != nil {
			return walReapBatchPayload{}, err
		}
		previousDeliveryToken, err := reader.readString()
		if err != nil {
			return walReapBatchPayload{}, err
		}
		targetState, err := reader.readMessageState()
		if err != nil {
			return walReapBatchPayload{}, err
		}
		hasNewReadySeq, err := reader.readBool()
		if err != nil {
			return walReapBatchPayload{}, err
		}
		var newReadySeq uint64
		if hasNewReadySeq {
			newReadySeq, err = reader.readUint64()
			if err != nil {
				return walReapBatchPayload{}, err
			}
		}
		reaps[i] = walReapedMessage{
			MessageID:             messageID,
			PreviousDeliveryToken: previousDeliveryToken,
			TargetState:           targetState,
			HasNewReadySeq:        hasNewReadySeq,
			NewReadySeq:           newReadySeq,
		}
	}
	return walReapBatchPayload{QueueID: queueID, Reaps: reaps}, nil
}

type walPayloadWriter struct {
	buf []byte
	err error
}

func (w *walPayloadWriter) writeBool(v bool) {
	if v {
		w.writeUint8(1)
		return
	}
	w.writeUint8(0)
}

func (w *walPayloadWriter) writeUint8(v uint8) {
	if w.err != nil {
		return
	}
	w.buf = append(w.buf, v)
}

func (w *walPayloadWriter) writeUint32(v uint32) {
	if w.err != nil {
		return
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	w.buf = append(w.buf, buf[:]...)
}

func (w *walPayloadWriter) writeUint64(v uint64) {
	if w.err != nil {
		return
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	w.buf = append(w.buf, buf[:]...)
}

func (w *walPayloadWriter) writeInt64(v int64) {
	w.writeUint64(uint64(v))
}

func (w *walPayloadWriter) writeInt(v int) {
	w.writeInt64(int64(v))
}

func (w *walPayloadWriter) writeCount(count int) {
	if count < 0 || uint64(count) > walMaxUint32 {
		w.err = fmt.Errorf("WAL payload count %d exceeds uint32", count)
		return
	}
	w.writeUint32(uint32(count))
}

func (w *walPayloadWriter) writeString(v string) {
	if w.err != nil {
		return
	}
	if uint64(len(v)) > walMaxUint32 {
		w.err = fmt.Errorf("WAL payload string length %d exceeds uint32", len(v))
		return
	}
	w.writeUint32(uint32(len(v)))
	// Appending a string to a []byte avoids the []byte(v) conversion alloc.
	w.buf = append(w.buf, v...)
}

func (w *walPayloadWriter) writeBytes(v []byte) {
	if w.err != nil {
		return
	}
	if uint64(len(v)) > walMaxUint32 {
		w.err = fmt.Errorf("WAL payload byte slice length %d exceeds uint32", len(v))
		return
	}
	w.writeUint32(uint32(len(v)))
	w.buf = append(w.buf, v...)
}

func (w *walPayloadWriter) writeTime(v time.Time) {
	if v.IsZero() {
		w.writeInt64(walZeroTimeUnixNano)
		return
	}
	w.writeInt64(v.UTC().UnixNano())
}

type walPayloadReader struct {
	data []byte
	off  int
}

func (r *walPayloadReader) remaining() int {
	return len(r.data) - r.off
}

func (r *walPayloadReader) read(n int) ([]byte, error) {
	if n < 0 || r.remaining() < n {
		return nil, fmt.Errorf("malformed WAL payload: %w", io.ErrUnexpectedEOF)
	}
	out := r.data[r.off : r.off+n]
	r.off += n
	return out, nil
}

func (r *walPayloadReader) readBool() (bool, error) {
	v, err := r.readUint8()
	if err != nil {
		return false, err
	}
	switch v {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("malformed WAL payload: invalid bool %d", v)
	}
}

func (r *walPayloadReader) readUint8() (uint8, error) {
	buf, err := r.read(1)
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (r *walPayloadReader) readUint32() (uint32, error) {
	buf, err := r.read(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf), nil
}

func (r *walPayloadReader) readUint64() (uint64, error) {
	buf, err := r.read(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf), nil
}

func (r *walPayloadReader) readInt64() (int64, error) {
	v, err := r.readUint64()
	return int64(v), err
}

func (r *walPayloadReader) readInt() (int, error) {
	v, err := r.readInt64()
	if err != nil {
		return 0, err
	}
	if int64(int(v)) != v {
		return 0, fmt.Errorf("malformed WAL payload: int value %d overflows int", v)
	}
	return int(v), nil
}

func (r *walPayloadReader) readCount() (int, error) {
	v, err := r.readUint32()
	if err != nil {
		return 0, err
	}
	if uint64(int(v)) != uint64(v) {
		return 0, fmt.Errorf("malformed WAL payload: count %d overflows int", v)
	}
	return int(v), nil
}

func (r *walPayloadReader) readString() (string, error) {
	v, err := r.readBytes()
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func (r *walPayloadReader) readBytes() ([]byte, error) {
	length, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	if uint64(r.remaining()) < uint64(length) {
		return nil, fmt.Errorf("malformed WAL payload: %w", io.ErrUnexpectedEOF)
	}
	buf, err := r.read(int(length))
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), buf...), nil
}

func (r *walPayloadReader) readTime() (time.Time, error) {
	v, err := r.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	if v == walZeroTimeUnixNano {
		return time.Time{}, nil
	}
	return time.Unix(0, v).UTC(), nil
}

func (r *walPayloadReader) readMessageState() (MessageState, error) {
	v, err := r.readString()
	if err != nil {
		return "", err
	}
	state := MessageState(v)
	if !isValidWalMessageState(state) {
		return "", fmt.Errorf("malformed WAL payload: invalid message state %q", state)
	}
	return state, nil
}

func isKnownWalOp(op walOp) bool {
	switch op {
	case opCreateQueue, opPublishBatch, opClaimBatch, opAckBatch, opNack, opReapBatch:
		return true
	default:
		return false
	}
}

func isValidWalMessageState(state MessageState) bool {
	switch state {
	case StateReady, StateInFlight, StateDead:
		return true
	default:
		return false
	}
}

func walPrefix() []byte {
	return []byte("wal|")
}

func walKey(lsn uint64) []byte {
	prefix := walPrefix()
	key := make([]byte, len(prefix)+8)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], lsn)
	return key
}

func parseWalKeyLSN(key []byte) (uint64, error) {
	prefix := walPrefix()
	if len(key) != len(prefix)+8 || !bytes.HasPrefix(key, prefix) {
		return 0, fmt.Errorf("invalid WAL key %q", string(key))
	}
	return binary.BigEndian.Uint64(key[len(prefix):]), nil
}

func walMetaNextLSNKey() []byte {
	return []byte("walmeta|next_lsn")
}

func walMetaLatestSnapshotLSNKey() []byte {
	return []byte("walmeta|latest_snapshot_lsn")
}

func readWalMetaUint64OrDefault(db *pebble.DB, key []byte, defaultValue uint64) (uint64, bool, error) {
	val, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return defaultValue, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer closer.Close()
	if len(val) != 8 {
		return 0, true, fmt.Errorf("invalid WAL metadata %q: got %d bytes, want 8", key, len(val))
	}
	return binary.BigEndian.Uint64(val), true, nil
}

func encodeUint64(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}
