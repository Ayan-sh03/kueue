package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// Phase 2.8: One-time migration from the legacy Pebble hot-path layout to the
// WAL/snapshot architecture.
//
// Legacy layout that we scan from (all written by the pre-WAL handlers):
//
//   - <queueID>                         -> JSON QueueConfig{Name, MaxRetries}
//   - seq:<queueID>                     -> 8-byte uint64 (next sequence counter)
//   - <queueID>|<8B seq>|<messageID>    -> JSON Message
//   - ready|<queueID>|<8B seq>|<msgID>  -> == messageKey bytes (ready index)
//   - inflight|<8B deadline>|<qID>|<mID>-> == messageKey bytes (inflight index)
//
// Migration produces a single snapshot@LSN 0 + advances walmeta|next_lsn to 1
// (no WAL entries ever existed) + sets walmeta|latest_snapshot_lsn to 0 and
// writes the durable marker `migration|pebble_hot_path_imported=true` — all
// in one atomic Pebble batch (Sync). After that the normal recovery path picks
// up the snapshot at LSN 0 and finds no WAL entries LSN > 0 to replay.
//
// Idempotency: once the marker exists, scan is skipped entirely. A corrupt or
// ambiguous old layout aborts startup; no partial marker is written.

const migrationMarkerValue = "true"

func migrationMarkerKey() []byte { return []byte("migration|pebble_hot_path_imported") }

// migrationMarkerExists reports whether the durable migration marker exists.
// Returns false cleanly if the key is absent.
func migrationMarkerExists(db *pebble.DB) (bool, error) {
	if db == nil {
		return false, errors.New("migration marker check: nil db")
	}
	_, closer, err := db.Get(migrationMarkerKey())
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// writeMigrationMarker writes the marker alone, atomically (Sync). Used when
// the legacy scan finds nothing so we never re-scan on subsequent starts, and
// when the DB was created directly on the new layout (walmeta|next_lsn > 1
// without a marker).
func writeMigrationMarker(db *pebble.DB) error {
	return db.Set(migrationMarkerKey(), []byte(migrationMarkerValue), pebble.Sync)
}

// legacyMigrationQueue collects, for each queue ID encountered during the
// legacy scan, its config, sequence counter (if any), and all message records.
type legacyMigrationQueue struct {
	QueueID     string
	Name        string
	MaxRetries  int
	SeqCounter  uint64
	HasSeqCount bool

	// HaveConfig is set when the bare <queueID> JSON config key was seen. We
	// only have to fail loudly if we see a queue via config / message without
	// its counterpart.
	HaveConfig bool

	Messages []legacyMessageRecord
}

type legacyMessageRecord struct {
	ID                 string
	Seq                uint64
	Body               []byte
	State              MessageState
	EnqueuedAt         time.Time
	DeliveryCount      int
	MaxDeliveryCount   int
	VisibilityDeadline time.Time
	DeliveryAttemptID  string
}

// maybeMigrateLegacyLayout runs once at startup. If the migration marker is
// already set, it is a no-op. Otherwise it scans the DB for legacy keys.
//
//   - If `next_lsn > 1` (real WAL activity has happened) the DB is already on
//     the new layout; we just record the marker and return.
//   - If legacy keys are present, we build a snapshot at LSN 0 and commit it
//     together with the WAL meta pointers and the marker in one atomic Sync
//     batch so survive crashes mid-migration.
//   - If no legacy keys are present we just record the marker.
//
// The atomic commit guarantees: a crash mid-migration leaves the DB exactly
// where it was (no partial marker, no half-applied snapshot); the next start
// re-runs the scan. After the snapshot is committed, subsequent restarts
// always find the marker and skip the scan — even if deleting the legacy
// keys' del batch hasn't finished.
//
// The transient snapshots + WAL entries produced post-migration are correct
// because ApplyWALEntry strictly validates state transitions: a missed or
// replayed entry cannot corrupt the migrated snapshot state.
func maybeMigrateLegacyLayout(ctx context.Context, db *pebble.DB, wal *walStore) error {
	if db == nil {
		return errors.New("migration: nil db")
	}
	if wal == nil {
		return errors.New("migration: nil wal store")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	marked, err := migrationMarkerExists(db)
	if err != nil {
		return fmt.Errorf("check migration marker: %w", err)
	}
	if marked {
		return nil
	}

	wal.mu.Lock()
	nextLSN := wal.nextLSN
	wal.mu.Unlock()

	// Already on the new layout with real WAL activity: just record the marker
	// so we never scan again. This also covers fresh-via-WAL DBs that simply
	// never had the marker written.
	if nextLSN > 1 {
		if err := writeMigrationMarker(db); err != nil {
			return fmt.Errorf("write migration marker (existing wal): %w", err)
		}
		return nil
	}

	queues, hasOld, err := scanLegacyLayout(db)
	if err != nil {
		return fmt.Errorf("scan legacy layout: %w", err)
	}
	if !hasOld {
		if err := writeMigrationMarker(db); err != nil {
			return fmt.Errorf("write migration marker (empty): %w", err)
		}
		return nil
	}

	// Validate that every queue we observed has a config. A queue seen only
	// via messages (or only via config) is ambiguous — bail loudly per the
	// issue's "Fail startup loudly on corrupt or ambiguous old data" criterion.
	for id, q := range queues {
		if !q.HaveConfig {
			return fmt.Errorf("migration: queue %q has messages but no config", id)
		}
		if q.MaxRetries < 0 {
			return fmt.Errorf("migration: queue %q has invalid MaxRetries %d", id, q.MaxRetries)
		}
	}

	data, err := buildSnapshotFromLegacy(queues)
	if err != nil {
		return fmt.Errorf("build migration snapshot: %w", err)
	}

	if err := commitMigrationSnapshot(db, wal, data); err != nil {
		return fmt.Errorf("commit migration snapshot: %w", err)
	}
	return nil
}

// scanLegacyLayout walks every key in db, ignoring the modern prefixes, and
// builds a map of queueID -> legacyMigrationQueue. Returns (queues, hasOld, err).
// hasOld is true if any legacy key (config, seq counter, or message) was found.
//
// Reserved (modern) prefixes that we never touch even with full-table scan:
//   - wal|, walmeta|, snapshot|, migration|  (new-layout keys)
//   - ready|, inflight|                     (legacy indexes we can ignore
//                                             safely; the message key under
//                                             <queueID>|<seq>|<messageID> is
//                                             authoritative)
func scanLegacyLayout(db *pebble.DB) (map[string]*legacyMigrationQueue, bool, error) {
	iter, err := db.NewIter(nil)
	if err != nil {
		return nil, false, err
	}
	defer iter.Close()

	queues := make(map[string]*legacyMigrationQueue)

	walP := walPrefix()
	walMetaP := []byte("walmeta|")
	snapP := snapshotPrefix()
	migP := []byte("migration|")
	readyP := []byte("ready|")
	inflightP := inflightPrefix()
	seqP := []byte("seq:")

	hasOld := false

	for iter.First(); iter.Valid(); iter.Next() {
		key := append([]byte(nil), iter.Key()...)

		// Skip any reserved modern prefix and the unwanted legacy indexes.
		if bytes.HasPrefix(key, walP) ||
			bytes.HasPrefix(key, walMetaP) ||
			bytes.HasPrefix(key, snapP) ||
			bytes.HasPrefix(key, migP) ||
			bytes.HasPrefix(key, readyP) ||
			bytes.HasPrefix(key, inflightP) {
			continue
		}

		val, vErr := iter.ValueAndErr()
		if vErr != nil {
			return nil, false, vErr
		}
		// Defensive copy — Pebble may reuse the internal value buffer.
		val = append([]byte(nil), val...)

		// seq:<queueID> = 8-byte uint64 next sequence counter.
		if bytes.HasPrefix(key, seqP) {
			qID := string(key[len(seqP):])
			if qID == "" {
				return nil, false, fmt.Errorf("invalid seq key: empty queue id")
			}
			if len(val) != 8 {
				return nil, false, fmt.Errorf("invalid seq value for queue %q: got %d bytes, want 8", qID, len(val))
			}
			n := binary.BigEndian.Uint64(val)
			q := getMigrationQueue(queues, qID)
			q.SeqCounter = n
			q.HasSeqCount = true
			hasOld = true
			continue
		}

		// Bare UUID key (no `|`, no `:`) — JSON QueueConfig.
		if bytes.IndexByte(key, '|') == -1 && bytes.IndexByte(key, ':') == -1 {
			qID := string(key)
			if qID == "" {
				return nil, false, fmt.Errorf("invalid queue config key: empty")
			}
			var cfg QueueConfig
			if err := json.Unmarshal(val, &cfg); err != nil {
				return nil, false, fmt.Errorf("decode queue config for %q: %w", qID, err)
			}
			q := getMigrationQueue(queues, qID)
			q.Name = cfg.Name
			q.MaxRetries = cfg.MaxRetries
			q.HaveConfig = true
			hasOld = true
			continue
		}

		// Otherwise: <queueID>|<8B seq>|<messageID> — JSON Message.
		qID, seq, msgID, err := parseLegacyMessageKey(key)
		if err != nil {
			return nil, false, fmt.Errorf("parse legacy message key %q: %w", string(key), err)
		}
		if msgID == "" {
			return nil, false, fmt.Errorf("invalid legacy message key: empty message id (%q)", string(key))
		}
		var msg Message
		if err := json.Unmarshal(val, &msg); err != nil {
			return nil, false, fmt.Errorf("decode message %q in queue %q: %w", msgID, qID, err)
		}
		if msg.ID == "" {
			return nil, false, fmt.Errorf("decoded message under key %q has empty ID", msgID)
		}
		if msg.ID != msgID {
			return nil, false, fmt.Errorf("message ID mismatch: key says %q, value says %q (queue %q)", msgID, msg.ID, qID)
		}
		if msg.State != StateReady && msg.State != StateInFlight && msg.State != StateDead {
			return nil, false, fmt.Errorf("message %q in queue %q has invalid state %q", msgID, qID, msg.State)
		}
		q := getMigrationQueue(queues, qID)
		q.Messages = append(q.Messages, legacyMessageRecord{
			ID:                 msg.ID,
			Seq:                seq,
			Body:               msg.Body,
			State:              msg.State,
			EnqueuedAt:         msg.EnqueuedAt,
			DeliveryCount:      msg.DeliveryCount,
			MaxDeliveryCount:   msg.MaxDeliveryCount,
			VisibilityDeadline: msg.VisibilityDeadline,
			DeliveryAttemptID:  msg.DeliveryAttemptID,
		})
		hasOld = true
	}
	if err := iter.Error(); err != nil {
		return nil, false, err
	}
	return queues, hasOld, nil
}

func getMigrationQueue(m map[string]*legacyMigrationQueue, id string) *legacyMigrationQueue {
	q, ok := m[id]
	if !ok {
		q = &legacyMigrationQueue{QueueID: id}
		m[id] = q
	}
	return q
}

// parseLegacyMessageKey splits "<queueID>|<8B seq>|<messageID>" into its parts.
// queueID runs up to the first '|'. Then exactly 8 bytes follow, then another
// '|', then messageID. Returns an error on any structural problem so the caller
// can fail startup loudly rather than silently drop messages.
func parseLegacyMessageKey(key []byte) (queueID string, seq uint64, messageID string, err error) {
	idx := bytes.IndexByte(key, '|')
	if idx <= 0 {
		return "", 0, "", errors.New("no queue id separator")
	}
	qID := string(key[:idx])
	rest := key[idx+1:]
	if len(rest) < 9 || rest[8] != '|' {
		return "", 0, "", errors.New("bad seq/messageID layout")
	}
	seq = binary.BigEndian.Uint64(rest[:8])
	msgID := string(rest[9:])
	if msgID == "" {
		return "", 0, "", errors.New("empty message id")
	}
	return qID, seq, msgID, nil
}

// buildSnapshotFromLegacy builds the migration snapshotData at LSN 0 from the
// legacy scan. Ready messages are sorted by seq for FIFO; inflight and dead
// are sorted by seq for determinism (this is informational — neither is
// list-ordered at runtime). Metrics counters are derived from observed state
// so the post-recovery /metrics matches the in-memory depth.
func buildSnapshotFromLegacy(queues map[string]*legacyMigrationQueue) (snapshotData, error) {
	data := snapshotData{SnapshotLSN: 0}
	if len(queues) == 0 {
		return data, nil
	}
	ids := make([]string, 0, len(queues))
	for id := range queues {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	data.Queues = make([]snapshotQueue, 0, len(ids))

	for _, id := range ids {
		lq := queues[id]

		var ready, inflight, dead []legacyMessageRecord
		for _, m := range lq.Messages {
			switch m.State {
			case StateReady:
				ready = append(ready, m)
			case StateInFlight:
				inflight = append(inflight, m)
			case StateDead:
				dead = append(dead, m)
			}
		}

		sortBySeq(ready)
		sortBySeq(inflight)
		sortBySeq(dead)

		// nextSeq = max(maxObservedSeq, seqCounter) + 1; if only seqCounter is
		// known (no messages) we use seqCounter + 1 (which may be 1 if the
		// counter was never written — fresh queue).
		var maxObservedSeq uint64
		for _, m := range lq.Messages {
			if m.Seq > maxObservedSeq {
				maxObservedSeq = m.Seq
			}
		}
		nextSeq := maxObservedSeq + 1
		if lq.HasSeqCount && lq.SeqCounter+1 > nextSeq {
			nextSeq = lq.SeqCounter + 1
		}

		sq := snapshotQueue{
			QueueID:    id,
			Name:       lq.Name,
			MaxRetries: lq.MaxRetries,
			NextSeq:    nextSeq,
		}

		sq.Ready = make([]snapshotMessage, 0, len(ready))
		for _, m := range ready {
			sq.Ready = append(sq.Ready, snapshotMessage{
				ID:                m.ID,
				Seq:               m.Seq,
				Body:              m.Body,
				EnqueuedAt:        m.EnqueuedAt,
				DeliveryCount:     m.DeliveryCount,
				MaxDeliveryCount:  m.MaxDeliveryCount,
			})
		}

		sq.Inflight = make([]snapshotInflight, 0, len(inflight))
		for _, m := range inflight {
			rh := receiptHandleForMessage(id, m.Seq, m.ID)
			sq.Inflight = append(sq.Inflight, snapshotInflight{
				MessageID:          m.ID,
				Seq:                m.Seq,
				Body:               m.Body,
				EnqueuedAt:         m.EnqueuedAt,
				DeliveryCount:      m.DeliveryCount,
				MaxDeliveryCount:   m.MaxDeliveryCount,
				ReceiptHandle:      rh,
				DeliveryToken:      m.DeliveryAttemptID,
				VisibilityDeadline: m.VisibilityDeadline,
			})
		}

		sq.Dead = make([]snapshotMessage, 0, len(dead))
		for _, m := range dead {
			sq.Dead = append(sq.Dead, snapshotMessage{
				ID:                m.ID,
				Seq:               m.Seq,
				Body:              m.Body,
				EnqueuedAt:        m.EnqueuedAt,
				DeliveryCount:     m.DeliveryCount,
				MaxDeliveryCount:  m.MaxDeliveryCount,
			})
		}

		sq.Metrics = snapshotMetrics{
			ReadyCount:     int64(len(ready)),
			InFlightCount:  int64(len(inflight)),
			DeadCount:      int64(len(dead)),
			TotalPublished: int64(len(lq.Messages)),
		}

		data.Queues = append(data.Queues, sq)
	}
	return data, nil
}

func sortBySeq(s []legacyMessageRecord) {
	sort.Slice(s, func(i, j int) bool { return s[i].Seq < s[j].Seq })
}

// commitMigrationSnapshot writes snapshot@0 + walmeta|next_lsn=1 +
// walmeta|latest_snapshot_lsn=0 + the migration marker in a single atomic
// Pebble batch (Sync). The in-memory walStore state is updated only after a
// successful batch commit. On batch commit failure the caller must abort
// startup; no marker is written so the next start will re-attempt the scan,
// which is idempotent and safe.
func commitMigrationSnapshot(db *pebble.DB, wal *walStore, data snapshotData) error {
	frame, err := encodeSnapshotEntry(data)
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}

	batch := db.NewBatch()
	defer batch.Close()
	if err := batch.Set(snapshotKey(0), frame, nil); err != nil {
		return fmt.Errorf("stage snapshot|0: %w", err)
	}
	if err := batch.Set(walMetaNextLSNKey(), encodeUint64(1), nil); err != nil {
		return fmt.Errorf("stage walmeta|next_lsn: %w", err)
	}
	if err := batch.Set(walMetaLatestSnapshotLSNKey(), encodeUint64(0), nil); err != nil {
		return fmt.Errorf("stage walmeta|latest_snapshot_lsn: %w", err)
	}
	if err := batch.Set(migrationMarkerKey(), []byte(migrationMarkerValue), nil); err != nil {
		return fmt.Errorf("stage migration marker: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit migration batch: %w", err)
	}

	wal.mu.Lock()
	wal.nextLSN = 1
	wal.latestSnapshotLSN = 0
	wal.mu.Unlock()
	return nil
}