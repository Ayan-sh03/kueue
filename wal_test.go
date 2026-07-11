package main

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

func TestWALEncoderRoundTripsOperationPayloads(t *testing.T) {
	now := time.Unix(1700000000, 123).UTC()
	deadline := now.Add(30 * time.Second)

	tests := []struct {
		name  string
		entry walEntry
	}{
		{
			name: "create queue",
			entry: walEntry{
				Op:    opCreateQueue,
				Flags: 1,
				Payload: walCreateQueuePayload{
					QueueID:    "queue-1",
					Name:       "jobs",
					MaxRetries: 3,
				},
			},
		},
		{
			name: "publish batch",
			entry: walEntry{
				Op: opPublishBatch,
				Payload: walPublishBatchPayload{
					QueueID: "queue-1",
					Messages: []walPublishedMessage{
						{
							MessageID:        "msg-1",
							Seq:              11,
							Body:             []byte("hello"),
							EnqueuedAt:       now,
							MaxDeliveryCount: 3,
						},
						{
							MessageID:        "msg-2",
							Seq:              12,
							Body:             []byte{0, 1, 2, 3},
							EnqueuedAt:       now.Add(time.Second),
							MaxDeliveryCount: 5,
						},
					},
				},
			},
		},
		{
			name: "claim batch",
			entry: walEntry{
				Op: opClaimBatch,
				Payload: walClaimBatchPayload{
					QueueID: "queue-1",
					Claims: []walClaimedMessage{
						{
							MessageID:          "msg-1",
							ReceiptHandle:      "receipt-1",
							DeliveryToken:      "token-1",
							VisibilityDeadline: deadline,
							DeliveryCount:      1,
						},
					},
				},
			},
		},
		{
			name: "ack batch",
			entry: walEntry{
				Op: opAckBatch,
				Payload: walAckBatchPayload{
					QueueID: "queue-1",
					Acks: []walAckedMessage{
						{MessageID: "msg-1", ReceiptHandle: "receipt-1", DeliveryToken: "token-1"},
					},
				},
			},
		},
		{
			name: "nack",
			entry: walEntry{
				Op: opNack,
				Payload: walNackPayload{
					QueueID:        "queue-1",
					MessageID:      "msg-1",
					ReceiptHandle:  "receipt-1",
					DeliveryToken:  "token-1",
					TargetState:    StateReady,
					HasNewReadySeq: true,
					NewReadySeq:    22,
				},
			},
		},
		{
			name: "reap batch",
			entry: walEntry{
				Op: opReapBatch,
				Payload: walReapBatchPayload{
					QueueID: "queue-1",
					Reaps: []walReapedMessage{
						{
							MessageID:             "msg-1",
							PreviousDeliveryToken: "token-1",
							TargetState:           StateReady,
							HasNewReadySeq:        true,
							NewReadySeq:           23,
						},
						{
							MessageID:             "msg-2",
							PreviousDeliveryToken: "token-2",
							TargetState:           StateDead,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeWalEntry(tt.entry)
			if err != nil {
				t.Fatalf("encode WAL entry: %v", err)
			}

			decoded, err := decodeWalEntry(42, encoded)
			if err != nil {
				t.Fatalf("decode WAL entry: %v", err)
			}
			if decoded.LSN != 42 {
				t.Fatalf("decoded LSN = %d, want 42", decoded.LSN)
			}

			decoded.LSN = 0
			if !reflect.DeepEqual(decoded, tt.entry) {
				t.Fatalf("decoded entry mismatch\n got: %#v\nwant: %#v", decoded, tt.entry)
			}
		})
	}
}

func TestWALDecodeRejectsCorruptCRC(t *testing.T) {
	encoded, err := encodeWalEntry(walEntry{
		Op: opCreateQueue,
		Payload: walCreateQueuePayload{
			QueueID:    "queue-1",
			Name:       "jobs",
			MaxRetries: 3,
		},
	})
	if err != nil {
		t.Fatalf("encode WAL entry: %v", err)
	}
	encoded[len(encoded)-1] ^= 0xFF

	_, err = decodeWalEntry(1, encoded)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "crc") {
		t.Fatalf("decode corrupt frame err = %v, want CRC error", err)
	}
}

func TestWALDecodeRejectsUnknownVersionAndOp(t *testing.T) {
	encoded, err := encodeWalEntry(walEntry{
		Op:      opCreateQueue,
		Payload: walCreateQueuePayload{QueueID: "queue-1", Name: "jobs"},
	})
	if err != nil {
		t.Fatalf("encode WAL entry: %v", err)
	}

	unknownVersion := append([]byte(nil), encoded...)
	binary.BigEndian.PutUint16(unknownVersion[4:6], walFrameVersion+1)
	_, err = decodeWalEntry(1, unknownVersion)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "unknown wal version") {
		t.Fatalf("decode unknown version err = %v", err)
	}

	unknownOp := append([]byte(nil), encoded...)
	unknownOp[6] = 255
	_, err = decodeWalEntry(1, unknownOp)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "unknown wal op") {
		t.Fatalf("decode unknown op err = %v", err)
	}
}

func TestWALDecodeRejectsTruncatedFrame(t *testing.T) {
	_, err := decodeWalEntry(1, []byte("KWA"))
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "short wal frame") {
		t.Fatalf("decode short frame err = %v, want short frame error", err)
	}
}

// TestWALStoreCloseGate covers the shutdown close gate: Close is idempotent,
// and appends after Close fail cleanly with ErrStorageClosed instead of
// touching the closed handle. (Close blocking until in-flight read-leases drain
// is a property of the RWMutex itself; the concurrent loop here exercises that
// path under -race for the post-close case.)
func TestWALStoreCloseGate(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store, err := newWalStore(db, walSyncNone)
	if err != nil {
		_ = db.Close()
		t.Fatalf("new WAL store: %v", err)
	}

	if _, _, err := store.Append(context.Background(), []walEntry{walCreateQueueEntry("q1")}); err != nil {
		t.Fatalf("append before close: %v", err)
	}

	// Close, then a second Close (idempotent — must not double-close Pebble).
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	// Appends after Close fail cleanly, without touching the closed DB.
	if _, _, err := store.Append(context.Background(), []walEntry{walCreateQueueEntry("q2")}); !errors.Is(err, ErrStorageClosed) {
		t.Fatalf("append after close err = %v, want ErrStorageClosed", err)
	}

	// Concurrent post-close appenders all fail cleanly; run under -race to catch
	// any unsynchronized access to the closed flag.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := store.Append(context.Background(), []walEntry{walCreateQueueEntry("qN")}); !errors.Is(err, ErrStorageClosed) {
				t.Errorf("concurrent post-close append err = %v, want ErrStorageClosed", err)
			}
		}()
	}
	wg.Wait()
}

func TestWALStoreAppendAssignsIncreasingLSNsAndPersistsNext(t *testing.T) {
	setupTestDB(t)

	store, err := newWalStore(Db, walSyncNone)
	if err != nil {
		t.Fatalf("new WAL store: %v", err)
	}

	first, last, err := store.Append(context.Background(), []walEntry{
		walCreateQueueEntry("queue-1"),
		walCreateQueueEntry("queue-2"),
	})
	if err != nil {
		t.Fatalf("append initial entries: %v", err)
	}
	if first != 1 || last != 2 {
		t.Fatalf("first,last = %d,%d; want 1,2", first, last)
	}

	first, last, err = store.Append(context.Background(), []walEntry{walCreateQueueEntry("queue-3")})
	if err != nil {
		t.Fatalf("append second entry: %v", err)
	}
	if first != 3 || last != 3 {
		t.Fatalf("first,last = %d,%d; want 3,3", first, last)
	}
	if got := readWalMetaUint64(t, []byte("walmeta|next_lsn")); got != 4 {
		t.Fatalf("next_lsn meta = %d, want 4", got)
	}

	reopened, err := newWalStore(Db, walSyncNone)
	if err != nil {
		t.Fatalf("reopen WAL store: %v", err)
	}
	first, last, err = reopened.Append(context.Background(), []walEntry{walCreateQueueEntry("queue-4")})
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if first != 4 || last != 4 {
		t.Fatalf("first,last after reopen = %d,%d; want 4,4", first, last)
	}
}

func TestWALStoreReplayReturnsEntriesInLSNOrder(t *testing.T) {
	setupTestDB(t)

	store, err := newWalStore(Db, walSyncNone)
	if err != nil {
		t.Fatalf("new WAL store: %v", err)
	}
	if _, _, err := store.Append(context.Background(), []walEntry{
		walCreateQueueEntry("queue-1"),
		walCreateQueueEntry("queue-2"),
		walCreateQueueEntry("queue-3"),
	}); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	var got []uint64
	err = store.Replay(context.Background(), 0, func(entry walEntry) error {
		got = append(got, entry.LSN)
		return nil
	})
	if err != nil {
		t.Fatalf("replay all: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{1, 2, 3}) {
		t.Fatalf("replay LSNs = %v, want [1 2 3]", got)
	}

	got = nil
	err = store.Replay(context.Background(), 1, func(entry walEntry) error {
		got = append(got, entry.LSN)
		return nil
	})
	if err != nil {
		t.Fatalf("replay after LSN: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{2, 3}) {
		t.Fatalf("replay after 1 LSNs = %v, want [2 3]", got)
	}
}

func TestWALStoreReplayEmptyStoreDoesNotApply(t *testing.T) {
	setupTestDB(t)

	store, err := newWalStore(Db, walSyncNone)
	if err != nil {
		t.Fatalf("new WAL store: %v", err)
	}

	called := false
	err = store.Replay(context.Background(), 0, func(entry walEntry) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("replay empty store: %v", err)
	}
	if called {
		t.Fatal("replay empty store called apply")
	}
}

func TestParseWalSyncMode(t *testing.T) {
	tests := []struct {
		value string
		want  walSyncMode
	}{
		{value: "", want: walSyncNone},
		{value: "none", want: walSyncNone},
		{value: "batch", want: walSyncBatch},
		{value: "always", want: walSyncAlways},
		{value: " batch ", want: walSyncBatch},
	}

	for _, tt := range tests {
		got, err := parseWalSyncMode(tt.value)
		if err != nil {
			t.Fatalf("parseWalSyncMode(%q): %v", tt.value, err)
		}
		if got != tt.want {
			t.Fatalf("parseWalSyncMode(%q) = %v, want %v", tt.value, got, tt.want)
		}
	}

	_, err := parseWalSyncMode("sometimes")
	if err == nil || !strings.Contains(err.Error(), "KUEUE_WAL_SYNC") {
		t.Fatalf("parse invalid sync mode err = %v, want KUEUE_WAL_SYNC error", err)
	}
}

func walCreateQueueEntry(queueID string) walEntry {
	return walEntry{
		Op: opCreateQueue,
		Payload: walCreateQueuePayload{
			QueueID:    queueID,
			Name:       queueID + "-name",
			MaxRetries: 3,
		},
	}
}

func readWalMetaUint64(t *testing.T, key []byte) uint64 {
	t.Helper()

	val, closer, err := Db.Get(key)
	if err != nil {
		t.Fatalf("read WAL meta %q: %v", key, err)
	}
	defer closer.Close()
	if len(val) != 8 {
		t.Fatalf("WAL meta %q length = %d, want 8", key, len(val))
	}
	return binary.BigEndian.Uint64(val)
}
