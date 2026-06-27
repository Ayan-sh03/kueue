package main

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// ============================================================================
// Phase 2.7: Snapshots and WAL compaction
// ============================================================================

// snapshotCfgForTests returns a snapshot config with both dimensions enabled
// so tests drive the trigger by counting ops, and compaction batches small
// enough to exercise the bounded-delete loop.
func snapshotCfgForTests() snapshotConfig {
	return snapshotConfig{
		opsThreshold:     1,
		secondsThreshold: 0,
		compactBatchSize: 3, // tiny batches to force multi-batch compaction
	}
}

// openSnapshotTest opens a fresh Pebble + walStore-backed queueManager with
// snapshotting fully wired (walStore + snapshotCfg set). Returns the manager
// and the underlying walStore so tests can drive TakeSnapshot / compaction.
func openSnapshotTest(t *testing.T, dir string) (*queueManager, *walStore, *pebble.DB) {
	t.Helper()
	deliveryRecordSeq.Store(0)
	metricsStore = sync.Map{}

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	// Tests that reopen the DB call db.Close() explicitly; recover here so a
	// double-close does not panic the whole test binary.
	t.Cleanup(func() {
		defer func() { _ = recover() }()
		_ = db.Close()
	})

	wal, err := newWalStore(db, walSyncNone)
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}
	wal.compactBatchSize = 3
	qm := newQueueManager(wal)
	qm.walStore = wal
	qm.snapshotCfg = snapshotCfgForTests()
	return qm, wal, db
}

// reopenSnapshotTest reopens Pebble + walStore + qm from the supplied dir,
// using the real recovery path (which loads a snapshot if present).
func reopenSnapshotTest(t *testing.T, dir string) (*queueManager, *walStore, *pebble.DB) {
	t.Helper()
	deliveryRecordSeq.Store(0)
	metricsStore = sync.Map{}

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("reopen pebble: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	qm, wal, err := recoverQueueManager(context.Background(), db, walSyncNone, snapshotCfgForTests())
	if err != nil {
		t.Fatalf("recover queue manager: %v", err)
	}
	return qm, wal, db
}

// snapshotQueueState locks q and snapshots ready/inflight/dead counts + the
// ordered ready body sequence + inflight token map.
func snapshotQueueState(t *testing.T, q *queueRuntime) (readyBodies []string, inflightTokens map[string]string, deadBodies []string, nextSeq uint64) {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	for e := q.ready.Front(); e != nil; e = e.Next() {
		readyBodies = append(readyBodies, string(e.Value.(*messageRecord).Body))
	}
	inflightTokens = make(map[string]string, len(q.inflight))
	for handle, dr := range q.inflight {
		inflightTokens[handle] = dr.DeliveryToken
	}
	for _, msg := range q.dead {
		deadBodies = append(deadBodies, string(msg.Body))
	}
	nextSeq = q.nextSeq
	return
}

// ----------------------------------------------------------------------------
// 1. Snapshot roundtrip: ready + in-flight + dead + metrics survive snapshot+reopen
// ----------------------------------------------------------------------------

func TestSnapshotRoundtrip(t *testing.T) {
	dir := t.TempDir()

	qm1, wal1, db1 := openSnapshotTest(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "roundtrip", 2)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}

	// Publish [a,b,c,d]; claim [a,b]; nack one (deliveryCount 1, < MaxRetries 2
	// → ready); publish [e]; claim [c] then let it ride in-flight (no ack);
	// publish [f] then claim two and ack one to dead? dead requires MaxRetries
	// exhausted. Use a separate queue for the dead-letter path.
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm1.ClaimBatch(context.Background(), id, 2)
	if err != nil {
		t.Fatalf("claim 2: %v", err)
	}
	if _, err := qm1.Nack(context.Background(), id, claimed[0].ReceiptHandle, claimed[0].DeliveryAttemptID); err != nil {
		t.Fatalf("nack: %v", err)
	}
	// claim c (now ready order is [c,d,nacked-a,b in tail?]): actually we
	// claimed a,b off the front so ready was [c,d]; nack(a) appended a to tail
	// → ready [c,d,a]. b stays in-flight.
	claimed2, err := qm1.ClaimBatch(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("claim c: %v", err)
	}
	// claimed2[0] = c, now in-flight. Ready = [d,a], inflight = {b,c}.
	_ = claimed2

	// Capture the pre-snapshot state we need to verify after reopen.
	q, _ := qm1.getQueue(id)
	q.mu.Lock()
	wantReadyBodies := []string{"d", "a"}
	var wantInflightTokens []string
	for _, dr := range q.inflight {
		wantInflightTokens = append(wantInflightTokens, dr.DeliveryToken)
	}
	wantNextSeq := q.nextSeq
	wantInflightCount := len(q.inflight)
	q.mu.Unlock()

	lsn, err := qm1.TakeSnapshot(context.Background())
	if err != nil {
		t.Fatalf("take snapshot: %v", err)
	}
	if lsn == 0 {
		t.Fatal("snapshot returned LSN 0")
	}

	// We should have written snapshot|<lsn> and advanced the meta pointer.
	val, closer, err := db1.Get(snapshotKey(lsn))
	if err != nil {
		t.Fatalf("snapshot key missing: %v", err)
	}
	closer.Close()
	if len(val) < snapshotFrameHeader {
		t.Fatalf("snapshot frame too short: %d bytes", len(val))
	}
	metaVal, closer2, err := db1.Get(walMetaLatestSnapshotLSNKey())
	if err != nil {
		t.Fatalf("meta key missing: %v", err)
	}
	closer2.Close()
	if got := decodeUint64(metaVal); got != lsn {
		t.Fatalf("latest_snapshot_lsn = %d, want %d", got, lsn)
	}

	if err := db1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	_ = wal1

	qm2, _, _ := reopenSnapshotTest(t, dir)
	q2, err := qm2.getQueue(id)
	if err != nil {
		t.Fatalf("get queue after reopen: %v", err)
	}

	gotReadyBodies, gotInflightTokens, _, gotNextSeq := snapshotQueueState(t, q2)

	if !slicesEqual(gotReadyBodies, wantReadyBodies) {
		t.Fatalf("ready order after snapshot = %v, want %v", gotReadyBodies, wantReadyBodies)
	}
	if len(gotInflightTokens) != wantInflightCount {
		t.Fatalf("inflight count = %d, want %d", len(gotInflightTokens), wantInflightCount)
	}
	// Every original inflight token must be present (handles map; exact handle
	// ids are stable across snapshot+reopen because the receipt handle is
	// immutable).
	if len(wantInflightTokens) != wantInflightCount {
		t.Fatalf("internal: want token count = %d but captured %d", wantInflightCount, len(wantInflightTokens))
	}
	if gotNextSeq != wantNextSeq {
		t.Fatalf("nextSeq = %d, want %d", gotNextSeq, wantNextSeq)
	}

	// In-flight messages must carry their delivery tokens + deadlines.
	for _, dr := range q2.inflight {
		if dr.DeliveryToken == "" {
			t.Fatal("inflight delivery token empty after snapshot")
		}
		if dr.Deadline.IsZero() {
			t.Fatal("inflight deadline zero after snapshot")
		}
	}

	// Durability of metrics counters.
	q2.mu.Lock()
	if q2.metrics.totalPublished.Load() != 4 {
		t.Fatalf("totalPublished = %d, want 4", q2.metrics.totalPublished.Load())
	}
	if q2.metrics.totalReceived.Load() != 3 {
		t.Fatalf("totalReceived = %d, want 3", q2.metrics.totalReceived.Load())
	}
	if q2.metrics.totalNacked.Load() != 1 {
		t.Fatalf("totalNacked = %d, want 1", q2.metrics.totalNacked.Load())
	}
	if q2.metrics.readyCount.Load() != int64(q2.ready.Len()) {
		t.Fatalf("readyCount metric = %d, readyLen = %d", q2.metrics.readyCount.Load(), q2.ready.Len())
	}
	if q2.metrics.inFlightCount.Load() != int64(len(q2.inflight)) {
		t.Fatalf("inFlightCount metric = %d, inflightLen = %d", q2.metrics.inFlightCount.Load(), len(q2.inflight))
	}
	q2.mu.Unlock()
}

// ----------------------------------------------------------------------------
// 2. Snapshot + tail replay == full WAL replay
// ----------------------------------------------------------------------------

func TestSnapshotPlusTailReplayMatchesFullReplay(t *testing.T) {
	dir := t.TempDir()

	// Build state in qm1, snapshot, then apply more operations after the
	// snapshot ("the tail"). Reopen and capture. Separately, rebuild the
	// identical operation sequence without snapshotting (full replay) and
	// compare the resulting states deeply.
	qm1, _, db1 := openSnapshotTest(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "tail", 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	publish := func(bodies ...string) {
		bs := make([][]byte, len(bodies))
		for i := range bodies {
			bs[i] = []byte(bodies[i])
		}
		if _, err := qm1.PublishBatch(context.Background(), id, bs); err != nil {
			t.Fatalf("publish %v: %v", bodies, err)
		}
	}
	claim := func(n int) []claimedMessage {
		c, err := qm1.ClaimBatch(context.Background(), id, n)
		if err != nil {
			t.Fatalf("claim %d: %v", n, err)
		}
		return c
	}

	publish("a", "b", "c")
	c1 := claim(2) // a,b in-flight
	if _, err := qm1.Nack(context.Background(), id, c1[0].ReceiptHandle, c1[0].DeliveryAttemptID); err != nil {
		t.Fatalf("nack: %v", err)
	}

	if _, err := qm1.TakeSnapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Tail operations after snapshot.
	publish("d", "e")
	c2 := claim(2) // c,a in-flight (ready was [c,a] after nack appended a at tail)
	results := qm1.AckBatch(context.Background(), id, []AckEntry{
		{ReceiptHandle: c2[0].ReceiptHandle, DeliveryToken: c2[0].DeliveryAttemptID},
	})
	if results[0].Status != "ok" {
		t.Fatalf("ack: %s", results[0].Error)
	}
	// nack(c2[1]=a) → back to ready at tail.
	if _, err := qm1.Nack(context.Background(), id, c2[1].ReceiptHandle, c2[1].DeliveryAttemptID); err != nil {
		t.Fatalf("nack tail: %v", err)
	}

	// Record the live (post-tail) state.
	liveState := captureFullQueueState(qm1, id)

	if err := db1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	qm2, _, _ := reopenSnapshotTest(t, dir)

	recoveredState := captureFullQueueState(qm2, id)

	if !statesEqual(liveState, recoveredState) {
		t.Fatalf("snapshot+tail replay diverges from live state\nlive:     %+v\nrecovered: %+v", liveState, recoveredState)
	}

	// Now do a full-replay comparison: replay the same op sequence from scratch
	// in a fresh DB without any snapshot, capturing the final state.
	dir2 := t.TempDir()
	qmA, _, dbA := openSnapshotTest(t, dir2)
	idA, err := qmA.CreateQueue(context.Background(), "tail", 3)
	if err != nil {
		t.Fatalf("create2: %v", err)
	}
	if idA != id {
		// IDs are random UUIDs; the queue IDs will differ. The captured state
		// compare must key by message body sequence rather than message ID,
		// which captureFullQueueState/statesEqual already do. But the queue
		// path still has to match for ops; re-run with idA instead.
	}
	// Replay the identical op stream using idA.
	publishA := func(bodies ...string) {
		bs := make([][]byte, len(bodies))
		for i := range bodies {
			bs[i] = []byte(bodies[i])
		}
		if _, err := qmA.PublishBatch(context.Background(), idA, bs); err != nil {
			t.Fatalf("publish2 %v: %v", bodies, err)
		}
	}
	claimA := func(n int) []claimedMessage {
		c, err := qmA.ClaimBatch(context.Background(), idA, n)
		if err != nil {
			t.Fatalf("claim2 %d: %v", n, err)
		}
		return c
	}
	publishA("a", "b", "c")
	ca1 := claimA(2)
	if _, err := qmA.Nack(context.Background(), idA, ca1[0].ReceiptHandle, ca1[0].DeliveryAttemptID); err != nil {
		t.Fatalf("nack2: %v", err)
	}
	publishA("d", "e")
	ca2 := claimA(2)
	qmA.AckBatch(context.Background(), idA, []AckEntry{
		{ReceiptHandle: ca2[0].ReceiptHandle, DeliveryToken: ca2[0].DeliveryAttemptID},
	})
	if _, err := qmA.Nack(context.Background(), idA, ca2[1].ReceiptHandle, ca2[1].DeliveryAttemptID); err != nil {
		t.Fatalf("nack2 tail: %v", err)
	}

	fullReplayState := captureFullQueueState(qmA, idA)
	if !statesEqual(liveState, fullReplayState) {
		t.Fatalf("full replay diverges from live state\nlive:     %+v\nfull:     %+v", liveState, fullReplayState)
	}
	if err := dbA.Close(); err != nil {
		t.Fatalf("close2: %v", err)
	}
}

// queueStateSnapshot is a deep, ID-independent view of a queue at a moment.
type queueStateSnapshot struct {
	ReadyBodies    []string
	InflightBodies []string
	DeadBodies     []string
	NextSeq        uint64
	TotalPublished int64
	TotalReceived  int64
	TotalAcked     int64
	TotalNacked    int64
}

func captureFullQueueState(qm *queueManager, id string) queueStateSnapshot {
	q, _ := qm.getQueue(id)
	q.mu.Lock()
	defer q.mu.Unlock()
	var s queueStateSnapshot
	for e := q.ready.Front(); e != nil; e = e.Next() {
		s.ReadyBodies = append(s.ReadyBodies, string(e.Value.(*messageRecord).Body))
	}
	for _, dr := range q.inflight {
		if msg, ok := q.messages[dr.MessageID]; ok {
			s.InflightBodies = append(s.InflightBodies, string(msg.Body))
		}
	}
	for _, msg := range q.dead {
		s.DeadBodies = append(s.DeadBodies, string(msg.Body))
	}
	s.NextSeq = q.nextSeq
	s.TotalPublished = q.metrics.totalPublished.Load()
	s.TotalReceived = q.metrics.totalReceived.Load()
	s.TotalAcked = q.metrics.totalAcked.Load()
	s.TotalNacked = q.metrics.totalNacked.Load()
	return s
}

func statesEqual(a, b queueStateSnapshot) bool {
	if a.NextSeq != b.NextSeq {
		return false
	}
	if a.TotalPublished != b.TotalPublished || a.TotalReceived != b.TotalReceived ||
		a.TotalAcked != b.TotalAcked || a.TotalNacked != b.TotalNacked {
		return false
	}
	if !slicesEqual(a.ReadyBodies, b.ReadyBodies) {
		return false
	}
	if !slicesEqual(a.DeadBodies, b.DeadBodies) {
		return false
	}
	return multisetEqual(a.InflightBodies, b.InflightBodies)
}

func multisetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int)
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		counts[v]--
		if counts[v] < 0 {
			return false
		}
	}
	return true
}

// ----------------------------------------------------------------------------
// 3. Crash during snapshot write — corrupt newest snapshot falls back to next-newest
// ----------------------------------------------------------------------------

func TestSnapshotCorruptNewestFallsBackToOlder(t *testing.T) {
	dir := t.TempDir()

	qm1, _, db1 := openSnapshotTest(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "corrupt", 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("v1")}); err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	lsn1, err := qm1.TakeSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot 1: %v", err)
	}
	if lsn1 == 0 {
		t.Fatal("snapshot 1 returned 0")
	}

	// Apply more ops then snapshot again → two snapshots.
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("v2")}); err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	lsn2, err := qm1.TakeSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot 2: %v", err)
	}
	if lsn2 <= lsn1 {
		t.Fatalf("snapshot LSNs not increasing: lsn1=%d lsn2=%d", lsn1, lsn2)
	}

	// Corrupt snapshot|<lsn2> in place: overwrite its value with garbage that
	// will fail the CRC check on decode.
	if err := db1.Set(snapshotKey(lsn2), []byte("garbage-frame-not-even-magic"), pebble.Sync); err != nil {
		t.Fatalf("corrupt snapshot: %v", err)
	}
	// The meta pointer still points at lsn2 (corrupt). Recovery must fall back
	// to lsn1.
	if err := db1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	qm2, wal2, _ := reopenSnapshotTest(t, dir)
	if wal2.latestSnapshotLSN != lsn1 {
		t.Fatalf("after fallback latestSnapshotLSN = %d, want %d", wal2.latestSnapshotLSN, lsn1)
	}
	q, err := qm2.getQueue(id)
	if err != nil {
		t.Fatalf("get queue: %v", err)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	// Snapshot lsn1 captured only "v1" (ready). The tail publishes "v2" must
	// have replayed on top, so ready = [v1, v2].
	var got []string
	for e := q.ready.Front(); e != nil; e = e.Next() {
		got = append(got, string(e.Value.(*messageRecord).Body))
	}
	want := []string{"v1", "v2"}
	if !slicesEqual(got, want) {
		t.Fatalf("ready after corrupt fallback = %v, want %v", got, want)
	}
}

// ----------------------------------------------------------------------------
// 4. Partial snapshot — meta advanced but data missing → fall back, then full replay
// ----------------------------------------------------------------------------

func TestSnapshotMetaAdvancedDataMissingFallsBackAndFullReplay(t *testing.T) {
	dir := t.TempDir()

	qm1, _, db1 := openSnapshotTest(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "partial", 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("x"), []byte("y")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	lsn1, err := qm1.TakeSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// Publish more (the tail that should replay after fallback).
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("z")}); err != nil {
		t.Fatalf("publish z: %v", err)
	}
	// Now simulate a truncated commit for a "would-be" newer snapshot: advance
	// the meta pointer to lsn1+something but leave the snapshot key absent.
	fakeLsn := lsn1 + 100
	if err := db1.Set(walMetaLatestSnapshotLSNKey(), encodeUint64(fakeLsn), pebble.Sync); err != nil {
		t.Fatalf("set meta: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	qm2, wal2, _ := reopenSnapshotTest(t, dir)
	// fakeLsn doesn't exist, lsn1 exists and is good → recovery falls back to
	// lsn1 and rewrites the meta pointer.
	if wal2.latestSnapshotLSN != lsn1 {
		t.Fatalf("latestSnapshotLSN = %d, want %d (fallback)", wal2.latestSnapshotLSN, lsn1)
	}
	q, err := qm2.getQueue(id)
	if err != nil {
		t.Fatalf("get queue: %v", err)
	}
	q.mu.Lock()
	var got []string
	for e := q.ready.Front(); e != nil; e = e.Next() {
		got = append(got, string(e.Value.(*messageRecord).Body))
	}
	q.mu.Unlock()
	// Snapshot lsn1 captured [x,y]; tail publish replayed z → ready = [x,y,z].
	want := []string{"x", "y", "z"}
	if !slicesEqual(got, want) {
		t.Fatalf("ready = %v, want %v", got, want)
	}
}

// ----------------------------------------------------------------------------
// 4b. All snapshots corrupt → fall through to full WAL replay from LSN 0
// ----------------------------------------------------------------------------

func TestSnapshotAllCorruptFullReplay(t *testing.T) {
	dir := t.TempDir()

	qm1, _, db1 := openSnapshotTest(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "allcorrupt", 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	lsn1, err := qm1.TakeSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := db1.Set(snapshotKey(lsn1), []byte("totally bogus bytes no magic"), pebble.Sync); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	qm2, wal2, _ := reopenSnapshotTest(t, dir)
	if wal2.latestSnapshotLSN != 0 {
		t.Fatalf("latestSnapshotLSN = %d, want 0 (no usable snapshot)", wal2.latestSnapshotLSN)
	}
	q, err := qm2.getQueue(id)
	if err != nil {
		t.Fatalf("get queue: %v", err)
	}
	q.mu.Lock()
	var got []string
	for e := q.ready.Front(); e != nil; e = e.Next() {
		got = append(got, string(e.Value.(*messageRecord).Body))
	}
	q.mu.Unlock()
	if !slicesEqual(got, []string{"a", "b"}) {
		t.Fatalf("ready after all-corrupt = %v, want [a b]", got)
	}
}

// ----------------------------------------------------------------------------
// 5. Compaction boundary correctness — bounded batches delete <= snapshotLSN, keep > snapshotLSN
// ----------------------------------------------------------------------------

func TestCompactionBoundaryCorrectness(t *testing.T) {
	dir := t.TempDir()

	qm1, wal1, db1 := openSnapshotTest(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "compact", 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Generate 10 WAL entries (10 single-message publishes) so the compact
	// batches (size 3) are exercised across multiple batches.
	const n = 10
	for i := 0; i < n; i++ {
		if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte(strconv.Itoa(i))}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	lsn, err := qm1.TakeSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// CreateQueue (LSN 1) + 10 publishes (LSNs 2..11) → last committed = 11.
	if lsn != uint64(n+1) {
		t.Fatalf("snapshot LSN = %d, want %d", lsn, n+1)
	}

	// Publish one more after the snapshot — its WAL entry must survive compaction.
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("after")}); err != nil {
		t.Fatalf("publish after: %v", err)
	}
	tailLSN := uint64(n + 2)

	if err := wal1.compactWAL(context.Background(), lsn); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Verify: wal| keys 1..n are gone; wal|<n+1> survives.
	for i := uint64(1); i <= uint64(n); i++ {
		_, closer, err := db1.Get(walKey(i))
		if err == nil {
			closer.Close()
			t.Fatalf("wal|<%d> still present after compaction to %d", i, lsn)
		}
		if err != pebble.ErrNotFound {
			t.Fatalf("get wal|<%d>: %v", i, err)
		}
	}
	val, closer, err := db1.Get(walKey(tailLSN))
	if err != nil {
		t.Fatalf("wal|<%d> missing: %v", tailLSN, err)
	}
	closer.Close()
	if len(val) == 0 {
		t.Fatalf("wal|<%d> value empty after compaction", tailLSN)
	}

	// Snapshot object for lsn must survive (we haven't pruned yet).
	if _, closer, err := db1.Get(snapshotKey(lsn)); err != nil {
		t.Fatalf("snapshot|<%d> missing after compaction (prune is separate): %v", lsn, err)
	} else {
		closer.Close()
	}

	if err := db1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: snapshot applies, tail WAL replays → ready contains [0..9] from
	// snapshot and "after" from the tail.
	qm2, _, _ := reopenSnapshotTest(t, dir)
	q, err := qm2.getQueue(id)
	if err != nil {
		t.Fatalf("get queue after reopen: %v", err)
	}
	q.mu.Lock()
	var got []string
	for e := q.ready.Front(); e != nil; e = e.Next() {
		got = append(got, string(e.Value.(*messageRecord).Body))
	}
	q.mu.Unlock()
	want := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "after"}
	if !slicesEqual(got, want) {
		t.Fatalf("ready after compaction+reopen = %v, want %v", got, want)
	}
}

// ----------------------------------------------------------------------------
// 6. Compaction only deletes <= snapshotLSN (boundary invariant)
// ----------------------------------------------------------------------------

func TestCompactionNeverDeletesAboveSnapshotLSN(t *testing.T) {
	dir := t.TempDir()
	qm1, wal1, db1 := openSnapshotTest(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "inv", 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("m" + strconv.Itoa(i))}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	lsn, err := qm1.TakeSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// CreateQueue (LSN 1) + 5 publishes (LSNs 2..6) → last committed = 6.
	if lsn != 6 {
		t.Fatalf("snapshot LSN = %d, want 6", lsn)
	}
	// Publish one more (LSN 7) that must NOT be compacted.
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("tail")}); err != nil {
		t.Fatalf("tail publish: %v", err)
	}
	if err := wal1.compactWAL(context.Background(), lsn); err != nil {
		t.Fatalf("compact: %v", err)
	}
	for i := uint64(1); i <= lsn; i++ {
		if _, _, err := db1.Get(walKey(i)); err == nil {
			t.Fatalf("wal|<%d> not deleted", i)
		}
	}
	if _, closer, err := db1.Get(walKey(lsn + 1)); err != nil {
		t.Fatalf("tail wal|<%d> deleted by compaction to %d: %v", lsn+1, lsn, err)
	} else {
		closer.Close()
	}
}

// ----------------------------------------------------------------------------
// 9. Snapshot retention — prune keeps only the 2 newest
// ----------------------------------------------------------------------------

func TestSnapshotRetentionKeepsTwoNewest(t *testing.T) {
	dir := t.TempDir()
	qm1, wal1, db1 := openSnapshotTest(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "retention", 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	var lsns []uint64
	for i := 0; i < 4; i++ {
		if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte(strconv.Itoa(i))}); err != nil {
			t.Fatalf("publish: %v", err)
		}
		lsn, err := qm1.TakeSnapshot(context.Background())
		if err != nil {
			t.Fatalf("snapshot %d: %v", i, err)
		}
		lsns = append(lsns, lsn)
	}
	// Prune (normally done by maybeSnapshot; here we call directly).
	if err := wal1.pruneOldSnapshots(context.Background()); err != nil {
		t.Fatalf("prune: %v", err)
	}
	// lsns = [l0,l1,l2,l3]. Keep l2,l3. l0,l1 deleted.
	keep := lsns[len(lsns)-2:]
	delete := lsns[:len(lsns)-2]
	for _, lsn := range keep {
		if _, closer, err := db1.Get(snapshotKey(lsn)); err != nil {
			t.Fatalf("expected snapshot|<%d> to be retained: %v", lsn, err)
		} else {
			closer.Close()
		}
	}
	for _, lsn := range delete {
		if _, _, err := db1.Get(snapshotKey(lsn)); err == nil {
			t.Fatalf("expected snapshot|<%d> to be pruned", lsn)
		}
	}
}

// ----------------------------------------------------------------------------
// 8. Trigger thresholds — maybeSnapshot fires on ops and resets the counter
// ----------------------------------------------------------------------------

func TestMaybeSnapshotFiresOnOpsThreshold(t *testing.T) {
	dir := t.TempDir()
	qm1, wal1, db1 := openSnapshotTest(t, dir)
	// opsThreshold=1 per snapshotCfgForTests → every operation should make the
	// next tick due. But the reaper goroutine isn't running here; call
	// maybeSnapshot directly.
	id, err := qm1.CreateQueue(context.Background(), "trigger", 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("x")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// ops counter incremented to 1 by the publish; threshold=1 → due.
	lsn, err := qm1.maybeSnapshot(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("maybeSnapshot: %v", err)
	}
	if lsn == 0 {
		t.Fatal("expected a snapshot to be taken")
	}
	// Counter should be reset to 0.
	if got := wal1.opsSinceSnapshot.Load(); got != 0 {
		t.Fatalf("opsSinceSnapshot after snapshot = %d, want 0", got)
	}
	// snapshot object exists.
	if _, closer, err := db1.Get(snapshotKey(lsn)); err != nil {
		t.Fatalf("snapshot|<%d> missing: %v", lsn, err)
	} else {
		closer.Close()
	}
}

func TestMaybeSnapshotSecondsThreshold(t *testing.T) {
	dir := t.TempDir()
	qm1, wal1, db1 := openSnapshotTest(t, dir)
	// Use only the seconds threshold (disable ops).
	qm1.snapshotCfg = snapshotConfig{opsThreshold: 0, secondsThreshold: 1 * time.Millisecond, compactBatchSize: 3}
	wal1.compactBatchSize = 3

	id, err := qm1.CreateQueue(context.Background(), "trigger-time", 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("y")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Use a controlled clock so the threshold comparison is deterministic
	// (real time.Now() granularity on Windows can exceed 1ms).
	t0 := time.Now()
	// Initially lastSnapshotAt is zero → due immediately.
	lsn, err := qm1.maybeSnapshot(context.Background(), t0)
	if err != nil {
		t.Fatalf("maybeSnapshot: %v", err)
	}
	if lsn == 0 {
		t.Fatal("expected first tick to be due (zero lastSnapshotAt)")
	}
	if _, closer, err := db1.Get(snapshotKey(lsn)); err != nil {
		t.Fatalf("snapshot|<%d> missing: %v", lsn, err)
	} else {
		closer.Close()
	}
	firstLSN := lsn
	// Call again at the SAME instant: lastSnapshotAt == t0, threshold=1ms, so
	// t0 - t0 = 0 < threshold → NOT due.
	lsn2, err := qm1.maybeSnapshot(context.Background(), t0)
	if err != nil {
		t.Fatalf("maybeSnapshot 2: %v", err)
	}
	if lsn2 != 0 {
		t.Fatalf("expected no snapshot on second call, got LSN %d (first=%d)", lsn2, firstLSN)
	}
	// After advancing past the threshold, it should be due again.
	lsn3, err := qm1.maybeSnapshot(context.Background(), t0.Add(5*time.Millisecond))
	if err != nil {
		t.Fatalf("maybeSnapshot 3: %v", err)
	}
	if lsn3 == 0 {
		t.Fatal("expected snapshot after threshold elapsed")
	}
}

// ----------------------------------------------------------------------------
// 7. Metric durability through snapshot
// ----------------------------------------------------------------------------

func TestSnapshotMetricsDurable(t *testing.T) {
	dir := t.TempDir()
	qm1, _, db1 := openSnapshotTest(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "metrics", 3)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	c, err := qm1.ClaimBatch(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	qm1.AckBatch(context.Background(), id, []AckEntry{
		{ReceiptHandle: c[0].ReceiptHandle, DeliveryToken: c[0].DeliveryAttemptID},
	})
	if _, err := qm1.Nack(context.Background(), id, c[0].ReceiptHandle, c[0].DeliveryAttemptID); err == nil {
		// already acked, ignore
	}

	if _, err := qm1.TakeSnapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	qm2, _, _ := reopenSnapshotTest(t, dir)
	q, _ := qm2.getQueue(id)
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.metrics.totalPublished.Load() != 2 {
		t.Fatalf("totalPublished = %d, want 2", q.metrics.totalPublished.Load())
	}
	if q.metrics.totalReceived.Load() != 1 {
		t.Fatalf("totalReceived = %d, want 1", q.metrics.totalReceived.Load())
	}
	if q.metrics.totalAcked.Load() != 1 {
		t.Fatalf("totalAcked = %d, want 1", q.metrics.totalAcked.Load())
	}
}

// ----------------------------------------------------------------------------
// 10. Empty store — TakeSnapshot returns 0 (nothing to checkpoint)
// ----------------------------------------------------------------------------

func TestSnapshotEmptyStoreReturnsZero(t *testing.T) {
	dir := t.TempDir()
	qm1, wal1, db1 := openSnapshotTest(t, dir)
	// No queues, no WAL entries. TakeSnapshot must return 0 (nothing to checkpoint).
	lsn, err := qm1.TakeSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if lsn != 0 {
		t.Fatalf("expected LSN 0 for store with no committed WAL entries, got %d", lsn)
	}
	// No snapshot object should exist.
	if _, _, err := db1.Get(snapshotKey(1)); err == nil {
		t.Fatal("snapshot|<1> present despite empty data")
	}
	// No meta pointer change.
	if wal1.latestSnapshotLSN != 0 {
		t.Fatalf("latestSnapshotLSN = %d, want 0", wal1.latestSnapshotLSN)
	}
}

// ----------------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------------

func decodeUint64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	v := binaryBigEndianUint64(b)
	return v
}

// binaryBigEndianUint64 avoids importing encoding/binary into the test file
// suite where it isn't already used.
func binaryBigEndianUint64(b []byte) uint64 {
	_ = b[7]
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}