package main

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

func setupTestDB(t *testing.T) {
	t.Helper()

	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}

	Db = db
	Queues = nil
	DeadLetterQueue = nil
	receiveChannel = make(chan struct{}, 1)
	queueReadyChans = map[string]chan struct{}{}
	metricsStore = sync.Map{}
	messageKeyCache = sync.Map{}
	deliveryRecordSeq.Store(0)

	qm, wal, err := initQueueManagerFromEnv(context.Background(), db)
	if err != nil {
		t.Fatalf("init queue manager: %v", err)
	}
	QueueManager = qm
	WAL = wal

	t.Cleanup(func() {
		_ = db.Close()
		Db = nil
		QueueManager = nil
		WAL = nil
	})
}

func decodeResponse[T any](t *testing.T, recorder *httptest.ResponseRecorder) T {
	t.Helper()

	var out T
	if err := json.NewDecoder(recorder.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return out
}

func createTestQueue(t *testing.T, name string) string {
	t.Helper()

	body, err := json.Marshal(CreateRequest{Name: name, MaxRetries: 3})
	if err != nil {
		t.Fatalf("marshal create request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	create(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	resp := decodeResponse[struct {
		ID string `json:"id"`
	}](t, recorder)
	if resp.ID == "" {
		t.Fatal("create returned empty queue id")
	}

	return resp.ID
}

func publishTestMessage(t *testing.T, queueID string, body []byte) string {
	t.Helper()

	reqBody, err := json.Marshal(PublishRequest{
		QueueId: queueID,
		Message: Message{
			Body: body,
		},
	})
	if err != nil {
		t.Fatalf("marshal publish request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(reqBody))
	recorder := httptest.NewRecorder()
	publish(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("publish status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	resp := decodeResponse[struct {
		ID string `json:"id"`
	}](t, recorder)
	if resp.ID == "" {
		t.Fatal("publish returned empty message id")
	}

	return resp.ID
}

func storedMessageKey(t *testing.T, queueID, messageID string) []byte {
	t.Helper()

	key, _, err := findMessageRecord(queueID, messageID)
	if err != nil {
		t.Fatalf("find stored message key: %v", err)
	}

	return key
}

func runtimeQueueState(t *testing.T, queueID string) (ready, inflight, dead int) {
	t.Helper()
	q, err := QueueManager.getQueue(queueID)
	if err != nil {
		t.Fatalf("get queue: %v", err)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.ready.Len(), len(q.inflight), len(q.dead)
}

type receiveResponse struct {
	ID            string       `json:"id"`
	Body          []byte       `json:"body"`
	State         MessageState `json:"state"`
	DeliveryToken string       `json:"deliveryToken"`
	ReceiptHandle string       `json:"receiptHandle"`
}

func receiveTestMessage(t *testing.T, queueID string) receiveResponse {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	recorder := httptest.NewRecorder()
	receive(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("receive status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	return decodeResponse[receiveResponse](t, recorder)
}

func TestReadyPartsFromKeyUsesKnownPrefix(t *testing.T) {
	prefix := readyPrefix("queue-a")
	key := readyKey("queue-a", 0x7c, "message-1")

	seq, msgID, err := readyPartsFromKey(key, prefix)
	if err != nil {
		t.Fatalf("readyPartsFromKey returned error: %v", err)
	}
	if seq != 0x7c {
		t.Fatalf("ready seq = %d, want %d", seq, 0x7c)
	}
	if string(msgID) != "message-1" {
		t.Fatalf("message id = %q, want message-1", string(msgID))
	}

	if _, _, err := readyPartsFromKey(key, readyPrefix("queue-b")); err == nil {
		t.Fatal("expected mismatched prefix error")
	}
}

func TestCreateAndGetQueue(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "test-queue")

	req := httptest.NewRequest(http.MethodGet, "/get?id="+queueID, nil)
	recorder := httptest.NewRecorder()
	getQueue(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	resp := decodeResponse[struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}](t, recorder)

	if resp.ID != queueID {
		t.Fatalf("expected queue id %s, got %s", queueID, resp.ID)
	}
	if resp.Name != "test-queue" {
		t.Fatalf("expected queue name test-queue, got %s", resp.Name)
	}
}

func TestPublishReceiveAck(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "ack-queue")
	messageID := publishTestMessage(t, queueID, []byte("hello"))

	resp := receiveTestMessage(t, queueID)

	if resp.ID != messageID {
		t.Fatalf("expected message id %s, got %s", messageID, resp.ID)
	}
	if string(resp.Body) != "hello" {
		t.Fatalf("expected body hello, got %q", string(resp.Body))
	}
	if resp.State != StateInFlight {
		t.Fatalf("expected in-flight state, got %s", resp.State)
	}
	if resp.DeliveryToken == "" {
		t.Fatal("expected non-empty delivery token")
	}
	if resp.ReceiptHandle == "" {
		t.Fatal("expected non-empty receipt handle")
	}

	ackBody, err := json.Marshal(AckRequest{QueueId: queueID, ReceiptHandle: resp.ReceiptHandle, DeliveryToken: resp.DeliveryToken})
	if err != nil {
		t.Fatalf("marshal ack request: %v", err)
	}

	ackReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
	ackRecorder := httptest.NewRecorder()
	ack(ackRecorder, ackReq)

	if ackRecorder.Code != http.StatusAccepted {
		t.Fatalf("ack status = %d, body = %s", ackRecorder.Code, ackRecorder.Body.String())
	}

	// Message must be gone from the runtime: a second receive returns 404 and
	// a second ack on the same handle is rejected.
	secondReq := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	secondRecorder := httptest.NewRecorder()
	receive(secondRecorder, secondReq)
	if secondRecorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after ack, got %d: %s", secondRecorder.Code, secondRecorder.Body.String())
	}

	dupAckReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
	dupAckRecorder := httptest.NewRecorder()
	ack(dupAckRecorder, dupAckReq)
	if dupAckRecorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for duplicate ack, got %d: %s", dupAckRecorder.Code, dupAckRecorder.Body.String())
	}

	ready, inflight, dead := runtimeQueueState(t, queueID)
	if ready != 0 || inflight != 0 || dead != 0 {
		t.Fatalf("after ack: ready=%d inflight=%d dead=%d, want all 0", ready, inflight, dead)
	}
}

func TestNackMakesMessageReceivableAgain(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "nack-queue")
	messageID := publishTestMessage(t, queueID, []byte("retry"))

	firstResp := receiveTestMessage(t, queueID)

	nackBody, err := json.Marshal(AckRequest{QueueId: queueID, ReceiptHandle: firstResp.ReceiptHandle, DeliveryToken: firstResp.DeliveryToken})
	if err != nil {
		t.Fatalf("marshal nack request: %v", err)
	}

	nackReq := httptest.NewRequest(http.MethodPost, "/nack", bytes.NewReader(nackBody))
	nackRecorder := httptest.NewRecorder()
	nack(nackRecorder, nackReq)
	if nackRecorder.Code != http.StatusAccepted {
		t.Fatalf("nack status = %d, body = %s", nackRecorder.Code, nackRecorder.Body.String())
	}

	secondResp := receiveTestMessage(t, queueID)

	if secondResp.ID != messageID {
		t.Fatalf("expected same message id after nack, got %s", secondResp.ID)
	}
	if string(secondResp.Body) != "retry" {
		t.Fatalf("expected body retry, got %q", string(secondResp.Body))
	}
}

func TestReceiveReturnsMessagesInEnqueueOrder(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "fifo-queue")
	firstID := publishTestMessage(t, queueID, []byte("first"))
	secondID := publishTestMessage(t, queueID, []byte("second"))
	thirdID := publishTestMessage(t, queueID, []byte("third"))

	for _, expected := range []struct {
		id   string
		body string
	}{
		{id: firstID, body: "first"},
		{id: secondID, body: "second"},
		{id: thirdID, body: "third"},
	} {
		resp := receiveTestMessage(t, queueID)

		if resp.ID != expected.id {
			t.Fatalf("expected message id %s, got %s", expected.id, resp.ID)
		}
		if string(resp.Body) != expected.body {
			t.Fatalf("expected body %s, got %q", expected.body, string(resp.Body))
		}

		ackBody, err := json.Marshal(AckRequest{QueueId: queueID, ReceiptHandle: resp.ReceiptHandle, DeliveryToken: resp.DeliveryToken})
		if err != nil {
			t.Fatalf("marshal ack request: %v", err)
		}

		ackReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
		ackRecorder := httptest.NewRecorder()
		ack(ackRecorder, ackReq)
		if ackRecorder.Code != http.StatusAccepted {
			t.Fatalf("ack status = %d, body = %s", ackRecorder.Code, ackRecorder.Body.String())
		}
	}
}

func TestReceiveLongPollUnblocksOnPublish(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "long-poll-queue")

	req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID+"&wait=true", nil)
	recorder := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		receive(recorder, req)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	messageID := publishTestMessage(t, queueID, []byte("delayed"))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("long-poll receive did not complete")
	}

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("receive status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	resp := decodeResponse[receiveResponse](t, recorder)

	if resp.ID != messageID {
		t.Fatalf("expected message id %s, got %s", messageID, resp.ID)
	}
	if string(resp.Body) != "delayed" {
		t.Fatalf("expected body delayed, got %q", string(resp.Body))
	}
}

func TestReceiveLongPollIgnoresOtherQueuePublishes(t *testing.T) {
	setupTestDB(t)

	queueAID := createTestQueue(t, "queue-a")
	queueBID := createTestQueue(t, "queue-b")

	req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueAID+"&wait=true", nil)
	recorder := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		receive(recorder, req)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	publishTestMessage(t, queueBID, []byte("wrong-queue"))

	select {
	case <-done:
		t.Fatal("queue A long-poll returned after publish to queue B")
	case <-time.After(250 * time.Millisecond):
	}

	messageID := publishTestMessage(t, queueAID, []byte("right-queue"))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("queue A long-poll did not complete after publish to queue A")
	}

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("receive status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	resp := decodeResponse[receiveResponse](t, recorder)

	if resp.ID != messageID {
		t.Fatalf("expected message id %s, got %s", messageID, resp.ID)
	}
	if string(resp.Body) != "right-queue" {
		t.Fatalf("expected body right-queue, got %q", string(resp.Body))
	}
}

func TestAckRejectsWrongDeliveryToken(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "token-queue")
	_ = publishTestMessage(t, queueID, []byte("secret"))

	resp := receiveTestMessage(t, queueID)

	ackBody, err := json.Marshal(AckRequest{QueueId: queueID, ReceiptHandle: resp.ReceiptHandle, DeliveryToken: "wrong-token"})
	if err != nil {
		t.Fatalf("marshal ack request: %v", err)
	}

	ackReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
	ackRecorder := httptest.NewRecorder()
	ack(ackRecorder, ackReq)

	if ackRecorder.Code != http.StatusConflict {
		t.Fatalf("expected 409 for wrong delivery token, got %d: %s", ackRecorder.Code, ackRecorder.Body.String())
	}
}

func TestNackRejectsWrongDeliveryToken(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "nack-token-queue")
	_ = publishTestMessage(t, queueID, []byte("secret"))

	resp := receiveTestMessage(t, queueID)

	nackBody, err := json.Marshal(AckRequest{QueueId: queueID, ReceiptHandle: resp.ReceiptHandle, DeliveryToken: "wrong-token"})
	if err != nil {
		t.Fatalf("marshal nack request: %v", err)
	}

	nackReq := httptest.NewRequest(http.MethodPost, "/nack", bytes.NewReader(nackBody))
	nackRecorder := httptest.NewRecorder()
	nack(nackRecorder, nackReq)

	if nackRecorder.Code != http.StatusConflict {
		t.Fatalf("expected 409 for wrong delivery token, got %d: %s", nackRecorder.Code, nackRecorder.Body.String())
	}
}

func TestAckRejectsMalformedReceiptHandle(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "bad-handle-queue")

	ackBody, err := json.Marshal(AckRequest{QueueId: queueID, ReceiptHandle: "not-base64!", DeliveryToken: "token"})
	if err != nil {
		t.Fatalf("marshal ack request: %v", err)
	}

	ackReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
	ackRecorder := httptest.NewRecorder()
	ack(ackRecorder, ackReq)

	if ackRecorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed receipt handle, got %d: %s", ackRecorder.Code, ackRecorder.Body.String())
	}
}

func TestAckRejectsWrongQueueReceiptHandle(t *testing.T) {
	setupTestDB(t)

	queueAID := createTestQueue(t, "queue-a")
	queueBID := createTestQueue(t, "queue-b")
	publishTestMessage(t, queueAID, []byte("secret"))

	resp := receiveTestMessage(t, queueAID)

	ackBody, err := json.Marshal(AckRequest{QueueId: queueBID, ReceiptHandle: resp.ReceiptHandle, DeliveryToken: resp.DeliveryToken})
	if err != nil {
		t.Fatalf("marshal ack request: %v", err)
	}

	ackReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
	ackRecorder := httptest.NewRecorder()
	ack(ackRecorder, ackReq)

	if ackRecorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for wrong queue receipt handle, got %d: %s", ackRecorder.Code, ackRecorder.Body.String())
	}
}

func TestAckDeletesInflightIndex(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "ack-index-queue")
	publishTestMessage(t, queueID, []byte("indexed"))

	resp := receiveTestMessage(t, queueID)

	// After receive the message is in-flight in the runtime.
	if _, inflight, _ := runtimeQueueState(t, queueID); inflight != 1 {
		t.Fatalf("expected 1 in-flight after receive, got %d", inflight)
	}

	ackBody, err := json.Marshal(AckRequest{QueueId: queueID, ReceiptHandle: resp.ReceiptHandle, DeliveryToken: resp.DeliveryToken})
	if err != nil {
		t.Fatalf("marshal ack request: %v", err)
	}
	ackReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
	ackRecorder := httptest.NewRecorder()
	ack(ackRecorder, ackReq)
	if ackRecorder.Code != http.StatusAccepted {
		t.Fatalf("ack status = %d, body = %s", ackRecorder.Code, ackRecorder.Body.String())
	}

	// After ack the in-flight entry is gone and the message is no longer
	// receivable or re-ackable.
	ready, inflight, dead := runtimeQueueState(t, queueID)
	if ready != 0 || inflight != 0 || dead != 0 {
		t.Fatalf("after ack: ready=%d inflight=%d dead=%d, want all 0", ready, inflight, dead)
	}

	secondReq := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	secondRecorder := httptest.NewRecorder()
	receive(secondRecorder, secondReq)
	if secondRecorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after ack, got %d: %s", secondRecorder.Code, secondRecorder.Body.String())
	}
}

func TestReapDeadLettersAfterMaxDeliveries(t *testing.T) {
	setupTestDB(t)

	body, err := json.Marshal(CreateRequest{Name: "dl-queue", MaxRetries: 1})
	if err != nil {
		t.Fatalf("marshal create request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	create(recorder, req)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	createResp := decodeResponse[struct {
		ID string `json:"id"`
	}](t, recorder)

	queueID := createResp.ID
	publishTestMessage(t, queueID, []byte("poison"))

	_ = receiveTestMessage(t, queueID)

	// Force the visibility timeout to elapse and reap from the runtime.
	transitions := QueueManager.ReapExpired(context.Background(), time.Now().Add(31*time.Second))
	if len(transitions) != 1 {
		t.Fatalf("expected one reap transition, got %d", len(transitions))
	}
	if transitions[0].QueueID != queueID || transitions[0].ToState != StateDead {
		t.Fatalf("unexpected transition: %+v", transitions[0])
	}

	if _, _, dead := runtimeQueueState(t, queueID); dead != 1 {
		t.Fatalf("expected 1 dead message after reap, got %d", dead)
	}

	// Dead messages must not be delivered.
	secondReq := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	secondRecorder := httptest.NewRecorder()
	receive(secondRecorder, secondReq)
	if secondRecorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for dead-lettered queue, got %d: %s", secondRecorder.Code, secondRecorder.Body.String())
	}
}

func TestReaperUsesInflightIndexWithReadyBacklog(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "indexed-reaper-queue")
	messageID := publishTestMessage(t, queueID, []byte("expired"))
	resp := receiveTestMessage(t, queueID)
	if resp.ID != messageID {
		t.Fatalf("expected received message %s, got %s", messageID, resp.ID)
	}
	for i := 0; i < 50; i++ {
		publishTestMessage(t, queueID, []byte("ready-backlog"))
	}

	// The claimed message is in-flight; the 50 new messages are ready.
	if ready, inflight, _ := runtimeQueueState(t, queueID); ready != 50 || inflight != 1 {
		t.Fatalf("before reap: ready=%d inflight=%d, want 50,1", ready, inflight)
	}

	transitions := QueueManager.ReapExpired(context.Background(), time.Now().Add(31*time.Second))
	if len(transitions) != 1 {
		t.Fatalf("expected one reaper transition, got %d", len(transitions))
	}
	if transitions[0].QueueID != queueID || transitions[0].ToState != StateReady {
		t.Fatalf("unexpected transition: %+v", transitions[0])
	}

	// Reaped message returns to ready tail; backlog is undisturbed.
	if ready, inflight, dead := runtimeQueueState(t, queueID); ready != 51 || inflight != 0 || dead != 0 {
		t.Fatalf("after reap: ready=%d inflight=%d dead=%d, want 51,0,0", ready, inflight, dead)
	}
}

func TestNackDeadLettersAfterMaxDeliveries(t *testing.T) {
	setupTestDB(t)

	body, err := json.Marshal(CreateRequest{Name: "nack-dl-queue", MaxRetries: 1})
	if err != nil {
		t.Fatalf("marshal create request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	create(recorder, req)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	createResp := decodeResponse[struct {
		ID string `json:"id"`
	}](t, recorder)

	queueID := createResp.ID
	messageID := publishTestMessage(t, queueID, []byte("nack-poison"))
	_ = messageID

	resp := receiveTestMessage(t, queueID)

	nackBody, err := json.Marshal(AckRequest{QueueId: queueID, ReceiptHandle: resp.ReceiptHandle, DeliveryToken: resp.DeliveryToken})
	if err != nil {
		t.Fatalf("marshal nack request: %v", err)
	}
	nackReq := httptest.NewRequest(http.MethodPost, "/nack", bytes.NewReader(nackBody))
	nackRecorder := httptest.NewRecorder()
	nack(nackRecorder, nackReq)
	if nackRecorder.Code != http.StatusAccepted {
		t.Fatalf("nack status = %d, body = %s", nackRecorder.Code, nackRecorder.Body.String())
	}

	nackResp := decodeResponse[struct {
		State MessageState `json:"state"`
	}](t, nackRecorder)
	if nackResp.State != StateDead {
		t.Fatalf("expected StateDead after nack with max deliveries, got %s", nackResp.State)
	}

	if _, _, dead := runtimeQueueState(t, queueID); dead != 1 {
		t.Fatalf("expected 1 dead message after nack, got %d", dead)
	}

	// Dead messages must not be redelivered.
	secondReq := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	secondRecorder := httptest.NewRecorder()
	receive(secondRecorder, secondReq)
	if secondRecorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for dead-lettered queue, got %d: %s", secondRecorder.Code, secondRecorder.Body.String())
	}
}

func TestQueueHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	recorder := httptest.NewRecorder()

	queueHandler(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("queue handler status = %d", recorder.Code)
	}
	if got := recorder.Body.String(); got != "Hello Consumer\n" {
		t.Fatalf("unexpected body %q", got)
	}
}

func BenchmarkReceiveLatencyVsDepth(b *testing.B) {
	depths := []int{100, 1000, 10000}
	for _, depth := range depths {
		b.Run(fmt.Sprintf("depth_%d", depth), func(b *testing.B) {
			db, err := pebble.Open(b.TempDir(), &pebble.Options{})
			if err != nil {
				b.Fatalf("open db: %v", err)
			}
			Db = db
			Queues = nil
			DeadLetterQueue = nil
			metricsStore = sync.Map{}
			messageKeyCache = sync.Map{}
			receiveChannel = make(chan struct{}, 1)
			queueReadyChans = map[string]chan struct{}{}
			deliveryRecordSeq.Store(0)
			qm, wal, err := initQueueManagerFromEnv(context.Background(), db)
			if err != nil {
				b.Fatalf("init queue manager: %v", err)
			}
			QueueManager = qm
			WAL = wal
			b.Cleanup(func() {
				db.Close()
				Db = nil
				QueueManager = nil
				WAL = nil
			})

			queueID := createBenchQueue(b, "bench-queue")

			for i := 0; i < depth; i++ {
				publishBenchMessage(b, queueID, []byte("fill"))
			}

			for i := 0; i < depth; i++ {
				receiveBenchMessage(b, queueID)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				publishBenchMessage(b, queueID, []byte("lat"))
				receiveBenchMessage(b, queueID)
			}
		})
	}
}

func createBenchQueue(b testing.TB, name string) string {
	b.Helper()
	body, _ := json.Marshal(CreateRequest{Name: name, MaxRetries: 3})
	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	create(rec, req)
	if rec.Code != http.StatusAccepted {
		b.Fatalf("create status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		ID string `json:"id"`
	}
	json.NewDecoder(rec.Body).Decode(&resp)
	return resp.ID
}

func publishBenchMessage(b testing.TB, queueID string, body []byte) {
	b.Helper()
	msgBody, _ := json.Marshal(PublishRequest{
		Message: Message{Body: body},
		QueueId: queueID,
	})
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(msgBody))
	rec := httptest.NewRecorder()
	publish(rec, req)
	if rec.Code != http.StatusAccepted {
		b.Fatalf("publish status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestAckRatePerSec_NewQueue(t *testing.T) {
	m := &queueMetrics{
		startedAt: time.Now().Add(-5 * time.Second),
	}
	for i := 0; i < 10; i++ {
		m.ackCountWindow.Add(1)
	}

	rate := m.ackRatePerSec()

	elapsed := math.Min(60.0, 5.0)
	expected := 10.0 / elapsed
	if math.Abs(rate-expected) > 0.01 {
		t.Errorf("ackRatePerSec = %.4f, want %.4f", rate, expected)
	}
}

func TestAckRatePerSec_MatureQueue(t *testing.T) {
	m := &queueMetrics{
		startedAt: time.Now().Add(-120 * time.Second),
	}
	m.totalAcked.Store(60)

	rate := m.ackRatePerSec()

	expected := 0.5
	if math.Abs(rate-expected) > 0.01 {
		t.Errorf("ackRatePerSec = %.4f, want %.4f", rate, expected)
	}
}

func TestAckRatePerSec_EmptyWindow(t *testing.T) {
	m := &queueMetrics{
		startedAt: time.Now(),
	}

	rate := m.ackRatePerSec()
	if rate != 0 {
		t.Errorf("ackRatePerSec = %.4f, want 0", rate)
	}
}

func TestReconcileMetricsFromDBIfStaleSkipsWithinInterval(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "metrics-throttle-queue")
	key := messageKey(queueID, 1, "bad-message")
	if err := Db.Set(key, []byte("{bad json"), pebble.Sync); err != nil {
		t.Fatalf("write malformed message: %v", err)
	}

	m := getOrCreateMetrics(queueID)
	now := time.Now()
	m.lastReconcile = now.Add(-metricsReconcileInterval / 2)

	if err := reconcileMetricsFromDBIfStale(queueID, m, now); err != nil {
		t.Fatalf("reconcile inside throttle interval returned error: %v", err)
	}
}

func TestReconcileMetricsFromDBIfStaleRunsAfterInterval(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "metrics-throttle-expired-queue")
	key := messageKey(queueID, 1, "bad-message")
	if err := Db.Set(key, []byte("{bad json"), pebble.Sync); err != nil {
		t.Fatalf("write malformed message: %v", err)
	}

	m := getOrCreateMetrics(queueID)
	now := time.Now()
	m.lastReconcile = now.Add(-metricsReconcileInterval)

	if err := reconcileMetricsFromDBIfStale(queueID, m, now); err == nil {
		t.Fatal("expected reconcile after throttle interval to scan DB and return an error")
	}
	if !m.lastReconcile.Equal(now) {
		t.Fatalf("lastReconcile = %v, want %v", m.lastReconcile, now)
	}
	if err := reconcileMetricsFromDBIfStale(queueID, m, now.Add(time.Second)); err != nil {
		t.Fatalf("expected failed reconcile to be throttled, got error: %v", err)
	}
}

func receiveBenchMessage(b testing.TB, queueID string) receiveResponse {
	b.Helper()
	req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	rec := httptest.NewRecorder()
	receive(rec, req)
	if rec.Code != http.StatusAccepted {
		b.Fatalf("receive status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp receiveResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	return resp
}

func ackClaimedMessagesBench(b testing.TB, queueID string, msgs []claimedMessage) {
	b.Helper()

	acks := make([]AckEntry, 0, len(msgs))
	for _, msg := range msgs {
		acks = append(acks, AckEntry{
			ReceiptHandle: msg.ReceiptHandle,
			DeliveryToken: msg.DeliveryAttemptID,
		})
	}

	body, err := json.Marshal(BatchAckRequest{QueueId: queueID, Acks: acks})
	if err != nil {
		b.Fatalf("marshal batch ack: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	ack(rec, req)
	if rec.Code != http.StatusAccepted {
		b.Fatalf("ack status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func setupDepthBenchmarkDB(b *testing.B) string {
	b.Helper()

	db, err := pebble.Open(b.TempDir(), &pebble.Options{})
	if err != nil {
		b.Fatalf("open bench db: %v", err)
	}

	Db = db
	Queues = nil
	DeadLetterQueue = nil
	receiveChannel = make(chan struct{}, 1)
	queueReadyChans = map[string]chan struct{}{}
	metricsStore = sync.Map{}
	messageKeyCache = sync.Map{}
	deliveryRecordSeq.Store(0)

	qm, wal, err := initQueueManagerFromEnv(context.Background(), db)
	if err != nil {
		b.Fatalf("init queue manager: %v", err)
	}
	QueueManager = qm
	WAL = wal

	b.Cleanup(func() {
		_ = db.Close()
		Db = nil
		QueueManager = nil
		WAL = nil
	})

	return createBenchQueue(b, "depth-bench-queue")
}

func BenchmarkBatchReceiveOnly(b *testing.B) {
	const batchSize = 10
	for _, depth := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("depth_%d", depth), func(b *testing.B) {
			queueID := setupDepthBenchmarkDB(b)
			for i := 0; i < depth+b.N*batchSize; i++ {
				publishBenchMessage(b, queueID, []byte("fill"))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msgs, err := QueueManager.ClaimBatch(context.Background(), queueID, batchSize)
				if err != nil {
					b.Fatalf("claim batch: %v", err)
				}
				if len(msgs) != batchSize {
					b.Fatalf("claimed %d messages, want %d", len(msgs), batchSize)
				}
			}
		})
	}
}

func BenchmarkBatchAckOnly(b *testing.B) {
	const batchSize = 10
	for _, depth := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("depth_%d", depth), func(b *testing.B) {
			queueID := setupDepthBenchmarkDB(b)
			for i := 0; i < depth+b.N*batchSize; i++ {
				publishBenchMessage(b, queueID, []byte("fill"))
			}

			batches := make([][]claimedMessage, 0, b.N)
			for i := 0; i < b.N; i++ {
				msgs, err := QueueManager.ClaimBatch(context.Background(), queueID, batchSize)
				if err != nil {
					b.Fatalf("claim batch: %v", err)
				}
				batches = append(batches, msgs)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ackClaimedMessagesBench(b, queueID, batches[i])
			}
		})
	}
}

func BenchmarkBatchReceiveAndAck(b *testing.B) {
	const batchSize = 10
	for _, depth := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("depth_%d", depth), func(b *testing.B) {
			queueID := setupDepthBenchmarkDB(b)
			for i := 0; i < depth+b.N*batchSize; i++ {
				publishBenchMessage(b, queueID, []byte("fill"))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msgs, err := QueueManager.ClaimBatch(context.Background(), queueID, batchSize)
				if err != nil {
					b.Fatalf("claim batch: %v", err)
				}
				ackClaimedMessagesBench(b, queueID, msgs)
			}
		})
	}
}

func BenchmarkReaperDueInflightIndex(b *testing.B) {
	for _, readyDepth := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("ready_depth_%d", readyDepth), func(b *testing.B) {
			queueID := setupDepthBenchmarkDB(b)
			for i := 0; i < readyDepth; i++ {
				publishBenchMessage(b, queueID, []byte("ready"))
			}

			for i := 0; i < b.N; i++ {
				publishBenchMessage(b, queueID, []byte("expired"))
				if _, err := QueueManager.ClaimBatch(context.Background(), queueID, 1); err != nil {
					b.Fatalf("claim for reap: %v", err)
				}
			}

			b.ResetTimer()
			transitions := QueueManager.ReapExpired(context.Background(), time.Now().Add(31*time.Second))
			b.StopTimer()
			if len(transitions) != b.N {
				b.Fatalf("reaped %d messages, want %d", len(transitions), b.N)
			}
		})
	}
}

type testBatchReceiveResponse struct {
	Messages []struct {
		ID            string       `json:"id"`
		Body          []byte       `json:"body"`
		State         MessageState `json:"state"`
		DeliveryToken string       `json:"deliveryToken"`
		ReceiptHandle string       `json:"receiptHandle"`
	} `json:"messages"`
}

type batchAckResult struct {
	MessageId     string `json:"messageId"`
	ReceiptHandle string `json:"receiptHandle"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
}

type batchAckResponse struct {
	Results []batchAckResult `json:"results"`
}

func TestBatchReceiveReturnsMultipleMessages(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "batch-queue")

	publishTestMessage(t, queueID, []byte("first"))
	publishTestMessage(t, queueID, []byte("second"))
	publishTestMessage(t, queueID, []byte("third"))

	req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID+"&max=5", nil)
	recorder := httptest.NewRecorder()
	receive(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("batch receive status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	resp := decodeResponse[testBatchReceiveResponse](t, recorder)
	if len(resp.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(resp.Messages))
	}
	if string(resp.Messages[0].Body) != "first" {
		t.Fatalf("first body = %q, want first", string(resp.Messages[0].Body))
	}
	if string(resp.Messages[1].Body) != "second" {
		t.Fatalf("second body = %q, want second", string(resp.Messages[1].Body))
	}
	if string(resp.Messages[2].Body) != "third" {
		t.Fatalf("third body = %q, want third", string(resp.Messages[2].Body))
	}
	for _, m := range resp.Messages {
		if m.State != StateInFlight {
			t.Fatalf("message %s state = %s, want in_flight", m.ID, m.State)
		}
		if m.DeliveryToken == "" {
			t.Fatalf("message %s has empty delivery token", m.ID)
		}
		if m.ReceiptHandle == "" {
			t.Fatalf("message %s has empty receipt handle", m.ID)
		}
	}
}

func TestBatchReceiveRespectsMaxLimit(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "max-limit-queue")

	for i := 0; i < 10; i++ {
		publishTestMessage(t, queueID, []byte("msg"))
	}

	req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID+"&max=3", nil)
	recorder := httptest.NewRecorder()
	receive(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("batch receive status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	resp := decodeResponse[testBatchReceiveResponse](t, recorder)
	if len(resp.Messages) != 3 {
		t.Fatalf("expected 3 messages (max=3), got %d", len(resp.Messages))
	}
}

func TestBatchReceiveMaxParamValidation(t *testing.T) {
	tests := []struct {
		max  string
		code int
	}{
		{max: "0", code: http.StatusBadRequest},
		{max: "-1", code: http.StatusBadRequest},
		{max: "101", code: http.StatusBadRequest},
		{max: "abc", code: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run("max="+tt.max, func(t *testing.T) {
			setupTestDB(t)
			queueID := createTestQueue(t, "validation-queue")
			req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID+"&max="+tt.max, nil)
			recorder := httptest.NewRecorder()
			receive(recorder, req)
			if recorder.Code != tt.code {
				t.Fatalf("expected status %d, got %d: %s", tt.code, recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestBatchAckMultipleMessages(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "batch-ack-queue")

	msg1ID := publishTestMessage(t, queueID, []byte("one"))
	msg2ID := publishTestMessage(t, queueID, []byte("two"))
	msg3ID := publishTestMessage(t, queueID, []byte("three"))

	batchResp := func() testBatchReceiveResponse {
		req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID+"&max=10", nil)
		rec := httptest.NewRecorder()
		receive(rec, req)
		return decodeResponse[testBatchReceiveResponse](t, rec)
	}()

	if len(batchResp.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(batchResp.Messages))
	}

	// Batch ack all three
	ackBody, _ := json.Marshal(BatchAckRequest{
		QueueId: queueID,
		Acks: []AckEntry{
			{ReceiptHandle: batchResp.Messages[0].ReceiptHandle, DeliveryToken: batchResp.Messages[0].DeliveryToken},
			{ReceiptHandle: batchResp.Messages[1].ReceiptHandle, DeliveryToken: batchResp.Messages[1].DeliveryToken},
			{ReceiptHandle: batchResp.Messages[2].ReceiptHandle, DeliveryToken: batchResp.Messages[2].DeliveryToken},
		},
	})

	ackReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
	ackRecorder := httptest.NewRecorder()
	ack(ackRecorder, ackReq)

	if ackRecorder.Code != http.StatusAccepted {
		t.Fatalf("batch ack status = %d, body = %s", ackRecorder.Code, ackRecorder.Body.String())
	}

	ackResp := decodeResponse[batchAckResponse](t, ackRecorder)
	if len(ackResp.Results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(ackResp.Results))
	}
	for i, res := range ackResp.Results {
		if res.Status != "ok" {
			t.Fatalf("ack[%d] status = %s, error = %s", i, res.Status, res.Error)
		}
	}

	// Verify all messages are gone from the runtime.
	ready, inflight, dead := runtimeQueueState(t, queueID)
	if ready != 0 || inflight != 0 || dead != 0 {
		t.Fatalf("after batch ack: ready=%d inflight=%d dead=%d, want all 0", ready, inflight, dead)
	}

	emptyReq := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	emptyRec := httptest.NewRecorder()
	receive(emptyRec, emptyReq)
	if emptyRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after batch ack, got %d: %s", emptyRec.Code, emptyRec.Body.String())
	}
	_ = msg1ID
	_ = msg2ID
	_ = msg3ID
}

func TestBatchAckReportsPartialErrors(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "batch-ack-err-queue")

	_ = publishTestMessage(t, queueID, []byte("one"))
	_ = publishTestMessage(t, queueID, []byte("two"))

	batchResp := func() testBatchReceiveResponse {
		req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID+"&max=10", nil)
		rec := httptest.NewRecorder()
		receive(rec, req)
		return decodeResponse[testBatchReceiveResponse](t, rec)
	}()

	if len(batchResp.Messages) < 2 {
		t.Fatalf("expected at least 2 messages, got %d", len(batchResp.Messages))
	}

	// Ack: msg1 with correct token, msg2 with wrong token
	ackBody, _ := json.Marshal(BatchAckRequest{
		QueueId: queueID,
		Acks: []AckEntry{
			{ReceiptHandle: batchResp.Messages[0].ReceiptHandle, DeliveryToken: batchResp.Messages[0].DeliveryToken},
			{ReceiptHandle: batchResp.Messages[1].ReceiptHandle, DeliveryToken: "wrong-token"},
		},
	})

	ackReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
	ackRecorder := httptest.NewRecorder()
	ack(ackRecorder, ackReq)

	if ackRecorder.Code != http.StatusAccepted {
		t.Fatalf("batch ack status = %d, body = %s", ackRecorder.Code, ackRecorder.Body.String())
	}

	ackResp := decodeResponse[batchAckResponse](t, ackRecorder)
	if len(ackResp.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(ackResp.Results))
	}

	if r0 := ackResp.Results[0]; r0.Status != "ok" {
		t.Fatalf("ack[0] expected ok, got %s: %s", r0.Status, r0.Error)
	}
	if r1 := ackResp.Results[1]; r1.Status != "error" {
		t.Fatalf("ack[1] expected error, got %s", r1.Status)
	}

	// msg2 (wrong token) must still be in-flight: re-acking with the correct
	// token succeeds, proving it was not deleted by the failed batch ack.
	reAckBody, _ := json.Marshal(BatchAckRequest{
		QueueId: queueID,
		Acks: []AckEntry{
			{ReceiptHandle: batchResp.Messages[1].ReceiptHandle, DeliveryToken: batchResp.Messages[1].DeliveryToken},
		},
	})
	reAckReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(reAckBody))
	reAckRecorder := httptest.NewRecorder()
	ack(reAckRecorder, reAckReq)
	if reAckRecorder.Code != http.StatusAccepted {
		t.Fatalf("re-ack of msg2 status = %d, body = %s", reAckRecorder.Code, reAckRecorder.Body.String())
	}
	reAckResp := decodeResponse[batchAckResponse](t, reAckRecorder)
	if reAckResp.Results[0].Status != "ok" {
		t.Fatalf("re-ack of msg2 expected ok, got %s: %s", reAckResp.Results[0].Status, reAckResp.Results[0].Error)
	}
}

func TestBatchReceiveFollowsFIFOOrder(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "batch-fifo-queue")

	count := 20
	for i := 0; i < count; i++ {
		publishTestMessage(t, queueID, []byte(fmt.Sprintf("msg-%d", i)))
	}

	req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID+"&max=10", nil)
	recorder := httptest.NewRecorder()
	receive(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("batch receive status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	resp := decodeResponse[testBatchReceiveResponse](t, recorder)
	if len(resp.Messages) != 10 {
		t.Fatalf("expected 10 messages, got %d", len(resp.Messages))
	}

	for i, m := range resp.Messages {
		expected := fmt.Sprintf("msg-%d", i)
		if string(m.Body) != expected {
			t.Fatalf("message[%d] body = %q, want %q", i, string(m.Body), expected)
		}
	}
}

func TestBatchReceiveLongPollWithWait(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "batch-longpoll")

	req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID+"&max=5&wait=true", nil)
	recorder := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		receive(recorder, req)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 3; i++ {
		publishTestMessage(t, queueID, []byte(fmt.Sprintf("batch-%d", i)))
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("batch long-poll receive did not complete")
	}

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("receive status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	resp := decodeResponse[testBatchReceiveResponse](t, recorder)
	if len(resp.Messages) == 0 {
		t.Fatal("expected at least 1 message from long poll")
	}
	if len(resp.Messages) > 3 {
		t.Fatalf("expected at most 3 messages, got %d", len(resp.Messages))
	}
	// FIFO: first published should be first received
	if string(resp.Messages[0].Body) != "batch-0" {
		t.Fatalf("first message body = %q, want batch-0", string(resp.Messages[0].Body))
	}
}

// ============================================================================
// Phase 2.4: create/publish handler runtime+WAL integration
// ============================================================================

func setupTestDBWithFakeWAL(t *testing.T) *fakeWAL {
	t.Helper()

	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}

	Db = db
	Queues = nil
	DeadLetterQueue = nil
	receiveChannel = make(chan struct{}, 1)
	queueReadyChans = map[string]chan struct{}{}
	metricsStore = sync.Map{}
	messageKeyCache = sync.Map{}
	deliveryRecordSeq.Store(0)

	wal := &fakeWAL{}
	QueueManager = newQueueManager(wal)

	t.Cleanup(func() {
		_ = db.Close()
		Db = nil
		QueueManager = nil
	})

	return wal
}

func collectReadySeqs(q *queueRuntime) []uint64 {
	var seqs []uint64
	for e := q.ready.Front(); e != nil; e = e.Next() {
		seqs = append(seqs, e.Value.(*messageRecord).Seq)
	}
	return seqs
}

func TestPublishHandlerWALFailureReturns500AndRollsBack(t *testing.T) {
	wal := setupTestDBWithFakeWAL(t)
	queueID := createTestQueue(t, "wal-fail-handler")

	publishTestMessage(t, queueID, []byte("ok"))

	wal.mu.Lock()
	beforeFail := len(wal.entries)
	wal.fail = true
	wal.mu.Unlock()

	body, _ := json.Marshal(PublishRequest{QueueId: queueID, Message: Message{Body: []byte("fail")}})
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	publish(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on WAL failure, got %d: %s", rec.Code, rec.Body.String())
	}

	wal.mu.Lock()
	afterFail := len(wal.entries)
	wal.fail = false
	wal.mu.Unlock()
	if afterFail != beforeFail {
		t.Fatalf("WAL entries changed on failed publish: %d -> %d", beforeFail, afterFail)
	}

	ready, inflight, dead := runtimeQueueState(t, queueID)
	if ready != 1 || inflight != 0 || dead != 0 {
		t.Fatalf("after failed publish: ready=%d inflight=%d dead=%d, want 1,0,0", ready, inflight, dead)
	}

	q, _ := QueueManager.getQueue(queueID)
	q.mu.Lock()
	nextSeq := q.nextSeq
	q.mu.Unlock()
	if nextSeq != 1 {
		t.Fatalf("nextSeq = %d after rolled-back publish, want 1", nextSeq)
	}
}

func TestPublishHandlerMemoryLimitReturns429(t *testing.T) {
	wal := setupTestDBWithFakeWAL(t)
	queueID := createTestQueue(t, "mem-limit-handler")

	q, err := QueueManager.getQueue(queueID)
	if err != nil {
		t.Fatalf("get queue: %v", err)
	}
	q.mu.Lock()
	q.maxMessages = 1
	q.mu.Unlock()

	publishTestMessage(t, queueID, []byte("first"))

	body, _ := json.Marshal(PublishRequest{QueueId: queueID, Message: Message{Body: []byte("second")}})
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	publish(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on memory limit, got %d: %s", rec.Code, rec.Body.String())
	}

	if ready, _, _ := runtimeQueueState(t, queueID); ready != 1 {
		t.Fatalf("ready = %d, want 1 (rejected publish must not mutate memory)", ready)
	}

	wal.mu.Lock()
	n := len(wal.entries)
	wal.mu.Unlock()
	if n != 2 {
		t.Fatalf("WAL entries = %d, want 2 (create + one publish; rejected publish appends nothing)", n)
	}
}

func TestPerQueueSequenceIndependence(t *testing.T) {
	setupTestDB(t)
	queueA := createTestQueue(t, "seq-a")
	queueB := createTestQueue(t, "seq-b")

	const n = 10
	for i := 0; i < n; i++ {
		publishTestMessage(t, queueA, []byte("a"))
		publishTestMessage(t, queueB, []byte("b"))
	}

	qA, _ := QueueManager.getQueue(queueA)
	qB, _ := QueueManager.getQueue(queueB)
	qA.mu.Lock()
	seqsA := collectReadySeqs(qA)
	qA.mu.Unlock()
	qB.mu.Lock()
	seqsB := collectReadySeqs(qB)
	qB.mu.Unlock()

	if len(seqsA) != n || len(seqsB) != n {
		t.Fatalf("seq counts = %d, %d, want %d each", len(seqsA), len(seqsB), n)
	}
	for i, s := range seqsA {
		if s != uint64(i) {
			t.Fatalf("seqA[%d] = %d, want %d", i, s, i)
		}
	}
	for i, s := range seqsB {
		if s != uint64(i) {
			t.Fatalf("seqB[%d] = %d, want %d", i, s, i)
		}
	}
}

func TestCreatePublishAPICompatibility(t *testing.T) {
	setupTestDB(t)

	// create bad JSON -> 400
	badCreate := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader([]byte("{bad")))
	badCreateRec := httptest.NewRecorder()
	create(badCreateRec, badCreate)
	if badCreateRec.Code != http.StatusBadRequest {
		t.Fatalf("create bad json status = %d, want 400", badCreateRec.Code)
	}

	// create happy path -> 202 + {id, state: ready}
	queueID := createTestQueue(t, "compat-queue")

	// single publish -> 202 + {id, state: ready}
	body, _ := json.Marshal(PublishRequest{QueueId: queueID, Message: Message{Body: []byte("x")}})
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	publish(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("publish status = %d, want 202", rec.Code)
	}
	var pr struct {
		ID    string       `json:"id"`
		State MessageState `json:"state"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&pr); err != nil {
		t.Fatalf("decode publish response: %v", err)
	}
	if pr.ID == "" {
		t.Fatal("publish returned empty id")
	}
	if pr.State != StateReady {
		t.Fatalf("publish state = %s, want ready", pr.State)
	}

	// publish-batch -> 202 + {ids:[...]}
	batchBody, _ := json.Marshal(BatchPublishRequest{
		QueueId:  queueID,
		Messages: []Message{{Body: []byte("y")}, {Body: []byte("z")}},
	})
	breq := httptest.NewRequest(http.MethodPost, "/publish-batch", bytes.NewReader(batchBody))
	brec := httptest.NewRecorder()
	publishBatch(brec, breq)
	if brec.Code != http.StatusAccepted {
		t.Fatalf("publish-batch status = %d, want 202", brec.Code)
	}
	var br BatchPublishResponse
	json.NewDecoder(brec.Body).Decode(&br)
	if len(br.IDs) != 2 {
		t.Fatalf("batch ids = %d, want 2", len(br.IDs))
	}

	// publish to missing queue -> 404
	missBody, _ := json.Marshal(PublishRequest{QueueId: "nope", Message: Message{Body: []byte("x")}})
	mreq := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(missBody))
	mrec := httptest.NewRecorder()
	publish(mrec, mreq)
	if mrec.Code != http.StatusNotFound {
		t.Fatalf("missing queue publish status = %d, want 404", mrec.Code)
	}

	// publish bad JSON -> 400
	badReq := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader([]byte("{bad")))
	badRec := httptest.NewRecorder()
	publish(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("bad json publish status = %d, want 400", badRec.Code)
	}

	// publish-batch empty messages -> 400
	emptyBatch, _ := json.Marshal(BatchPublishRequest{QueueId: queueID, Messages: nil})
	emptyReq := httptest.NewRequest(http.MethodPost, "/publish-batch", bytes.NewReader(emptyBatch))
	emptyRec := httptest.NewRecorder()
	publishBatch(emptyRec, emptyReq)
	if emptyRec.Code != http.StatusBadRequest {
		t.Fatalf("empty batch status = %d, want 400", emptyRec.Code)
	}
}

// ============================================================================
// Phase 2.5: receive/ack/nack handler-level validation
// ============================================================================

func TestCompetingConsumersNoDuplicateDelivery(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "competing-consumers")
	const totalMsgs = 100
	const consumers = 10

	for i := 0; i < totalMsgs; i++ {
		publishTestMessage(t, queueID, []byte(fmt.Sprintf("msg-%d", i)))
	}

	type consumerResult struct {
		ids []string
		err error
	}

	var mu sync.Mutex
	results := make([]consumerResult, consumers)
	var wg sync.WaitGroup
	wg.Add(consumers)

	for c := 0; c < consumers; c++ {
		go func(consumerIdx int) {
			defer wg.Done()
			var ids []string
			for {
				req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
				rec := httptest.NewRecorder()
				receive(rec, req)
				if rec.Code == http.StatusNotFound {
					break
				}
				if rec.Code != http.StatusAccepted {
					results[consumerIdx].err = fmt.Errorf("consumer %d: receive status = %d, body = %s", consumerIdx, rec.Code, rec.Body.String())
					return
				}
				resp := decodeResponse[receiveResponse](t, rec)
				ids = append(ids, resp.ID)
			}
			mu.Lock()
			results[consumerIdx].ids = ids
			mu.Unlock()
		}(c)
	}

	wg.Wait()

	for i, r := range results {
		if r.err != nil {
			t.Fatalf("consumer %d failed: %v", i, r.err)
		}
	}

	allIDs := make(map[string]int)
	totalConsumed := 0
	for _, r := range results {
		for _, id := range r.ids {
			if allIDs[id] > 0 {
				t.Fatalf("duplicate delivery: message %s delivered to multiple consumers", id)
			}
			allIDs[id]++
			totalConsumed++
		}
	}

	if totalConsumed != totalMsgs {
		t.Fatalf("total consumed = %d, want %d", totalConsumed, totalMsgs)
	}

	minCount, maxCount := totalMsgs, 0
	for _, r := range results {
		count := len(r.ids)
		if count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
	}
	if minCount == 0 {
		t.Fatalf("a consumer received 0 messages: distribution min=%d max=%d", minCount, maxCount)
	}
}

func TestHandlerStaleAckTokenAfterRedeliveryReturns409(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "stale-token-handler")

	publishTestMessage(t, queueID, []byte("poison"))

	firstResp := receiveTestMessage(t, queueID)
	oldToken := firstResp.DeliveryToken
	receiptHandle := firstResp.ReceiptHandle

	nackBody, _ := json.Marshal(AckRequest{
		QueueId:       queueID,
		ReceiptHandle: receiptHandle,
		DeliveryToken: oldToken,
	})
	nackReq := httptest.NewRequest(http.MethodPost, "/nack", bytes.NewReader(nackBody))
	nackRec := httptest.NewRecorder()
	nack(nackRec, nackReq)
	if nackRec.Code != http.StatusAccepted {
		t.Fatalf("nack status = %d, want 202, body = %s", nackRec.Code, nackRec.Body.String())
	}

	secondResp := receiveTestMessage(t, queueID)
	if secondResp.DeliveryToken == oldToken {
		t.Fatal("expected new delivery token after redelivery")
	}
	if secondResp.ReceiptHandle != receiptHandle {
		t.Fatalf("receipt handle changed: got %q, want %q (immutable across redeliveries)", secondResp.ReceiptHandle, receiptHandle)
	}

	ackBody, _ := json.Marshal(AckRequest{
		QueueId:       queueID,
		ReceiptHandle: receiptHandle,
		DeliveryToken: oldToken,
	})
	ackReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
	ackRec := httptest.NewRecorder()
	ack(ackRec, ackReq)
	if ackRec.Code != http.StatusConflict {
		t.Fatalf("stale token ack status = %d, want 409, body = %s", ackRec.Code, ackRec.Body.String())
	}

	ackBody2, _ := json.Marshal(AckRequest{
		QueueId:       queueID,
		ReceiptHandle: receiptHandle,
		DeliveryToken: secondResp.DeliveryToken,
	})
	ackReq2 := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody2))
	ackRec2 := httptest.NewRecorder()
	ack(ackRec2, ackReq2)
	if ackRec2.Code != http.StatusAccepted {
		t.Fatalf("fresh token ack status = %d, want 202, body = %s", ackRec2.Code, ackRec2.Body.String())
	}
}

func TestHandlerClaimWALFailureReturns500AndRollsBack(t *testing.T) {
	wal := setupTestDBWithFakeWAL(t)
	queueID := createTestQueue(t, "claim-wal-fail")

	publishTestMessage(t, queueID, []byte("pending"))

	wal.mu.Lock()
	wal.fail = true
	wal.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	rec := httptest.NewRecorder()
	receive(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("receive during WAL failure: status = %d, want 500, body = %s", rec.Code, rec.Body.String())
	}

	ready, inflight, _ := runtimeQueueState(t, queueID)
	if ready != 1 || inflight != 0 {
		t.Fatalf("after WAL failure rollback: ready=%d inflight=%d, want 1,0", ready, inflight)
	}

	wal.mu.Lock()
	wal.fail = false
	wal.mu.Unlock()

	resp := receiveTestMessage(t, queueID)
	if resp.ID == "" {
		t.Fatal("expected non-empty message id after successful receive post-rollback")
	}
}

// ============================================================================
// Phase 2.2: queueRuntime test harness
// ============================================================================

type fakeWAL struct {
	mu           sync.Mutex
	entries      []walEntry
	nextLSN      uint64
	fail         bool
	beforeAppend func(context.Context, []walEntry) error
}

func (f *fakeWAL) Append(ctx context.Context, entries []walEntry) (uint64, uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	if len(entries) == 0 {
		return 0, 0, nil
	}
	if f.beforeAppend != nil {
		if err := f.beforeAppend(ctx, entries); err != nil {
			return 0, 0, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return 0, 0, errors.New("fake WAL failure")
	}
	first := f.nextLSN + 1
	for i := range entries {
		f.nextLSN = first + uint64(i)
		entries[i].LSN = f.nextLSN
		f.entries = append(f.entries, entries[i])
	}
	return first, f.nextLSN, nil
}

func setupRuntimeTest(t *testing.T) (*queueManager, *fakeWAL) {
	t.Helper()
	deliveryRecordSeq.Store(0)
	wal := &fakeWAL{}
	qm := newQueueManager(wal)
	return qm, wal
}

func TestFakeWALAppendStoresReturnedLSNs(t *testing.T) {
	wal := &fakeWAL{}
	first, last, err := wal.Append(context.Background(), []walEntry{
		{Op: opCreateQueue, Payload: walCreateQueuePayload{QueueID: "q1", Name: "one"}},
		{Op: opCreateQueue, Payload: walCreateQueuePayload{QueueID: "q2", Name: "two"}},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if first != 1 || last != 2 {
		t.Fatalf("first,last = %d,%d; want 1,2", first, last)
	}
	if len(wal.entries) != 2 {
		t.Fatalf("stored entries = %d, want 2", len(wal.entries))
	}
	if wal.entries[0].LSN != 1 || wal.entries[1].LSN != 2 {
		t.Fatalf("stored LSNs = %d,%d; want 1,2", wal.entries[0].LSN, wal.entries[1].LSN)
	}
}

// ============================================================================
// T1: CreateQueue
// ============================================================================

func TestRuntimeCreateQueue(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, err := qm.CreateQueue(ctx, "test-queue", 3)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty queue ID")
	}

	q, err := qm.getQueue(id)
	if err != nil {
		t.Fatalf("get queue: %v", err)
	}
	if q.config.Name != "test-queue" {
		t.Fatalf("queue name = %q, want test-queue", q.config.Name)
	}
	if q.config.MaxRetries != 3 {
		t.Fatalf("maxRetries = %d, want 3", q.config.MaxRetries)
	}
}

// ============================================================================
// T2: PublishBatch FIFO
// ============================================================================

func TestRuntimePublishBatchFIFO(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "fifo", 3)
	bodies := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}
	_, err := qm.PublishBatch(ctx, id, bodies)
	if err != nil {
		t.Fatalf("publish batch: %v", err)
	}

	claimed, err := qm.ClaimBatch(ctx, id, 5)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}
	if len(claimed) != 5 {
		t.Fatalf("claimed %d, want 5", len(claimed))
	}
	for i, msg := range claimed {
		want := bodies[i]
		if !bytes.Equal(msg.Body, want) {
			t.Fatalf("message %d body = %q, want %q", i, msg.Body, want)
		}
	}
}

// ============================================================================
// T3: Claim empty queue
// ============================================================================

func TestRuntimeClaimEmptyQueue(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "empty", 3)
	_, err := qm.ClaimBatch(ctx, id, 1)
	if !errors.Is(err, ErrNoReadyMessages) {
		t.Fatalf("expected ErrNoReadyMessages, got %v", err)
	}
}

// ============================================================================
// T4: ClaimBatch respects max
// ============================================================================

func TestRuntimeClaimBatchMax(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "max-test", 3)
	bodies := make([][]byte, 10)
	for i := range bodies {
		bodies[i] = []byte(fmt.Sprintf("msg-%d", i))
	}
	_, err := qm.PublishBatch(ctx, id, bodies)
	if err != nil {
		t.Fatalf("publish batch: %v", err)
	}

	claimed, err := qm.ClaimBatch(ctx, id, 3)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d, want 3", len(claimed))
	}

	q, _ := qm.getQueue(id)
	q.mu.Lock()
	readyCount := q.ready.Len()
	q.mu.Unlock()
	if readyCount != 7 {
		t.Fatalf("ready count = %d, want 7", readyCount)
	}
}

// ============================================================================
// T5: Ack removes from state
// ============================================================================

func TestRuntimeAckRemovesFromState(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "ack-test", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatal("expected 1 claimed message")
	}

	results := qm.AckBatch(ctx, id, []AckEntry{
		{ReceiptHandle: claimed[0].ReceiptHandle, DeliveryToken: claimed[0].DeliveryAttemptID},
	})
	if len(results) != 1 || results[0].Status != "ok" {
		t.Fatalf("ack result = %+v", results)
	}

	q, _ := qm.getQueue(id)
	q.mu.Lock()
	if len(q.messages) != 0 {
		t.Fatalf("messages len = %d, want 0", len(q.messages))
	}
	if len(q.inflight) != 0 {
		t.Fatalf("inflight len = %d, want 0", len(q.inflight))
	}
	if q.deadlines.Len() != 0 {
		t.Fatalf("deadlines len = %d, want 0", q.deadlines.Len())
	}
	q.mu.Unlock()
}

// ============================================================================
// T6: Nack returns to ready
// ============================================================================

func TestRuntimeNackReturnsToReady(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "nack-test", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	_, err = qm.Nack(ctx, id, claimed[0].ReceiptHandle, claimed[0].DeliveryAttemptID)
	if err != nil {
		t.Fatalf("nack: %v", err)
	}

	q, _ := qm.getQueue(id)
	q.mu.Lock()
	readyCount := q.ready.Len()
	q.mu.Unlock()
	if readyCount != 1 {
		t.Fatalf("ready count after nack = %d, want 1", readyCount)
	}

	// Should be receivable again.
	claimed2, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("reclaim after nack: %v", err)
	}
	if !bytes.Equal(claimed2[0].Body, []byte("hello")) {
		t.Fatalf("reclaimed body = %q, want hello", claimed2[0].Body)
	}
}

// ============================================================================
// T7: Nack appends to ready tail
// ============================================================================

func TestRuntimeNackToReadyTail(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "tail-test", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("A"), []byte("B")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	claimed, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if string(claimed[0].Body) != "A" {
		t.Fatalf("first claim = %q, want A", claimed[0].Body)
	}

	_, err = qm.Nack(ctx, id, claimed[0].ReceiptHandle, claimed[0].DeliveryAttemptID)
	if err != nil {
		t.Fatalf("nack: %v", err)
	}

	claimed2, err := qm.ClaimBatch(ctx, id, 2)
	if err != nil {
		t.Fatalf("claim 2: %v", err)
	}
	if len(claimed2) != 2 {
		t.Fatalf("claimed %d, want 2", len(claimed2))
	}
	if string(claimed2[0].Body) != "B" {
		t.Fatalf("first after nack = %q, want B", claimed2[0].Body)
	}
	if string(claimed2[1].Body) != "A" {
		t.Fatalf("second after nack = %q, want A", claimed2[1].Body)
	}
}

// ============================================================================
// T8: Dead letter after max deliveries via nack
// ============================================================================

func TestRuntimeDeadLetterAfterMaxDeliveries(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "dead-test", 1)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed[0].DeliveryCount != 1 {
		t.Fatalf("delivery count = %d, want 1", claimed[0].DeliveryCount)
	}

	state, err := qm.Nack(ctx, id, claimed[0].ReceiptHandle, claimed[0].DeliveryAttemptID)
	if err != nil {
		t.Fatalf("nack: %v", err)
	}
	if state != StateDead {
		t.Fatalf("state = %q, want dead", state)
	}

	q, _ := qm.getQueue(id)
	q.mu.Lock()
	deadCount := len(q.dead)
	q.mu.Unlock()
	if deadCount != 1 {
		t.Fatalf("dead count = %d, want 1", deadCount)
	}

	// Should NOT be receivable.
	_, err = qm.ClaimBatch(ctx, id, 1)
	if !errors.Is(err, ErrNoReadyMessages) {
		t.Fatalf("expected ErrNoReadyMessages after dead letter, got %v", err)
	}
}

// ============================================================================
// T9: Stale ack token rejected after redelivery
// ============================================================================

func TestRuntimeStaleAckRejected(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "stale-test", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	oldToken := claimed[0].DeliveryAttemptID

	_, err = qm.Nack(ctx, id, claimed[0].ReceiptHandle, oldToken)
	if err != nil {
		t.Fatalf("nack: %v", err)
	}

	claimed2, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if claimed2[0].DeliveryAttemptID == oldToken {
		t.Fatal("expected new delivery token after redelivery")
	}

	// Using the old token with the current receipt handle should yield
	// a delivery token mismatch, not "receipt handle not found".
	// Seq is now immutable across redeliveries, so the receipt handle
	// stays the same and the inflight lookup succeeds.
	results := qm.AckBatch(ctx, id, []AckEntry{
		{ReceiptHandle: claimed2[0].ReceiptHandle, DeliveryToken: oldToken},
	})
	if len(results) != 1 || results[0].Status != "error" {
		t.Fatalf("expected stale ack rejected, got %+v", results)
	}
	if !strings.Contains(results[0].Error, "delivery token mismatch") {
		t.Fatalf("expected delivery token mismatch error, got: %s", results[0].Error)
	}
}

// ============================================================================
// T10: Reap expired to ready
// ============================================================================

func TestRuntimeReapExpiredToReady(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "reap-ready", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	_, err = qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Manually expire the message.
	q, _ := qm.getQueue(id)
	q.mu.Lock()
	for _, msg := range q.messages {
		msg.VisibilityDeadline = time.Now().Add(-1 * time.Second)
	}
	// Rebuild deadlines heap with the new deadline.
	q.deadlines = q.deadlines[:0]
	for _, dr := range q.inflight {
		dr.heapIndex = -1
		dr.Deadline = time.Now().Add(-1 * time.Second)
		heap.Push(&q.deadlines, dr)
	}
	q.mu.Unlock()

	transitions := qm.ReapExpired(ctx, time.Now())
	if len(transitions) != 1 {
		t.Fatalf("reap transitions = %d, want 1", len(transitions))
	}
	if transitions[0].ToState != StateReady {
		t.Fatalf("reap toState = %q, want ready", transitions[0].ToState)
	}

	// Should be receivable again.
	claimed2, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("reclaim after reap: %v", err)
	}
	if !bytes.Equal(claimed2[0].Body, []byte("hello")) {
		t.Fatalf("reclaimed body = %q, want hello", claimed2[0].Body)
	}
}

func TestRuntimeReapDeadlineEqualNowIsExpired(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "reap-equal-now", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	_, err = qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	now := time.Now()
	q, _ := qm.getQueue(id)
	q.mu.Lock()
	for _, msg := range q.messages {
		msg.VisibilityDeadline = now
	}
	q.deadlines = q.deadlines[:0]
	for _, dr := range q.inflight {
		dr.heapIndex = -1
		dr.Deadline = now
		heap.Push(&q.deadlines, dr)
	}
	q.mu.Unlock()

	transitions := qm.ReapExpired(ctx, now)
	if len(transitions) != 1 {
		t.Fatalf("reap transitions = %d, want 1", len(transitions))
	}
	if transitions[0].ToState != StateReady {
		t.Fatalf("reap toState = %q, want ready", transitions[0].ToState)
	}
}

// T9b: Receipt handle is preserved across redeliveries
func TestRuntimeReceiptHandlePreservedAcrossRedelivery(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "rh-preserve", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	firstRH := claimed[0].ReceiptHandle

	_, err = qm.Nack(ctx, id, claimed[0].ReceiptHandle, claimed[0].DeliveryAttemptID)
	if err != nil {
		t.Fatalf("nack: %v", err)
	}

	claimed2, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	// Receipt handle must be the same after redelivery because Seq is immutable.
	if claimed2[0].ReceiptHandle != firstRH {
		t.Fatalf("receipt handle changed after redelivery: got %q, want %q", claimed2[0].ReceiptHandle, firstRH)
	}
}

// ============================================================================
// T11: Reap dead letter
// ============================================================================

func TestRuntimeReapDeadLetter(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "reap-dead", 1)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	_, err = qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	q, _ := qm.getQueue(id)
	q.mu.Lock()
	for _, msg := range q.messages {
		msg.VisibilityDeadline = time.Now().Add(-1 * time.Second)
	}
	q.deadlines = q.deadlines[:0]
	for _, dr := range q.inflight {
		dr.heapIndex = -1
		dr.Deadline = time.Now().Add(-1 * time.Second)
		heap.Push(&q.deadlines, dr)
	}
	q.mu.Unlock()

	transitions := qm.ReapExpired(ctx, time.Now())
	if len(transitions) != 1 {
		t.Fatalf("reap transitions = %d, want 1", len(transitions))
	}
	if transitions[0].ToState != StateDead {
		t.Fatalf("reap toState = %q, want dead", transitions[0].ToState)
	}

	q.mu.Lock()
	deadCount := len(q.dead)
	q.mu.Unlock()
	if deadCount != 1 {
		t.Fatalf("dead count = %d, want 1", deadCount)
	}
}

// ============================================================================
// T12: Concurrent queues do not block each other
// ============================================================================

func TestRuntimeConcurrentQueuesDontBlock(t *testing.T) {
	qm, wal := setupRuntimeTest(t)
	ctx := context.Background()

	id1, _ := qm.CreateQueue(ctx, "q1", 3)
	id2, _ := qm.CreateQueue(ctx, "q2", 3)

	_, err := qm.PublishBatch(ctx, id1, [][]byte{[]byte("a")})
	if err != nil {
		t.Fatalf("publish q1: %v", err)
	}
	_, err = qm.PublishBatch(ctx, id2, [][]byte{[]byte("b")})
	if err != nil {
		t.Fatalf("publish q2: %v", err)
	}

	blockEntered := make(chan struct{})
	releaseBlock := make(chan struct{})
	var blockOnce sync.Once
	wal.beforeAppend = func(ctx context.Context, entries []walEntry) error {
		if len(entries) == 0 || entries[0].Op != opClaimBatch {
			return nil
		}
		payload, ok := entries[0].Payload.(walClaimBatchPayload)
		if !ok || payload.QueueID != id1 {
			return nil
		}
		blockOnce.Do(func() {
			close(blockEntered)
		})
		select {
		case <-releaseBlock:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	var got1 []claimedMessage
	q1Done := make(chan error, 1)
	go func() {
		var err1 error
		got1, err1 = qm.ClaimBatch(ctx, id1, 1)
		q1Done <- err1
	}()

	select {
	case <-blockEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("q1 claim did not reach blocked WAL append")
	}

	var got2 []claimedMessage
	q2Done := make(chan error, 1)
	go func() {
		var err2 error
		got2, err2 = qm.ClaimBatch(ctx, id2, 1)
		q2Done <- err2
	}()

	select {
	case err := <-q2Done:
		if err != nil {
			t.Fatalf("claim q2: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(releaseBlock)
		t.Fatal("q2 claim blocked behind q1 claim")
	}

	close(releaseBlock)
	select {
	case err := <-q1Done:
		if err != nil {
			t.Fatalf("claim q1: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("q1 claim did not finish after unblocking WAL")
	}

	if len(got1) != 1 || string(got1[0].Body) != "a" {
		t.Fatalf("q1 claim wrong: %+v", got1)
	}
	if len(got2) != 1 || string(got2[0].Body) != "b" {
		t.Fatalf("q2 claim wrong: %+v", got2)
	}
}

func TestRuntimeVisibilityHeapOrdersEqualDeadlinesByClaimOrder(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "equal-deadlines", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm.ClaimBatch(ctx, id, 3)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	q, _ := qm.getQueue(id)
	q.mu.Lock()
	defer q.mu.Unlock()

	got := make([]string, 0, 3)
	for q.deadlines.Len() > 0 {
		dr := heap.Pop(&q.deadlines).(*deliveryRecord)
		got = append(got, dr.ReceiptHandle)
	}
	want := []string{claimed[0].ReceiptHandle, claimed[1].ReceiptHandle, claimed[2].ReceiptHandle}
	if !slicesEqual(got, want) {
		t.Fatalf("heap pop order = %v, want %v", got, want)
	}
}

// ============================================================================
// T13: WAL append is called
// ============================================================================

func TestRuntimeWALAppendCalled(t *testing.T) {
	qm, wal := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "wal-test", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	wal.mu.Lock()
	count := len(wal.entries)
	wal.mu.Unlock()
	if count < 2 {
		t.Fatalf("expected at least 2 WAL entries (create + publish), got %d", count)
	}

	var hasCreate, hasPublish bool
	wal.mu.Lock()
	for _, e := range wal.entries {
		switch e.Op {
		case opCreateQueue:
			hasCreate = true
		case opPublishBatch:
			hasPublish = true
		}
	}
	wal.mu.Unlock()
	if !hasCreate {
		t.Fatal("missing opCreateQueue in WAL")
	}
	if !hasPublish {
		t.Fatal("missing opPublishBatch in WAL")
	}
}

// ============================================================================
// T14: WAL failure rolls back memory state
// ============================================================================

func TestRuntimeWALFailureRollsBack(t *testing.T) {
	qm, wal := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "rollback-test", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	q, _ := qm.getQueue(id)
	q.mu.Lock()
	beforeReady := q.ready.Len()
	beforeNextSeq := q.nextSeq
	q.mu.Unlock()
	if beforeReady != 1 {
		t.Fatalf("ready before = %d, want 1", beforeReady)
	}

	wal.fail = true
	_, err = qm.PublishBatch(ctx, id, [][]byte{[]byte("world")})
	if err == nil {
		t.Fatal("expected WAL failure error")
	}

	q.mu.Lock()
	afterReady := q.ready.Len()
	afterNextSeq := q.nextSeq
	q.mu.Unlock()
	if afterReady != beforeReady {
		t.Fatalf("ready after rollback = %d, want %d", afterReady, beforeReady)
	}
	if afterNextSeq != beforeNextSeq {
		t.Fatalf("nextSeq after rollback = %d, want %d", afterNextSeq, beforeNextSeq)
	}
}

// ============================================================================
// T15: Memory limit rejects publish
// ============================================================================

func TestRuntimeMemoryLimitReject(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "limit-test", 3)
	q, _ := qm.getQueue(id)
	q.maxMessages = 1

	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("first")})
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}

	_, err = qm.PublishBatch(ctx, id, [][]byte{[]byte("second")})
	if err == nil {
		t.Fatal("expected memory limit error")
	}

	q.mu.Lock()
	readyCount := q.ready.Len()
	q.mu.Unlock()
	if readyCount != 1 {
		t.Fatalf("ready count = %d, want 1", readyCount)
	}
}

// ============================================================================
// T16: Reap WAL failure preserves inflight state for re-reaping
// ============================================================================

func TestRuntimeReapWALFailPreservesInflight(t *testing.T) {
	qm, wal := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "reap-wal-fail", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	_, err = qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	q, _ := qm.getQueue(id)
	q.mu.Lock()
	var rh string
	for k := range q.inflight {
		rh = k
		break
	}
	if rh == "" {
		t.Fatal("expected inflight delivery record")
	}

	for _, msg := range q.messages {
		msg.VisibilityDeadline = time.Now().Add(-1 * time.Second)
	}
	q.deadlines = q.deadlines[:0]
	for _, dr := range q.inflight {
		dr.heapIndex = -1
		dr.Deadline = time.Now().Add(-1 * time.Second)
		heap.Push(&q.deadlines, dr)
	}
	q.mu.Unlock()

	wal.fail = true
	transitions := qm.ReapExpired(ctx, time.Now())
	if len(transitions) != 0 {
		t.Fatalf("expected no transitions on WAL failure, got %d", len(transitions))
	}

	wal.fail = false

	q.mu.Lock()
	if len(q.inflight) != 1 {
		t.Fatalf("inflight len = %d, want 1 after WAL failure", len(q.inflight))
	}
	if len(q.dead) != 0 {
		t.Fatalf("dead len = %d, want 0 after WAL failure", len(q.dead))
	}
	if q.ready.Len() != 0 {
		t.Fatalf("ready len = %d, want 0 after WAL failure", q.ready.Len())
	}
	if q.deadlines.Len() != 1 {
		t.Fatalf("deadlines len = %d, want 1 after WAL failure", q.deadlines.Len())
	}

	for _, msg := range q.messages {
		if msg.State != StateInFlight {
			t.Fatalf("message state = %q, want in_flight", msg.State)
		}
	}
	q.mu.Unlock()

	transitions = qm.ReapExpired(ctx, time.Now())
	if len(transitions) != 1 {
		t.Fatalf("reap after WAL recovery: transitions = %d, want 1", len(transitions))
	}
	if transitions[0].ToState != StateReady {
		t.Fatalf("reap toState = %q, want ready", transitions[0].ToState)
	}

	claimed, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim after reap: %v", err)
	}
	if !bytes.Equal(claimed[0].Body, []byte("hello")) {
		t.Fatalf("body = %q, want hello", claimed[0].Body)
	}
}

// ============================================================================
// T17: Ack releases byte quota for subsequent publishes
// ============================================================================

func TestRuntimeAckReleasesByteQuota(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "byte-quota", 3)
	q, _ := qm.getQueue(id)
	q.maxBytes = 5

	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("12345")})
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}

	// Second publish should exceed byte limit (5+5 > 5 is false, but 5+1 > 5 is true
	// since bytesInMem is already 5). Actually: bytesInMem(5) + totalBytes(5) > maxBytes(5).
	_, err = qm.PublishBatch(ctx, id, [][]byte{[]byte("67890")})
	if err == nil {
		t.Fatal("expected byte limit exceeded error")
	}

	// Claim and ack the first message.
	claimed, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	results := qm.AckBatch(ctx, id, []AckEntry{
		{ReceiptHandle: claimed[0].ReceiptHandle, DeliveryToken: claimed[0].DeliveryAttemptID},
	})
	if len(results) != 1 || results[0].Status != "ok" {
		t.Fatalf("ack result = %+v", results)
	}

	// Byte quota should now be released; publish should succeed.
	_, err = qm.PublishBatch(ctx, id, [][]byte{[]byte("abcde")})
	if err != nil {
		t.Fatalf("publish after ack should succeed: %v", err)
	}

	q.mu.Lock()
	bytesInMem := q.bytesInMem
	q.mu.Unlock()
	if bytesInMem != 5 {
		t.Fatalf("bytesInMem = %d, want 5", bytesInMem)
	}
}

// ============================================================================
// T18: Duplicate receipt handles in ack batch are rejected
// ============================================================================

func TestRuntimeAckBatchRejectsDuplicateReceiptHandle(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "dup-rh", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm.ClaimBatch(ctx, id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Same receipt handle and token twice in one batch.
	results := qm.AckBatch(ctx, id, []AckEntry{
		{ReceiptHandle: claimed[0].ReceiptHandle, DeliveryToken: claimed[0].DeliveryAttemptID},
		{ReceiptHandle: claimed[0].ReceiptHandle, DeliveryToken: claimed[0].DeliveryAttemptID},
	})
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Status != "ok" {
		t.Fatalf("first result = %s, want ok", results[0].Status)
	}
	if results[1].Status != "error" {
		t.Fatalf("second result = %s, want error (duplicate)", results[1].Status)
	}
	if !strings.Contains(results[1].Error, "duplicate") {
		t.Fatalf("second result error = %q, want duplicate receipt handle", results[1].Error)
	}

	// Verify inFlightCount only decremented by 1.
	q, _ := qm.getQueue(id)
	q.mu.Lock()
	inFlight := q.metrics.inFlightCount.Load()
	q.mu.Unlock()
	if inFlight != 0 {
		t.Fatalf("inFlightCount = %d, want 0", inFlight)
	}
}

// ============================================================================
// T19: Claim WAL failure leaves no stale inflight entries
// ============================================================================

func TestRuntimeClaimWALFailNoStaleInflight(t *testing.T) {
	qm, wal := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "claim-rollback", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	q, _ := qm.getQueue(id)
	q.mu.Lock()
	readyBefore := q.ready.Len()
	q.mu.Unlock()
	if readyBefore != 2 {
		t.Fatalf("ready before = %d, want 2", readyBefore)
	}

	wal.fail = true
	_, err = qm.ClaimBatch(ctx, id, 2)
	if err == nil {
		t.Fatal("expected WAL failure error")
	}

	q.mu.Lock()
	inflightLen := len(q.inflight)
	deadlinesLen := q.deadlines.Len()
	readyAfter := q.ready.Len()
	q.mu.Unlock()

	if inflightLen != 0 {
		t.Fatalf("inflight len after rollback = %d, want 0", inflightLen)
	}
	if deadlinesLen != 0 {
		t.Fatalf("deadlines len after rollback = %d, want 0", deadlinesLen)
	}
	if readyAfter != 2 {
		t.Fatalf("ready len after rollback = %d, want 2", readyAfter)
	}

	// After WAL failure, the messages should be claimable again.
	wal.fail = false
	claimed, err := qm.ClaimBatch(ctx, id, 2)
	if err != nil {
		t.Fatalf("claim after rollback: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d messages, want 2", len(claimed))
	}
}

func TestRuntimeReapSignalsWhenAnyTransitionReturnsReady(t *testing.T) {
	qm, _ := setupRuntimeTest(t)
	ctx := context.Background()

	id, _ := qm.CreateQueue(ctx, "reap-signal", 3)
	_, err := qm.PublishBatch(ctx, id, [][]byte{[]byte("dead"), []byte("ready")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm.ClaimBatch(ctx, id, 2)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d, want 2", len(claimed))
	}

	now := time.Now()
	q, _ := qm.getQueue(id)
	q.mu.Lock()
	for _, msg := range q.messages {
		switch string(msg.Body) {
		case "dead":
			msg.MaxDeliveryCount = 1
			msg.VisibilityDeadline = now.Add(-2 * time.Second)
		case "ready":
			msg.MaxDeliveryCount = 3
			msg.VisibilityDeadline = now.Add(-1 * time.Second)
		}
	}
	q.deadlines = q.deadlines[:0]
	for _, dr := range q.inflight {
		msg := q.messages[dr.MessageID]
		dr.heapIndex = -1
		dr.Deadline = msg.VisibilityDeadline
		heap.Push(&q.deadlines, dr)
	}
	for {
		select {
		case <-q.readyCh:
		default:
			q.mu.Unlock()
			goto drained
		}
	}

drained:
	transitions := qm.ReapExpired(ctx, now)
	if len(transitions) != 2 {
		t.Fatalf("transitions = %d, want 2", len(transitions))
	}
	if transitions[0].ToState != StateDead || transitions[1].ToState != StateReady {
		t.Fatalf("transition order = %+v, want dead then ready", transitions)
	}

	select {
	case <-q.readyCh:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected ready signal when a later reap transition returned a message to ready")
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ============================================================================
// Phase 2.3: WAL replay into queueManager
// ============================================================================

func openRuntimeWAL(t *testing.T, dir string) (*queueManager, *walStore, *pebble.DB) {
	t.Helper()
	deliveryRecordSeq.Store(0)
	metricsStore = sync.Map{}

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}

	wal, err := newWalStore(db, walSyncNone)
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}
	qm := newQueueManager(wal)
	return qm, wal, db
}

func recoverRuntimeWAL(t *testing.T, dir string) (*queueManager, *walStore, *pebble.DB) {
	t.Helper()
	deliveryRecordSeq.Store(0)
	metricsStore = sync.Map{}

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("reopen pebble: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	wal, err := newWalStore(db, walSyncNone)
	if err != nil {
		t.Fatalf("new wal store on reopen: %v", err)
	}
	qm := newQueueManager(wal)
	if err := wal.Replay(context.Background(), wal.latestSnapshotLSN, qm.ApplyWALEntry); err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	return qm, wal, db
}

func TestReplayCreateQueueSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	qm1, _, db1 := openRuntimeWAL(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "replay-test", 3)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	qm2, _, _ := recoverRuntimeWAL(t, dir)
	q, err := qm2.getQueue(id)
	if err != nil {
		t.Fatalf("get queue after replay: %v", err)
	}
	q.mu.Lock()
	if q.config.Name != "replay-test" || q.config.MaxRetries != 3 {
		t.Fatalf("config = %+v, want name=replay-test retries=3", q.config)
	}
	q.mu.Unlock()
}

func TestReplayReadyMessagesFIFO(t *testing.T) {
	dir := t.TempDir()

	qm1, _, db1 := openRuntimeWAL(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "fifo", 3)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	ids, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	qm2, _, _ := recoverRuntimeWAL(t, dir)
	q, err := qm2.getQueue(id)
	if err != nil {
		t.Fatalf("get queue after replay: %v", err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.ready.Len() != 3 {
		t.Fatalf("ready len = %d, want 3", q.ready.Len())
	}
	var got []string
	for e := q.ready.Front(); e != nil; e = e.Next() {
		got = append(got, string(e.Value.(*messageRecord).Body))
	}
	want := []string{"a", "b", "c"}
	if !slicesEqual(got, want) {
		t.Fatalf("ready order = %v, want %v", got, want)
	}
	for _, id := range ids {
		if _, ok := q.messages[id]; !ok {
			t.Fatalf("message %s missing after replay", id)
		}
	}
	if q.nextSeq < 3 {
		t.Fatalf("nextSeq = %d, want >= 3", q.nextSeq)
	}
}

func TestReplayInFlightPreservesHandleTokenDeadline(t *testing.T) {
	dir := t.TempDir()

	qm1, _, db1 := openRuntimeWAL(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "inflight", 3)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("x")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm1.ClaimBatch(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	orig := claimed[0]
	if err := db1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	qm2, _, _ := recoverRuntimeWAL(t, dir)
	q, err := qm2.getQueue(id)
	if err != nil {
		t.Fatalf("get queue after replay: %v", err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.ready.Len() != 0 {
		t.Fatalf("ready len = %d, want 0", q.ready.Len())
	}
	if len(q.inflight) != 1 {
		t.Fatalf("inflight len = %d, want 1", len(q.inflight))
	}
	dr := q.inflight[orig.ReceiptHandle]
	if dr == nil {
		t.Fatal("delivery record missing after replay")
	}
	if dr.DeliveryToken != orig.DeliveryAttemptID {
		t.Fatalf("token = %q, want %q", dr.DeliveryToken, orig.DeliveryAttemptID)
	}
	if !dr.Deadline.Equal(orig.VisibilityDeadline) {
		t.Fatalf("deadline = %v, want %v", dr.Deadline, orig.VisibilityDeadline)
	}
	if q.deadlines.Len() != 1 {
		t.Fatalf("deadlines heap len = %d, want 1", q.deadlines.Len())
	}
	msg := q.messages[orig.ID]
	if msg == nil || msg.State != StateInFlight {
		t.Fatalf("message state = %v, want in_flight", msg.State)
	}
}

func TestReplayAckedMessagesDoNotReappear(t *testing.T) {
	dir := t.TempDir()

	qm1, _, db1 := openRuntimeWAL(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "ack", 3)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("x")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm1.ClaimBatch(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	results := qm1.AckBatch(context.Background(), id, []AckEntry{
		{ReceiptHandle: claimed[0].ReceiptHandle, DeliveryToken: claimed[0].DeliveryAttemptID},
	})
	if results[0].Status != "ok" {
		t.Fatalf("ack failed: %s", results[0].Error)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	qm2, _, _ := recoverRuntimeWAL(t, dir)
	q, err := qm2.getQueue(id)
	if err != nil {
		t.Fatalf("get queue after replay: %v", err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.messages) != 0 {
		t.Fatalf("messages len = %d, want 0", len(q.messages))
	}
	if len(q.inflight) != 0 {
		t.Fatalf("inflight len = %d, want 0", len(q.inflight))
	}
	if q.ready.Len() != 0 {
		t.Fatalf("ready len = %d, want 0", q.ready.Len())
	}
}

func TestReplayNackReturnsToTail(t *testing.T) {
	dir := t.TempDir()

	qm1, _, db1 := openRuntimeWAL(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "nack", 3)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("first"), []byte("second")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm1.ClaimBatch(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := qm1.Nack(context.Background(), id, claimed[0].ReceiptHandle, claimed[0].DeliveryAttemptID); err != nil {
		t.Fatalf("nack: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	qm2, _, _ := recoverRuntimeWAL(t, dir)
	q, err := qm2.getQueue(id)
	if err != nil {
		t.Fatalf("get queue after replay: %v", err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.ready.Len() != 2 {
		t.Fatalf("ready len = %d, want 2", q.ready.Len())
	}
	var bodies []string
	for e := q.ready.Front(); e != nil; e = e.Next() {
		bodies = append(bodies, string(e.Value.(*messageRecord).Body))
	}
	want := []string{"second", "first"}
	if !slicesEqual(bodies, want) {
		t.Fatalf("ready order = %v, want %v", bodies, want)
	}
	if got := q.metrics.totalNacked.Load(); got != 1 {
		t.Fatalf("totalNacked = %d, want 1", got)
	}
}

func TestReplayExpiredInflightBecomesReady(t *testing.T) {
	dir := t.TempDir()

	qm1, _, db1 := openRuntimeWAL(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "expired", 3)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("x")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := qm1.ClaimBatch(context.Background(), id, 1); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	qm2, _, _ := recoverRuntimeWAL(t, dir)
	q, err := qm2.getQueue(id)
	if err != nil {
		t.Fatalf("get queue after replay: %v", err)
	}

	q.mu.Lock()
	if len(q.inflight) != 1 {
		t.Fatalf("inflight len = %d, want 1", len(q.inflight))
	}
	for _, dr := range q.inflight {
		// Force an expired deadline by moving it into the past.
		dr.Deadline = time.Now().Add(-time.Second)
	}
	for _, msg := range q.messages {
		msg.VisibilityDeadline = time.Now().Add(-time.Second)
	}
	q.deadlines = q.deadlines[:0]
	for _, dr := range q.inflight {
		dr.heapIndex = -1
		heap.Push(&q.deadlines, dr)
	}
	q.mu.Unlock()

	transitions := qm2.ReapExpired(context.Background(), time.Now())
	if len(transitions) != 1 || transitions[0].ToState != StateReady {
		t.Fatalf("transitions = %+v, want one ready", transitions)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.ready.Len() != 1 {
		t.Fatalf("ready len after startup reap = %d, want 1", q.ready.Len())
	}
	if len(q.inflight) != 0 {
		t.Fatalf("inflight len after startup reap = %d, want 0", len(q.inflight))
	}
}

func TestReplayExpiredInflightMaxDeliveryBecomesDead(t *testing.T) {
	dir := t.TempDir()

	qm1, _, db1 := openRuntimeWAL(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "dead-letter", 1)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("x")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := qm1.ClaimBatch(context.Background(), id, 1); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	qm2, _, _ := recoverRuntimeWAL(t, dir)
	q, err := qm2.getQueue(id)
	if err != nil {
		t.Fatalf("get queue after replay: %v", err)
	}

	q.mu.Lock()
	for _, msg := range q.messages {
		msg.VisibilityDeadline = time.Now().Add(-time.Second)
	}
	q.deadlines = q.deadlines[:0]
	for _, dr := range q.inflight {
		dr.Deadline = time.Now().Add(-time.Second)
		dr.heapIndex = -1
		heap.Push(&q.deadlines, dr)
	}
	q.mu.Unlock()

	transitions := qm2.ReapExpired(context.Background(), time.Now())
	if len(transitions) != 1 || transitions[0].ToState != StateDead {
		t.Fatalf("transitions = %+v, want one dead", transitions)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.dead) != 1 {
		t.Fatalf("dead len = %d, want 1", len(q.dead))
	}
	if len(q.inflight) != 0 {
		t.Fatalf("inflight len = %d, want 0", len(q.inflight))
	}
}

func TestReplayStaleAckTokenRejected(t *testing.T) {
	dir := t.TempDir()

	qm1, _, db1 := openRuntimeWAL(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "stale", 3)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("x")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed1, err := qm1.ClaimBatch(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	oldToken := claimed1[0].DeliveryAttemptID

	// Let the message expire and be reaped, then claimed again with a new token.
	q, _ := qm1.getQueue(id)
	q.mu.Lock()
	for _, msg := range q.messages {
		msg.VisibilityDeadline = time.Now().Add(-time.Second)
	}
	q.deadlines = q.deadlines[:0]
	for _, dr := range q.inflight {
		dr.Deadline = time.Now().Add(-time.Second)
		dr.heapIndex = -1
		heap.Push(&q.deadlines, dr)
	}
	q.mu.Unlock()
	qm1.ReapExpired(context.Background(), time.Now())

	claimed2, err := qm1.ClaimBatch(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	qm2, _, _ := recoverRuntimeWAL(t, dir)
	// Try to ack with the old token.
	results := qm2.AckBatch(context.Background(), id, []AckEntry{
		{ReceiptHandle: claimed2[0].ReceiptHandle, DeliveryToken: oldToken},
	})
	if results[0].Status == "ok" {
		t.Fatal("expected stale token ack to fail")
	}
}

func TestReplayInconsistentRecordFailsStartup(t *testing.T) {
	dir := t.TempDir()

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	wal, err := newWalStore(db, walSyncNone)
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}

	// Append a create queue followed by an ack for a non-existent message.
	if _, _, err := wal.Append(context.Background(), []walEntry{
		{Op: opCreateQueue, Payload: walCreateQueuePayload{QueueID: "q", Name: "q", MaxRetries: 3}},
		{Op: opAckBatch, Payload: walAckBatchPayload{
			QueueID: "q",
			Acks:    []walAckedMessage{{MessageID: "m1", ReceiptHandle: "rh", DeliveryToken: "tok"}},
		}},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	_ = db.Close()

	db2, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("reopen pebble: %v", err)
	}
	defer db2.Close()
	wal2, err := newWalStore(db2, walSyncNone)
	if err != nil {
		t.Fatalf("new wal store on reopen: %v", err)
	}
	qm := newQueueManager(wal2)
	if err := wal2.Replay(context.Background(), wal2.latestSnapshotLSN, qm.ApplyWALEntry); err == nil {
		t.Fatal("expected replay to fail on inconsistent ack record")
	}
}

func TestReplayAckMessageIDMismatchFailsStartup(t *testing.T) {
	dir := t.TempDir()

	qm1, wal1, db1 := openRuntimeWAL(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "ack-mismatch", 3)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	published, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("first"), []byte("second")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm1.ClaimBatch(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Append a bad ack that uses the correct receipt handle and token but a
	// MessageID that belongs to a different (still ready) message.
	wrongMessageID := published[1]
	if _, _, err := wal1.Append(context.Background(), []walEntry{
		{Op: opAckBatch, Payload: walAckBatchPayload{
			QueueID: id,
			Acks: []walAckedMessage{{
				MessageID:     wrongMessageID,
				ReceiptHandle: claimed[0].ReceiptHandle,
				DeliveryToken: claimed[0].DeliveryAttemptID,
			}},
		}},
	}); err != nil {
		t.Fatalf("append bad ack: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	db2, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("reopen pebble: %v", err)
	}
	defer db2.Close()
	wal2, err := newWalStore(db2, walSyncNone)
	if err != nil {
		t.Fatalf("new wal store on reopen: %v", err)
	}
	qm2 := newQueueManager(wal2)
	if err := wal2.Replay(context.Background(), wal2.latestSnapshotLSN, qm2.ApplyWALEntry); err == nil {
		t.Fatal("expected replay to fail on ack MessageID mismatch")
	}
}

func TestReplayNackMessageIDMismatchFailsStartup(t *testing.T) {
	dir := t.TempDir()

	qm1, wal1, db1 := openRuntimeWAL(t, dir)
	id, err := qm1.CreateQueue(context.Background(), "nack-mismatch", 3)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	published, err := qm1.PublishBatch(context.Background(), id, [][]byte{[]byte("first"), []byte("second")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	claimed, err := qm1.ClaimBatch(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Append a bad nack that uses the correct receipt handle and token but a
	// MessageID that belongs to a different (still ready) message.
	wrongMessageID := published[1]
	if _, _, err := wal1.Append(context.Background(), []walEntry{
		{Op: opNack, Payload: walNackPayload{
			QueueID:        id,
			MessageID:      wrongMessageID,
			ReceiptHandle:  claimed[0].ReceiptHandle,
			DeliveryToken:  claimed[0].DeliveryAttemptID,
			TargetState:    StateReady,
			HasNewReadySeq: false,
		}},
	}); err != nil {
		t.Fatalf("append bad nack: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	db2, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("reopen pebble: %v", err)
	}
	defer db2.Close()
	wal2, err := newWalStore(db2, walSyncNone)
	if err != nil {
		t.Fatalf("new wal store on reopen: %v", err)
	}
	qm2 := newQueueManager(wal2)
	if err := wal2.Replay(context.Background(), wal2.latestSnapshotLSN, qm2.ApplyWALEntry); err == nil {
		t.Fatal("expected replay to fail on nack MessageID mismatch")
	}
}

