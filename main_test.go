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

	t.Cleanup(func() {
		_ = db.Close()
		Db = nil
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

	if recorder.Code != http.StatusAccepted {
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

	storedKey := storedMessageKey(t, queueID, messageID)
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

	_, closer, err := Db.Get(storedKey)
	if closer != nil {
		closer.Close()
	}
	if err != pebble.ErrNotFound {
		t.Fatalf("expected message to be deleted after ack, got %v", err)
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

func TestReapExpiredMessagesResetsPersistedInFlightMessage(t *testing.T) {
	t.Skip("TODO: fix nested batch write issue in nextMessageSequence")
	setupTestDB(t)

	queueID := createTestQueue(t, "reaper-queue")
	messageID := publishTestMessage(t, queueID, []byte("expired"))

	firstResp := receiveTestMessage(t, queueID)
	firstToken := firstResp.DeliveryToken
	firstReceiptHandle := firstResp.ReceiptHandle

	storedKey := storedMessageKey(t, queueID, messageID)
	batch := Db.NewIndexedBatch()
	defer batch.Close()
	val, closer, err := batch.Get(storedKey)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	var msg Message
	if err := json.Unmarshal(val, &msg); err != nil {
		closer.Close()
		t.Fatalf("unmarshal message: %v", err)
	}
	closer.Close()

	if err := deleteInflightIndex(batch, queueID, msg); err != nil {
		t.Fatalf("delete inflight index: %v", err)
	}
	msg.VisibilityDeadline = time.Now().Add(-1 * time.Second)

	updated, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}

	if err := batch.Set(storedKey, updated, nil); err != nil {
		t.Fatalf("set message: %v", err)
	}
	if err := setInflightIndex(batch, queueID, msg, storedKey); err != nil {
		t.Fatalf("set inflight index: %v", err)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatalf("commit batch: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	recoveredTransitions, err := reapExpiredMessages(time.Now())
	if err != nil {
		t.Fatalf("reap expired messages: %v", err)
	}
	if len(recoveredTransitions) != 1 || recoveredTransitions[0].QueueID != queueID {
		t.Fatal("expected reapExpiredMessages to recover the expired message")
	}

	secondResp := receiveTestMessage(t, queueID)

	if secondResp.ID != messageID {
		t.Fatalf("expected same message id after reap, got %s", secondResp.ID)
	}
	if string(secondResp.Body) != "expired" {
		t.Fatalf("expected body expired, got %q", string(secondResp.Body))
	}
	if secondResp.State != StateInFlight {
		t.Fatalf("expected in-flight state after re-receive, got %s", secondResp.State)
	}

	// Stale delivery token should be rejected
	ackBody, err := json.Marshal(AckRequest{QueueId: queueID, ReceiptHandle: firstReceiptHandle, DeliveryToken: firstToken})
	if err != nil {
		t.Fatalf("marshal ack request: %v", err)
	}
	ackReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
	ackRecorder := httptest.NewRecorder()
	ack(ackRecorder, ackReq)
	if ackRecorder.Code != http.StatusConflict {
		t.Fatalf("expected 409 for stale delivery token, got %d: %s", ackRecorder.Code, ackRecorder.Body.String())
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

	key, err := messageKeyFromReceiptHandle(queueID, resp.ReceiptHandle)
	if err != nil {
		t.Fatalf("message key from receipt handle: %v", err)
	}
	val, closer, err := Db.Get(key)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	var msg Message
	if err := json.Unmarshal(val, &msg); err != nil {
		closer.Close()
		t.Fatalf("unmarshal message: %v", err)
	}
	closer.Close()
	_, closer, err = Db.Get(inflightKey(queueID, msg.VisibilityDeadline, msg.ID))
	if closer != nil {
		closer.Close()
	}
	if err != nil {
		t.Fatalf("expected in-flight index after receive: %v", err)
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

	prefix := inflightPrefix()
	iter, _ := Db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	defer iter.Close()
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		t.Fatalf("expected no in-flight index keys, found %q", iter.Key())
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
	messageID := publishTestMessage(t, queueID, []byte("poison"))

	_ = receiveTestMessage(t, queueID)

	storedKey := storedMessageKey(t, queueID, messageID)
	batch := Db.NewIndexedBatch()
	val, closer, err := batch.Get(storedKey)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	var deadMsg Message
	if err := json.Unmarshal(val, &deadMsg); err != nil {
		closer.Close()
		t.Fatalf("unmarshal message: %v", err)
	}
	closer.Close()
	if err := deleteInflightIndex(batch, queueID, deadMsg); err != nil {
		t.Fatalf("delete inflight index: %v", err)
	}
	deadMsg.VisibilityDeadline = time.Now().Add(-1 * time.Second)
	updated, err := json.Marshal(deadMsg)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}
	if err := batch.Set(storedKey, updated, nil); err != nil {
		t.Fatalf("set message: %v", err)
	}
	if err := setInflightIndex(batch, queueID, deadMsg, storedKey); err != nil {
		t.Fatalf("set inflight index: %v", err)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatalf("commit batch: %v", err)
	}
	batch.Close()

	_, err = reapExpiredMessages(time.Now())
	if err != nil {
		t.Fatalf("reap expired messages: %v", err)
	}

	_, foundMsg, err := findMessageRecord(queueID, messageID)
	if err != nil {
		t.Fatalf("find message record: %v", err)
	}
	if foundMsg.State != StateDead {
		t.Fatalf("expected StateDead after max deliveries, got %s", foundMsg.State)
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

	storedKey := storedMessageKey(t, queueID, messageID)
	batch := Db.NewIndexedBatch()
	val, closer, err := batch.Get(storedKey)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	var expiredMsg Message
	if err := json.Unmarshal(val, &expiredMsg); err != nil {
		closer.Close()
		t.Fatalf("unmarshal message: %v", err)
	}
	closer.Close()
	if err := deleteInflightIndex(batch, queueID, expiredMsg); err != nil {
		t.Fatalf("delete inflight index: %v", err)
	}
	expiredMsg.VisibilityDeadline = time.Now().Add(-1 * time.Second)
	updated, err := json.Marshal(expiredMsg)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}
	if err := batch.Set(storedKey, updated, nil); err != nil {
		t.Fatalf("set message: %v", err)
	}
	if err := setInflightIndex(batch, queueID, expiredMsg, storedKey); err != nil {
		t.Fatalf("set inflight index: %v", err)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatalf("commit batch: %v", err)
	}
	batch.Close()

	transitions, err := reapExpiredMessages(time.Now())
	if err != nil {
		t.Fatalf("reap expired messages: %v", err)
	}
	if len(transitions) != 1 {
		t.Fatalf("expected one indexed reaper transition, got %d", len(transitions))
	}
	if transitions[0].QueueID != queueID || transitions[0].ToState != StateReady {
		t.Fatalf("unexpected transition: %+v", transitions[0])
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

	_, foundMsg, err := findMessageRecord(queueID, messageID)
	if err != nil {
		t.Fatalf("find message record: %v", err)
	}
	if foundMsg.State != StateDead {
		t.Fatalf("expected persisted StateDead, got %s", foundMsg.State)
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
			metricsStore = sync.Map{}
			receiveChannel = make(chan struct{}, 1)
			queueReadyChans = map[string]chan struct{}{}
			b.Cleanup(func() {
				db.Close()
				Db = nil
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

	b.Cleanup(func() {
		_ = db.Close()
		Db = nil
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
				msgs, err := claimReadyMessages(queueID, batchSize)
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
				msgs, err := claimReadyMessages(queueID, batchSize)
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
				msgs, err := claimReadyMessages(queueID, batchSize)
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
				resp := receiveBenchMessage(b, queueID)
				key, err := messageKeyFromReceiptHandle(queueID, resp.ReceiptHandle)
				if err != nil {
					b.Fatalf("decode receipt handle: %v", err)
				}
				err = func() error {
					batch := Db.NewIndexedBatch()
					defer batch.Close()
					val, closer, err := batch.Get(key)
					if err != nil {
						return err
					}
					var benchMsg Message
					if err := json.Unmarshal(val, &benchMsg); err != nil {
						closer.Close()
						return err
					}
					closer.Close()
					if err := deleteInflightIndex(batch, queueID, benchMsg); err != nil {
						return err
					}
					benchMsg.VisibilityDeadline = time.Now().Add(-1 * time.Second)
					updated, err := json.Marshal(benchMsg)
					if err != nil {
						return err
					}
					if err := batch.Set(key, updated, nil); err != nil {
						return err
					}
					if err := setInflightIndex(batch, queueID, benchMsg, key); err != nil {
						return err
					}
					return batch.Commit(pebble.NoSync)
				}()
				if err != nil {
					b.Fatalf("prepare expired message: %v", err)
				}
			}

			b.ResetTimer()
			transitions, err := reapExpiredMessages(time.Now())
			b.StopTimer()
			if err != nil {
				b.Fatalf("reap expired messages: %v", err)
			}
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

	// Verify all messages are deleted
	for _, msgID := range []string{msg1ID, msg2ID, msg3ID} {
		key, _, err := findMessageRecord(queueID, msgID)
		if err == nil {
			t.Fatalf("expected message %s to be deleted, found at %x", msgID, key)
		}
		if err != pebble.ErrNotFound {
			t.Fatalf("unexpected error finding message %s: %v", msgID, err)
		}
	}
}

func TestBatchAckReportsPartialErrors(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "batch-ack-err-queue")

	_ = publishTestMessage(t, queueID, []byte("one"))
	msg2ID := publishTestMessage(t, queueID, []byte("two"))

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

	// msg1 should be deleted, msg2 should still exist
	var msg2Exists bool
	_, _, err := findMessageRecord(queueID, msg2ID)
	if err == pebble.ErrNotFound {
		msg2Exists = false
	} else if err != nil {
		t.Fatalf("check msg2: %v", err)
	} else {
		msg2Exists = true
	}
	if !msg2Exists {
		t.Fatal("msg2 should not have been deleted")
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
// Phase 2.2: queueRuntime test harness
// ============================================================================

type fakeWAL struct {
	mu      sync.Mutex
	entries []walEntry
	nextLSN uint64
	fail    bool
}

func (f *fakeWAL) Append(ctx context.Context, entries []walEntry) (uint64, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return 0, 0, errors.New("fake WAL failure")
	}
	first := f.nextLSN
	if f.nextLSN == 0 {
		f.nextLSN = 1
		first = 1
	}
	for i := range entries {
		f.nextLSN++
		entries[i].LSN = f.nextLSN
		f.entries = append(f.entries, entries[i])
	}
	return first, f.nextLSN, nil
}

func setupRuntimeTest(t *testing.T) (*queueManager, *fakeWAL) {
	t.Helper()
	wal := &fakeWAL{}
	qm := newQueueManager(wal)
	return qm, wal
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
	qm, _ := setupRuntimeTest(t)
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

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	var got1, got2 []claimedMessage
	go func() {
		defer wg.Done()
		<-start
		var err1 error
		got1, err1 = qm.ClaimBatch(ctx, id1, 1)
		if err1 != nil {
			t.Errorf("claim q1: %v", err1)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		var err2 error
		got2, err2 = qm.ClaimBatch(ctx, id2, 1)
		if err2 != nil {
			t.Errorf("claim q2: %v", err2)
		}
	}()

	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent claims blocked — possible global mutex contention")
	}

	if len(got1) != 1 || string(got1[0].Body) != "a" {
		t.Fatalf("q1 claim wrong: %+v", got1)
	}
	if len(got2) != 1 || string(got2[0].Body) != "b" {
		t.Fatalf("q2 claim wrong: %+v", got2)
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
