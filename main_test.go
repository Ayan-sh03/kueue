package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func setupTestDB(t *testing.T) {
	t.Helper()

	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}

	Db = db
	Queues = nil
	DeadLetterQueue = nil
	receiveChannel = make(chan struct{}, 1)
	queueReadyChans = map[string]chan struct{}{}
	metricsStore = sync.Map{}

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

	var key []byte
	err := Db.View(func(txn *badger.Txn) error {
		foundKey, _, err := findMessageRecord(txn, queueID, messageID)
		if err != nil {
			return err
		}

		key = foundKey
		return nil
	})
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

	storedKey := storedMessageKey(t, queueID, messageID)
	ackBody, err := json.Marshal(AckRequest{QueueId: queueID, MessageId: messageID, DeliveryToken: resp.DeliveryToken})
	if err != nil {
		t.Fatalf("marshal ack request: %v", err)
	}

	ackReq := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
	ackRecorder := httptest.NewRecorder()
	ack(ackRecorder, ackReq)

	if ackRecorder.Code != http.StatusAccepted {
		t.Fatalf("ack status = %d, body = %s", ackRecorder.Code, ackRecorder.Body.String())
	}

	err = Db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(storedKey)
		return err
	})
	if err != badger.ErrKeyNotFound {
		t.Fatalf("expected message to be deleted after ack, got %v", err)
	}
}

func TestNackMakesMessageReceivableAgain(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "nack-queue")
	messageID := publishTestMessage(t, queueID, []byte("retry"))

	firstResp := receiveTestMessage(t, queueID)

	nackBody, err := json.Marshal(AckRequest{QueueId: queueID, MessageId: messageID, DeliveryToken: firstResp.DeliveryToken})
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

		ackBody, err := json.Marshal(AckRequest{QueueId: queueID, MessageId: resp.ID, DeliveryToken: resp.DeliveryToken})
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
	setupTestDB(t)

	queueID := createTestQueue(t, "reaper-queue")
	messageID := publishTestMessage(t, queueID, []byte("expired"))

	firstResp := receiveTestMessage(t, queueID)
	firstToken := firstResp.DeliveryToken

	storedKey := storedMessageKey(t, queueID, messageID)
	err := Db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(storedKey)
		if err != nil {
			return err
		}

		return item.Value(func(v []byte) error {
			var msg Message
			if err := json.Unmarshal(v, &msg); err != nil {
				return err
			}

			msg.VisibilityDeadline = time.Now().Add(-1 * time.Second)

			updated, err := json.Marshal(msg)
			if err != nil {
				return err
			}

			return txn.Set(storedKey, updated)
		})
	})
	if err != nil {
		t.Fatalf("prepare expired in-flight message: %v", err)
	}

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
	ackBody, err := json.Marshal(AckRequest{QueueId: queueID, MessageId: messageID, DeliveryToken: firstToken})
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
	messageID := publishTestMessage(t, queueID, []byte("secret"))

	_ = receiveTestMessage(t, queueID)

	ackBody, err := json.Marshal(AckRequest{QueueId: queueID, MessageId: messageID, DeliveryToken: "wrong-token"})
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
	messageID := publishTestMessage(t, queueID, []byte("secret"))

	_ = receiveTestMessage(t, queueID)

	nackBody, err := json.Marshal(AckRequest{QueueId: queueID, MessageId: messageID, DeliveryToken: "wrong-token"})
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
	err = Db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(storedKey)
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			var msg Message
			if err := json.Unmarshal(v, &msg); err != nil {
				return err
			}
			msg.VisibilityDeadline = time.Now().Add(-1 * time.Second)
			updated, err := json.Marshal(msg)
			if err != nil {
				return err
			}
			return txn.Set(storedKey, updated)
		})
	})
	if err != nil {
		t.Fatalf("prepare expired in-flight message: %v", err)
	}

	_, err = reapExpiredMessages(time.Now())
	if err != nil {
		t.Fatalf("reap expired messages: %v", err)
	}

	err = Db.View(func(txn *badger.Txn) error {
		_, msg, err := findMessageRecord(txn, queueID, messageID)
		if err != nil {
			return err
		}
		if msg.State != StateDead {
			t.Fatalf("expected StateDead after max deliveries, got %s", msg.State)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("find message record: %v", err)
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

	nackBody, err := json.Marshal(AckRequest{QueueId: queueID, MessageId: messageID, DeliveryToken: resp.DeliveryToken})
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

	err = Db.View(func(txn *badger.Txn) error {
		_, msg, err := findMessageRecord(txn, queueID, messageID)
		if err != nil {
			return err
		}
		if msg.State != StateDead {
			t.Fatalf("expected persisted StateDead, got %s", msg.State)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("find message record: %v", err)
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
			db, err := badger.Open(badger.DefaultOptions(b.TempDir()).WithLogger(nil))
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
		m.ackWindow = append(m.ackWindow, time.Now())
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
	for i := 0; i < 60; i++ {
		m.ackWindow = append(m.ackWindow, time.Now())
	}

	rate := m.ackRatePerSec()

	expected := 1.0
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

type batchReceiveResponse struct {
	Messages []struct {
		ID            string       `json:"id"`
		Body          []byte       `json:"body"`
		State         MessageState `json:"state"`
		DeliveryToken string       `json:"deliveryToken"`
	} `json:"messages"`
}

type batchAckResult struct {
	MessageId string `json:"messageId"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
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

	resp := decodeResponse[batchReceiveResponse](t, recorder)
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

	resp := decodeResponse[batchReceiveResponse](t, recorder)
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

	batchResp := func() batchReceiveResponse {
		req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID+"&max=10", nil)
		rec := httptest.NewRecorder()
		receive(rec, req)
		return decodeResponse[batchReceiveResponse](t, rec)
	}()

	if len(batchResp.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(batchResp.Messages))
	}

	// Batch ack all three
	ackBody, _ := json.Marshal(BatchAckRequest{
		QueueId: queueID,
		Acks: []AckEntry{
			{MessageId: msg1ID, DeliveryToken: batchResp.Messages[0].DeliveryToken},
			{MessageId: msg2ID, DeliveryToken: batchResp.Messages[1].DeliveryToken},
			{MessageId: msg3ID, DeliveryToken: batchResp.Messages[2].DeliveryToken},
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
		err := Db.View(func(txn *badger.Txn) error {
			key, _, err := findMessageRecord(txn, queueID, msgID)
			if err != nil {
				return err
			}
			t.Fatalf("expected message %s to be deleted, found at %x", msgID, key)
			return nil
		})
		if err == nil {
			t.Fatalf("message %s was not deleted", msgID)
		}
	}
}

func TestBatchAckReportsPartialErrors(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "batch-ack-err-queue")

	msg1ID := publishTestMessage(t, queueID, []byte("one"))
	msg2ID := publishTestMessage(t, queueID, []byte("two"))

	batchResp := func() batchReceiveResponse {
		req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID+"&max=10", nil)
		rec := httptest.NewRecorder()
		receive(rec, req)
		return decodeResponse[batchReceiveResponse](t, rec)
	}()

	if len(batchResp.Messages) < 2 {
		t.Fatalf("expected at least 2 messages, got %d", len(batchResp.Messages))
	}

	// Ack: msg1 with correct token, msg2 with wrong token
	ackBody, _ := json.Marshal(BatchAckRequest{
		QueueId: queueID,
		Acks: []AckEntry{
			{MessageId: msg1ID, DeliveryToken: batchResp.Messages[0].DeliveryToken},
			{MessageId: msg2ID, DeliveryToken: "wrong-token"},
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
	err := Db.View(func(txn *badger.Txn) error {
		_, _, err := findMessageRecord(txn, queueID, msg2ID)
		if err == badger.ErrKeyNotFound {
			msg2Exists = false
			return nil
		}
		msg2Exists = true
		return err
	})
	if err != nil {
		t.Fatalf("check msg2: %v", err)
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

	resp := decodeResponse[batchReceiveResponse](t, recorder)
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

	resp := decodeResponse[batchReceiveResponse](t, recorder)
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
