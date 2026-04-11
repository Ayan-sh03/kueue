package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

	req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	recorder := httptest.NewRecorder()
	receive(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("receive status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	resp := decodeResponse[struct {
		ID    string       `json:"id"`
		Body  []byte       `json:"body"`
		State MessageState `json:"state"`
	}](t, recorder)

	if resp.ID != messageID {
		t.Fatalf("expected message id %s, got %s", messageID, resp.ID)
	}
	if string(resp.Body) != "hello" {
		t.Fatalf("expected body hello, got %q", string(resp.Body))
	}
	if resp.State != StateInFlight {
		t.Fatalf("expected in-flight state, got %s", resp.State)
	}

	storedKey := storedMessageKey(t, queueID, messageID)
	ackBody, err := json.Marshal(AckRequest{QueueId: queueID, MessageId: messageID})
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

	firstReceive := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	firstRecorder := httptest.NewRecorder()
	receive(firstRecorder, firstReceive)
	if firstRecorder.Code != http.StatusAccepted {
		t.Fatalf("first receive status = %d, body = %s", firstRecorder.Code, firstRecorder.Body.String())
	}

	nackBody, err := json.Marshal(AckRequest{QueueId: queueID, MessageId: messageID})
	if err != nil {
		t.Fatalf("marshal nack request: %v", err)
	}

	nackReq := httptest.NewRequest(http.MethodPost, "/nack", bytes.NewReader(nackBody))
	nackRecorder := httptest.NewRecorder()
	nack(nackRecorder, nackReq)
	if nackRecorder.Code != http.StatusAccepted {
		t.Fatalf("nack status = %d, body = %s", nackRecorder.Code, nackRecorder.Body.String())
	}

	secondReceive := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	secondRecorder := httptest.NewRecorder()
	receive(secondRecorder, secondReceive)
	if secondRecorder.Code != http.StatusAccepted {
		t.Fatalf("second receive status = %d, body = %s", secondRecorder.Code, secondRecorder.Body.String())
	}

	resp := decodeResponse[struct {
		ID   string `json:"id"`
		Body []byte `json:"body"`
	}](t, secondRecorder)

	if resp.ID != messageID {
		t.Fatalf("expected same message id after nack, got %s", resp.ID)
	}
	if string(resp.Body) != "retry" {
		t.Fatalf("expected body retry, got %q", string(resp.Body))
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
		req := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
		recorder := httptest.NewRecorder()
		receive(recorder, req)

		if recorder.Code != http.StatusAccepted {
			t.Fatalf("receive status = %d, body = %s", recorder.Code, recorder.Body.String())
		}

		resp := decodeResponse[struct {
			ID   string `json:"id"`
			Body []byte `json:"body"`
		}](t, recorder)

		if resp.ID != expected.id {
			t.Fatalf("expected message id %s, got %s", expected.id, resp.ID)
		}
		if string(resp.Body) != expected.body {
			t.Fatalf("expected body %s, got %q", expected.body, string(resp.Body))
		}

		ackBody, err := json.Marshal(AckRequest{QueueId: queueID, MessageId: resp.ID})
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

	resp := decodeResponse[struct {
		ID   string `json:"id"`
		Body []byte `json:"body"`
	}](t, recorder)

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

	resp := decodeResponse[struct {
		ID   string `json:"id"`
		Body []byte `json:"body"`
	}](t, recorder)

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

	firstReceive := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	firstRecorder := httptest.NewRecorder()
	receive(firstRecorder, firstReceive)
	if firstRecorder.Code != http.StatusAccepted {
		t.Fatalf("first receive status = %d, body = %s", firstRecorder.Code, firstRecorder.Body.String())
	}

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

	recoveredQueueIDs, err := reapExpiredMessages(time.Now())
	if err != nil {
		t.Fatalf("reap expired messages: %v", err)
	}
	if len(recoveredQueueIDs) != 1 || recoveredQueueIDs[0] != queueID {
		t.Fatal("expected reapExpiredMessages to recover the expired message")
	}

	secondReceive := httptest.NewRequest(http.MethodGet, "/receive?id="+queueID, nil)
	secondRecorder := httptest.NewRecorder()
	receive(secondRecorder, secondReceive)
	if secondRecorder.Code != http.StatusAccepted {
		t.Fatalf("second receive status = %d, body = %s", secondRecorder.Code, secondRecorder.Body.String())
	}

	resp := decodeResponse[struct {
		ID    string       `json:"id"`
		Body  []byte       `json:"body"`
		State MessageState `json:"state"`
	}](t, secondRecorder)

	if resp.ID != messageID {
		t.Fatalf("expected same message id after reap, got %s", resp.ID)
	}
	if string(resp.Body) != "expired" {
		t.Fatalf("expected body expired, got %q", string(resp.Body))
	}
	if resp.State != StateInFlight {
		t.Fatalf("expected in-flight state after re-receive, got %s", resp.State)
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
