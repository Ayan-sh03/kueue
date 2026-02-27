package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCreate(t *testing.T) {
	Queues = []Queue{}
	body := CreateRequest{Name: "test-queue", MaxRetries: 3}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/create", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()

	create(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, w.Code)
	}

	if len(Queues) != 1 {
		t.Errorf("expected 1 queue, got %d", len(Queues))
	}
}

func TestGetQueue(t *testing.T) {
	Queues = []Queue{{Id: "queue-1", Name: "test"}}

	req := httptest.NewRequest("GET", "/get?id=queue-1", nil)
	w := httptest.NewRecorder()

	getQueue(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, w.Code)
	}
}

func TestGetQueueNotFound(t *testing.T) {
	Queues = []Queue{}

	req := httptest.NewRequest("GET", "/get?id=invalid", nil)
	w := httptest.NewRecorder()

	getQueue(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestPublish(t *testing.T) {
	Queues = []Queue{{Id: "queue-1", Name: "test", Messages: []Message{}}}

	msg := Message{Body: []byte("test")}
	pubReq := PublishRequest{Message: msg, QueueId: "queue-1"}
	bodyBytes, _ := json.Marshal(pubReq)

	req := httptest.NewRequest("POST", "/publish", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()

	publish(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, w.Code)
	}

	if len(Queues[0].Messages) != 1 {
		t.Errorf("expected 1 message in queue, got %d", len(Queues[0].Messages))
	}
}

func TestReceive(t *testing.T) {
	Queues = []Queue{{
		Id: "queue-1",
		Messages: []Message{{
			ID:    "msg-1",
			State: StateReady,
		}},
	}}

	req := httptest.NewRequest("GET", "/receive?id=queue-1", nil)
	w := httptest.NewRecorder()

	receive(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, w.Code)
	}

	if Queues[0].Messages[0].State != StateInFlight {
		t.Errorf("expected state InFlight, got %s", Queues[0].Messages[0].State)
	}
}

func TestAck(t *testing.T) {
	Queues = []Queue{{
		Id: "queue-1",
		Messages: []Message{{
			ID:    "msg-1",
			State: StateInFlight,
		}},
	}}

	req := httptest.NewRequest("GET", "/ack?messageId=msg-1&queueId=queue-1", nil)
	w := httptest.NewRecorder()

	ack(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, w.Code)
	}

	if len(Queues[0].Messages) != 0 {
		t.Errorf("expected 0 messages after ack, got %d", len(Queues[0].Messages))
	}
}

func TestReaper(t *testing.T) {
	Queues = []Queue{{
		Id:         "queue-1",
		MaxRetries: 2,
		Messages: []Message{{
			ID:                 "msg-1",
			State:              StateInFlight,
			VisibilityDeadline: time.Now().Add(-1 * time.Second),
			DeliveryCount:      1,
		}},
	}}

	reaper()
	time.Sleep(2 * time.Second)

	if Queues[0].Messages[0].State != StateReady {
		t.Errorf("expected state Ready after deadline, got %s", Queues[0].Messages[0].State)
	}
}

func TestQueueHandler(t *testing.T) {
	req := httptest.NewRequest("POST", "/", nil)
	w := httptest.NewRecorder()

	queueHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
}
