package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestKueueTargetAckIncludesDeliveryToken(t *testing.T) {
	type batchAckEntry struct {
		ReceiptHandle string `json:"receiptHandle"`
		DeliveryToken string `json:"deliveryToken"`
	}
	type batchAckRequest struct {
		QueueID string          `json:"queueId"`
		Acks    []batchAckEntry `json:"acks"`
	}

	ackRequests := make(chan batchAckRequest, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ack":
			var req batchAckRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad ack request", http.StatusBadRequest)
				return
			}
			ackRequests <- req
			w.WriteHeader(http.StatusAccepted)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"results": []map[string]any{
					{
						"receiptHandle": "handle-1",
						"status":        "ok",
					},
				},
			}); err != nil {
				t.Errorf("encode ack response: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	target := newKueueTarget(server.URL)
	target.queueID = "queue-1"
	target.ackFlushSize = 1

	if err := target.queueAck(context.Background(), ackEntryBench{
		ReceiptHandle: "handle-1",
		DeliveryToken: "token-1",
	}); err != nil {
		t.Fatalf("queueAck returned error: %v", err)
	}

	select {
	case req := <-ackRequests:
		if req.QueueID != "queue-1" {
			t.Fatalf("ack queue id = %q, want queue-1", req.QueueID)
		}
		if len(req.Acks) != 1 {
			t.Fatalf("expected 1 ack entry, got %d", len(req.Acks))
		}
		if req.Acks[0].ReceiptHandle != "handle-1" {
			t.Fatalf("ack receipt handle = %q, want handle-1", req.Acks[0].ReceiptHandle)
		}
		if req.Acks[0].DeliveryToken != "token-1" {
			t.Fatalf("ack delivery token = %q, want token-1", req.Acks[0].DeliveryToken)
		}
	default:
		t.Fatal("ack endpoint was not called")
	}
}
