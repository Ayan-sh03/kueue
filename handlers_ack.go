package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

func ack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	// Try batch ack first
	var batchReq BatchAckRequest
	if json.Unmarshal(body, &batchReq) == nil && len(batchReq.Acks) > 0 {
		handleBatchAck(w, batchReq)
		return
	}

	// Fall back to single ack
	var ackReq AckRequest
	if err := json.Unmarshal(body, &ackReq); err != nil {
		http.Error(w, "Bad Request: "+err.Error(), http.StatusBadRequest)
		return
	}

	if ackReq.QueueId == "" {
		http.Error(w, "queueId is required", http.StatusBadRequest)
		return
	}
	if ackReq.ReceiptHandle == "" {
		http.Error(w, "receiptHandle is required", http.StatusBadRequest)
		return
	}
	if ackReq.DeliveryToken == "" {
		http.Error(w, "deliveryToken is required", http.StatusBadRequest)
		return
	}

	batch := Db.NewIndexedBatch()
	defer batch.Close()
	if _, closer, err := batch.Get([]byte(ackReq.QueueId)); err != nil {
		batch.Close()
		if err == pebble.ErrNotFound {
			http.Error(w, "Queue or message not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to acknowledge message: "+err.Error(), http.StatusInternalServerError)
		return
	} else {
		closer.Close()
	}
	key, msg, err := messageByReceiptHandle(batch, ackReq.QueueId, ackReq.ReceiptHandle)
	if err != nil {
		batch.Close()
		if _, ok := err.(*ErrInvalidReceiptHandle); ok {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err == pebble.ErrNotFound {
			http.Error(w, "Queue or message not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to acknowledge message: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if msg.State != StateInFlight {
		batch.Close()
		http.Error(w, ErrMessageNotInFlight.Error(), http.StatusConflict)
		return
	}
	if msg.DeliveryAttemptID != ackReq.DeliveryToken {
		batch.Close()
		http.Error(w, (&ErrDeliveryTokenMismatch{Expected: msg.DeliveryAttemptID, Got: ackReq.DeliveryToken}).Error(), http.StatusConflict)
		return
	}
	if err := deleteInflightIndex(batch, ackReq.QueueId, *msg); err != nil {
		batch.Close()
		http.Error(w, "Failed to acknowledge message: "+err.Error(), http.StatusInternalServerError)
		return
	}
	deleteCachedMessageKey(ackReq.ReceiptHandle)
	if err := batch.Delete(key, nil); err != nil {
		batch.Close()
		http.Error(w, "Failed to acknowledge message: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		http.Error(w, "Failed to acknowledge message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"message": "Message Acknowledged and removed from queue",
	})

	m := getOrCreateMetrics(ackReq.QueueId)
	m.recordAck()
}

func handleBatchAck(w http.ResponseWriter, batchReq BatchAckRequest) {
	if batchReq.QueueId == "" {
		http.Error(w, "queueId is required", http.StatusBadRequest)
		return
	}

	type batchAckResult struct {
		MessageId     string `json:"messageId,omitempty"`
		ReceiptHandle string `json:"receiptHandle,omitempty"`
		Status        string `json:"status"`
		Error         string `json:"error,omitempty"`
	}

	results := make([]batchAckResult, len(batchReq.Acks))

	batch := Db.NewIndexedBatch()
	defer batch.Close()
	if _, closer, err := batch.Get([]byte(batchReq.QueueId)); err != nil {
		if err == pebble.ErrNotFound {
			http.Error(w, "Queue not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to acknowledge: "+err.Error(), http.StatusInternalServerError)
		return
	} else {
		closer.Close()
	}

	for i, entry := range batchReq.Acks {
		results[i].MessageId = entry.MessageId
		results[i].ReceiptHandle = entry.ReceiptHandle

		key, msg, err := messageByReceiptHandle(batch, batchReq.QueueId, entry.ReceiptHandle)
		if err != nil {
			results[i].Status = "error"
			results[i].Error = fmt.Sprintf("message not found: %v", err)
			continue
		}
		results[i].MessageId = msg.ID
		if msg.State != StateInFlight {
			results[i].Status = "error"
			results[i].Error = ErrMessageNotInFlight.Error()
			continue
		}
		if msg.DeliveryAttemptID != entry.DeliveryToken {
			results[i].Status = "error"
			results[i].Error = fmt.Sprintf("delivery token mismatch: expected %q, got %q", msg.DeliveryAttemptID, entry.DeliveryToken)
			continue
		}
		if err := deleteInflightIndex(batch, batchReq.QueueId, *msg); err != nil {
			results[i].Status = "error"
			results[i].Error = fmt.Sprintf("delete in-flight index failed: %v", err)
			continue
		}
		if err := batch.Delete(key, nil); err != nil {
			results[i].Status = "error"
			results[i].Error = fmt.Sprintf("delete failed: %v", err)
			continue
		}
		deleteCachedMessageKey(entry.ReceiptHandle)
		results[i].Status = "ok"
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		http.Error(w, "Failed to acknowledge: "+err.Error(), http.StatusInternalServerError)
		return
	}

	m := getOrCreateMetrics(batchReq.QueueId)
	for _, res := range results {
		if res.Status == "ok" {
			m.recordAck()
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"results": results,
	})
}

func ackBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	var req BatchAckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Acks) == 0 {
		http.Error(w, "acks array is required", http.StatusBadRequest)
		return
	}
	if req.QueueId == "" {
		http.Error(w, "queueId is required", http.StatusBadRequest)
		return
	}
	for _, ack := range req.Acks {
		if ack.ReceiptHandle == "" {
			http.Error(w, "receiptHandle is required for all acks", http.StatusBadRequest)
			return
		}
		if ack.DeliveryToken == "" {
			http.Error(w, "deliveryToken is required for all acks", http.StatusBadRequest)
			return
		}
	}
	handleBatchAck(w, req)
}

func nack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	var ackReq AckRequest
	err := json.NewDecoder(r.Body).Decode(&ackReq)
	if err != nil {
		http.Error(w, "Bad Request: "+err.Error(), http.StatusBadRequest)
		return
	}

	if ackReq.QueueId == "" {
		http.Error(w, "queueId is required", http.StatusBadRequest)
		return
	}
	if ackReq.ReceiptHandle == "" {
		http.Error(w, "receiptHandle is required", http.StatusBadRequest)
		return
	}
	if ackReq.DeliveryToken == "" {
		http.Error(w, "deliveryToken is required", http.StatusBadRequest)
		return
	}

	var nackResultState MessageState
	var needReadyPointer bool

	batch := Db.NewIndexedBatch()
	defer batch.Close()
	if _, closer, err := batch.Get([]byte(ackReq.QueueId)); err != nil {
		batch.Close()
		if err == pebble.ErrNotFound {
			http.Error(w, "Queue or message not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to nack message: "+err.Error(), http.StatusInternalServerError)
		return
	} else {
		closer.Close()
	}

	key, msg, err := messageByReceiptHandle(batch, ackReq.QueueId, ackReq.ReceiptHandle)
	if err != nil {
		batch.Close()
		if _, ok := err.(*ErrInvalidReceiptHandle); ok {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err == pebble.ErrNotFound {
			http.Error(w, "Queue or message not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to nack message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if msg.State != StateInFlight {
		batch.Close()
		http.Error(w, ErrMessageNotInFlight.Error(), http.StatusConflict)
		return
	}
	if msg.DeliveryAttemptID != ackReq.DeliveryToken {
		batch.Close()
		http.Error(w, (&ErrDeliveryTokenMismatch{Expected: msg.DeliveryAttemptID, Got: ackReq.DeliveryToken}).Error(), http.StatusConflict)
		return
	}
	if err := deleteInflightIndex(batch, ackReq.QueueId, *msg); err != nil {
		batch.Close()
		http.Error(w, "Failed to nack message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if msg.MaxDeliveryCount > 0 && msg.DeliveryCount >= msg.MaxDeliveryCount {
		msg.State = StateDead
	} else {
		msg.State = StateReady
	}
	msg.VisibilityDeadline = time.Time{}
	msg.DeliveryAttemptID = ""

	nackResultState = msg.State
	needReadyPointer = nackResultState == StateReady

	updated, err := json.Marshal(msg)
	if err != nil {
		batch.Close()
		http.Error(w, "Failed to nack message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := batch.Set(key, updated, nil); err != nil {
		batch.Close()
		http.Error(w, "Failed to nack message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if needReadyPointer {
		newSeq, err := nextMessageSequence(ackReq.QueueId)
		if err != nil {
			batch.Close()
			http.Error(w, fmt.Sprintf("allocate nack sequence: %v", err), http.StatusInternalServerError)
			return
		}
		if err := batch.Set(readyKey(ackReq.QueueId, newSeq, msg.ID), readyValue(key), nil); err != nil {
			batch.Close()
			http.Error(w, "Failed to nack message: "+err.Error(), http.StatusInternalServerError)
			return
		}
		cacheMessageKey(ackReq.ReceiptHandle, key)
	} else {
		deleteCachedMessageKey(ackReq.ReceiptHandle)
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		http.Error(w, "Failed to nack message: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"message": "Message Nacked and state updated",
		"state":   nackResultState,
	})

	if needReadyPointer {
		signalQueueReady(ackReq.QueueId)
	}

	m := getOrCreateMetrics(ackReq.QueueId)
	m.totalNacked.Add(1)
	m.inFlightCount.Add(-1)
	if nackResultState == StateReady {
		m.readyCount.Add(1)
	} else if nackResultState == StateDead {
		m.deadCount.Add(1)
	}

}
