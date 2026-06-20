package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
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

	var batchReq BatchAckRequest
	if json.Unmarshal(body, &batchReq) == nil && len(batchReq.Acks) > 0 {
		handleBatchAck(w, r, batchReq)
		return
	}

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

	results := QueueManager.AckBatch(r.Context(), ackReq.QueueId, []AckEntry{{
		MessageId:     ackReq.MessageId,
		ReceiptHandle: ackReq.ReceiptHandle,
		DeliveryToken: ackReq.DeliveryToken,
	}})
	res := results[0]
	if res.Status != "ok" {
		respondAckError(w, ackReq.QueueId, res.Error)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"message": "Message Acknowledged and removed from queue",
	})
}

func handleBatchAck(w http.ResponseWriter, r *http.Request, batchReq BatchAckRequest) {
	if batchReq.QueueId == "" {
		http.Error(w, "queueId is required", http.StatusBadRequest)
		return
	}

	if _, err := QueueManager.getQueue(batchReq.QueueId); err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			http.Error(w, "Queue not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to acknowledge: "+err.Error(), http.StatusInternalServerError)
		return
	}

	results := QueueManager.AckBatch(r.Context(), batchReq.QueueId, batchReq.Acks)

	type batchAckResult struct {
		MessageId     string `json:"messageId,omitempty"`
		ReceiptHandle string `json:"receiptHandle,omitempty"`
		Status        string `json:"status"`
		Error         string `json:"error,omitempty"`
	}

	out := make([]batchAckResult, len(results))
	for i, res := range results {
		out[i] = batchAckResult{
			MessageId:     res.MessageID,
			ReceiptHandle: res.ReceiptHandle,
			Status:        res.Status,
			Error:         res.Error,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"results": out,
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
	handleBatchAck(w, r, req)
}

func respondAckError(w http.ResponseWriter, queueID, errMsg string) {
	switch {
	case strings.Contains(errMsg, "queue not found"):
		http.Error(w, "Queue or message not found", http.StatusNotFound)
	case strings.Contains(errMsg, "receipt handle not found") || strings.Contains(errMsg, "duplicate receipt handle"):
		http.Error(w, "invalid receipt handle: "+errMsg, http.StatusBadRequest)
	case strings.Contains(errMsg, "delivery token mismatch"):
		http.Error(w, errMsg, http.StatusConflict)
	default:
		http.Error(w, "Failed to acknowledge message: "+errMsg, http.StatusInternalServerError)
	}
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

	state, err := QueueManager.Nack(r.Context(), ackReq.QueueId, ackReq.ReceiptHandle, ackReq.DeliveryToken)
	if err != nil {
		var tokenMismatch *ErrDeliveryTokenMismatch
		var invalidHandle *ErrInvalidReceiptHandle
		switch {
		case errors.Is(err, ErrQueueNotFound):
			http.Error(w, "Queue or message not found", http.StatusNotFound)
		case errors.As(err, &invalidHandle):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, ErrMessageNotFound):
			http.Error(w, "Queue or message not found", http.StatusNotFound)
		case errors.As(err, &tokenMismatch):
			http.Error(w, err.Error(), http.StatusConflict)
		default:
			http.Error(w, "Failed to nack message: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	if state == StateReady {
		signalQueueReady(ackReq.QueueId)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"message": "Message Nacked and state updated",
		"state":   state,
	})
}
