package main

import (
	"encoding/json"
	"errors"
	"net/http"
)

func publish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	var message PublishRequest

	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		http.Error(w, "Bad Request : "+err.Error(), http.StatusBadRequest)
		return
	}

	ids, err := QueueManager.PublishBatch(r.Context(), message.QueueId, [][]byte{message.Message.Body})
	if err != nil {
		respondPublishError(w, err)
		return
	}

	signalQueueReady(message.QueueId)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"id":    ids[0],
		"state": StateReady,
	})
}

func publishBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	var req BatchPublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Messages) == 0 {
		http.Error(w, "messages array is required", http.StatusBadRequest)
		return
	}
	if req.QueueId == "" {
		http.Error(w, "queueId is required", http.StatusBadRequest)
		return
	}

	bodies := make([][]byte, len(req.Messages))
	for i, msg := range req.Messages {
		bodies[i] = msg.Body
	}

	ids, err := QueueManager.PublishBatch(r.Context(), req.QueueId, bodies)
	if err != nil {
		respondPublishError(w, err)
		return
	}

	signalQueueReady(req.QueueId)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(BatchPublishResponse{IDs: ids})
}

func respondPublishError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrQueueNotFound):
		http.Error(w, "Queue Not Found", http.StatusNotFound)
	case errors.Is(err, ErrMessageLimitExceeded), errors.Is(err, ErrByteLimitExceeded):
		http.Error(w, err.Error(), http.StatusTooManyRequests)
	default:
		http.Error(w, "Error Saving Message: "+err.Error(), http.StatusInternalServerError)
	}
}
