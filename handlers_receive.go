package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"
)

func claimWithWait(ctx context.Context, id string, max int, wait bool) ([]claimedMessage, error) {
	msgs, err := QueueManager.ClaimBatch(ctx, id, max)
	if err == nil {
		return msgs, nil
	}
	if !errors.Is(err, ErrNoReadyMessages) {
		return nil, err
	}
	if !wait {
		return nil, ErrNoReadyMessages
	}

	readyCh := queueReadyChan(id)
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	for {
		msgs, err := QueueManager.ClaimBatch(ctx, id, max)
		if err == nil {
			return msgs, nil
		}
		if !errors.Is(err, ErrNoReadyMessages) {
			return nil, err
		}
		select {
		case <-readyCh:
			readyCh = queueReadyChan(id)
			continue
		case <-timer.C:
			return nil, ErrNoReadyMessages
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func receive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	params := r.URL.Query()
	id := params.Get("id")
	if id == "" {
		http.Error(w, "id is required in the url", http.StatusBadRequest)
		return
	}

	if _, err := QueueManager.getQueue(id); err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
			return
		}
		log.Println(err)
		http.Error(w, "Error retrieving queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	max := 1
	maxSpecified := false
	if maxStr := params.Get("max"); maxStr != "" {
		parsed, parseErr := strconv.Atoi(maxStr)
		if parseErr != nil || parsed < 1 || parsed > 100 {
			http.Error(w, "max must be an integer between 1 and 100", http.StatusBadRequest)
			return
		}
		max = parsed
		maxSpecified = true
	}

	wait := params.Get("wait") == "true"

	if max == 1 && !maxSpecified {
		msgs, err := claimWithWait(r.Context(), id, 1, wait)
		if err != nil {
			if errors.Is(err, ErrNoReadyMessages) {
				http.Error(w, "No Ready Messages in Queue: "+id, http.StatusNotFound)
				return
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			http.Error(w, "Error retrieving message: "+err.Error(), http.StatusInternalServerError)
			return
		}
		msg := msgs[0]

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(batchReceiveMessage{
			ID:            msg.ID,
			Body:          msg.Body,
			State:         StateInFlight,
			DeliveryToken: msg.DeliveryAttemptID,
			ReceiptHandle: msg.ReceiptHandle,
		})
		return
	}

	msgs, err := claimWithWait(r.Context(), id, max, wait)
	if err != nil {
		if errors.Is(err, ErrNoReadyMessages) {
			http.Error(w, "No Ready Messages in Queue: "+id, http.StatusNotFound)
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		http.Error(w, "Error retrieving messages: "+err.Error(), http.StatusInternalServerError)
		return
	}

	batch := make([]batchReceiveMessage, 0, len(msgs))
	for _, msg := range msgs {
		batch = append(batch, batchReceiveMessage{
			ID:            msg.ID,
			Body:          msg.Body,
			State:         StateInFlight,
			DeliveryToken: msg.DeliveryAttemptID,
			ReceiptHandle: msg.ReceiptHandle,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(batchReceiveResponse{Messages: batch})
}

func receiveBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	params := r.URL.Query()
	id := params.Get("id")
	if id == "" {
		http.Error(w, "id is required in the url", http.StatusBadRequest)
		return
	}

	max := 1
	if m := params.Get("max"); m != "" {
		if v, err := strconv.Atoi(m); err == nil && v > 0 {
			max = v
		}
	}

	if _, err := QueueManager.getQueue(id); err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
			return
		}
		log.Println(err)
		http.Error(w, "Error retrieving queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	wait := params.Get("wait") == "true"

	msgs, err := claimWithWait(r.Context(), id, max, wait)
	if err != nil {
		if errors.Is(err, ErrNoReadyMessages) {
			http.Error(w, "No Ready Messages in Queue: "+id, http.StatusNotFound)
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		http.Error(w, "Error retrieving messages: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp := batchReceiveResponse{Messages: make([]batchReceiveMessage, len(msgs))}
	for i, msg := range msgs {
		resp.Messages[i] = batchReceiveMessage{
			ID:            msg.ID,
			Body:          msg.Body,
			State:         msg.State,
			DeliveryToken: msg.DeliveryAttemptID,
			ReceiptHandle: msg.ReceiptHandle,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(resp)
}
