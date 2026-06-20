package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
)

var claimMu sync.Mutex

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
		json.NewEncoder(w).Encode(map[string]any{
			"id":            msg.ID,
			"body":          msg.Body,
			"state":         StateInFlight,
			"deliveryToken": msg.DeliveryAttemptID,
			"receiptHandle": msg.ReceiptHandle,
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
	json.NewEncoder(w).Encode(map[string]any{
		"messages": batch,
	})
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

// Legacy Pebble-based claim path. Retained for benchmark compatibility while
// the live handlers route through queueManager.ClaimBatch. Pending removal in
// Phase 2.10.

func claimNextReadyMessage(queueId string) (*claimedMessage, error) {
	msgs, err := claimReadyMessages(queueId, 1)
	if err != nil {
		return nil, err
	}
	return &msgs[0], nil
}

func claimReadyMessages(queueId string, max int) ([]claimedMessage, error) {
	claimMu.Lock()
	defer claimMu.Unlock()
	var claimed []claimedMessage
	batch := Db.NewIndexedBatch()
	defer batch.Close()

	prefix := readyPrefix(queueId)
	iter, _ := batch.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid() && len(claimed) < max; iter.Next() {
		rKey := append([]byte(nil), iter.Key()...)
		_, msgID, err := readyPartsFromKey(rKey, prefix)
		if err != nil {
			return nil, fmt.Errorf("parse ready key: %w", err)
		}

		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("read ready value: %w", err)
		}
		msgKey, err := parseReadyValue(val)
		if err != nil {
			return nil, fmt.Errorf("parse ready value: %w", err)
		}

		msgVal, closer, err := batch.Get(msgKey)
		if err != nil {
			if err == pebble.ErrNotFound {
				readySeq, _, parseErr := readyPartsFromKey(rKey, prefix)
				if parseErr != nil {
					return nil, fmt.Errorf("parse ready key fallback: %w", parseErr)
				}
				msgKey = messageKeyBytes(queueId, readySeq, msgID)
				msgVal, closer, err = batch.Get(msgKey)
				if err == pebble.ErrNotFound {
					if delErr := batch.Delete(rKey, nil); delErr != nil {
						return nil, fmt.Errorf("delete stale ready pointer %x: %w", rKey, delErr)
					}
					continue
				}
				if err != nil {
					return nil, fmt.Errorf("get message for ready key: %w", err)
				}
			} else {
				return nil, fmt.Errorf("get message for ready key: %w", err)
			}
		}

		var msg Message
		if err := json.Unmarshal(msgVal, &msg); err != nil {
			closer.Close()
			return nil, fmt.Errorf("unmarshal message: %w", err)
		}
		closer.Close()

		if msg.State != StateReady {
			batch.Delete(rKey, nil)
			continue
		}

		if err := batch.Delete(rKey, nil); err != nil {
			return nil, fmt.Errorf("delete ready key: %w", err)
		}

		msg.State = StateInFlight
		msg.VisibilityDeadline = time.Now().Add(30 * time.Second)
		msg.DeliveryCount++
		msg.DeliveryAttemptID = uuid.NewString()

		updated, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("marshal claimed message: %w", err)
		}
		if err := batch.Set(msgKey, updated, nil); err != nil {
			return nil, fmt.Errorf("set claimed message: %w", err)
		}
		if err := setInflightIndex(batch, queueId, msg, msgKey); err != nil {
			return nil, fmt.Errorf("set in-flight index: %w", err)
		}

		receiptHandle := receiptHandleForMessageKey(msgKey)
		cacheMessageKey(receiptHandle, msgKey)
		claimed = append(claimed, claimedMessage{
			Message:       msg,
			ReceiptHandle: receiptHandle,
		})
	}

	if len(claimed) == 0 {
		iter.Close()
		batch.Close()
		return nil, ErrNoReadyMessages
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return nil, fmt.Errorf("commit claim batch: %w", err)
	}
	return claimed, nil
}
