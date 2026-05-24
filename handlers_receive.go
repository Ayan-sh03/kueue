package main

import (
	"encoding/json"
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

	// verify queue exists
	_, closer, err := Db.Get([]byte(id))
	if err != nil {
		if err == pebble.ErrNotFound {
			http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
			return
		}
		log.Println(err)
		http.Error(w, "Error retrieving queue: "+err.Error(), http.StatusInternalServerError)
		return
	}
	closer.Close()

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

	if max == 1 && !maxSpecified {
		var msg *claimedMessage
		var claimErr error

		if wait := params.Get("wait"); wait == "true" {
			msg, claimErr = claimNextReadyMessage(id)
			if claimErr != nil && claimErr != ErrNoReadyMessages {
				http.Error(w, "Error retrieving message: "+claimErr.Error(), http.StatusInternalServerError)
				return
			}
			if claimErr == ErrNoReadyMessages {
				readyCh := queueReadyChan(id)
				timer := time.NewTimer(30 * time.Second)
				defer timer.Stop()
			waitLoop:
				for {
					msg, claimErr = claimNextReadyMessage(id)
					if claimErr == nil {
						break
					}
					if claimErr != nil && claimErr != ErrNoReadyMessages {
						http.Error(w, "Error retrieving message: "+claimErr.Error(), http.StatusInternalServerError)
						return
					}
					select {
					case <-readyCh:
						readyCh = queueReadyChan(id)
						continue
					case <-timer.C:
						break waitLoop
					case <-r.Context().Done():
						return
					}
				}
			}
		} else {
			msg, claimErr = claimNextReadyMessage(id)
		}

		if claimErr != nil && claimErr != ErrNoReadyMessages {
			http.Error(w, "Error retrieving message: "+claimErr.Error(), http.StatusInternalServerError)
			return
		}
		if claimErr == ErrNoReadyMessages || msg == nil {
			http.Error(w, "No Ready Messages in Queue: "+id, http.StatusNotFound)
			return
		}

		m := getOrCreateMetrics(id)
		m.totalReceived.Add(1)
		m.readyCount.Add(-1)
		m.inFlightCount.Add(1)

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

	// batch receive (max > 1)
	msgs, claimErr := claimReadyMessages(id, max)
	if claimErr != nil && claimErr != ErrNoReadyMessages {
		http.Error(w, "Error retrieving messages: "+claimErr.Error(), http.StatusInternalServerError)
		return
	}

	if wait := params.Get("wait"); wait == "true" {
		if len(msgs) == 0 {
			readyCh := queueReadyChan(id)
			timer := time.NewTimer(30 * time.Second)
			defer timer.Stop()
		waitLoopBatch:
			for {
				msgs, claimErr = claimReadyMessages(id, max)
				if claimErr != nil && claimErr != ErrNoReadyMessages {
					http.Error(w, "Error retrieving messages: "+claimErr.Error(), http.StatusInternalServerError)
					return
				}
				if len(msgs) > 0 {
					break
				}
				select {
				case <-readyCh:
					readyCh = queueReadyChan(id)
					continue
				case <-timer.C:
					break waitLoopBatch
				case <-r.Context().Done():
					return
				}
			}
		}
	}

	if len(msgs) == 0 {
		http.Error(w, "No Ready Messages in Queue: "+id, http.StatusNotFound)
		return
	}

	type batchMessage struct {
		ID            string       `json:"id"`
		Body          []byte       `json:"body"`
		State         MessageState `json:"state"`
		DeliveryToken string       `json:"deliveryToken"`
		ReceiptHandle string       `json:"receiptHandle"`
	}
	batch := make([]batchMessage, 0, len(msgs))
	for _, msg := range msgs {
		batch = append(batch, batchMessage{
			ID:            msg.ID,
			Body:          msg.Body,
			State:         StateInFlight,
			DeliveryToken: msg.DeliveryAttemptID,
			ReceiptHandle: msg.ReceiptHandle,
		})
	}

	m := getOrCreateMetrics(id)
	for range msgs {
		m.totalReceived.Add(1)
		m.readyCount.Add(-1)
		m.inFlightCount.Add(1)
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

	_, closer, err := Db.Get([]byte(id))
	if err != nil {
		if err == pebble.ErrNotFound {
			http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
			return
		}
		log.Println(err)
		http.Error(w, "Error retrieving queue: "+err.Error(), http.StatusInternalServerError)
		return
	}
	closer.Close()

	var msgs []claimedMessage
	var claimErr error

	if wait := params.Get("wait"); wait == "true" {
		msgs, claimErr = claimReadyMessages(id, max)
		if claimErr != nil && claimErr != ErrNoReadyMessages {
			http.Error(w, "Error retrieving messages: "+claimErr.Error(), http.StatusInternalServerError)
			return
		}
		if claimErr == ErrNoReadyMessages || len(msgs) == 0 {
			readyCh := queueReadyChan(id)
			timer := time.NewTimer(30 * time.Second)
			defer timer.Stop()
		waitLoop:
			for {
				msgs, claimErr = claimReadyMessages(id, max)
				if claimErr == nil && len(msgs) > 0 {
					break
				}
				if claimErr != nil && claimErr != ErrNoReadyMessages {
					http.Error(w, "Error retrieving messages: "+claimErr.Error(), http.StatusInternalServerError)
					return
				}
				select {
				case <-readyCh:
					readyCh = queueReadyChan(id)
					continue
				case <-timer.C:
					break waitLoop
				case <-r.Context().Done():
					return
				}
			}
		}
	} else {
		msgs, claimErr = claimReadyMessages(id, max)
	}

	if claimErr != nil && claimErr != ErrNoReadyMessages {
		http.Error(w, "Error retrieving messages: "+claimErr.Error(), http.StatusInternalServerError)
		return
	}
	if claimErr == ErrNoReadyMessages || len(msgs) == 0 {
		http.Error(w, "No Ready Messages in Queue: "+id, http.StatusNotFound)
		return
	}

	m := getOrCreateMetrics(id)
	for range msgs {
		m.totalReceived.Add(1)
		m.readyCount.Add(-1)
		m.inFlightCount.Add(1)
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
