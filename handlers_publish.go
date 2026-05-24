package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
)

func publish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	// publish should , 1. take task 2. put it in queue 3. return id
	var message PublishRequest

	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		http.Error(w, "Bad Request : "+err.Error(), http.StatusBadRequest)
	}

	queueId := message.QueueId
	var queueConfig QueueConfig
	val, closer, err := Db.Get([]byte(queueId))
	if err != nil {
		if err == pebble.ErrNotFound {
			http.Error(w, "Queue Not Found for id: "+queueId, http.StatusNotFound)
			return
		}
		http.Error(w, "Error retrieving queue: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := json.Unmarshal(val, &queueConfig); err != nil {
		http.Error(w, "Error decoding queue: "+err.Error(), http.StatusInternalServerError)
		closer.Close()
		return
	}
	closer.Close()

	// push to queue and return id
	seq, err := nextMessageSequence(queueId)
	if err != nil {
		http.Error(w, "Error Allocating Message Sequence: "+err.Error(), http.StatusInternalServerError)
		return
	}

	message.Message.ID = uuid.NewString()
	message.Message.State = StateReady
	message.Message.EnqueuedAt = time.Now()
	message.Message.MaxDeliveryCount = queueConfig.MaxRetries

	messageJson, err := json.Marshal(message.Message)
	if err != nil {
		http.Error(w, "Bad Reqeust: "+err.Error(), http.StatusBadRequest)
		return
	}

	msgKey := messageKey(queueId, seq, message.Message.ID)
	err = func() error {
		batch := Db.NewIndexedBatch()
		defer batch.Close()
		if err := batch.Set(msgKey, messageJson, nil); err != nil {
			return err
		}
		cacheMessageKey(receiptHandleForMessageKey(msgKey), msgKey)
		if err := batch.Set(readyKey(queueId, seq, message.Message.ID), readyValue(msgKey), nil); err != nil {
			return err
		}
		return batch.Commit(pebble.NoSync)
	}()
	if err != nil {
		http.Error(w, "Error Saving Message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// queue.Messages = append(queue.Messages, message.Message)

	signalQueueReady(queueId)

	m := getOrCreateMetrics(queueId)
	m.totalPublished.Add(1)
	m.readyCount.Add(1)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"id":    message.Message.ID,
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

	var queueConfig QueueConfig
	val, closer, err := Db.Get([]byte(req.QueueId))
	if err != nil {
		if err == pebble.ErrNotFound {
			http.Error(w, "Queue Not Found for id: "+req.QueueId, http.StatusNotFound)
			return
		}
		http.Error(w, "Error retrieving queue: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := json.Unmarshal(val, &queueConfig); err != nil {
		http.Error(w, "Error decoding queue: "+err.Error(), http.StatusInternalServerError)
		closer.Close()
		return
	}
	closer.Close()

	ids := make([]string, len(req.Messages))
	now := time.Now()

	seqs, err := nextMessageSequenceN(req.QueueId, len(req.Messages))
	if err != nil {
		http.Error(w, "Error Allocating Message Sequences: "+err.Error(), http.StatusInternalServerError)
		return
	}

	batch := Db.NewIndexedBatch()
	defer batch.Close()
	for i, msg := range req.Messages {
		msg.ID = uuid.NewString()
		msg.State = StateReady
		msg.EnqueuedAt = now
		msg.MaxDeliveryCount = queueConfig.MaxRetries

		msgJson, err := json.Marshal(msg)
		if err != nil {
			http.Error(w, "Error encoding message: "+err.Error(), http.StatusInternalServerError)
			return
		}

		msgKey := messageKey(req.QueueId, seqs[i], msg.ID)
		if err := batch.Set(msgKey, msgJson, nil); err != nil {
			http.Error(w, "Error Saving Messages: "+err.Error(), http.StatusInternalServerError)
			return
		}
		cacheMessageKey(receiptHandleForMessageKey(msgKey), msgKey)
		if err := batch.Set(readyKey(req.QueueId, seqs[i], msg.ID), readyValue(msgKey), nil); err != nil {
			http.Error(w, "Error Saving Messages: "+err.Error(), http.StatusInternalServerError)
			return
		}
		ids[i] = msg.ID
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		http.Error(w, "Error Saving Messages: "+err.Error(), http.StatusInternalServerError)
		return
	}

	signalQueueReady(req.QueueId)

	m := getOrCreateMetrics(req.QueueId)
	m.totalPublished.Add(int64(len(ids)))
	m.readyCount.Add(int64(len(ids)))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(BatchPublishResponse{IDs: ids})
}
