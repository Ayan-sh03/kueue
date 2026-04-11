package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

var queue []int

type MessageState string

const (
	StateReady    MessageState = "ready"
	StateInFlight MessageState = "in_flight"
	StateDead     MessageState = "dead"
)

type Message struct {
	ID                 string       `json:"id"`
	Body               []byte       `json:"body"`
	State              MessageState `json:"state"`
	EnqueuedAt         time.Time    `json:"enqueuedAt"`
	DeliveryCount      int          `json:"deliveryCount"`
	VisibilityDeadline time.Time    `json:"visibilityDeadline"`
}

type Queue struct {
	Id         string    `json:"id"`
	Name       string    `json:"name"`
	Messages   []Message `json:"messages"`
	MaxRetries int       `json:"maxRetries"`
}

var Db *badger.DB

var Queues []Queue
var DeadLetterQueue []Message

// channel for long polling in receive
var receiveChannel = make(chan struct{}, 1)
var queueReadyChans = map[string]chan struct{}{}
var queueReadyChansMu sync.Mutex

func queueMessagePrefix(queueID string) []byte {
	return []byte(queueID + "|")
}

func messageKey(queueID string, seq uint64, messageID string) []byte {
	prefix := queueMessagePrefix(queueID)
	key := make([]byte, 0, len(prefix)+8+1+len(messageID))
	key = append(key, prefix...)

	var seqBytes [8]byte
	binary.BigEndian.PutUint64(seqBytes[:], seq)
	key = append(key, seqBytes[:]...)
	key = append(key, '|')
	key = append(key, messageID...)

	return key
}

func queueSequenceKey(queueID string) []byte {
	return []byte("seq:" + queueID)
}

func nextMessageSequence(queueID string) (uint64, error) {
	seq, err := Db.GetSequence(queueSequenceKey(queueID), 1)
	if err != nil {
		return 0, err
	}
	defer seq.Release()

	return seq.Next()
}

func findMessageRecord(txn *badger.Txn, queueID, messageID string) ([]byte, *Message, error) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.Prefix = queueMessagePrefix(queueID)
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		var found *Message

		err := item.Value(func(v []byte) error {
			var msg Message
			if err := json.Unmarshal(v, &msg); err != nil {
				return err
			}
			if msg.ID != messageID {
				return nil
			}

			found = &msg
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
		if found != nil {
			return item.KeyCopy(nil), found, nil
		}
	}

	return nil, nil, badger.ErrKeyNotFound
}

func queueReadyChan(queueID string) chan struct{} {
	queueReadyChansMu.Lock()
	defer queueReadyChansMu.Unlock()

	ch, ok := queueReadyChans[queueID]
	if !ok {
		ch = make(chan struct{})
		queueReadyChans[queueID] = ch
	}

	return ch
}

func signalQueueReady(queueID string) {
	queueReadyChansMu.Lock()
	ch, ok := queueReadyChans[queueID]
	if !ok {
		ch = make(chan struct{})
	}
	close(ch)
	queueReadyChans[queueID] = make(chan struct{})
	queueReadyChansMu.Unlock()

	select {
	case receiveChannel <- struct{}{}:
	default:
	}
}

func queueHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	fmt.Fprintln(w, "Hello Consumer")

}

type PublishRequest struct {
	Message Message `json:"message"`
	QueueId string  `json:"queueId"`
}

type CreateRequest struct {
	Name       string `json:"name"`
	MaxRetries int    `json:"maxRetries"`
}

type AckRequest struct {
	MessageId string `json:"messageId"`
	QueueId   string `json:"queueId"`
}

func create(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	var publishRequest CreateRequest

	err := json.NewDecoder(r.Body).Decode(&publishRequest)

	if err != nil {
		http.Error(w, "Bad Requst Error: "+err.Error(), http.StatusBadRequest)

	}

	queue := Queue{
		Id:         uuid.NewString(),
		Name:       publishRequest.Name,
		MaxRetries: publishRequest.MaxRetries,
	}

	err = Db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(queue.Id), []byte(queue.Name))
	})
	if err != nil {
		http.Error(w, "Failed to create queue: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// 	Queues = append(Queues, queue)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"id":    queue.Id,
		"state": StateReady,
	})
}

func getQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	params := r.URL.Query()
	id := params.Get("id")

	if id == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{
			"Error": "id is required in the url",
		})
	}

	//get from db

	err := Db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]any{
				"id":   id,
				"name": string(val),
			})
			return nil
		})
	})
	if err != nil {
		log.Println(err)
		http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
		return
	}

}

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
	var queue *Queue
	err = Db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(queueId))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			queue = &Queue{
				Id:   queueId,
				Name: string(val),
			}
			return nil
		})
	})
	if queue == nil {
		http.Error(w, "Queue Not Found for id: "+queueId, http.StatusNotFound)
		return
	}

	// push to queue and return id
	seq, err := nextMessageSequence(queueId)
	if err != nil {
		http.Error(w, "Error Allocating Message Sequence: "+err.Error(), http.StatusInternalServerError)
		return
	}

	message.Message.ID = uuid.NewString()
	message.Message.State = StateReady
	message.Message.EnqueuedAt = time.Now()

	messageJson, err := json.Marshal(message.Message)
	if err != nil {
		http.Error(w, "Bad Reqeust: "+err.Error(), http.StatusBadRequest)
		return
	}

	err = Db.Update(func(txn *badger.Txn) error {
		return txn.Set(messageKey(queueId, seq, message.Message.ID), messageJson)
	})
	if err != nil {
		http.Error(w, "Error Saving Message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// queue.Messages = append(queue.Messages, message.Message)

	signalQueueReady(queueId)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"id":    message.Message.ID,
		"state": StateReady,
	})

}

// claimNextReadyMessage scans all messages for a queue (prefix queueId|),
// finds the first one in StateReady, atomically updates it to StateInFlight
// in the same Db.Update transaction, and returns it.
func claimNextReadyMessage(queueId string) (*Message, error) {
	var claimed *Message
	err := Db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = queueMessagePrefix(queueId)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				var msg Message
				if err := json.Unmarshal(v, &msg); err != nil {
					return err
				}
				if msg.State != StateReady {
					return nil // skip non-ready messages
				}
				// claim: flip to in-flight
				msg.State = StateInFlight
				msg.VisibilityDeadline = time.Now().Add(30 * time.Second)
				msg.DeliveryCount++
				updated, err := json.Marshal(msg)
				if err != nil {
					return err
				}
				// must copy the key — item.Key() is only valid inside this callback
				if err := txn.Set(item.KeyCopy(nil), updated); err != nil {
					return err
				}
				claimed = &msg
				return nil
			})
			if err != nil {
				return err
			}
			if claimed != nil {
				return nil // stop after first ready message
			}
		}
		return nil
	})
	return claimed, err
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
	err := Db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(id))
		return err
	})
	if err != nil {
		log.Println(err)
		http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
		return
	}

	var msg *Message

	if wait := params.Get("wait"); wait == "true" {
		msg, err = claimNextReadyMessage(id)
		if err != nil {
			http.Error(w, "Error retrieving message: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if msg == nil {
			readyCh := queueReadyChan(id)

			// Re-check after subscribing to avoid missing a publish between the
			// initial claim attempt and the wait setup.
			msg, err = claimNextReadyMessage(id)
			if err != nil {
				http.Error(w, "Error retrieving message: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if msg == nil {
				select {
				case <-readyCh:
					msg, err = claimNextReadyMessage(id)
				case <-time.After(30 * time.Second):
					// timed out — fall through with msg == nil
				}
			}
		}
	} else {
		msg, err = claimNextReadyMessage(id)
	}

	if err != nil {
		http.Error(w, "Error retrieving message: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if msg == nil {
		http.Error(w, "No Ready Messages in Queue: "+id, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"id":    msg.ID,
		"body":  msg.Body,
		"state": StateInFlight,
	})
}

func ack(w http.ResponseWriter, r *http.Request) {
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

	if ackReq.MessageId == "" {
		http.Error(w, "messageId is required", http.StatusBadRequest)
		return
	}
	if ackReq.QueueId == "" {
		http.Error(w, "queueId is required", http.StatusBadRequest)
		return
	}

	err = Db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get([]byte(ackReq.QueueId)); err != nil {
			return err
		}
		key, _, err := findMessageRecord(txn, ackReq.QueueId, ackReq.MessageId)
		if err != nil {
			return err
		}
		return txn.Delete(key)
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			http.Error(w, "Queue or message not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to acknowledge message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"message": "Message Acknowledged and removed from queue",
	})

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

	if ackReq.MessageId == "" {
		http.Error(w, "messageId is required", http.StatusBadRequest)
		return
	}
	if ackReq.QueueId == "" {
		http.Error(w, "queueId is required", http.StatusBadRequest)
		return
	}

	err = Db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get([]byte(ackReq.QueueId)); err != nil {
			return err
		}

		key, msg, err := findMessageRecord(txn, ackReq.QueueId, ackReq.MessageId)
		if err != nil {
			return err
		}

		msg.State = StateReady
		msg.VisibilityDeadline = time.Time{}

		updated, err := json.Marshal(msg)
		if err != nil {
			return err
		}

		return txn.Set(key, updated)
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			http.Error(w, "Queue or message not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to nack message: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"message": "Message Nacked and state updated to ready",
	})

}

func reapExpiredMessages(now time.Time) ([]string, error) {
	recoveredQueues := map[string]struct{}{}

	err := Db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			pipeIndex := bytes.IndexByte(key, '|')
			if pipeIndex == -1 {
				continue
			}
			queueID := string(key[:pipeIndex])

			err := item.Value(func(v []byte) error {
				var msg Message
				if err := json.Unmarshal(v, &msg); err != nil {
					return err
				}
				if msg.State != StateInFlight {
					return nil
				}
				if msg.VisibilityDeadline.IsZero() || now.Before(msg.VisibilityDeadline) {
					return nil
				}

				msg.State = StateReady
				msg.VisibilityDeadline = time.Time{}

				updated, err := json.Marshal(msg)
				if err != nil {
					return err
				}
				if err := txn.Set(item.KeyCopy(nil), updated); err != nil {
					return err
				}

				recoveredQueues[queueID] = struct{}{}
				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	})

	queueIDs := make([]string, 0, len(recoveredQueues))
	for queueID := range recoveredQueues {
		queueIDs = append(queueIDs, queueID)
	}

	return queueIDs, err
}

// runs every second and resets expired in-flight messages back to ready in Badger.
func reaper() {

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			recoveredQueueIDs, err := reapExpiredMessages(time.Now())
			if err != nil {
				log.Println("reaper:", err)
				continue
			}

			for _, queueID := range recoveredQueueIDs {
				signalQueueReady(queueID)
			}
		}
	}()

}

func main() {
	dbPath := os.Getenv("KUEUE_DB_PATH")
	if dbPath == "" {
		dbPath = "./tmp/badger"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	db, err := badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		fmt.Println("Error opening BadgerDB:", err)
		return
	}
	Db = db
	defer Db.Close()
	fmt.Println("DB initialised successfully")
	http.HandleFunc("/", queueHandler)
	http.HandleFunc("/create", create)
	http.HandleFunc("/get", getQueue)
	http.HandleFunc("/publish", publish)
	http.HandleFunc("/ack", ack)
	http.HandleFunc("/nack", nack)
	http.HandleFunc("/receive", receive)

	reaper()
	fmt.Println("Producer Running on Port " + port)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("server failed: %v", err)
	}

}
