package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

var queue []int

type ErrDeliveryTokenMismatch struct {
	Expected string
	Got      string
}

func (e *ErrDeliveryTokenMismatch) Error() string {
	return fmt.Sprintf("delivery token mismatch: expected %q, got %q", e.Expected, e.Got)
}

var ErrNoReadyMessages = errors.New("no ready messages")

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
	MaxDeliveryCount   int          `json:"maxDeliveryCount"`
	VisibilityDeadline time.Time    `json:"visibilityDeadline"`
	DeliveryAttemptID  string       `json:"deliveryAttemptId"`
}

type QueueConfig struct {
	Name       string `json:"name"`
	MaxRetries int    `json:"maxRetries"`
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

var messageKeyCache sync.Map // key: "queueID\x00messageID" -> value: []byte (message key)

func cacheMessageKey(queueID, messageID string, key []byte) {
	messageKeyCache.Store(queueID+"\x00"+messageID, key)
}

func getCachedMessageKey(queueID, messageID string) ([]byte, bool) {
	val, ok := messageKeyCache.Load(queueID + "\x00" + messageID)
	if !ok {
		return nil, false
	}
	return val.([]byte), true
}

func deleteCachedMessageKey(queueID, messageID string) {
	messageKeyCache.Delete(queueID + "\x00" + messageID)
}

type queueMetrics struct {
	readyCount     atomic.Int64
	inFlightCount  atomic.Int64
	deadCount      atomic.Int64
	totalPublished atomic.Int64
	totalReceived  atomic.Int64
	totalAcked     atomic.Int64
	totalNacked    atomic.Int64
	ackCountWindow atomic.Int64 // acks in last second (approximate, updated by reaper)
	startedAt      time.Time
}

var metricsStore sync.Map

func snapshotMax(counter *atomic.Int64, val int64) {
	for {
		cur := counter.Load()
		if cur >= val {
			return
		}
		if counter.CompareAndSwap(cur, val) {
			return
		}
	}
}

func (m *queueMetrics) recordAck() {
	m.totalAcked.Add(1)
	m.inFlightCount.Add(-1)
	m.ackCountWindow.Add(1)
}

func (m *queueMetrics) ackRatePerSec() float64 {
	uptime := time.Since(m.startedAt).Seconds()
	if uptime <= 0 {
		return 0
	}
	// Use sliding window count if available, else total/uptime
	windowCount := m.ackCountWindow.Load()
	if windowCount > 0 && uptime < 60 {
		return float64(windowCount) / math.Min(60.0, uptime)
	}
	return float64(m.totalAcked.Load()) / uptime
}

func (m *queueMetrics) resetAckWindow() {
	m.ackCountWindow.Store(0)
}

func getOrCreateMetrics(queueID string) *queueMetrics {
	if m, ok := metricsStore.Load(queueID); ok {
		return m.(*queueMetrics)
	}
	m := &queueMetrics{
		startedAt: time.Now(),
	}
	actual, _ := metricsStore.LoadOrStore(queueID, m)
	return actual.(*queueMetrics)
}

func reconcileMetricsFromDB(queueID string, m *queueMetrics) error {
	var ready, inFlight, dead int64
	err := Db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = queueMessagePrefix(queueID)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if err := item.Value(func(v []byte) error {
				var msg Message
				if err := json.Unmarshal(v, &msg); err != nil {
					return err
				}
				switch msg.State {
				case StateReady:
					ready++
				case StateInFlight:
					inFlight++
				case StateDead:
					dead++
				}
				return nil
			}); err != nil {
				log.Printf("reconcileMetricsFromDB: error reading message in queue %s: %v", queueID, err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	snapshotMax(&m.readyCount, ready)
	snapshotMax(&m.inFlightCount, inFlight)
	snapshotMax(&m.deadCount, dead)
	return nil
}

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

const readyKeySep = "|"

func readyKey(queueID string, seq uint64, messageID string) []byte {
	prefix := readyPrefix(queueID)
	key := make([]byte, 0, len(prefix)+8+1+len(messageID))
	key = append(key, prefix...)
	var seqBytes [8]byte
	binary.BigEndian.PutUint64(seqBytes[:], seq)
	key = append(key, seqBytes[:]...)
	key = append(key, '|')
	key = append(key, messageID...)
	return key
}

func readyPrefix(queueID string) []byte {
	return []byte("ready|" + queueID + readyKeySep)
}

func readyValue(originalSeq uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], originalSeq)
	return buf[:]
}

func parseReadyValue(val []byte) (uint64, error) {
	if len(val) != 8 {
		return 0, fmt.Errorf("invalid ready value length: %d", len(val))
	}
	return binary.BigEndian.Uint64(val), nil
}

func parseMessageKeySeq(key []byte) (uint64, error) {
	idx := bytes.IndexByte(key, '|')
	if idx == -1 {
		return 0, fmt.Errorf("invalid message key format: no delimiter")
	}
	seqStart := idx + 1
	seqEnd := seqStart + 8
	if seqEnd > len(key) {
		return 0, fmt.Errorf("invalid message key format: seq too short")
	}
	return binary.BigEndian.Uint64(key[seqStart:seqEnd]), nil
}

func parseReadyKey(key []byte) (seq uint64, messageID string, err error) {
	// Key format: ready|<queueID>|<8-byte-seq>|<messageID>
	// Find the start of the seq by locating the second '|' after "ready|"
	firstPipe := bytes.IndexByte(key, '|')
	if firstPipe == -1 {
		return 0, "", fmt.Errorf("invalid ready key: no pipes: %s", string(key))
	}
	secondPipe := bytes.IndexByte(key[firstPipe+1:], '|')
	if secondPipe == -1 {
		return 0, "", fmt.Errorf("invalid ready key: missing queueID delimiter: %s", string(key))
	}
	seqStart := firstPipe + 1 + secondPipe + 1
	if seqStart+8 > len(key) {
		return 0, "", fmt.Errorf("invalid ready key: too short: %s", string(key))
	}
	seq = binary.BigEndian.Uint64(key[seqStart : seqStart+8])
	if key[seqStart+8] != '|' {
		return 0, "", fmt.Errorf("invalid ready key: missing delimiter after seq: %s", string(key))
	}
	messageID = string(key[seqStart+8+1:])
	return seq, messageID, nil
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
	MessageId     string `json:"messageId"`
	QueueId       string `json:"queueId"`
	DeliveryToken string `json:"deliveryToken"`
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
		config, err := json.Marshal(QueueConfig{Name: queue.Name, MaxRetries: queue.MaxRetries})
		if err != nil {
			return err
		}
		return txn.Set([]byte(queue.Id), config)
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
			var config QueueConfig
			if err := json.Unmarshal(val, &config); err != nil {
				return err
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]any{
				"id":   id,
				"name": config.Name,
			})
			return nil
		})
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
			return
		}
		log.Println(err)
		http.Error(w, "Error retrieving queue: "+err.Error(), http.StatusInternalServerError)
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
	var queueConfig QueueConfig
	err = Db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(queueId))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &queueConfig)
		})
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			http.Error(w, "Queue Not Found for id: "+queueId, http.StatusNotFound)
			return
		}
		http.Error(w, "Error retrieving queue: "+err.Error(), http.StatusInternalServerError)
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
	message.Message.MaxDeliveryCount = queueConfig.MaxRetries

	messageJson, err := json.Marshal(message.Message)
	if err != nil {
		http.Error(w, "Bad Reqeust: "+err.Error(), http.StatusBadRequest)
		return
	}

	err = Db.Update(func(txn *badger.Txn) error {
		msgKey := messageKey(queueId, seq, message.Message.ID)
		if err := txn.Set(msgKey, messageJson); err != nil {
			return err
		}
		cacheMessageKey(queueId, message.Message.ID, msgKey)
		return txn.Set(readyKey(queueId, seq, message.Message.ID), readyValue(seq))
	})
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

// claimNextReadyMessage seeks the first ready pointer for the queue,
// reads the corresponding message, atomically transitions it to StateInFlight,
// and deletes the ready pointer — all in a single Db.Update transaction.
// Returns ErrNoReadyMessages if no ready messages are available.
func claimNextReadyMessage(queueId string) (*Message, error) {
	var claimed *Message
	err := Db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = readyPrefix(queueId)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			rKey := item.KeyCopy(nil)
			_, msgID, err := parseReadyKey(rKey)
			if err != nil {
				return fmt.Errorf("parse ready key: %w", err)
			}

			var originalSeq uint64
			if err := item.Value(func(v []byte) error {
				originalSeq, err = parseReadyValue(v)
				return err
			}); err != nil {
				return fmt.Errorf("parse ready value: %w", err)
			}

			msgKey := messageKey(queueId, originalSeq, msgID)

			msgItem, err := txn.Get(msgKey)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					txn.Delete(rKey)
					continue
				}
				return fmt.Errorf("get message for ready key: %w", err)
			}

			var msg Message
			if err := msgItem.Value(func(v []byte) error {
				return json.Unmarshal(v, &msg)
			}); err != nil {
				return fmt.Errorf("unmarshal message: %w", err)
			}

			if msg.State != StateReady {
				txn.Delete(rKey)
				continue
			}

			if err := txn.Delete(rKey); err != nil {
				return fmt.Errorf("delete ready key: %w", err)
			}

			msg.State = StateInFlight
			msg.VisibilityDeadline = time.Now().Add(30 * time.Second)
			msg.DeliveryCount++
			msg.DeliveryAttemptID = uuid.NewString()

			updated, err := json.Marshal(msg)
			if err != nil {
				return fmt.Errorf("marshal claimed message: %w", err)
			}
			if err := txn.Set(msgKey, updated); err != nil {
				return fmt.Errorf("set claimed message: %w", err)
			}

			claimed = &msg
			return nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if claimed == nil {
		return nil, ErrNoReadyMessages
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
	err := Db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(id))
		return err
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
			return
		}
		log.Println(err)
		http.Error(w, "Error retrieving queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var msg *Message
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
	if ackReq.DeliveryToken == "" {
		http.Error(w, "deliveryToken is required", http.StatusBadRequest)
		return
	}

	err = Db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get([]byte(ackReq.QueueId)); err != nil {
			return err
		}
		msgKey, ok := getCachedMessageKey(ackReq.QueueId, ackReq.MessageId)
		if !ok {
			return badger.ErrKeyNotFound
		}
		item, err := txn.Get(msgKey)
		if err != nil {
			return err
		}
		var msg Message
		if err := item.Value(func(v []byte) error {
			return json.Unmarshal(v, &msg)
		}); err != nil {
			return err
		}
		if msg.DeliveryAttemptID != ackReq.DeliveryToken {
			return &ErrDeliveryTokenMismatch{Expected: msg.DeliveryAttemptID, Got: ackReq.DeliveryToken}
		}
		deleteCachedMessageKey(ackReq.QueueId, ackReq.MessageId)
		return txn.Delete(msgKey)
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			http.Error(w, "Queue or message not found", http.StatusNotFound)
			return
		}
		if _, ok := err.(*ErrDeliveryTokenMismatch); ok {
			http.Error(w, err.Error(), http.StatusConflict)
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

	m := getOrCreateMetrics(ackReq.QueueId)
	m.recordAck()

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
	if ackReq.DeliveryToken == "" {
		http.Error(w, "deliveryToken is required", http.StatusBadRequest)
		return
	}

var nackResultState MessageState
	var needReadyPointer bool

	err = Db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get([]byte(ackReq.QueueId)); err != nil {
			return err
		}

		msgKey, ok := getCachedMessageKey(ackReq.QueueId, ackReq.MessageId)
		if !ok {
			return badger.ErrKeyNotFound
		}
		item, err := txn.Get(msgKey)
		if err != nil {
			return err
		}
		var msg Message
		if err := item.Value(func(v []byte) error {
			return json.Unmarshal(v, &msg)
		}); err != nil {
			return err
		}

		if msg.DeliveryAttemptID != ackReq.DeliveryToken {
			return &ErrDeliveryTokenMismatch{Expected: msg.DeliveryAttemptID, Got: ackReq.DeliveryToken}
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
			return err
		}

		if err := txn.Set(msgKey, updated); err != nil {
			return err
		}

		if needReadyPointer {
			originalSeq, err := parseMessageKeySeq(msgKey)
			if err != nil {
				return fmt.Errorf("parse message key seq: %w", err)
			}
			newSeq, err := nextMessageSequence(ackReq.QueueId)
			if err != nil {
				return fmt.Errorf("allocate nack sequence: %w", err)
			}
			if err := txn.Set(readyKey(ackReq.QueueId, newSeq, ackReq.MessageId), readyValue(originalSeq)); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			http.Error(w, "Queue or message not found", http.StatusNotFound)
			return
		}
		if _, ok := err.(*ErrDeliveryTokenMismatch); ok {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
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

type reapTransition struct {
	QueueID string
	ToState MessageState
}

func reapExpiredMessages(now time.Time) ([]reapTransition, error) {
	type expiredMsg struct {
		queueID     string
		msgID       string
		key         []byte
		originalSeq uint64
		toDead      bool
	}

	var expired []expiredMsg

	err := Db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if bytes.HasPrefix(key, []byte("ready|")) || bytes.HasPrefix(key, []byte("seq:")) {
				continue
			}
			pipeIndex := bytes.IndexByte(key, '|')
			if pipeIndex == -1 {
				continue
			}
			queueID := string(key[:pipeIndex])

			if err := item.Value(func(v []byte) error {
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

				originalSeq, err := parseMessageKeySeq(key)
				if err != nil {
					return err
				}

				toDead := msg.MaxDeliveryCount > 0 && msg.DeliveryCount >= msg.MaxDeliveryCount
				expired = append(expired, expiredMsg{
					queueID:     queueID,
					msgID:       msg.ID,
					key:         key,
					originalSeq: originalSeq,
					toDead:      toDead,
				})
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	transitions := make([]reapTransition, 0, len(expired))

	err = Db.Update(func(txn *badger.Txn) error {
		for _, exp := range expired {
			item, err := txn.Get(exp.key)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					continue
				}
				return err
			}

			var msg Message
			if err := item.Value(func(v []byte) error {
				return json.Unmarshal(v, &msg)
			}); err != nil {
				return err
			}

			if msg.State != StateInFlight {
				continue
			}
			if msg.VisibilityDeadline.IsZero() || now.Before(msg.VisibilityDeadline) {
				continue
			}

			if exp.toDead {
				msg.State = StateDead
			} else {
				msg.State = StateReady
			}
			msg.VisibilityDeadline = time.Time{}
			msg.DeliveryAttemptID = ""

			updated, err := json.Marshal(msg)
			if err != nil {
				return err
			}
			if err := txn.Set(exp.key, updated); err != nil {
				return err
			}

			if msg.State == StateReady {
				newSeq, err := nextMessageSequence(exp.queueID)
				if err != nil {
					return fmt.Errorf("allocate reaper sequence: %w", err)
				}
				if err := txn.Set(readyKey(exp.queueID, newSeq, exp.msgID), readyValue(exp.originalSeq)); err != nil {
					return err
				}
			}

			transitions = append(transitions, reapTransition{QueueID: exp.queueID, ToState: msg.State})
		}
		return nil
	})

return transitions, err
}

// runs every second and resets expired in-flight messages back to ready in Badger.
func reaper() {

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			transitions, err := reapExpiredMessages(time.Now())
			if err != nil {
				log.Println("reaper:", err)
				continue
			}

			signaled := map[string]struct{}{}
			for _, t := range transitions {
				m := getOrCreateMetrics(t.QueueID)
				m.inFlightCount.Add(-1)
				if t.ToState == StateReady {
					m.readyCount.Add(1)
					if _, ok := signaled[t.QueueID]; !ok {
						signalQueueReady(t.QueueID)
						signaled[t.QueueID] = struct{}{}
					}
				} else if t.ToState == StateDead {
					m.deadCount.Add(1)
				}
			}

			metricsStore.Range(func(_, value any) bool {
				value.(*queueMetrics).resetAckWindow()
				return true
			})
		}
	}()

}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	params := r.URL.Query()
	id := params.Get("id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	err := Db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(id))
		return err
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
			return
		}
		http.Error(w, "Error checking queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	m := getOrCreateMetrics(id)
	if err := reconcileMetricsFromDB(id, m); err != nil {
		http.Error(w, "Error reconciling metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}
	m.resetAckWindow()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"queueId":        id,
		"readyCount":     m.readyCount.Load(),
		"inFlightCount":  m.inFlightCount.Load(),
		"deadCount":      m.deadCount.Load(),
		"totalPublished": m.totalPublished.Load(),
		"totalReceived":  m.totalReceived.Load(),
		"totalAcked":     m.totalAcked.Load(),
		"totalNacked":    m.totalNacked.Load(),
		"ackRatePerSec":  m.ackRatePerSec(),
		"uptimeSeconds":  time.Now().Sub(m.startedAt).Seconds(),
	})
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
	http.HandleFunc("/metrics", metricsHandler)

	reaper()
	fmt.Println("Producer Running on Port " + port)

	if profFile := os.Getenv("KUEUE_CPU_PROFILE"); profFile != "" {
		f, err := os.Create(profFile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
		log.Println("CPU profiling enabled, writing to", profFile)
	}

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("server failed: %v", err)
	}

}
