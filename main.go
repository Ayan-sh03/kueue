package main

import (
	"bytes"
	"container/heap"
	"container/list"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
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

type ErrInvalidReceiptHandle struct {
	Reason string
}

func (e *ErrInvalidReceiptHandle) Error() string {
	if e.Reason == "" {
		return "invalid receipt handle"
	}
	return "invalid receipt handle: " + e.Reason
}

var ErrNoReadyMessages = errors.New("no ready messages")
var ErrMessageNotInFlight = errors.New("message is not in flight")

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

type claimedMessage struct {
	Message
	ReceiptHandle string `json:"receiptHandle"`
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

var Db *pebble.DB

var Queues []Queue
var DeadLetterQueue []Message

var messageKeyCache sync.Map // key: receiptHandle -> value: []byte (message key)
var claimMu sync.Mutex

func cacheMessageKey(receiptHandle string, key []byte) {
	messageKeyCache.Store(receiptHandle, append([]byte(nil), key...))
}

func getCachedMessageKey(receiptHandle string) ([]byte, bool) {
	val, ok := messageKeyCache.Load(receiptHandle)
	if !ok {
		return nil, false
	}
	return append([]byte(nil), val.([]byte)...), true
}

func deleteCachedMessageKey(receiptHandle string) {
	messageKeyCache.Delete(receiptHandle)
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
	reconcileMu    sync.Mutex
	lastReconcile  time.Time
}

var metricsStore sync.Map

const metricsReconcileInterval = 10 * time.Second

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
	prefix := queueMessagePrefix(queueID)
	snap := Db.NewSnapshot()
	defer snap.Close()
	iter, _ := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	defer iter.Close()
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			log.Printf("reconcileMetricsFromDB: error reading message in queue %s: %v", queueID, err)
			return err
		}
		var msg Message
		if err := json.Unmarshal(val, &msg); err != nil {
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
	}
	snapshotMax(&m.readyCount, ready)
	snapshotMax(&m.inFlightCount, inFlight)
	snapshotMax(&m.deadCount, dead)
	return nil
}

func reconcileMetricsFromDBIfStale(queueID string, m *queueMetrics, now time.Time) error {
	m.reconcileMu.Lock()
	defer m.reconcileMu.Unlock()

	if !m.lastReconcile.IsZero() && now.Sub(m.lastReconcile) < metricsReconcileInterval {
		return nil
	}
	err := reconcileMetricsFromDB(queueID, m)
	m.lastReconcile = now
	return err
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

func messageKeyBytes(queueID string, seq uint64, messageID []byte) []byte {
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

var seqMu sync.Mutex

func nextMessageSequence(queueID string) (uint64, error) {
	seqMu.Lock()
	defer seqMu.Unlock()

	key := queueSequenceKey(queueID)
	val, closer, err := Db.Get(key)
	var current uint64
	if err == pebble.ErrNotFound {
		current = 0
	} else if err != nil {
		return 0, err
	} else {
		current = binary.BigEndian.Uint64(val)
		closer.Close()
	}
	next := current + 1
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], next)
	if err := Db.Set(key, buf[:], pebble.NoSync); err != nil {
		return 0, err
	}
	return next, nil
}

func nextMessageSequenceN(queueID string, n int) ([]uint64, error) {
	if n <= 0 {
		return nil, nil
	}
	seqMu.Lock()
	defer seqMu.Unlock()

	key := queueSequenceKey(queueID)
	val, closer, err := Db.Get(key)
	var current uint64
	if err == pebble.ErrNotFound {
		current = 0
	} else if err != nil {
		return nil, err
	} else {
		current = binary.BigEndian.Uint64(val)
		closer.Close()
	}

	seqs := make([]uint64, n)
	for i := 0; i < n; i++ {
		current++
		seqs[i] = current
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], current)
	if err := Db.Set(key, buf[:], pebble.NoSync); err != nil {
		return nil, err
	}
	return seqs, nil
}

func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix)+1)
	copy(upper, prefix)
	upper[len(prefix)] = 0xFF
	return upper
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

func readyValue(msgKey []byte) []byte {
	return msgKey
}

func parseReadyValue(val []byte) ([]byte, error) {
	if len(val) == 0 {
		return nil, fmt.Errorf("invalid ready value: empty")
	}
	return append([]byte(nil), val...), nil
}

func receiptHandleForMessageKey(key []byte) string {
	return base64.RawURLEncoding.EncodeToString(key)
}

func messageKeyFromReceiptHandle(queueID, receiptHandle string) ([]byte, error) {
	if receiptHandle == "" {
		return nil, &ErrInvalidReceiptHandle{Reason: "receiptHandle is required"}
	}

	if key, ok := getCachedMessageKey(receiptHandle); ok {
		if !bytes.HasPrefix(key, queueMessagePrefix(queueID)) {
			return nil, &ErrInvalidReceiptHandle{Reason: "queue mismatch"}
		}
		if _, err := parseMessageKeySeq(key); err != nil {
			return nil, &ErrInvalidReceiptHandle{Reason: err.Error()}
		}
		return key, nil
	}

	key, err := base64.RawURLEncoding.DecodeString(receiptHandle)
	if err != nil {
		return nil, &ErrInvalidReceiptHandle{Reason: "base64 decode failed"}
	}
	if !bytes.HasPrefix(key, queueMessagePrefix(queueID)) {
		return nil, &ErrInvalidReceiptHandle{Reason: "queue mismatch"}
	}
	if _, err := parseMessageKeySeq(key); err != nil {
		return nil, &ErrInvalidReceiptHandle{Reason: err.Error()}
	}
	cacheMessageKey(receiptHandle, key)
	return key, nil
}

func inflightPrefix() []byte {
	return []byte("inflight|")
}

func inflightKey(queueID string, deadline time.Time, messageID string) []byte {
	prefix := inflightPrefix()
	key := make([]byte, 0, len(prefix)+8+1+len(queueID)+1+len(messageID))
	key = append(key, prefix...)

	var deadlineBytes [8]byte
	binary.BigEndian.PutUint64(deadlineBytes[:], uint64(deadline.UnixNano()))
	key = append(key, deadlineBytes[:]...)
	key = append(key, '|')
	key = append(key, queueID...)
	key = append(key, '|')
	key = append(key, messageID...)
	return key
}

func inflightScanUpperBound(now time.Time) []byte {
	prefix := inflightPrefix()
	key := make([]byte, 0, len(prefix)+8)
	key = append(key, prefix...)

	var deadlineBytes [8]byte
	binary.BigEndian.PutUint64(deadlineBytes[:], uint64(now.UnixNano()))
	key = append(key, deadlineBytes[:]...)
	return key
}

func setInflightIndex(batch *pebble.Batch, queueID string, msg Message, msgKey []byte) error {
	if msg.VisibilityDeadline.IsZero() {
		return nil
	}
	return batch.Set(inflightKey(queueID, msg.VisibilityDeadline, msg.ID), msgKey, nil)
}

func deleteInflightIndex(batch *pebble.Batch, queueID string, msg Message) error {
	if msg.VisibilityDeadline.IsZero() || msg.ID == "" {
		return nil
	}
	err := batch.Delete(inflightKey(queueID, msg.VisibilityDeadline, msg.ID), nil)
	if err == pebble.ErrNotFound {
		return nil
	}
	return err
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

func parseMessageKeyQueueID(key []byte) (string, error) {
	idx := bytes.IndexByte(key, '|')
	if idx == -1 {
		return "", fmt.Errorf("invalid message key format: no delimiter")
	}
	if idx == 0 {
		return "", fmt.Errorf("invalid message key format: empty queue id")
	}
	return string(key[:idx]), nil
}

func readyPartsFromKey(key, prefix []byte) (uint64, []byte, error) {
	if !bytes.HasPrefix(key, prefix) {
		return 0, nil, fmt.Errorf("ready key does not match prefix")
	}
	rest := key[len(prefix):]
	if len(rest) < 9 {
		return 0, nil, fmt.Errorf("invalid ready key: too short")
	}
	if rest[8] != '|' {
		return 0, nil, fmt.Errorf("invalid ready key: missing delimiter after seq")
	}
	if len(rest[9:]) == 0 {
		return 0, nil, fmt.Errorf("invalid ready key: missing message id")
	}
	return binary.BigEndian.Uint64(rest[:8]), rest[9:], nil
}

func findMessageRecord(queueID, messageID string) ([]byte, *Message, error) {
	prefix := queueMessagePrefix(queueID)
	iter, _ := Db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, nil, err
		}
		var msg Message
		if err := json.Unmarshal(val, &msg); err != nil {
			return nil, nil, err
		}
		if msg.ID == messageID {
			return append([]byte(nil), iter.Key()...), &msg, nil
		}
	}

	return nil, nil, pebble.ErrNotFound
}

func messageByReceiptHandle(batch *pebble.Batch, queueID, receiptHandle string) ([]byte, *Message, error) {
	key, err := messageKeyFromReceiptHandle(queueID, receiptHandle)
	if err != nil {
		return nil, nil, err
	}

	val, closer, err := batch.Get(key)
	if err != nil {
		return nil, nil, err
	}
	defer closer.Close()

	var msg Message
	if err := json.Unmarshal(val, &msg); err != nil {
		return nil, nil, err
	}

	return key, &msg, nil
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
	MessageId     string `json:"messageId,omitempty"`
	QueueId       string `json:"queueId"`
	ReceiptHandle string `json:"receiptHandle"`
	DeliveryToken string `json:"deliveryToken"`
}

type AckEntry struct {
	MessageId     string `json:"messageId,omitempty"`
	ReceiptHandle string `json:"receiptHandle"`
	DeliveryToken string `json:"deliveryToken"`
}

type BatchAckRequest struct {
	QueueId string     `json:"queueId"`
	Acks    []AckEntry `json:"acks"`
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

	err = func() error {
		batch := Db.NewIndexedBatch()
		defer batch.Close()
		config, err := json.Marshal(QueueConfig{Name: queue.Name, MaxRetries: queue.MaxRetries})
		if err != nil {
			return err
		}
		if err := batch.Set([]byte(queue.Id), config, nil); err != nil {
			return err
		}
		return batch.Commit(pebble.NoSync)
	}()
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

	val, closer, err := Db.Get([]byte(id))
	if err != nil {
		if err == pebble.ErrNotFound {
			http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
			return
		}
		log.Println(err)
		http.Error(w, "Error retrieving queue: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer closer.Close()
	var config QueueConfig
	if err := json.Unmarshal(val, &config); err != nil {
		http.Error(w, "Error decoding queue config: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"id":   id,
		"name": config.Name,
	})

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

type BatchPublishRequest struct {
	Messages []Message `json:"messages"`
	QueueId  string    `json:"queueId"`
}

type BatchPublishResponse struct {
	IDs []string `json:"ids"`
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

// claimNextReadyMessage seeks the first ready pointer for the queue,
// reads the corresponding message, atomically transitions it to StateInFlight,
// and deletes the ready pointer — all in a single IndexedBatch commit.
// Returns ErrNoReadyMessages if no ready messages are available.
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

type batchReceiveResponse struct {
	Messages []batchReceiveMessage `json:"messages"`
}

type batchReceiveMessage struct {
	ID            string       `json:"id"`
	Body          []byte       `json:"body"`
	State         MessageState `json:"state"`
	DeliveryToken string       `json:"deliveryToken"`
	ReceiptHandle string       `json:"receiptHandle"`
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

type reapTransition struct {
	QueueID string
	ToState MessageState
}

func reapExpiredMessages(now time.Time) ([]reapTransition, error) {
	type expiredMsg struct {
		indexKey []byte
		msgKey   []byte
	}
	var expired []expiredMsg

	prefix := inflightPrefix()
	snap := Db.NewSnapshot()
	defer snap.Close()
	iter, _ := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	defer iter.Close()

	upper := inflightScanUpperBound(now)
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := append([]byte(nil), iter.Key()...)
		if len(key) >= len(upper) && bytes.Compare(key[:len(upper)], upper) > 0 {
			break
		}

		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}
		expired = append(expired, expiredMsg{
			indexKey: key,
			msgKey:   append([]byte(nil), val...),
		})
	}

	const reapBatch = 1024
	transitions := make([]reapTransition, 0, len(expired))

	for i := 0; i < len(expired); i += reapBatch {
		end := i + reapBatch
		if end > len(expired) {
			end = len(expired)
		}
		chunk := expired[i:end]

		batch := Db.NewIndexedBatch()
		for _, exp := range chunk {
			msgVal, closer, err := batch.Get(exp.msgKey)
			if err != nil {
				if err == pebble.ErrNotFound {
					_ = batch.Delete(exp.indexKey, nil)
					continue
				}
				batch.Close()
				return transitions, err
			}

			var msg Message
			if err := json.Unmarshal(msgVal, &msg); err != nil {
				closer.Close()
				batch.Close()
				return transitions, err
			}
			closer.Close()

			if msg.State != StateInFlight {
				_ = batch.Delete(exp.indexKey, nil)
				continue
			}
			if msg.VisibilityDeadline.IsZero() || now.Before(msg.VisibilityDeadline) {
				_ = batch.Delete(exp.indexKey, nil)
				continue
			}

			queueID, err := parseMessageKeyQueueID(exp.msgKey)
			if err != nil {
				batch.Close()
				return transitions, err
			}

			if msg.MaxDeliveryCount > 0 && msg.DeliveryCount >= msg.MaxDeliveryCount {
				msg.State = StateDead
			} else {
				msg.State = StateReady
			}
			msg.VisibilityDeadline = time.Time{}
			msg.DeliveryAttemptID = ""

			updated, err := json.Marshal(msg)
			if err != nil {
				batch.Close()
				return transitions, err
			}
			if err := batch.Set(exp.msgKey, updated, nil); err != nil {
				batch.Close()
				return transitions, err
			}
			_ = batch.Delete(exp.indexKey, nil)

			if msg.State == StateReady {
				newSeq, err := nextMessageSequence(queueID)
				if err != nil {
					batch.Close()
					return transitions, fmt.Errorf("allocate reaper sequence: %w", err)
				}
				if err := batch.Set(readyKey(queueID, newSeq, msg.ID), readyValue(exp.msgKey), nil); err != nil {
					batch.Close()
					return transitions, err
				}
			}

			transitions = append(transitions, reapTransition{QueueID: queueID, ToState: msg.State})
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			return transitions, err
		}
		batch.Close()
	}

	return transitions, nil
}

// runs every second and resets expired in-flight messages back to ready.
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

	_, closer, err := Db.Get([]byte(id))
	if err != nil {
		if err == pebble.ErrNotFound {
			http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
			return
		}
		http.Error(w, "Error checking queue: "+err.Error(), http.StatusInternalServerError)
		return
	}
	closer.Close()

	m := getOrCreateMetrics(id)
	if err := reconcileMetricsFromDBIfStale(id, m, time.Now()); err != nil {
		http.Error(w, "Error reconciling metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}

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
		dbPath = "./tmp/pebble"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		fmt.Println("Error opening Pebble:", err)
		return
	}
	Db = db
	defer Db.Close()
	fmt.Println("DB initialised successfully")
	http.HandleFunc("/", queueHandler)
	http.HandleFunc("/create", create)
	http.HandleFunc("/get", getQueue)
	http.HandleFunc("/publish", publish)
	http.HandleFunc("/publish-batch", publishBatch)
	http.HandleFunc("/ack", ack)
	http.HandleFunc("/ack-batch", ackBatch)
	http.HandleFunc("/nack", nack)
	http.HandleFunc("/receive", receive)
	http.HandleFunc("/receive-batch", receiveBatch)
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

// ============================================================================
// Phase 2.2: In-memory queueRuntime state model
// ============================================================================

// walAppender is the minimal interface the queue manager needs from the WAL.
// walStore already satisfies this. A fake implementation is used in tests.
type walAppender interface {
	Append(ctx context.Context, entries []walEntry) (firstLSN, lastLSN uint64, err error)
}

// messageRecord is the in-memory representation of a message.
// Body is immutable after publish (caller must copy input).
type messageRecord struct {
	ID               string
	QueueID          string
	Seq              uint64
	Body             []byte
	State            MessageState
	EnqueuedAt       time.Time
	DeliveryCount    int
	MaxDeliveryCount int

	CurrentReceiptHandle string
	CurrentDeliveryToken string
	VisibilityDeadline   time.Time

	readyElement *list.Element // list node for O(1) removal from ready list
	heapIndex    int           // index in visibilityHeap; -1 when not in heap
}

// deliveryRecord tracks an in-flight delivery for O(1) ack/nack lookup.
type deliveryRecord struct {
	MessageID     string
	ReceiptHandle string
	DeliveryToken string
	Deadline      time.Time
	DeliveryCount int
	seq           uint64
	heapIndex     int // index in visibilityHeap
}

var deliveryRecordSeq atomic.Uint64

// ============================================================================
// visibilityHeap
// ============================================================================

// visibilityHeap is a min-heap of *deliveryRecord ordered by Deadline.
type visibilityHeap []*deliveryRecord

func (h visibilityHeap) Len() int { return len(h) }
func (h visibilityHeap) Less(i, j int) bool {
	if h[i].Deadline.Equal(h[j].Deadline) {
		return h[i].seq < h[j].seq
	}
	return h[i].Deadline.Before(h[j].Deadline)
}
func (h visibilityHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *visibilityHeap) Push(x any) {
	dr := x.(*deliveryRecord)
	dr.heapIndex = len(*h)
	*h = append(*h, dr)
}

func (h *visibilityHeap) Pop() any {
	old := *h
	n := len(old)
	dr := old[n-1]
	dr.heapIndex = -1
	*h = old[0 : n-1]
	return dr
}

// ============================================================================
// queueRuntime: per-queue in-memory state
// ============================================================================

type queueRuntime struct {
	mu sync.Mutex

	id     string
	config QueueConfig

	nextSeq uint64

	ready    *list.List                 // []*messageRecord — FIFO order
	messages map[string]*messageRecord  // keyed by message ID
	inflight map[string]*deliveryRecord // keyed by receiptHandle
	dead     map[string]*messageRecord

	deadlines visibilityHeap // min-heap of *deliveryRecord

	readyCh chan struct{}
	metrics *queueMetrics

	maxMessages int64
	maxBytes    int64
	bytesInMem  int64
}

func newQueueRuntime(id string, config QueueConfig, metrics *queueMetrics) *queueRuntime {
	q := &queueRuntime{
		id:          id,
		config:      config,
		ready:       list.New(),
		messages:    make(map[string]*messageRecord),
		inflight:    make(map[string]*deliveryRecord),
		dead:        make(map[string]*messageRecord),
		readyCh:     make(chan struct{}, 1),
		metrics:     metrics,
		maxMessages: parseInt64Env("KUEUE_MAX_IN_MEMORY_MESSAGES", 0),
		maxBytes:    parseInt64Env("KUEUE_MAX_IN_MEMORY_BYTES", 0),
	}
	heap.Init(&q.deadlines)
	return q
}

func parseInt64Env(name string, defaultVal int64) int64 {
	s := os.Getenv(name)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return defaultVal
	}
	return v
}

// receiptHandleForMessage returns a deterministic receipt handle for a message.
func receiptHandleForMessage(queueID string, seq uint64, messageID string) string {
	raw := queueID + "|" + strconv.FormatUint(seq, 10) + "|" + messageID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func (q *queueRuntime) signalReady() {
	select {
	case q.readyCh <- struct{}{}:
	default:
	}
}

// ============================================================================
// queueManager: process-wide manager
// ============================================================================

type queueManager struct {
	mu     sync.RWMutex
	queues map[string]*queueRuntime
	wal    walAppender
}

func newQueueManager(wal walAppender) *queueManager {
	return &queueManager{
		queues: make(map[string]*queueRuntime),
		wal:    wal,
	}
}

// getQueue returns the queueRuntime for an existing queue.
func (qm *queueManager) getQueue(queueID string) (*queueRuntime, error) {
	qm.mu.RLock()
	q, ok := qm.queues[queueID]
	qm.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("queue %q not found", queueID)
	}
	return q, nil
}

// ============================================================================
// CreateQueue
// ============================================================================

func (qm *queueManager) CreateQueue(ctx context.Context, name string, maxRetries int) (string, error) {
	queueID := uuid.NewString()
	metrics := getOrCreateMetrics(queueID)

	entry := walEntry{
		Op: opCreateQueue,
		Payload: walCreateQueuePayload{
			QueueID:    queueID,
			Name:       name,
			MaxRetries: maxRetries,
		},
	}
	if _, _, err := qm.wal.Append(ctx, []walEntry{entry}); err != nil {
		return "", fmt.Errorf("wal append create queue: %w", err)
	}

	config := QueueConfig{Name: name, MaxRetries: maxRetries}
	q := newQueueRuntime(queueID, config, metrics)

	qm.mu.Lock()
	qm.queues[queueID] = q
	qm.mu.Unlock()

	return queueID, nil
}

// ============================================================================
// PublishBatch
// ============================================================================

func (qm *queueManager) PublishBatch(ctx context.Context, queueID string, bodies [][]byte) ([]string, error) {
	q, err := qm.getQueue(queueID)
	if err != nil {
		return nil, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	n := len(bodies)
	if n == 0 {
		return nil, nil
	}

	// Enforce memory limits.
	if q.maxMessages > 0 && int64(len(q.messages)+n) > q.maxMessages {
		return nil, errors.New("queue message limit exceeded")
	}
	var totalBytes int64
	for _, b := range bodies {
		totalBytes += int64(len(b))
	}
	if q.maxBytes > 0 && q.bytesInMem+totalBytes > q.maxBytes {
		return nil, errors.New("queue byte limit exceeded")
	}

	// Allocate seq range.
	startSeq := q.nextSeq
	q.nextSeq += uint64(n)

	records := make([]*messageRecord, n)
	now := time.Now()
	walMsgs := make([]walPublishedMessage, n)
	ids := make([]string, n)

	for i, body := range bodies {
		msgID := uuid.NewString()
		seq := startSeq + uint64(i)
		msg := &messageRecord{
			ID:               msgID,
			QueueID:          queueID,
			Seq:              seq,
			Body:             bytes.Clone(body),
			State:            StateReady,
			EnqueuedAt:       now,
			DeliveryCount:    0,
			MaxDeliveryCount: q.config.MaxRetries,
		}
		records[i] = msg
		walMsgs[i] = walPublishedMessage{
			MessageID:        msgID,
			Seq:              seq,
			Body:             msg.Body,
			EnqueuedAt:       now,
			MaxDeliveryCount: msg.MaxDeliveryCount,
		}
		ids[i] = msgID
	}

	entry := walEntry{
		Op: opPublishBatch,
		Payload: walPublishBatchPayload{
			QueueID:  queueID,
			Messages: walMsgs,
		},
	}
	if _, _, err := qm.wal.Append(ctx, []walEntry{entry}); err != nil {
		q.nextSeq = startSeq // rollback seq allocation
		return nil, fmt.Errorf("wal append publish batch: %w", err)
	}

	// WAL succeeded — install into memory.
	for _, msg := range records {
		msg.readyElement = q.ready.PushBack(msg)
		q.messages[msg.ID] = msg
	}
	q.bytesInMem += totalBytes
	q.metrics.totalPublished.Add(int64(n))
	q.metrics.readyCount.Add(int64(n))
	q.signalReady()

	return ids, nil
}

// ============================================================================
// ClaimBatch — O(1) list pop, not map scan
// ============================================================================

func (qm *queueManager) ClaimBatch(ctx context.Context, queueID string, max int) ([]claimedMessage, error) {
	q, err := qm.getQueue(queueID)
	if err != nil {
		return nil, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Pop up to max from ready list front.
	var popped []*messageRecord
	for i := 0; i < max; i++ {
		front := q.ready.Front()
		if front == nil {
			break
		}
		msg := front.Value.(*messageRecord)
		q.ready.Remove(front)
		msg.readyElement = nil
		popped = append(popped, msg)
	}
	if len(popped) == 0 {
		return nil, ErrNoReadyMessages
	}

	now := time.Now()
	vt := 30 * time.Second
	claims := make([]walClaimedMessage, len(popped))

	for i, msg := range popped {
		msg.State = StateInFlight
		msg.DeliveryCount++
		msg.CurrentReceiptHandle = receiptHandleForMessage(queueID, msg.Seq, msg.ID)
		msg.CurrentDeliveryToken = uuid.NewString()
		msg.VisibilityDeadline = now.Add(vt)

		dr := &deliveryRecord{
			MessageID:     msg.ID,
			ReceiptHandle: msg.CurrentReceiptHandle,
			DeliveryToken: msg.CurrentDeliveryToken,
			Deadline:      msg.VisibilityDeadline,
			DeliveryCount: msg.DeliveryCount,
			seq:           deliveryRecordSeq.Add(1),
		}
		q.inflight[dr.ReceiptHandle] = dr
		heap.Push(&q.deadlines, dr)

		claims[i] = walClaimedMessage{
			MessageID:          msg.ID,
			ReceiptHandle:      dr.ReceiptHandle,
			DeliveryToken:      dr.DeliveryToken,
			VisibilityDeadline: msg.VisibilityDeadline,
			DeliveryCount:      msg.DeliveryCount,
		}
	}

	entry := walEntry{
		Op: opClaimBatch,
		Payload: walClaimBatchPayload{
			QueueID: queueID,
			Claims:  claims,
		},
	}
	if _, _, err := qm.wal.Append(ctx, []walEntry{entry}); err != nil {
		// Rollback: restore popped records to ready front in reverse order
		// and remove the delivery records added to inflight/heap.
		for i := len(popped) - 1; i >= 0; i-- {
			msg := popped[i]
			rh := msg.CurrentReceiptHandle
			msg.State = StateReady
			msg.CurrentReceiptHandle = ""
			msg.CurrentDeliveryToken = ""
			msg.VisibilityDeadline = time.Time{}
			msg.DeliveryCount--
			msg.readyElement = q.ready.PushFront(msg)
			delete(q.inflight, rh)
		}
		q.deadlines = q.deadlines[:0]
		for _, dr := range q.inflight {
			dr.heapIndex = -1
			heap.Push(&q.deadlines, dr)
		}
		return nil, fmt.Errorf("wal append claim batch: %w", err)
	}

	q.metrics.readyCount.Add(-int64(len(popped)))
	q.metrics.inFlightCount.Add(int64(len(popped)))
	q.metrics.totalReceived.Add(int64(len(popped)))

	result := make([]claimedMessage, len(popped))
	for i, msg := range popped {
		result[i] = msg.toClaimedMessage()
	}
	return result, nil
}

func (msg *messageRecord) toClaimedMessage() claimedMessage {
	return claimedMessage{
		Message: Message{
			ID:                 msg.ID,
			Body:               msg.Body,
			State:              msg.State,
			EnqueuedAt:         msg.EnqueuedAt,
			DeliveryCount:      msg.DeliveryCount,
			MaxDeliveryCount:   msg.MaxDeliveryCount,
			VisibilityDeadline: msg.VisibilityDeadline,
			DeliveryAttemptID:  msg.CurrentDeliveryToken,
		},
		ReceiptHandle: msg.CurrentReceiptHandle,
	}
}

// ============================================================================
// AckBatch
// ============================================================================

type runtimeAckResult struct {
	ReceiptHandle string
	Status        string // "ok" or "error"
	Error         string
}

func (qm *queueManager) AckBatch(ctx context.Context, queueID string, acks []AckEntry) []runtimeAckResult {
	q, err := qm.getQueue(queueID)
	if err != nil {
		results := make([]runtimeAckResult, len(acks))
		for i := range acks {
			results[i] = runtimeAckResult{
				ReceiptHandle: acks[i].ReceiptHandle,
				Status:        "error",
				Error:         err.Error(),
			}
		}
		return results
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Validate each entry and collect valid ones for WAL.
	valid := make([]*deliveryRecord, 0, len(acks))
	seen := make(map[string]bool, len(acks))
	results := make([]runtimeAckResult, len(acks))
	for i, entry := range acks {
		results[i].ReceiptHandle = entry.ReceiptHandle
		if seen[entry.ReceiptHandle] {
			results[i].Status = "error"
			results[i].Error = "duplicate receipt handle"
			continue
		}
		dr, ok := q.inflight[entry.ReceiptHandle]
		if !ok {
			results[i].Status = "error"
			results[i].Error = "receipt handle not found"
			continue
		}
		if dr.DeliveryToken != entry.DeliveryToken {
			results[i].Status = "error"
			results[i].Error = (&ErrDeliveryTokenMismatch{Expected: dr.DeliveryToken, Got: entry.DeliveryToken}).Error()
			continue
		}
		seen[entry.ReceiptHandle] = true
		valid = append(valid, dr)
		results[i].Status = "ok"
	}

	if len(valid) == 0 {
		return results
	}

	walAcks := make([]walAckedMessage, len(valid))
	for i, dr := range valid {
		walAcks[i] = walAckedMessage{
			MessageID:     dr.MessageID,
			ReceiptHandle: dr.ReceiptHandle,
			DeliveryToken: dr.DeliveryToken,
		}
	}
	entry := walEntry{
		Op: opAckBatch,
		Payload: walAckBatchPayload{
			QueueID: queueID,
			Acks:    walAcks,
		},
	}
	if _, _, err := qm.wal.Append(ctx, []walEntry{entry}); err != nil {
		for i := range results {
			results[i].Status = "error"
			results[i].Error = "wal append failed: " + err.Error()
		}
		return results
	}

	// WAL succeeded — remove from memory.
	for _, dr := range valid {
		if dr.heapIndex >= 0 && dr.heapIndex < len(q.deadlines) {
			heap.Remove(&q.deadlines, dr.heapIndex)
		}
		delete(q.inflight, dr.ReceiptHandle)
		if msg, ok := q.messages[dr.MessageID]; ok {
			q.bytesInMem -= int64(len(msg.Body))
			delete(q.messages, dr.MessageID)
		}
	}
	q.metrics.inFlightCount.Add(-int64(len(valid)))
	q.metrics.totalAcked.Add(int64(len(valid)))
	q.metrics.ackCountWindow.Add(int64(len(valid)))

	return results
}

// ============================================================================
// Nack
// ============================================================================

func (qm *queueManager) Nack(ctx context.Context, queueID, receiptHandle, deliveryToken string) (MessageState, error) {
	q, err := qm.getQueue(queueID)
	if err != nil {
		return "", err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	dr, ok := q.inflight[receiptHandle]
	if !ok {
		return "", &ErrInvalidReceiptHandle{Reason: "receipt handle not found"}
	}
	if dr.DeliveryToken != deliveryToken {
		return "", &ErrDeliveryTokenMismatch{Expected: dr.DeliveryToken, Got: deliveryToken}
	}

	msg, ok := q.messages[dr.MessageID]
	if !ok {
		return "", errors.New("message not found")
	}

	// Remove from inflight and deadlines.
	if dr.heapIndex >= 0 && dr.heapIndex < len(q.deadlines) {
		heap.Remove(&q.deadlines, dr.heapIndex)
	}
	delete(q.inflight, receiptHandle)

	// Determine target state.
	var targetState MessageState
	if msg.MaxDeliveryCount > 0 && msg.DeliveryCount >= msg.MaxDeliveryCount {
		targetState = StateDead
		msg.State = StateDead
		q.dead[msg.ID] = msg
	} else {
		targetState = StateReady
		msg.State = StateReady
		msg.CurrentReceiptHandle = ""
		msg.CurrentDeliveryToken = ""
		msg.VisibilityDeadline = time.Time{}
		msg.readyElement = q.ready.PushBack(msg)
	}

	walEntryVal := walEntry{
		Op: opNack,
		Payload: walNackPayload{
			QueueID:        queueID,
			MessageID:      msg.ID,
			ReceiptHandle:  dr.ReceiptHandle,
			DeliveryToken:  dr.DeliveryToken,
			TargetState:    targetState,
			HasNewReadySeq: false,
			NewReadySeq:    0,
		},
	}
	if _, _, err := qm.wal.Append(ctx, []walEntry{walEntryVal}); err != nil {
		// Rollback: restore delivery record.
		q.inflight[receiptHandle] = dr
		dr.heapIndex = -1
		heap.Push(&q.deadlines, dr)
		if targetState == StateReady {
			q.ready.Remove(msg.readyElement)
			msg.readyElement = nil
			msg.State = StateInFlight
			msg.CurrentReceiptHandle = dr.ReceiptHandle
			msg.CurrentDeliveryToken = dr.DeliveryToken
			msg.VisibilityDeadline = dr.Deadline
		} else {
			delete(q.dead, msg.ID)
			msg.State = StateInFlight
		}
		return "", fmt.Errorf("wal append nack: %w", err)
	}

	q.metrics.inFlightCount.Add(-1)
	if targetState == StateReady {
		q.metrics.readyCount.Add(1)
		q.signalReady()
	} else {
		q.metrics.deadCount.Add(1)
	}

	return targetState, nil
}

// ============================================================================
// ReapExpired — deadline heap peek, not full scan
// ============================================================================

func (qm *queueManager) ReapExpired(ctx context.Context, now time.Time) []reapTransition {
	qm.mu.RLock()
	ids := make([]string, 0, len(qm.queues))
	for id := range qm.queues {
		ids = append(ids, id)
	}
	qm.mu.RUnlock()

	var allTransitions []reapTransition

	for _, queueID := range ids {
		q, err := qm.getQueue(queueID)
		if err != nil {
			continue
		}

		q.mu.Lock()
		var transitions []reapTransition
		var reaps []walReapedMessage

		// Collect expired deliveries without mutating state yet.
		type pendingReap struct {
			dr          *deliveryRecord
			msg         *messageRecord
			targetState MessageState
		}
		var pending []pendingReap

		for len(q.deadlines) > 0 && !q.deadlines[0].Deadline.After(now) {
			dr := heap.Pop(&q.deadlines).(*deliveryRecord)
			msg, ok := q.messages[dr.MessageID]
			if !ok {
				continue
			}
			if msg.State != StateInFlight {
				continue
			}
			if msg.CurrentDeliveryToken != dr.DeliveryToken {
				continue
			}
			if msg.VisibilityDeadline.After(now) {
				continue
			}

			var targetState MessageState
			if msg.MaxDeliveryCount > 0 && msg.DeliveryCount >= msg.MaxDeliveryCount {
				targetState = StateDead
			} else {
				targetState = StateReady
			}

			pending = append(pending, pendingReap{
				dr:          dr,
				msg:         msg,
				targetState: targetState,
			})

			reaps = append(reaps, walReapedMessage{
				MessageID:             msg.ID,
				PreviousDeliveryToken: dr.DeliveryToken,
				TargetState:           targetState,
				HasNewReadySeq:        false,
				NewReadySeq:           0,
			})
			transitions = append(transitions, reapTransition{QueueID: queueID, ToState: targetState})
		}

		if len(reaps) == 0 {
			// Nothing expired, but we may have popped stale entries from the heap.
			// Push any valid inflight entries back.
			q.deadlines = q.deadlines[:0]
			for _, dr := range q.inflight {
				dr.heapIndex = -1
				heap.Push(&q.deadlines, dr)
			}
			q.mu.Unlock()
			continue
		}

		entry := walEntry{
			Op: opReapBatch,
			Payload: walReapBatchPayload{
				QueueID: queueID,
				Reaps:   reaps,
			},
		}
		if _, _, err := qm.wal.Append(ctx, []walEntry{entry}); err != nil {
			// WAL failed — no mutations were applied. The delivery records
			// are still in q.inflight. Rebuild the heap from the inflight
			// map (which is unchanged).
			q.deadlines = q.deadlines[:0]
			for _, dr := range q.inflight {
				dr.heapIndex = -1
				heap.Push(&q.deadlines, dr)
			}
			q.mu.Unlock()
			continue
		}

		// WAL succeeded — apply all pending mutations.
		for _, p := range pending {
			delete(q.inflight, p.dr.ReceiptHandle)
			msg := p.msg
			if p.targetState == StateDead {
				msg.State = StateDead
				q.dead[msg.ID] = msg
			} else {
				msg.State = StateReady
				msg.CurrentReceiptHandle = ""
				msg.CurrentDeliveryToken = ""
				msg.VisibilityDeadline = time.Time{}
				msg.readyElement = q.ready.PushBack(msg)
			}
		}

		hasReadyTransition := false
		for _, tr := range transitions {
			q.metrics.inFlightCount.Add(-1)
			if tr.ToState == StateReady {
				q.metrics.readyCount.Add(1)
				hasReadyTransition = true
			} else {
				q.metrics.deadCount.Add(1)
			}
		}
		if hasReadyTransition {
			q.signalReady()
		}

		allTransitions = append(allTransitions, transitions...)
		q.mu.Unlock()
	}

	return allTransitions
}
