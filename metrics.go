package main

import (
	"encoding/json"
	"errors"
	"log"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

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

	if _, err := QueueManager.getQueue(id); err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
			return
		}
		http.Error(w, "Error checking queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

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
