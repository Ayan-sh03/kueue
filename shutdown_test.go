package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// setupTestDBNoClose mirrors setupTestDB but does not Close the db in cleanup
// — the graceful-shutdown tests drive teardown via the app, which closes
// Pebble itself. We only reset globals to nil after the test.
func setupTestDBNoClose(t *testing.T) {
	t.Helper()
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	Db = db
	metricsStore = sync.Map{}
	deliveryRecordSeq.Store(0)

	qm, wal, err := initQueueManagerFromEnv(context.Background(), db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("init queue manager: %v", err)
	}
	QueueManager = qm
	WAL = wal

	t.Cleanup(func() {
		Db = nil
		QueueManager = nil
		WAL = nil
	})
}

// TestGracefulShutdownDrain starts the server on an ephemeral port, issues an
// in-flight request that blocks in a handler, then triggers shutdown. It
// asserts that Shutdown waits for the in-flight handler to finish (the request
// completes with 200), that run returns without error, and that new connections
// are refused after teardown.
func TestGracefulShutdownDrain(t *testing.T) {
	setupTestDBNoClose(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	// Custom mux: real routes plus a /block route we control so we can hold
	// a request open long enough to observe Shutdown draining it.
	mux := newRouter()

	startedCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var handlerRan atomic.Bool
	mux.HandleFunc("/block", func(w http.ResponseWriter, r *http.Request) {
		close(startedCh)
		<-releaseCh
		handlerRan.Store(true)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "done")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &app{
		db:              Db,
		qm:              QueueManager,
		wal:             WAL,
		srv:             &http.Server{Handler: mux},
		ln:              ln,
		shutdownTimeout: 3 * time.Second,
	}

	runErr := make(chan error, 1)
	go func() { runErr <- a.run(ctx) }()

	// Issue the blocking request in its own goroutine; it will only complete
	// once we close releaseCh (which we do after shutdown begins).
	type result struct {
		resp *http.Response
		err  error
	}
	resCh := make(chan result, 1)
	client := &http.Client{Timeout: 10 * time.Second}
	go func() {
		resp, err := client.Get(fmt.Sprintf("http://%s/block", addr))
		resCh <- result{resp, err}
	}()

	// Wait until the handler is actually in-flight before triggering shutdown.
	select {
	case <-startedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking request never reached the handler")
	}

	// Trigger shutdown (cancels reaper ctx and begins srv.Shutdown), then
	// immediately release the handler so the drain completes promptly.
	cancel()
	close(releaseCh)

	// The in-flight request must complete successfully — Shutdown waited for it.
	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("in-flight request error: %v", res.err)
		}
		if res.resp.StatusCode != http.StatusOK {
			t.Fatalf("in-flight status = %d, want 200", res.resp.StatusCode)
		}
		res.resp.Body.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request did not complete within shutdown window")
	}
	if !handlerRan.Load() {
		t.Fatal("handler did not run to completion")
	}

	// run() must return cleanly.
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after shutdown")
	}

	// Reaper goroutine must have exited (done channel closed).
	select {
	case <-a.reaperDone:
	default:
		t.Fatal("reaper goroutine still running after shutdown")
	}

	// New connections must be refused now.
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err == nil {
		c.Close()
		t.Fatal("listener still accepting connections after shutdown")
	}
}

// TestShutdownNoServerStart verifies stopReaper is safe even when the reaper
// finished and the server was never started (covers the early-return branch).
func TestShutdownNoServerStart(t *testing.T) {
	setupTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &app{
		db:              nil, // no close needed
		shutdownTimeout: time.Second,
	}
	done := startReaper(ctx)
	a.reaperCancel = cancel
	a.reaperDone = done
	a.stopReaper()

	select {
	case <-done:
	default:
		t.Fatal("reaper did not stop")
	}
}