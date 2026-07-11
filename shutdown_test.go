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

// TestGracefulShutdownTimeoutReturns503 exercises the drain-timeout path: a
// handler still running past KUEUE_SHUTDOWN_TIMEOUT_SEC that reaches the WAL
// write path must get a clean 503 (its context was cancelled) rather than a
// 500 from writing to a closed Pebble. It also asserts run() still tears down
// cleanly and closes the DB only after the straggler has returned.
func TestGracefulShutdownTimeoutReturns503(t *testing.T) {
	setupTestDBNoClose(t)

	// Create the queue up front, while the DB is alive, so PublishBatch reaches
	// the WAL append (and thus the ctx.Err guard) instead of short-circuiting
	// on ErrQueueNotFound.
	queueID, err := QueueManager.CreateQueue(context.Background(), "drain-test", 3)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	mux := newRouter()
	startedCh := make(chan struct{})
	// /slowpublish blocks until its request context is cancelled — which only
	// happens once shutdown's drain deadline is exceeded and baseCancel fires —
	// then publishes on that cancelled context and lets respondPublishError map
	// the outcome to a status code.
	mux.HandleFunc("/slowpublish", func(w http.ResponseWriter, r *http.Request) {
		close(startedCh)
		<-r.Context().Done()
		_, err := QueueManager.PublishBatch(r.Context(), queueID, [][]byte{[]byte("x")})
		if err != nil {
			respondPublishError(w, err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &app{
		db:              Db,
		qm:              QueueManager,
		wal:             WAL,
		srv:             &http.Server{Handler: mux},
		ln:              ln,
		shutdownTimeout: 200 * time.Millisecond,
	}

	runErr := make(chan error, 1)
	go func() { runErr <- a.run(ctx) }()

	type result struct {
		resp *http.Response
		err  error
	}
	resCh := make(chan result, 1)
	client := &http.Client{Timeout: 10 * time.Second}
	go func() {
		resp, err := client.Get(fmt.Sprintf("http://%s/slowpublish", addr))
		resCh <- result{resp, err}
	}()

	select {
	case <-startedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("slowpublish handler never ran")
	}

	// Begin shutdown. The handler stays blocked past the 200ms drain budget, so
	// Shutdown times out, baseCancel fires, and the handler unblocks into the
	// cancelled-context publish.
	cancel()

	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("request error: %v", res.err)
		}
		defer res.resp.Body.Close()
		if res.resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503 (clean shutdown), not a closed-storage 500", res.resp.StatusCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slowpublish request did not complete")
	}

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after shutdown")
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