package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// app owns the server lifecycle: HTTP server, reaper goroutine, and Pebble
// close. main constructs one and calls run; tests construct one reusing the
// globals set by setupTestDB and call run with a cancellable context.
type app struct {
	db  *pebble.DB
	qm  *queueManager
	wal *walStore
	srv *http.Server
	ln  net.Listener // injected listener so tests can grab an ephemeral port

	shutdownTimeout time.Duration

	reaperCancel context.CancelFunc
	reaperDone   <-chan struct{}

	// baseCtx is the parent of every request context (installed via
	// srv.BaseContext). It is independent of the signal context so a SIGTERM
	// does not cancel in-flight requests immediately — they get the full drain
	// budget first. baseCancel is fired only when the drain deadline is
	// exceeded, to unwind stragglers cleanly before Pebble closes. inflight
	// tracks handlers so shutdown can wait for them to return after cancelling.
	baseCtx    context.Context
	baseCancel context.CancelFunc
	inflight   sync.WaitGroup
}

// newRouter builds the HTTP route table on a private ServeMux so the server
// does not depend on (or mutate) the global DefaultServeMux.
func newRouter() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/", queueHandler)
	mux.HandleFunc("/create", create)
	mux.HandleFunc("/get", getQueue)
	mux.HandleFunc("/publish", publish)
	mux.HandleFunc("/publish-batch", publishBatch)
	mux.HandleFunc("/ack", ack)
	mux.HandleFunc("/ack-batch", ackBatch)
	mux.HandleFunc("/nack", nack)
	mux.HandleFunc("/receive", receive)
	mux.HandleFunc("/receive-batch", receiveBatch)
	mux.HandleFunc("/metrics", metricsHandler)
	return mux
}

// run starts the reaper and HTTP server, then blocks until ctx is cancelled
// (by a signal handler) or the server fails on its own. On shutdown it drains
// HTTP, stops the reaper, and closes Pebble in that order. Returns a non-nil
// error only on hard server failure or Pebble close error; HTTP drain timeouts
// and reaper-stop timeouts are logged but do not change the exit code.
func (a *app) run(ctx context.Context) error {
	// baseCtx parents every request context but is deliberately rooted in
	// Background, not ctx, so signalling shutdown does not instantly cancel
	// in-flight handlers. shutdown() cancels it only after the drain budget is
	// spent. The defer covers the clean-exit paths where it is never fired.
	a.baseCtx, a.baseCancel = context.WithCancel(context.Background())
	defer a.baseCancel()
	a.srv.BaseContext = func(net.Listener) context.Context { return a.baseCtx }
	a.srv.Handler = a.trackInflight(a.srv.Handler)

	reaperCtx, cancel := context.WithCancel(ctx)
	a.reaperCancel = cancel
	a.reaperDone = startReaper(reaperCtx)

	errCh := make(chan error, 1)
	go func() {
		if a.ln != nil {
			errCh <- a.srv.Serve(a.ln)
		} else {
			errCh <- a.srv.ListenAndServe()
		}
	}()

	select {
	case <-ctx.Done():
		return a.shutdown()
	case err := <-errCh:
		// Server exited on its own (e.g. bind error). Tear the rest down.
		a.stopReaper()
		_ = a.closeStorage()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

// shutdown runs the ordered teardown: stop accepting + drain HTTP, stop the
// reaper, then close Pebble. Each stage is bounded by a.shutdownTimeout.
func (a *app) shutdown() error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.shutdownTimeout)
	defer cancel()

	if err := a.srv.Shutdown(shutdownCtx); err != nil {
		// Drain deadline hit with requests still in flight. Cancel their
		// contexts so the write path unwinds through walStore.Append's guards
		// (a clean 503) instead of racing db.Close, then wait — bounded — for
		// those handlers to return before we close storage.
		log.Printf("http shutdown: %v", err)
		if a.baseCancel != nil {
			a.baseCancel()
		}
		a.waitInflight(a.shutdownTimeout)
	}

	a.stopReaper()

	// closeStorage takes the walStore close gate, which waits for any in-flight
	// Append or reaper snapshot to finish and forces later ones to fail with
	// ErrStorageClosed. This is the hard guarantee behind the bounded drains
	// above: even if a straggler handler or a reaper tick outlived its timeout,
	// Pebble is never closed underneath live storage work.
	if err := a.closeStorage(); err != nil {
		return fmt.Errorf("close db: %w", err)
	}
	return nil
}

// closeStorage closes Pebble through the walStore close gate when a walStore is
// present, falling back to the raw handle otherwise (e.g. tests with a db-only
// app). Idempotent via walStore.Close.
func (a *app) closeStorage() error {
	if a.wal != nil {
		return a.wal.Close()
	}
	if a.db != nil {
		return a.db.Close()
	}
	return nil
}

// trackInflight wraps the handler so shutdown can wait for in-flight requests
// to return after cancelling their contexts.
func (a *app) trackInflight(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.inflight.Add(1)
		defer a.inflight.Done()
		next.ServeHTTP(w, r)
	})
}

// waitInflight blocks until every in-flight handler has returned or timeout
// elapses. The bound guarantees a handler that ignores its cancelled context
// can never wedge shutdown; if it fires we proceed to close storage anyway,
// and the walStore gate turns any late write into a clean ErrStorageClosed
// (503) rather than a closed-DB failure.
func (a *app) waitInflight(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		a.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		log.Println("in-flight requests did not drain within shutdown timeout after cancel")
	}
}

// stopReaper cancels the reaper context and waits for the goroutine to exit,
// bounded by the shutdown timeout so a stuck tick never blocks exit.
func (a *app) stopReaper() {
	if a.reaperCancel != nil {
		a.reaperCancel()
	}
	if a.reaperDone != nil {
		select {
		case <-a.reaperDone:
		case <-time.After(a.shutdownTimeout):
			log.Println("reaper did not stop within shutdown timeout")
		}
	}
}

func shutdownTimeoutFromEnv() time.Duration {
	s := os.Getenv("KUEUE_SHUTDOWN_TIMEOUT_SEC")
	if s == "" {
		return 10 * time.Second
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return 10 * time.Second
	}
	return time.Duration(v) * time.Second
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
	fmt.Println("DB initialised successfully")

	qm, wal, err := initQueueManagerFromEnv(context.Background(), db)
	if err != nil {
		_ = db.Close()
		log.Fatalf("recovery failed: %v", err)
	}
	QueueManager = qm
	WAL = wal
	fmt.Println("WAL replay complete")

	if profFile := os.Getenv("KUEUE_CPU_PROFILE"); profFile != "" {
		f, err := os.Create(profFile)
		if err != nil {
			_ = db.Close()
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = db.Close()
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
		log.Println("CPU profiling enabled, writing to", profFile)
	}

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		_ = db.Close()
		log.Fatalf("listen: %v", err)
	}

	a := &app{
		db:              db,
		qm:              qm,
		wal:             wal,
		srv:             &http.Server{Handler: newRouter()},
		ln:              ln,
		shutdownTimeout: shutdownTimeoutFromEnv(),
	}

	// NotifyContext cancels the main context on SIGINT/SIGTERM, which makes
	// run() enter its ordered shutdown path. SIGHUP is intentionally ignored.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Println("Producer Running on Port " + port)

	if err := a.run(ctx); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}