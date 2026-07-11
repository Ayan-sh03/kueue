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
		_ = a.db.Close()
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
		log.Printf("http shutdown: %v", err)
	}

	a.stopReaper()

	if err := a.db.Close(); err != nil {
		return fmt.Errorf("close db: %w", err)
	}
	return nil
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