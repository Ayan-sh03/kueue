package main

import (
	"context"
	"log"
	"time"
)

type reapTransition struct {
	QueueID string
	ToState MessageState
}

// startReaper launches the background reaper goroutine that, every second,
// reaps expired in-flight messages, resets per-second ack windows, and runs
// WAL snapshot/compaction when thresholds are hit. The goroutine runs until
// ctx is cancelled; the returned done channel is closed when the goroutine
// has fully exited (any in-flight tick is allowed to finish before checking
// ctx, so callers can wait for a clean stop).
func startReaper(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runReaperTick(ctx)
			}
		}
	}()
	return done
}

// runReaperTick performs one reaper pass: reap expired deliveries, reset ack
// windows, and maybe snapshot+compact the WAL. It uses context.Background()
// for the mutating work so an in-flight tick is not interrupted mid-WAL-append
// by shutdown cancellation; only the loop control (the select above) observes
// ctx, so the current tick always runs to completion.
func runReaperTick(ctx context.Context) {
	now := time.Now()
	transitions := QueueManager.ReapExpired(context.Background(), now)

	signaled := map[string]struct{}{}
	for _, t := range transitions {
		if t.ToState == StateReady {
			if _, ok := signaled[t.QueueID]; !ok {
				signalQueueReady(t.QueueID)
				signaled[t.QueueID] = struct{}{}
			}
		}
	}

	metricsStore.Range(func(_, value any) bool {
		value.(*queueMetrics).resetAckWindow()
		return true
	})

	// Checkpoint + compact the WAL if the ops/seconds thresholds are
	// due. No-op when snapshots are disabled or the manager is backed
	// by a fake WAL (e.g. in tests).
	if _, err := QueueManager.maybeSnapshot(context.Background(), now); err != nil {
		log.Printf("snapshot: %v", err)
	}
}