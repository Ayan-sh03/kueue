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

func reaper() {

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
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
	}()

}
