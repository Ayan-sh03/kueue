package main

import (
	"context"
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
			transitions := QueueManager.ReapExpired(context.Background(), time.Now())

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
		}
	}()

}
