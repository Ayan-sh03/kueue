package main

// Long-poll ready signaling is per-queue, living in the queueRuntime. These
// helpers resolve the queue and delegate, so there is no global map or mutex on
// the publish/nack/reap hot paths.

func queueReadyChan(queueID string) <-chan struct{} {
	q, err := QueueManager.getQueue(queueID)
	if err != nil {
		// Unknown queue: hand back an already-closed channel so any waiter wakes
		// immediately and re-checks (it will then observe the queue is gone).
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return q.waitChan()
}

func signalQueueReady(queueID string) {
	if q, err := QueueManager.getQueue(queueID); err == nil {
		q.notify()
	}
}
