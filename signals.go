package main

import "sync"

var receiveChannel = make(chan struct{}, 1)

var queueReadyChans = map[string]chan struct{}{}

var queueReadyChansMu sync.Mutex

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
