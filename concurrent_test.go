package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestConcurrentClaimNoConflicts(t *testing.T) {
	setupTestDB(t)

	queueID := createTestQueue(t, "concurrent-claim")
	const numMessages = 200

	for i := 0; i < numMessages; i++ {
		publishTestMessage(t, queueID, []byte(fmt.Sprintf("msg-%d", i)))
	}

	const goroutines = 5
	var wg sync.WaitGroup
	claimedIDs := sync.Map{}

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msgs, err := claimReadyMessages(queueID, 10)
				if err == ErrNoReadyMessages {
					return
				}
				if err != nil {
					return
				}
				for _, m := range msgs {
					if _, loaded := claimedIDs.LoadOrStore(m.ID, true); loaded {
						t.Errorf("duplicate claim: message %s claimed twice", m.ID)
					}
				}
				// Brief stagger between claims so other goroutines can win
				time.Sleep(time.Millisecond)
			}
		}()
	}
	wg.Wait()

	count := 0
	claimedIDs.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	if count != numMessages {
		t.Errorf("claimed %d of %d messages", count, numMessages)
	}
}
