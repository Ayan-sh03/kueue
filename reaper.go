package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

type reapTransition struct {
	QueueID string
	ToState MessageState
}

func reapExpiredMessages(now time.Time) ([]reapTransition, error) {
	type expiredMsg struct {
		indexKey []byte
		msgKey   []byte
	}
	var expired []expiredMsg

	prefix := inflightPrefix()
	snap := Db.NewSnapshot()
	defer snap.Close()
	iter, _ := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	defer iter.Close()

	upper := inflightScanUpperBound(now)
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := append([]byte(nil), iter.Key()...)
		if len(key) >= len(upper) && bytes.Compare(key[:len(upper)], upper) > 0 {
			break
		}

		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}
		expired = append(expired, expiredMsg{
			indexKey: key,
			msgKey:   append([]byte(nil), val...),
		})
	}

	const reapBatch = 1024
	transitions := make([]reapTransition, 0, len(expired))

	for i := 0; i < len(expired); i += reapBatch {
		end := i + reapBatch
		if end > len(expired) {
			end = len(expired)
		}
		chunk := expired[i:end]

		batch := Db.NewIndexedBatch()
		for _, exp := range chunk {
			msgVal, closer, err := batch.Get(exp.msgKey)
			if err != nil {
				if err == pebble.ErrNotFound {
					_ = batch.Delete(exp.indexKey, nil)
					continue
				}
				batch.Close()
				return transitions, err
			}

			var msg Message
			if err := json.Unmarshal(msgVal, &msg); err != nil {
				closer.Close()
				batch.Close()
				return transitions, err
			}
			closer.Close()

			if msg.State != StateInFlight {
				_ = batch.Delete(exp.indexKey, nil)
				continue
			}
			if msg.VisibilityDeadline.IsZero() || now.Before(msg.VisibilityDeadline) {
				_ = batch.Delete(exp.indexKey, nil)
				continue
			}

			queueID, err := parseMessageKeyQueueID(exp.msgKey)
			if err != nil {
				batch.Close()
				return transitions, err
			}

			if msg.MaxDeliveryCount > 0 && msg.DeliveryCount >= msg.MaxDeliveryCount {
				msg.State = StateDead
			} else {
				msg.State = StateReady
			}
			msg.VisibilityDeadline = time.Time{}
			msg.DeliveryAttemptID = ""

			updated, err := json.Marshal(msg)
			if err != nil {
				batch.Close()
				return transitions, err
			}
			if err := batch.Set(exp.msgKey, updated, nil); err != nil {
				batch.Close()
				return transitions, err
			}
			_ = batch.Delete(exp.indexKey, nil)

			if msg.State == StateReady {
				newSeq, err := nextMessageSequence(queueID)
				if err != nil {
					batch.Close()
					return transitions, fmt.Errorf("allocate reaper sequence: %w", err)
				}
				if err := batch.Set(readyKey(queueID, newSeq, msg.ID), readyValue(exp.msgKey), nil); err != nil {
					batch.Close()
					return transitions, err
				}
			}

			transitions = append(transitions, reapTransition{QueueID: queueID, ToState: msg.State})
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			return transitions, err
		}
		batch.Close()
	}

	return transitions, nil
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
