package main

import (
	"encoding/binary"
	"encoding/json"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

var Db *pebble.DB

func queueSequenceKey(queueID string) []byte {
	return []byte("seq:" + queueID)
}

var seqMu sync.Mutex

func nextMessageSequence(queueID string) (uint64, error) {
	seqMu.Lock()
	defer seqMu.Unlock()

	key := queueSequenceKey(queueID)
	val, closer, err := Db.Get(key)
	var current uint64
	if err == pebble.ErrNotFound {
		current = 0
	} else if err != nil {
		return 0, err
	} else {
		current = binary.BigEndian.Uint64(val)
		closer.Close()
	}
	next := current + 1
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], next)
	if err := Db.Set(key, buf[:], pebble.NoSync); err != nil {
		return 0, err
	}
	return next, nil
}

func nextMessageSequenceN(queueID string, n int) ([]uint64, error) {
	if n <= 0 {
		return nil, nil
	}
	seqMu.Lock()
	defer seqMu.Unlock()

	key := queueSequenceKey(queueID)
	val, closer, err := Db.Get(key)
	var current uint64
	if err == pebble.ErrNotFound {
		current = 0
	} else if err != nil {
		return nil, err
	} else {
		current = binary.BigEndian.Uint64(val)
		closer.Close()
	}

	seqs := make([]uint64, n)
	for i := 0; i < n; i++ {
		current++
		seqs[i] = current
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], current)
	if err := Db.Set(key, buf[:], pebble.NoSync); err != nil {
		return nil, err
	}
	return seqs, nil
}

func findMessageRecord(queueID, messageID string) ([]byte, *Message, error) {
	prefix := queueMessagePrefix(queueID)
	iter, _ := Db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, nil, err
		}
		var msg Message
		if err := json.Unmarshal(val, &msg); err != nil {
			return nil, nil, err
		}
		if msg.ID == messageID {
			return append([]byte(nil), iter.Key()...), &msg, nil
		}
	}

	return nil, nil, pebble.ErrNotFound
}

func messageByReceiptHandle(batch *pebble.Batch, queueID, receiptHandle string) ([]byte, *Message, error) {
	key, err := messageKeyFromReceiptHandle(queueID, receiptHandle)
	if err != nil {
		return nil, nil, err
	}

	val, closer, err := batch.Get(key)
	if err != nil {
		return nil, nil, err
	}
	defer closer.Close()

	var msg Message
	if err := json.Unmarshal(val, &msg); err != nil {
		return nil, nil, err
	}

	return key, &msg, nil
}
