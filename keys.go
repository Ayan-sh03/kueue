package main

import (
	"encoding/binary"
	"time"
)

func queueMessagePrefix(queueID string) []byte {
	return []byte(queueID + "|")
}

func messageKey(queueID string, seq uint64, messageID string) []byte {
	prefix := queueMessagePrefix(queueID)
	key := make([]byte, 0, len(prefix)+8+1+len(messageID))
	key = append(key, prefix...)

	var seqBytes [8]byte
	binary.BigEndian.PutUint64(seqBytes[:], seq)
	key = append(key, seqBytes[:]...)
	key = append(key, '|')
	key = append(key, messageID...)

	return key
}

func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix)+1)
	copy(upper, prefix)
	upper[len(prefix)] = 0xFF
	return upper
}

const readyKeySep = "|"

func readyKey(queueID string, seq uint64, messageID string) []byte {
	prefix := readyPrefix(queueID)
	key := make([]byte, 0, len(prefix)+8+1+len(messageID))
	key = append(key, prefix...)
	var seqBytes [8]byte
	binary.BigEndian.PutUint64(seqBytes[:], seq)
	key = append(key, seqBytes[:]...)
	key = append(key, '|')
	key = append(key, messageID...)
	return key
}

func readyPrefix(queueID string) []byte {
	return []byte("ready|" + queueID + readyKeySep)
}

func inflightPrefix() []byte {
	return []byte("inflight|")
}

func inflightKey(queueID string, deadline time.Time, messageID string) []byte {
	prefix := inflightPrefix()
	key := make([]byte, 0, len(prefix)+8+1+len(queueID)+1+len(messageID))
	key = append(key, prefix...)

	var deadlineBytes [8]byte
	binary.BigEndian.PutUint64(deadlineBytes[:], uint64(deadline.UnixNano()))
	key = append(key, deadlineBytes[:]...)
	key = append(key, '|')
	key = append(key, queueID...)
	key = append(key, '|')
	key = append(key, messageID...)
	return key
}
