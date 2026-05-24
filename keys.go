package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

var messageKeyCache sync.Map

func cacheMessageKey(receiptHandle string, key []byte) {
	messageKeyCache.Store(receiptHandle, append([]byte(nil), key...))
}

func getCachedMessageKey(receiptHandle string) ([]byte, bool) {
	val, ok := messageKeyCache.Load(receiptHandle)
	if !ok {
		return nil, false
	}
	return append([]byte(nil), val.([]byte)...), true
}

func deleteCachedMessageKey(receiptHandle string) {
	messageKeyCache.Delete(receiptHandle)
}

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

func messageKeyBytes(queueID string, seq uint64, messageID []byte) []byte {
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

func readyValue(msgKey []byte) []byte {
	return msgKey
}

func parseReadyValue(val []byte) ([]byte, error) {
	if len(val) == 0 {
		return nil, fmt.Errorf("invalid ready value: empty")
	}
	return append([]byte(nil), val...), nil
}

func receiptHandleForMessageKey(key []byte) string {
	return base64.RawURLEncoding.EncodeToString(key)
}

func messageKeyFromReceiptHandle(queueID, receiptHandle string) ([]byte, error) {
	if receiptHandle == "" {
		return nil, &ErrInvalidReceiptHandle{Reason: "receiptHandle is required"}
	}

	if key, ok := getCachedMessageKey(receiptHandle); ok {
		if !bytes.HasPrefix(key, queueMessagePrefix(queueID)) {
			return nil, &ErrInvalidReceiptHandle{Reason: "queue mismatch"}
		}
		if _, err := parseMessageKeySeq(key); err != nil {
			return nil, &ErrInvalidReceiptHandle{Reason: err.Error()}
		}
		return key, nil
	}

	key, err := base64.RawURLEncoding.DecodeString(receiptHandle)
	if err != nil {
		return nil, &ErrInvalidReceiptHandle{Reason: "base64 decode failed"}
	}
	if !bytes.HasPrefix(key, queueMessagePrefix(queueID)) {
		return nil, &ErrInvalidReceiptHandle{Reason: "queue mismatch"}
	}
	if _, err := parseMessageKeySeq(key); err != nil {
		return nil, &ErrInvalidReceiptHandle{Reason: err.Error()}
	}
	cacheMessageKey(receiptHandle, key)
	return key, nil
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

func inflightScanUpperBound(now time.Time) []byte {
	prefix := inflightPrefix()
	key := make([]byte, 0, len(prefix)+8)
	key = append(key, prefix...)

	var deadlineBytes [8]byte
	binary.BigEndian.PutUint64(deadlineBytes[:], uint64(now.UnixNano()))
	key = append(key, deadlineBytes[:]...)
	return key
}

func setInflightIndex(batch *pebble.Batch, queueID string, msg Message, msgKey []byte) error {
	if msg.VisibilityDeadline.IsZero() {
		return nil
	}
	return batch.Set(inflightKey(queueID, msg.VisibilityDeadline, msg.ID), msgKey, nil)
}

func deleteInflightIndex(batch *pebble.Batch, queueID string, msg Message) error {
	if msg.VisibilityDeadline.IsZero() || msg.ID == "" {
		return nil
	}
	err := batch.Delete(inflightKey(queueID, msg.VisibilityDeadline, msg.ID), nil)
	if err == pebble.ErrNotFound {
		return nil
	}
	return err
}

func parseMessageKeySeq(key []byte) (uint64, error) {
	idx := bytes.IndexByte(key, '|')
	if idx == -1 {
		return 0, fmt.Errorf("invalid message key format: no delimiter")
	}
	seqStart := idx + 1
	seqEnd := seqStart + 8
	if seqEnd > len(key) {
		return 0, fmt.Errorf("invalid message key format: seq too short")
	}
	return binary.BigEndian.Uint64(key[seqStart:seqEnd]), nil
}

func parseMessageKeyQueueID(key []byte) (string, error) {
	idx := bytes.IndexByte(key, '|')
	if idx == -1 {
		return "", fmt.Errorf("invalid message key format: no delimiter")
	}
	if idx == 0 {
		return "", fmt.Errorf("invalid message key format: empty queue id")
	}
	return string(key[:idx]), nil
}

func readyPartsFromKey(key, prefix []byte) (uint64, []byte, error) {
	if !bytes.HasPrefix(key, prefix) {
		return 0, nil, fmt.Errorf("ready key does not match prefix")
	}
	rest := key[len(prefix):]
	if len(rest) < 9 {
		return 0, nil, fmt.Errorf("invalid ready key: too short")
	}
	if rest[8] != '|' {
		return 0, nil, fmt.Errorf("invalid ready key: missing delimiter after seq")
	}
	if len(rest[9:]) == 0 {
		return 0, nil, fmt.Errorf("invalid ready key: missing message id")
	}
	return binary.BigEndian.Uint64(rest[:8]), rest[9:], nil
}
