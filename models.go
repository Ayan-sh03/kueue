package main

import "time"

var queue []int

type MessageState string

const (
	StateReady    MessageState = "ready"
	StateInFlight MessageState = "in_flight"
	StateDead     MessageState = "dead"
)

type Message struct {
	ID                 string       `json:"id"`
	Body               []byte       `json:"body"`
	State              MessageState `json:"state"`
	EnqueuedAt         time.Time    `json:"enqueuedAt"`
	DeliveryCount      int          `json:"deliveryCount"`
	MaxDeliveryCount   int          `json:"maxDeliveryCount"`
	VisibilityDeadline time.Time    `json:"visibilityDeadline"`
	DeliveryAttemptID  string       `json:"deliveryAttemptId"`
}

type claimedMessage struct {
	Message
	ReceiptHandle string `json:"receiptHandle"`
}

type QueueConfig struct {
	Name       string `json:"name"`
	MaxRetries int    `json:"maxRetries"`
}

type Queue struct {
	Id         string    `json:"id"`
	Name       string    `json:"name"`
	Messages   []Message `json:"messages"`
	MaxRetries int       `json:"maxRetries"`
}

var Queues []Queue

var DeadLetterQueue []Message

type PublishRequest struct {
	Message Message `json:"message"`
	QueueId string  `json:"queueId"`
}

type CreateRequest struct {
	Name       string `json:"name"`
	MaxRetries int    `json:"maxRetries"`
}

type AckRequest struct {
	MessageId     string `json:"messageId,omitempty"`
	QueueId       string `json:"queueId"`
	ReceiptHandle string `json:"receiptHandle"`
	DeliveryToken string `json:"deliveryToken"`
}

type AckEntry struct {
	MessageId     string `json:"messageId,omitempty"`
	ReceiptHandle string `json:"receiptHandle"`
	DeliveryToken string `json:"deliveryToken"`
}

type BatchAckRequest struct {
	QueueId string     `json:"queueId"`
	Acks    []AckEntry `json:"acks"`
}

type BatchPublishRequest struct {
	Messages []Message `json:"messages"`
	QueueId  string    `json:"queueId"`
}

type BatchPublishResponse struct {
	IDs []string `json:"ids"`
}

type batchReceiveResponse struct {
	Messages []batchReceiveMessage `json:"messages"`
}

type batchReceiveMessage struct {
	ID            string       `json:"id"`
	Body          []byte       `json:"body"`
	State         MessageState `json:"state"`
	DeliveryToken string       `json:"deliveryToken"`
	ReceiptHandle string       `json:"receiptHandle"`
}
