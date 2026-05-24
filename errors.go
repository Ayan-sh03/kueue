package main

import (
	"errors"
	"fmt"
)

type ErrDeliveryTokenMismatch struct {
	Expected string
	Got      string
}

func (e *ErrDeliveryTokenMismatch) Error() string {
	return fmt.Sprintf("delivery token mismatch: expected %q, got %q", e.Expected, e.Got)
}

type ErrInvalidReceiptHandle struct {
	Reason string
}

func (e *ErrInvalidReceiptHandle) Error() string {
	if e.Reason == "" {
		return "invalid receipt handle"
	}
	return "invalid receipt handle: " + e.Reason
}

var ErrNoReadyMessages = errors.New("no ready messages")

var ErrMessageNotInFlight = errors.New("message is not in flight")
