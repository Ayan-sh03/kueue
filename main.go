package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

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
	VisibilityDeadline time.Time    `json:"visibilityDeadline"`
}

type Queue struct {
	Id         string    `json:"id"`
	Name       string    `json:"name"`
	Messages   []Message `json:"messages"`
	MaxRetries int       `json:"maxRetries"`
}

var Queues []Queue
var DeadLetterQueue []Message

func queueHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	fmt.Fprintln(w, "Hello Consumer")

}

type PublishRequest struct {
	Message Message `json:"message"`
	QueueId string  `json:"queueId"`
}

type CreateRequest struct {
	Name       string `json:"name"`
	MaxRetries int    `json:"maxRetries"`
}

type AckRequest struct {
	MessageId string `json:"messageId"`
	QueueId   string `json:"queueId"`
}

func create(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	var publishRequest CreateRequest

	err := json.NewDecoder(r.Body).Decode(&publishRequest)

	if err != nil {
		http.Error(w, "Bad Requst Error: "+err.Error(), http.StatusBadRequest)

	}

	queue := Queue{
		Id:         uuid.NewString(),
		Name:       publishRequest.Name,
		MaxRetries: publishRequest.MaxRetries,
	}

	Queues = append(Queues, queue)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"id":    queue.Id,
		"state": StateReady,
	})
}

func getQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	params := r.URL.Query()
	id := params.Get("id")

	if id == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{
			"Error": "id is required in the url",
		})
	}

	for _, q := range Queues {
		if q.Id == id {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]any{
				"id":   id,
				"name": q.Name,
			})
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	json.NewEncoder(w).Encode(map[string]any{
		"Error": "Queue Not Found for id: " + id,
	})

}

func publish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	// publish should , 1. take task 2. put it in queue 3. return id
	var message PublishRequest

	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		http.Error(w, "Bad Request : "+err.Error(), http.StatusBadRequest)
	}

	queueId := message.QueueId
	var queue *Queue
	for i, q := range Queues {
		if q.Id == queueId {
			queue = &Queues[i]
			break
		}

	}
	if queue == nil {
		http.Error(w, "Queue Not Found for id: "+queueId, http.StatusNotFound)
		return
	}

	// push to queue and return id
	message.Message.ID = uuid.NewString()
	message.Message.State = StateReady
	message.Message.EnqueuedAt = time.Now()
	queue.Messages = append(queue.Messages, message.Message)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"id":    message.Message.ID,
		"state": StateReady,
	})

}

func receive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	// get the queue id from url
	params := r.URL.Query()
	id := params.Get("id")
	if id == "" {
		http.Error(w, "id is required in the url", http.StatusBadRequest)
		return
	}

	// find queue from the id
	var queue *Queue
	for i, q := range Queues {
		if q.Id == id {
			queue = &Queues[i]
			break
		}
	}

	if queue == nil {
		http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
		return
	}

	// return the first message in readyState
	for i, msg := range queue.Messages {
		if msg.State == StateReady {
			// update the message state to in-flight and set visibility deadline
			queue.Messages[i].State = StateInFlight
			queue.Messages[i].VisibilityDeadline = time.Now().Add(30 * time.Second)
			queue.Messages[i].DeliveryCount += 1
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]any{
				"id":    msg.ID,
				"state": StateInFlight,
			})
			return
		}
	}

	http.Error(w, "No Ready Messages in Queue: "+id, http.StatusNotFound)

}

func ack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	var ackReq AckRequest
	err := json.NewDecoder(r.Body).Decode(&ackReq)
	if err != nil {
		http.Error(w, "Bad Request: "+err.Error(), http.StatusBadRequest)
		return
	}

	if ackReq.MessageId == "" {
		http.Error(w, "messageId is required", http.StatusBadRequest)
		return
	}
	if ackReq.QueueId == "" {
		http.Error(w, "queueId is required", http.StatusBadRequest)
		return
	}

	// find queue from the id
	var queue *Queue
	for i, q := range Queues {
		if q.Id == ackReq.QueueId {
			queue = &Queues[i]
			break
		}
	}

	if queue == nil {
		http.Error(w, "Queue Not Found for id: "+ackReq.QueueId, http.StatusNotFound)
		return
	}
	// find the message in the queue and remove it
	for i, msg := range queue.Messages {
		if msg.ID == ackReq.MessageId {
			// remove the message from the queue
			queue.Messages = append(queue.Messages[:i], queue.Messages[i+1:]...)
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"message": "Message Acknowledged and removed from queue",
	})

}

func nack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	var ackReq AckRequest
	err := json.NewDecoder(r.Body).Decode(&ackReq)
	if err != nil {
		http.Error(w, "Bad Request: "+err.Error(), http.StatusBadRequest)
		return
	}

	if ackReq.MessageId == "" {
		http.Error(w, "messageId is required", http.StatusBadRequest)
		return
	}
	if ackReq.QueueId == "" {
		http.Error(w, "queueId is required", http.StatusBadRequest)
		return
	}

	// find queue from the id
	var queue *Queue
	for i, q := range Queues {
		if q.Id == ackReq.QueueId {
			queue = &Queues[i]
			break
		}
	}

	if queue == nil {
		http.Error(w, "Queue Not Found for id: "+ackReq.QueueId, http.StatusNotFound)
		return
	}

	// find the message in the queue and update its state to ready
	for i, msg := range queue.Messages {
		if msg.ID == ackReq.MessageId {
			queue.Messages[i].State = StateReady
			break
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"message": "Message Nacked and state updated to ready",
	})

}

// runs every second and checks for each queue and each messsage that if now > visibility deadline and message is in-flight then change the state to ready if delivery count < max retries else change the state to dead
func reaper() {

	go func() {
		for {
			time.Sleep(1 * time.Second)
			for i, q := range Queues {
				for j, msg := range q.Messages {
					if msg.State == StateInFlight && time.Now().After(msg.VisibilityDeadline) {
						if msg.DeliveryCount < q.MaxRetries {
							Queues[i].Messages[j].State = StateReady
						} else {
							Queues[i].Messages[j].State = StateDead
							DeadLetterQueue = append(DeadLetterQueue, msg)
						}
					}
				}
			}
		}
	}()

}

func main() {
	http.HandleFunc("/", queueHandler)
	http.HandleFunc("/create", create)
	http.HandleFunc("/get", getQueue)
	http.HandleFunc("/publish", publish)
	http.HandleFunc("/ack", ack)
	http.HandleFunc("/nack", nack)
	http.HandleFunc("/receive", receive)

	reaper()
	fmt.Println("Producer Running on Port 8080")

	http.ListenAndServe(":8080", nil)

}
