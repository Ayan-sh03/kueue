package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

func queueHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	fmt.Fprintln(w, "Hello Consumer")

}

func create(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	var publishRequest CreateRequest

	if err := json.NewDecoder(r.Body).Decode(&publishRequest); err != nil {
		http.Error(w, "Bad Requst Error: "+err.Error(), http.StatusBadRequest)
		return
	}

	queueID, err := QueueManager.CreateQueue(r.Context(), publishRequest.Name, publishRequest.MaxRetries)
	if err != nil {
		http.Error(w, "Failed to create queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"id":    queueID,
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
		return
	}

	q, err := QueueManager.getQueue(id)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
			return
		}
		http.Error(w, "Error retrieving queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"id":   id,
		"name": q.config.Name,
	})
}
