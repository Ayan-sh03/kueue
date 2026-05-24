package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
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

	err := json.NewDecoder(r.Body).Decode(&publishRequest)

	if err != nil {
		http.Error(w, "Bad Requst Error: "+err.Error(), http.StatusBadRequest)

	}

	queue := Queue{
		Id:         uuid.NewString(),
		Name:       publishRequest.Name,
		MaxRetries: publishRequest.MaxRetries,
	}

	err = func() error {
		batch := Db.NewIndexedBatch()
		defer batch.Close()
		config, err := json.Marshal(QueueConfig{Name: queue.Name, MaxRetries: queue.MaxRetries})
		if err != nil {
			return err
		}
		if err := batch.Set([]byte(queue.Id), config, nil); err != nil {
			return err
		}
		return batch.Commit(pebble.NoSync)
	}()
	if err != nil {
		http.Error(w, "Failed to create queue: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// 	Queues = append(Queues, queue)

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

	//get from db

	val, closer, err := Db.Get([]byte(id))
	if err != nil {
		if err == pebble.ErrNotFound {
			http.Error(w, "Queue Not Found for id: "+id, http.StatusNotFound)
			return
		}
		log.Println(err)
		http.Error(w, "Error retrieving queue: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer closer.Close()
	var config QueueConfig
	if err := json.Unmarshal(val, &config); err != nil {
		http.Error(w, "Error decoding queue config: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"id":   id,
		"name": config.Name,
	})

}
