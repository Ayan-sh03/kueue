package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime/pprof"

	"github.com/cockroachdb/pebble/v2"
)

func main() {
	dbPath := os.Getenv("KUEUE_DB_PATH")
	if dbPath == "" {
		dbPath = "./tmp/pebble"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		fmt.Println("Error opening Pebble:", err)
		return
	}
	Db = db
	defer Db.Close()
	fmt.Println("DB initialised successfully")
	http.HandleFunc("/", queueHandler)
	http.HandleFunc("/create", create)
	http.HandleFunc("/get", getQueue)
	http.HandleFunc("/publish", publish)
	http.HandleFunc("/publish-batch", publishBatch)
	http.HandleFunc("/ack", ack)
	http.HandleFunc("/ack-batch", ackBatch)
	http.HandleFunc("/nack", nack)
	http.HandleFunc("/receive", receive)
	http.HandleFunc("/receive-batch", receiveBatch)
	http.HandleFunc("/metrics", metricsHandler)

	reaper()
	fmt.Println("Producer Running on Port " + port)

	if profFile := os.Getenv("KUEUE_CPU_PROFILE"); profFile != "" {
		f, err := os.Create(profFile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
		log.Println("CPU profiling enabled, writing to", profFile)
	}

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("server failed: %v", err)
	}

}
