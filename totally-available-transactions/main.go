package main

import (
	"context"
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"os"
	"sync"
	"time"
)

var db map[int]int
var mu *sync.Mutex

// GO ROUTINES ARE FIFO
// db.go where we accept transactions on a channel
// how do we want to sync outbound?

func main() {
	n := maelstrom.NewNode()
	db = make(map[int]int)
	mu = &sync.Mutex{}

	n.Handle("txn", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		body := getBody(msg)
		txn := body["txn"].([]any)

		handleTransaction(txn)
		//sync all nodes
		for _, neighbour := range n.NodeIDs() {
			if neighbour == n.ID() {
				continue
			}

			//need a new body (copy) to avoid memory overwriting from other threads
			syncBody := map[string]any{"type": "sync", "txn": txn}

			go func() {
				for i := range 10 {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)

					_, err := n.SyncRPC(ctx, neighbour, syncBody)

					if err == nil {
						cancel()
						break
					} else {
						if i > 6 {
							appendToLogFile(struct {
								Retry int
								Err   string
							}{
								Retry: i,
								Err:   err.Error(),
							})
						}
					}
					cancel()
				}
			}()
		}

		body["type"] = "txn_ok"
		return n.Reply(msg, body)
	})

	n.Handle("sync", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		body := getBody(msg)
		txn := body["txn"].([]any)

		handleTransaction(txn)

		body["type"] = "sync_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func handleTransaction(txn []any) {
	for i := range txn {
		transaction := txn[i].([]any)
		operation := transaction[0].(string)
		key := int(transaction[1].(float64))

		if operation == "r" {
			//handle read
			val, ok := db[key]

			if ok {
				transaction[2] = val
			}
		} else {
			//handle write
			val := int(transaction[2].(float64))
			db[key] = val
		}
	}
}

// -------  UTILS  -------
//txn entry ["r", 1 (key), null] OR ["w", 1 (key), 3 (val)]

func getBody(msg maelstrom.Message) map[string]any {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Fatal(err)
	}
	return body
}

func appendToLogFile(object any) {
	// Open file in append mode, create if doesn't exist
	file, err := os.OpenFile("/home/cameron/Documents/LearningProjects/GossipGlomers/totally-available-transactions/log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer file.Close()

	// Marshal the map to JSON
	data, err := json.MarshalIndent(object, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	_, err = fmt.Fprintln(file, string(data))
	if err != nil {
		log.Fatal(err)
	}
}
