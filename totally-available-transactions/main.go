package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
	"time"
)

var db map[int]int
var mu *sync.Mutex

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

			// need a new body (copy) to avoid memory overwriting from other threads
			syncBody := map[string]any{"type": "sync", "txn": txn}

			// split sync RPC into a go func, we want to release lock immediately to avoid inter-node deadlock
			// we only need read-commited consistency so ordering/timing can be off
			go rpcWithRetries(n, neighbour, syncBody)
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

func rpcWithRetries(n *maelstrom.Node, neighbour string, syncBody map[string]any) {
	for range 10 { // max retries
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		_, err := n.SyncRPC(ctx, neighbour, syncBody)

		if err == nil {
			cancel()
			break
		}
		cancel()
	}
}

// txn[i] = ["r", 1 (key), null] OR ["w", 1 (key), 3 (val)]
func handleTransaction(txn []any) {
	for i := range txn {
		transaction := txn[i].([]any)
		operation := transaction[0].(string)
		key := int(transaction[1].(float64))

		if operation == "r" { //handle read
			val, ok := db[key]

			if ok {
				transaction[2] = val
			}
		} else { //handle write
			val := int(transaction[2].(float64))
			db[key] = val
		}
	}
}

func getBody(msg maelstrom.Message) map[string]any {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Fatal(err)
	}
	return body
}
