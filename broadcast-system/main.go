package main

import (
	"context"
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"maps"
	"os"
	"slices"
	"sync"
	"time"
)

var (
	messageHistory  map[float64]bool
	messageBatch    map[float64]bool
	topology        map[string][]string
	mu              sync.Mutex
	batchInProgress = false
	batchTimer      *time.Timer
	batchInterval   = 500 * time.Millisecond
)

func main() {
	n := maelstrom.NewNode()
	messageHistory = make(map[float64]bool)
	topology = make(map[string][]string)
	messageBatch = make(map[float64]bool)

	n.Handle("broadcast", broadcastBatchHandler(n))

	n.Handle("sync", func(msg maelstrom.Message) error {
		body := getBody(msg)

		mu.Lock()

		messagesSlice, _ := body["messages"].([]interface{})

		for _, msgAny := range messagesSlice {
			if msgVal, ok := msgAny.(float64); ok {
				messageHistory[msgVal] = true
			}
		}
		mu.Unlock()

		//return OK to sender
		delete(body, "messages")
		body["type"] = "sync_ok"

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		body := getBody(msg)

		body["type"] = "read_ok"
		body["messages"] = slices.Collect(maps.Keys(messageHistory))

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		body := getBody(msg)

		topology = make(map[string][]string)
		for key, val := range body["topology"].(map[string]interface{}) {
			nodes := val.([]interface{})
			strNodes := make([]string, len(nodes))
			for i, node := range nodes {
				strNodes[i] = node.(string)
			}
			topology[key] = strNodes
		}

		delete(body, "topology")
		body["type"] = "topology_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func broadcastHandler(n *maelstrom.Node) func(msg maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		body := getBody(msg)
		next := body["message"].(float64)

		//we only add and propagate if we haven't seen the value before
		mu.Lock()
		_, exists := messageHistory[next]
		if !exists {
			messageHistory[next] = true
		}
		mu.Unlock()

		if !exists && body["visited"] == nil {
			// let's propagate to all neighbours given by the topology handler
			// this only works if every node is in the same connected component
			for _, id := range n.NodeIDs() {

				go func() {
					retryAmt := 0
					//SyncRPC?
					for retryAmt < 10 {
						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						defer cancel()

						_, err := n.SyncRPC(ctx, id, map[string]any{
							"type":    "broadcast",
							"message": next,
							"visited": "true",
						})

						if err == nil {
							break
						}

						//appendToLogFile(ErrorLog{
						//	id,
						//	retryAmt,
						//	"error: " + err.Error(),
						//})
						retryAmt++
					}
				}()
			}
		}

		delete(body, "message")
		body["type"] = "broadcast_ok"

		return n.Reply(msg, body)
	}
}

func broadcastBatchHandler(n *maelstrom.Node) func(msg maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		body := getBody(msg)
		next := body["message"].(float64)

		mu.Lock()
		_, exists := messageHistory[next]
		if !exists {
			messageHistory[next] = true

			// If it's a new message and not a propagation (no visited flag) add it to our batch
			if body["visited"] == nil {
				// Add to batch
				messageBatch[next] = true

				// Start a new batch timer if one isn't already running
				if !batchInProgress {
					batchInProgress = true

					// Create and start the timer
					batchTimer = time.AfterFunc(batchInterval, func() {
						processBatch(n)
					})
				}
			}
		}
		mu.Unlock()

		delete(body, "message")
		body["type"] = "broadcast_ok"

		return n.Reply(msg, body)
	}
}

func processBatch(n *maelstrom.Node) {
	mu.Lock()
	//defer mu.Unlock() MIGHT NOT BE CORRECT

	// Skip if batch is empty
	if len(messageBatch) == 0 {
		batchInProgress = false
		return
	}

	// Copy the current batch and clear it for the next round
	currentBatch := slices.Collect(maps.Keys(messageBatch))
	messageBatch = make(map[float64]bool)
	batchInProgress = false

	mu.Unlock()

	// Propagate the batch to all neighbors
	for _, id := range n.NodeIDs() {
		go func(nodeID string, batch []float64) {
			retryAmt := 0
			for retryAmt < 5 {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

				// Send the entire batch in one request
				_, err := n.SyncRPC(ctx, nodeID, map[string]any{
					"type":     "sync",
					"messages": batch,
					"visited":  "true",
				})

				cancel() // Important to prevent context leak

				if err == nil {
					break
				}

				retryAmt++
				time.Sleep(100 * time.Millisecond) // Small delay between retries
			}
		}(id, currentBatch)
	}
}

func getBody(msg maelstrom.Message) map[string]any {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Fatal(err)
	}
	return body
}

type ErrorLog struct {
	Dest    string
	Retries int
	Error   string
}

type MessageBody struct {
	Type    string  `json:"type"`
	Message float64 `json:"message"`
}

func appendToLogFile(object any) {
	// Open file in append mode, create if doesn't exist
	file, err := os.OpenFile("/home/cameron/Documents/LearningProjects/GossipGlomers/broadcast-system/log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer file.Close()

	// Marshal the map to JSON
	data, err := json.MarshalIndent(object, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	// Write to file with newline
	_, err = fmt.Fprintln(file, string(data))
	if err != nil {
		log.Fatal(err)
	}
}
