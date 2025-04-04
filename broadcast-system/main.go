package main

import (
	"context"
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"os"
	"slices"
	"sync"
	"time"
)

var messageHistory []float64
var topology map[string][]string
var mu sync.Mutex

func main() {
	n := maelstrom.NewNode()
	messageHistory = make([]float64, 0)
	topology = make(map[string][]string)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		body := getBody(msg)
		next := body["message"].(float64)

		//we only add and propagate if we haven't seen the value before
		mu.Lock()
		newMessage := !slices.Contains(messageHistory, next)
		if newMessage {
			messageHistory = append(messageHistory, next)
		}
		mu.Unlock()

		if newMessage {
			// let's propagate to all neighbours given by the topology handler
			// this only works if every node is in the same connected component
			for _, id := range topology[n.ID()] {
				go func() {
					retryAmt := 0
					//SyncRPC?
					for retryAmt < 10 {
						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						defer cancel()

						_, err := n.SyncRPC(ctx, id, map[string]any{
							"type":    "broadcast",
							"message": next,
						})

						if err == nil {
							break
						}

						appendToLogFile(ErrorLog{
							id,
							retryAmt,
							"error: " + err.Error(),
						})
						retryAmt++
					}
				}()
			}
		}

		delete(body, "message")
		body["type"] = "broadcast_ok"

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		body := getBody(msg)

		body["type"] = "read_ok"
		body["messages"] = messageHistory

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
		//appendToLogFile(topology)

		delete(body, "topology")
		body["type"] = "topology_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
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
