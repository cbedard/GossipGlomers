package main

import (
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"os"
	"slices"
	"sync"
)

var messageHistory []float64
var topology map[string][]string
var mu sync.Mutex

//mu lock??

func main() {
	n := maelstrom.NewNode()
	messageHistory = make([]float64, 0)
	topology = make(map[string][]string)

	n.Handle("sync", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		next := body["message"].(float64)
		//we only propogate if we havent seen it
		mu.Lock()
		if !slices.Contains(messageHistory, next) {
			messageHistory = append(messageHistory, next)

			//lets propagate this message to our all neighbours!
			neighbours := n.NodeIDs()
			for _, id := range neighbours {

				//requestJSON, _ := json.Marshal(req)
				//appendTopologyToFile(neighbours)

				n.RPC(id, map[string]any{
					"type":    "broadcast",
					"message": next,
				}, func(msg maelstrom.Message) error {
					return nil
				})
			}
		}
		mu.Unlock()

		delete(body, "message")
		body["type"] = "broadcast_ok"

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = messageHistory

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology = make(map[string][]string)
		for key, val := range body["topology"].(map[string]interface{}) {
			nodes := val.([]interface{})
			strNodes := make([]string, len(nodes))
			for i, node := range nodes {
				strNodes[i] = node.(string)
			}
			topology[key] = strNodes
		}
		_ = appendTopologyToFile(topology)

		delete(body, "topology")
		body["type"] = "topology_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type MessageBody struct {
	Type    string  `json:"type"`
	Message float64 `json:"message"`
}

type BroadcastRequest struct {
	Src  string      `json:"src"`
	Dest string      `json:"dest"`
	Body MessageBody `json:"body"`
}

func appendTopologyToFile(topology any) error {
	// Open file in append mode, create if doesn't exist
	file, err := os.OpenFile("/home/cameron/Documents/LearningProjects/GossipGlomers/broadcast-system/log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer file.Close()

	// Marshal the map to JSON
	data, err := json.MarshalIndent(topology, "", "  ")

	// Write to file with newline
	_, err = fmt.Fprintln(file, string(data))
	return err
}
