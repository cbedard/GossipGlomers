package main

import (
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"os"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("txn", func(msg maelstrom.Message) error {
		body := getBody(msg)

		key := body["type"].(string)
		value := body["msg_id"].(float64)
		txn := body["txn"].([]any)

		appendToLogFile(struct {
			key string
			val float64
			txn []any
		}{
			key: key,
			val: value,
			txn: txn,
		})

		body["type"] = "txn_ok"
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
