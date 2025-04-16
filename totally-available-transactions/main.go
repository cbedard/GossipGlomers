package main

import (
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"os"
	"sync"
)

var db map[int]int
var mu *sync.Mutex

func main() {
	n := maelstrom.NewNode()
	db = make(map[int]int)
	mu = &sync.Mutex{}

	n.Handle("txn", func(msg maelstrom.Message) error {
		body := getBody(msg)
		txn := body["txn"].([]any)

		mu.Lock()
		defer mu.Unlock()

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

		body["type"] = "txn_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
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
