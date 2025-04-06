package main

import (
	"context"
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"os"
	"time"
)

var key = "counter"

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("init", func(msg maelstrom.Message) error {
		body := getBody(msg)

		ctx := context.Background()
		_, err := kv.Read(ctx, key)

		if err != nil {
			kv.Write(ctx, key, 0)
		}

		body["type"] = "init_ok"

		return n.Reply(msg, body)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		body := getBody(msg)

		body["type"] = "add_ok"
		delta := body["delta"].(float64)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		prevVal, err := kv.ReadInt(ctx, key)
		base := float64(prevVal)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}

		err = kv.CompareAndSwap(ctx, key, base, base+delta, false)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}

		delete(body, "delta")
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := getBody(msg)

		body["type"] = "read_ok"

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		x, _ := kv.ReadInt(ctx, "counter")

		body["value"] = x
		body["type"] = "read_ok"

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
	Retry int
	Err   string
}

func appendToLogFile(object any) {
	// Open file in append mode, create if doesn't exist
	file, err := os.OpenFile("/home/cameron/Documents/LearningProjects/GossipGlomers/grow-only-counter/log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
