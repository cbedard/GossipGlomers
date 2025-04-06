package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"os"
	"sync"
	"time"
)

var key = "counter"
var mu sync.Mutex

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
		mu.Lock()
		defer mu.Unlock()

		body := getBody(msg)
		delta := body["delta"].(float64)

		atomicAddWithRetries(kv, delta)

		body["type"] = "add_ok"
		delete(body, "delta")

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		body := getBody(msg)

		//BS write to force consistency
		kv.Write(context.Background(), uuid.NewString(), 0)

		//contexts are shared between threads?? processes?
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		val, err := kv.ReadInt(ctx, "counter")
		if err != nil {
			appendToLogFile(ErrorLog{-1, err.Error()})
		}
		cancel()

		body["value"] = val
		body["type"] = "read_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func atomicAddWithRetries(kv *maelstrom.KV, delta float64) {
	for i := range 50 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		prevVal, err := kv.ReadInt(ctx, key)
		base := float64(prevVal)
		if err != nil {
			appendToLogFile(ErrorLog{i, err.Error()})
		} else {
			ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
			err = kv.CompareAndSwap(ctx2, key, base, base+delta, false)
			if err != nil {
				appendToLogFile(ErrorLog{i, err.Error()})
			} else {
				cancel()
				cancel2()
				break
			}
			cancel2()
		}
		cancel()
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
	Type int
	Err  string
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
