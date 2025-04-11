package main

import (
	"context"
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"os"
	"sync"
)

var logs Logs
var lock *sync.Mutex

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	lock = &sync.Mutex{}

	logs = Logs{make(map[string]*[][]float64), kv, &sync.Mutex{}, &sync.Mutex{}}

	n.Handle("send", func(msg maelstrom.Message) error {
		body := getBody(msg)

		key := body["key"].(string)
		value, _ := body["msg"].(float64)

		// we need this lock to ensure our log entry is fully synced before returning,
		// or we can get a missing entry on "poll"
		lock.Lock()
		defer lock.Unlock()
		body["offset"] = logs.Put(key, value, -1)

		//we sync after we set the offset so it doesn't have to be computed on the syncing node
		for _, id := range n.NodeIDs() {
			if n.ID() != id {
				body["type"] = "sync"
				_, err := n.SyncRPC(context.Background(), id, body)
				if err != nil {
					appendToLogFile(ErrorMessage{fmt.Sprintf("Error syncing logs: %s", err.Error())})
				}
			}
		}

		//handle proper return fields
		body["type"] = "send_ok"
		delete(body, "key")
		delete(body, "msg")

		return n.Reply(msg, body)
	})

	n.Handle("sync", func(msg maelstrom.Message) error {
		body := getBody(msg)

		key := body["key"].(string)
		value, _ := body["msg"].(float64)
		offset := body["offset"].(float64)

		logs.Put(key, value, int(offset))

		body["type"] = "sync_ok"
		return n.Reply(msg, body)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		body := getBody(msg)

		offsets, _ := ConvertToMapStringFloat64(body["offsets"].(map[string]any))

		// need the same lock on poll as we have on send such that a commited entry+reply isn't received by the sender
		// before the poll has returned with a complete set of entries
		lock.Lock()
		defer lock.Unlock()

		body["msgs"] = logs.Poll(offsets)

		//handle proper return fields
		body["type"] = "poll_ok"
		delete(body, "offsets")

		return n.Reply(msg, body)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		body := getBody(msg)

		offsets, _ := ConvertToMapStringFloat64(body["offsets"].(map[string]any))
		logs.Commit(offsets)

		//handle proper return fields
		body["type"] = "commit_offsets_ok"
		delete(body, "offsets")

		return n.Reply(msg, body)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		body := getBody(msg)

		keys, _ := ConvertToStringSlice(body["keys"].([]any))
		body["offsets"] = logs.ListCommits(keys)

		//handle proper return fields
		body["type"] = "list_committed_offsets_ok"
		delete(body, "keys")

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

/*** UTILS ***/

func ConvertToMapStringFloat64(input map[string]any) (map[string]float64, error) {
	result := make(map[string]float64)
	for k, v := range input {
		if floatVal, ok := v.(float64); ok {
			result[k] = floatVal
		} else {
			return nil, fmt.Errorf("value for key %s is not a float64: %T", k, v)
		}
	}
	return result, nil
}

func ConvertToStringSlice(input []any) ([]string, error) {
	result := make([]string, len(input))
	for i, v := range input {
		if strVal, ok := v.(string); ok {
			result[i] = strVal
		} else {
			return nil, fmt.Errorf("value at index %d is not a string: %T", i, v)
		}
	}
	return result, nil
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
	file, err := os.OpenFile("/home/cameron/Documents/LearningProjects/GossipGlomers/replicated-logs/log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
