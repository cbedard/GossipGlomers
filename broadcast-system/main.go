package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

var messageHistory []float64

//mu lock??

func main() {
	n := maelstrom.NewNode()
	messageHistory = make([]float64, 0)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		next := body["message"].(float64)
		messageHistory = append(messageHistory, next)

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

		delete(body, "topology")
		body["type"] = "topology_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type Request struct {
	Src  string `json:"src"`
	Dest string `json:"dest"`
	Body struct {
		Type    string `json:"type"`
		MsgId   int    `json:"msg_id"`
		Message string `json:"message"`
	} `json:"body"`
}
