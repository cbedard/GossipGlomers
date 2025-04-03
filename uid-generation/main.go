package main

import (
	"encoding/json"
	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		id := uuid.New().String()

		body["type"] = "generate_ok"
		body["id"] = id

		// Echo the original message back with the updated message type.
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
		Type  string `json:"type"`
		MsgId int    `json:"msg_id"`
		Echo  string `json:"echo"`
	} `json:"body"`
}
