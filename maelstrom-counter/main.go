package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))

		// The only way to eliminate race conditions is by eliminating contention,
		// so while the node is "stateless" in the sense that it doesn't store any state in memory,
		// it does keep its own state in the kv store.
		current, err := kv.ReadInt(context.Background(), n.ID());
		if err != nil {
			current = 0
		}
		err = kv.CompareAndSwap(context.Background(), n.ID(), current, current+delta, true)

		// Clean up the response
		delete(body, "delta")
		body["type"] = "add_ok"

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// By adding all the values from all the nodes, we can get the total expected value.
		// This will eventually be correct as long as each node adds its own values.
		total := 0
		for _, nid := range n.NodeIDs() {
			value, err := kv.ReadInt(context.Background(), nid)
			if err != nil {
				value += 0
			} else {
				total += value
			}
		}
		body["value"] = total
		body["type"] = "read_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
