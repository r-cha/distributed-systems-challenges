package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    n := maelstrom.NewNode()

    var neighbors []string
    var messages []float64

    n.Handle("broadcast", func(msg maelstrom.Message) error {
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        // Get the message
        message := body["message"].(float64)
        messages = append(messages, message)

        // Broadcast to neighbors
        to_send := map[string]interface{}{
            "type": "broadcast",
            "message": message,
        }
        for _, dest := range neighbors {
            // (Don't send back to the sender)
            if msg.Src != n.ID() {
                n.Send(dest, to_send)
            }
        }

        // Clean up the response
        delete(body, "message")
        body["type"] = "broadcast_ok"

        return n.Reply(msg, body)
    })

    n.Handle("read", func(msg maelstrom.Message) error {
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        body["messages"] = messages
        body["type"] = "read_ok"

        return n.Reply(msg, body)
    })

    n.Handle("topology", func(msg maelstrom.Message) error {
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        // Get the topology from the message
        topo, ok := body["topology"].(map[string]interface{})[n.ID()].([]interface{})
        if ok {
            for _, neighbor := range topo {
                // Not using ... so I can coerce the type 🙃
                neighbors = append(neighbors, neighbor.(string))
            }
        }

        // Clean up the response
        delete(body, "topology")
        body["type"] = "topology_ok"

        return n.Reply(msg, body)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }

}
