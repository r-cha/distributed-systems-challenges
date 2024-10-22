package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type MaelstromMessage struct {
	Type string `json:"type"`
	MsgId int `json:"msg_id"`
}

type MaelstromReply struct {
	Type string `json:"type"`
	MsgId int `json:"in_reply_to"`
}

type Op struct {
	Function string
	Key      int
	Value    int
}

// We want to customize the JSON here because an Op is actually an array.
func (o Op) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{o.Function, o.Key, o.Value})
}

func (o *Op) UnmarshalJSON(data []byte) error {
	// Unmarshall into an array
	var arr []any
	if err := json.Unmarshal(data, &arr); err != nil {
		return err
	}
	// Then into the struct
	o.Function = arr[0].(string)
	o.Key = int(arr[1].(float64))
	if arr[2] != nil {
		o.Value = int(arr[2].(float64))
	}
	return nil
}

type TxnMessage struct {
	MaelstromMessage
	Txn []Op `json:"txn"`
}

type TxnReply struct {
	MaelstromReply
	Txn []Op `json:"txn"`
}

func main() {
	n := maelstrom.NewNode()

	kv := make(map[int]int)

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body TxnMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for _, op := range body.Txn {
			switch op.Function {
			case "r":
				op.Value = kv[op.Key]
			case "w":
				kv[op.Key] = op.Value
			}
		}

		reply := TxnReply{MaelstromReply{"txn_ok", body.MsgId}, body.Txn}

		return n.Reply(msg, reply)


		// return n.Reply(msg, TxnReply{MaelstromReply{"txn_ok", body.MsgId}, body.Txn})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
