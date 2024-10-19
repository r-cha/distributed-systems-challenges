package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type LogEntry struct {
	Offset int
	Msg    int
}

// To get our little struct encoded as an array
func (le LogEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal([]int{le.Offset, le.Msg})
}

type MaelstromMessage struct {
	Type string `json:"type"`
}

type SendMessage struct {
	MaelstromMessage
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

type SendResponse struct {
	MaelstromMessage
	Offset int `json:"offset"`
}

type PollMessage struct {
	MaelstromMessage
	Offsets map[string]int `json:"offsets"`
}

type PollResponse struct {
	MaelstromMessage
	Msgs map[string][]LogEntry `json:"msgs"`
}

type CommitOffsetsMessage struct {
	MaelstromMessage
	Offsets map[string]int `json:"offsets"`
}

type CommitOffsetsResponse struct {
	MaelstromMessage
}

type ListCommittedOffsetsMessage struct {
	MaelstromMessage
	Keys []string `json:"keys"`
}

type ListCommittedOffsetsResponse struct {
	MaelstromMessage
	Offsets map[string]int `json:"offsets"`
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body SendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Determine the current offset for this key
		prevOffset, err := kv.ReadInt(context.Background(), body.Key+"-prev")
		if err != nil {
			if err.(*maelstrom.RPCError).Code == maelstrom.KeyDoesNotExist {
				prevOffset = -1
			} else {
				return err
			}
		}

		// Claim the offset, incrementing if necessary.
		var offset int
		for offset = prevOffset+1; ; offset++ {
			if err := kv.CompareAndSwap(context.Background(), body.Key+"-prev", offset-1, offset, true); err != nil {
				if err.(*maelstrom.RPCError).Code == maelstrom.PreconditionFailed {
					continue
				} else {
					return err
				}
			}
			break
		}

		// Write new log
		if err := kv.Write(context.Background(), fmt.Sprintf("%s-%d-%s", body.Key, offset, "entry"), body.Msg); err != nil {
			return err
		}

		// Return the offset of the message
		return n.Reply(msg, SendResponse{MaelstromMessage{"send_ok"}, offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body PollMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := make(map[string][]LogEntry)
		for key, reqOffset := range body.Offsets {
			// For each key, query values until there are no more.
			for offset := reqOffset; ; offset++ {
				v, err := kv.ReadInt(context.Background(), fmt.Sprintf("%s-%d-%s", key, offset, "entry"))
				if err != nil {
					if err.(*maelstrom.RPCError).Code == maelstrom.KeyDoesNotExist {
						break
					}
					return err
				}
				msgs[key] = append(msgs[key], LogEntry{offset, v})
			}
		}

		return n.Reply(msg, PollResponse{MaelstromMessage{"poll_ok"}, msgs})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body CommitOffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the offsets for each key
		for key, offset := range body.Offsets {
			if err := kv.Write(context.Background(), key+"-committed", offset); err != nil {
				return err
			}
		}

		return n.Reply(msg, CommitOffsetsResponse{MaelstromMessage{"commit_offsets_ok"}})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body ListCommittedOffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Return the current offsets for each key
		retOff := make(map[string]int)
		for _, key := range body.Keys {
			commOffset, err := kv.ReadInt(context.Background(), key+"-committed")
			if err != nil {
				if err.(*maelstrom.RPCError).Code == maelstrom.KeyDoesNotExist {
					continue
				} else {
					return err
				}
			}
			retOff[key] = commOffset
		}

		return n.Reply(msg, ListCommittedOffsetsResponse{MaelstromMessage{"list_committed_offsets_ok"}, retOff})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
