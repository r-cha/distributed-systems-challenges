package main

import (
	"context"
	"encoding/json"
	"log"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type LogEntry struct {
	Offset int 
	Msg    int 
}

type MaelstromMessage struct {
	Type string `json:"type"`
}

type SendMessage struct {
	MaelstromMessage
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

// To get our little struct encoded as an array
func (le LogEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal([]int{le.Offset, le.Msg})
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

func getCommitOffset(kv *maelstrom.KV, key string) int {
	commOffset, err := kv.ReadInt(context.Background(), key + "-committed")
	if err != nil {
		kv.CompareAndSwap(context.Background(), key + "-committed", 0, 0, true)
		commOffset = 0
	}
	return commOffset
}

func getPreviousOffset(kv *maelstrom.KV, key string) int {
	prevOffset, err := kv.ReadInt(context.Background(), key + "-prev")
	if err != nil {
		kv.CompareAndSwap(context.Background(), key + "-prev", 0, 0, true)
		prevOffset = 0
	}
	return prevOffset
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
		var offset int
		prevOffset := getPreviousOffset(kv, body.Key)
		// commOffset := getCommitOffset(kv, body.Key)
		// if commOffset > prevOffset {
		// 	panic("committed offset is greater than previous offset")
		// }
		offset = prevOffset

		// Claim the offset
		for ; ; offset++ {
			if err := kv.CompareAndSwap(context.Background(), body.Key + "-prev", prevOffset, offset, false); err == nil {
				break
			}
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
		for key, offset := range body.Offsets {
			for ; ; offset++ {
				v, err := kv.ReadInt(context.Background(), fmt.Sprintf("%s-%d-%s", key, offset-1, "entry"))
				if err != nil {
					break
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
			for true {
				// Read the offset
				currOffset := getCommitOffset(kv, key)
				// If it's less than the current commit, ignore it
				if offset > currOffset {
					// Otherwise, CAS. If err, retry?
					err := kv.CompareAndSwap(context.Background(), key + "-committed", currOffset, offset, true)
					if err == nil {
						break
					}
				}
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
			commOffset, err := kv.ReadInt(context.Background(), key + "-committed")
			if err != nil {
				continue
			}
			retOff[key] = commOffset
		}

		return n.Reply(msg, ListCommittedOffsetsResponse{MaelstromMessage{"list_committed_offsets_ok"}, retOff})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
