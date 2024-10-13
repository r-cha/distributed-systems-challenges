package main

import (
	"encoding/json"
	"log"

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


func main() {
	n := maelstrom.NewNode()

	committedOffsets := make(map[string]int)
	prevOffsets := make(map[string]int)
	logs := make(map[string][]LogEntry)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body SendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Determine the current offset for this key
		var offset int
		prevOffset, ok := prevOffsets[body.Key]
		if !ok {
			prevOffset = 0
		}
		commOffset, ok := committedOffsets[body.Key]
		if !ok {
			commOffset = 0
		}
		if commOffset > prevOffset {
			panic("committed offset is greater than previous offset")
		}
		offset = prevOffset + 1

		// Append the message to the log
		logs[body.Key] = append(logs[body.Key], LogEntry{offset, body.Msg})

		// Update the previous offset
		prevOffsets[body.Key] = offset

		// Return the offset of the message
		return n.Reply(msg, SendResponse{MaelstromMessage{"send_ok"}, offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body PollMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := make(map[string][]LogEntry)
		// For each key
		for key, offset := range body.Offsets {
			// Get the log for this key
			log, ok := logs[key]
			if !ok {
				log = []LogEntry{}
			}
			// And filter them based on the given offset
			for _, entry := range log {
				if entry.Offset >= offset {
					msgs[key] = append(msgs[key], entry)
				}
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
			committedOffsets[key] = offset
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
			offset, ok := committedOffsets[key]
			if !ok {
				continue
			}
			retOff[key] = offset
		}

		return n.Reply(msg, ListCommittedOffsetsResponse{MaelstromMessage{"list_committed_offsets_ok"}, retOff})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
