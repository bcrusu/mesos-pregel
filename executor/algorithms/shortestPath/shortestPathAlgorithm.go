package algorithms

import (
	"log"

	"github.com/bcrusu/pregel"
)

type ShortestPathAlgorithm struct {
}

type message struct {
	pathLength int
}

func (this message) Combine(other pregel.Message) pregel.Message {
	if other == nil {
		return this
	}

	otherMessage, ok := other.(message)
	if !ok {
		log.Panic("unexpected message type")
	}

	if otherMessage.pathLength < this.pathLength {
		return otherMessage
	}

	return this
}

func (this message) Serialize() []byte {
	return nil
}
