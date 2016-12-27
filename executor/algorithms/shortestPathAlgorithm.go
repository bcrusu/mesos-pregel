package algorithms

import (
	"errors"

	"github.com/bcrusu/pregel/protos/algorithms"
	"github.com/gogo/protobuf/proto"
)

type ShortestPathAlgorithm struct {
}

func (this *ShortestPathAlgorithm) Combine(first proto.Message, second proto.Message) (proto.Message, error) {
	if first == nil || second == nil {
		return nil, errors.New("cannot combine nil message")
	}

	firstMessage, ok := first.(*algorithms.ShortestPathMessage)
	if !ok {
		return nil, errors.New("unexpected message type for 'first'")
	}

	secondMessage, ok := second.(*algorithms.ShortestPathMessage)
	if !ok {
		return nil, errors.New("unexpected message type for 'second'")
	}

	// compare path lengths and return the min
	if firstMessage.GetPathLength() < secondMessage.GetPathLength() {
		return firstMessage, nil
	}

	return secondMessage, nil
}
