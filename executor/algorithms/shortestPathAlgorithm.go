package algorithms

import (
	"errors"

	"github.com/bcrusu/pregel/protos"
	"github.com/gogo/protobuf/proto"
)

type ShortestPathAlgorithm struct {
	params protos.ShortestPathAlgorithParams
}

func NewShortestPathAlgorithm(params protos.ShortestPathAlgorithParams) Algorithm {
	result := new(ShortestPathAlgorithm)
	result.params = params
	return result
}

func (this *ShortestPathAlgorithm) Combine(first proto.Message, second proto.Message) (proto.Message, error) {
	if first == nil || second == nil {
		return nil, errors.New("cannot combine nil message")
	}

	firstMessage, ok := first.(*protos.ShortestPathMessage)
	if !ok {
		return nil, errors.New("unexpected message type for 'first'")
	}

	secondMessage, ok := second.(*protos.ShortestPathMessage)
	if !ok {
		return nil, errors.New("unexpected message type for 'second'")
	}

	// compare path lengths and return the min
	if firstMessage.PathLength < secondMessage.PathLength {
		return firstMessage, nil
	}

	return secondMessage, nil
}
