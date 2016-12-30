package algorithmImpl

import (
	"errors"

	"encoding"

	"github.com/bcrusu/pregel/encoding"
	"github.com/bcrusu/pregel/executor/algorithm"
	"github.com/bcrusu/pregel/protos"
	"github.com/gogo/protobuf/proto"
)

type ShortestPathAlgorithm struct {
	params protos.ShortestPathAlgorithParams
}

func NewShortestPathAlgorithm(params protos.ShortestPathAlgorithParams) algorithm.Algorithm {
	result := new(ShortestPathAlgorithm)
	result.params = params
	return result
}

func (algo *ShortestPathAlgorithm) Combine(first proto.Message, second proto.Message) (proto.Message, error) {
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

func (algo *ShortestPathAlgorithm) Compute(context *algorithm.VertexContext, messages []proto.Message) error {
	//TODO
	return nil
}

func (algo *ShortestPathAlgorithm) VertexMessageEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ShortestPathMessage) })
}

func (algo *ShortestPathAlgorithm) VertexValueEncoder() encoding.Encoder {
	return encoding.NewInt32ValueEncoder()
}

func (algo *ShortestPathAlgorithm) EdgeValueEncoder() encoding.Encoder {
	return encoding.NewInt32ValueEncoder()
}
