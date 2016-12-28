package algorithms

import (
	"fmt"

	"github.com/bcrusu/pregel/protos"
	"github.com/gogo/protobuf/proto"
)

type Algorithm interface {
	Combine(first proto.Message, second proto.Message) (proto.Message, error)
}

func NewAlgorithm(algorithmType protos.AlgorithmType, params []byte) (Algorithm, error) {
	switch algorithmType {
	case protos.AlgorithmType_ShortestPath:
		paramsMsg := new(protos.ShortestPathAlgorithParams)
		if err := proto.Unmarshal(params, paramsMsg); err != nil {
			return nil, err
		}

		return NewShortestPathAlgorithm(*paramsMsg), nil
	default:
		return nil, fmt.Errorf("Invalid algorithm type '%s'", algorithmType)
	}
}
