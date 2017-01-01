package algorithmImpl

import (
	"fmt"

	"github.com/bcrusu/mesos-pregel/executor/algorithm"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
)

func NewAlgorithm(algorithmType protos.AlgorithmType, params []byte) (algorithm.Algorithm, error) {
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
