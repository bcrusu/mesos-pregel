package algorithmImpl

import (
	"fmt"

	"github.com/bcrusu/mesos-pregel/executor/algorithm"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
)

func NewAlgorithm(algorithmType string, params []byte) (algorithm.Algorithm, error) {
	switch algorithmType {
	case "shortest_path":
		paramsMsg := new(protos.ShortestPathAlgorithParams)
		if err := proto.Unmarshal(params, paramsMsg); err != nil {
			return nil, err
		}

		return NewShortestPathAlgorithm(*paramsMsg), nil
	default:
		return nil, fmt.Errorf("Invalid algorithm type '%s'", algorithmType)
	}
}
