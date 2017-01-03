package algorithmImpl

import (
	"fmt"

	"github.com/bcrusu/mesos-pregel/executor/algorithm"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
)

func NewAlgorithm(algorithm string, params []byte) (algorithm.Algorithm, error) {
	switch algorithm {
	case "ShortestPath":
		paramsMsg := new(protos.ShortestPathAlgorithmParams)
		if err := proto.Unmarshal(params, paramsMsg); err != nil {
			return nil, err
		}

		return NewShortestPathAlgorithm(*paramsMsg), nil
	default:
		return nil, fmt.Errorf("Invalid algorithm '%s'", algorithm)
	}
}
