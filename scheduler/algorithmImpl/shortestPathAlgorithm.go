package algorithmImpl

import (
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/algorithm"
	"github.com/gogo/protobuf/proto"
)

type shortestPathAlgorithm struct {
	params protos.ShortestPathAlgorithmParams
}

func NewShortestPathAlgorithm(params protos.ShortestPathAlgorithmParams) algorithm.Algorithm {
	return &shortestPathAlgorithm{params}
}

func (algo *shortestPathAlgorithm) Init() {
	//TODO
}

func (algo *shortestPathAlgorithm) GetResult() interface{} {
	//TODO
	return nil
}

func (algo *shortestPathAlgorithm) ResultEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ShortestPathAlgorithmResult) })
}
