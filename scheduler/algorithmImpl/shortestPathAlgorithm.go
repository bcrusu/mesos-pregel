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

type shortestPathAlgorithmFactory struct {
}

func (f *shortestPathAlgorithmFactory) Create(params interface{}) (algorithm.Algorithm, error) {
	return &shortestPathAlgorithm{*params.(*protos.ShortestPathAlgorithmParams)}, nil
}

func (f *shortestPathAlgorithmFactory) ParamsEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ShortestPathAlgorithmParams) })
}
