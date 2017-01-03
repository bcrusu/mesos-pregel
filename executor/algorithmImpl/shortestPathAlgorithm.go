package algorithmImpl

import (
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/executor/algorithm"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
)

type shortestPathAlgorithm struct {
	params protos.ShortestPathAlgorithmParams
}

func (algo *shortestPathAlgorithm) Compute(context *algorithm.VertexContext, msg interface{}) error {
	//value := context.Value.(*protos.Int32Value)
	//message := msg.(*protos.ShortestPathMessage)

	//TODO
	return nil
}

func (algo *shortestPathAlgorithm) VertexMessageCombiner() algorithm.VertexMessageCombiner {
	return algorithm.VertexMessageCombiner(combineVertexMessages)
}

func (algo *shortestPathAlgorithm) VertexMessageEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ShortestPathMessage) })
}

func (algo *shortestPathAlgorithm) VertexValueEncoder() encoding.Encoder {
	return encoding.NewInt32ValueEncoder()
}

func (algo *shortestPathAlgorithm) EdgeValueEncoder() encoding.Encoder {
	return encoding.NewInt32ValueEncoder()
}

func (algo *shortestPathAlgorithm) Handlers() *algorithm.Handlers {
	return algorithm.DefaultHandlers()
}

func combineVertexMessages(firstMsg interface{}, secondMsg interface{}) interface{} {
	first := firstMsg.(*protos.ShortestPathMessage)
	second := secondMsg.(*protos.ShortestPathMessage)

	// compare path lengths and return the min
	if first.PathLength < second.PathLength {
		return first
	}

	return second
}

type shortestPathAlgorithmFactory struct {
}

func (f *shortestPathAlgorithmFactory) Create(params interface{}) (algorithm.Algorithm, error) {
	return &shortestPathAlgorithm{*params.(*protos.ShortestPathAlgorithmParams)}, nil
}

func (f *shortestPathAlgorithmFactory) ParamsEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ShortestPathAlgorithmParams) })
}
