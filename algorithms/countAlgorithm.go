package algorithms

import (
	"fmt"

	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/algorithm"
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
)

// Simple algorithm that counts the number of vertices and edges in the graph
type countAlgorithm struct {
}

func (algo *countAlgorithm) Compute(context *algorithm.VertexContext, msg interface{}) error {
	glog.Infof("countAlgorithm.Compute: vertexID=%s, edges=%d", context.ID(), len(context.Edges))

	context.SetAggregator("vertices", aggregator.SumInt, 1)
	context.SetAggregator("edges", aggregator.SumInt, len(context.Edges))

	context.VoteToHalt()
	return nil
}

func (algo *countAlgorithm) GetResult(aggregators *aggregator.ImmutableAggregatorSet) interface{} {
	result := &protos.CountAlgorithmResult{}

	if c, ok := aggregators.GetValue("vertices"); ok {
		result.Vertices = c.(int64)
	}

	if c, ok := aggregators.GetValue("edges"); ok {
		result.Edges = c.(int64)
	}

	return result
}

func (algo *countAlgorithm) GetResultDisplayValue(result interface{}) string {
	r := result.(*protos.CountAlgorithmResult)
	return fmt.Sprintf("vertices count=%d; edges count=%d", r.Vertices, r.Edges)
}

func (algo *countAlgorithm) VertexMessageCombiner() algorithm.VertexMessageCombiner {
	return nil
}

func (algo *countAlgorithm) VertexMessageEncoder() encoding.Encoder {
	return encoding.NullEncoder()
}

func (algo *countAlgorithm) VertexValueEncoder() encoding.Encoder {
	return encoding.NullEncoder()
}

func (algo *countAlgorithm) EdgeValueEncoder() encoding.Encoder {
	return encoding.NullEncoder()
}

func (algo *countAlgorithm) VertexMutableValueEncoder() encoding.Encoder {
	return encoding.NullEncoder()
}

func (algo *countAlgorithm) EdgeMutableValueEncoder() encoding.Encoder {
	return encoding.NullEncoder()
}

func (algo *countAlgorithm) ResultEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.CountAlgorithmResult) })
}

func (algo *countAlgorithm) Handlers() *algorithm.Handlers {
	return algorithm.DefaultHandlers()
}

type countAlgorithmFactory struct {
}

func (f *countAlgorithmFactory) Create(params interface{}) (algorithm.Algorithm, error) {
	return &countAlgorithm{}, nil
}

func (f *countAlgorithmFactory) ParamsEncoder() encoding.Encoder {
	return encoding.NullEncoder()
}
