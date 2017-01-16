package algorithms

import (
	"fmt"
	"math"

	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/algorithm"
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
)

type shortestPathAlgorithm struct {
	params protos.ShortestPathAlgorithmParams
}

func (algo *shortestPathAlgorithm) Compute(context *algorithm.VertexContext, msg interface{}) error {
	sendMessages := func(currentPathLength int64) {
		for _, edge := range context.Edges {
			edgeValue := edge.Value.(*protos.Int64Value)
			context.SendMessageTo(edge.To, &protos.ShortestPathAlgorithmMessage{
				PathLength: currentPathLength + edgeValue.Value,
			})
		}
	}

	getCurrentPathLength := func() int64 {
		if context.MutableValue == nil {
			return math.MaxInt64
		}

		return (context.MutableValue.(*protos.Int64Value)).Value
	}

	if msg == nil {
		if context.ID() == algo.params.From {
			sendMessages(0)
		}
	} else {
		currentPathLength := getCurrentPathLength()
		msgPathLength := msg.(*protos.ShortestPathAlgorithmMessage).PathLength

		if msgPathLength < currentPathLength {
			context.SetValue(&protos.Int64Value{Value: msgPathLength})

			if context.ID() != algo.params.To {
				sendMessages(msgPathLength)
			} else {
				context.Aggregators.Add(aggregator.MinInt, "pathLength", msgPathLength)
			}
		}
	}

	context.VoteToHalt()
	return nil
}

func (algo *shortestPathAlgorithm) GetResult(aggregators *aggregator.AggregatorSet) interface{} {
	result := &protos.ShortestPathAlgorithmResult{PathLength: -1}

	if pathLength, ok := aggregators.GetValue("pathLength"); ok {
		result.PathLength = pathLength.(int64)
	}

	return result
}

func (algo *shortestPathAlgorithm) GetResultDisplayValue(result interface{}) string {
	r := result.(*protos.ShortestPathAlgorithmResult)
	return fmt.Sprintf("path length from '%s' to '%s'=%d", algo.params.From, algo.params.To, r.PathLength)
}

func (algo *shortestPathAlgorithm) VertexMessageCombiner() algorithm.VertexMessageCombiner {
	return combineVertexMessages
}

func (algo *shortestPathAlgorithm) VertexMessageEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ShortestPathAlgorithmMessage) })
}

func (algo *shortestPathAlgorithm) VertexValueEncoder() encoding.Encoder {
	return encoding.Coordinate2DValueEncoder()
}

func (algo *shortestPathAlgorithm) EdgeValueEncoder() encoding.Encoder {
	return encoding.Int64ValueEncoder()
}

func (algo *shortestPathAlgorithm) VertexMutableValueEncoder() encoding.Encoder {
	return encoding.Int64ValueEncoder()
}

func (algo *shortestPathAlgorithm) EdgeMutableValueEncoder() encoding.Encoder {
	return encoding.Int64ValueEncoder()
}

func (algo *shortestPathAlgorithm) ResultEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ShortestPathAlgorithmResult) })
}

func (algo *shortestPathAlgorithm) Handlers() *algorithm.Handlers {
	return algorithm.DefaultHandlers()
}

func combineVertexMessages(firstMsg interface{}, secondMsg interface{}) interface{} {
	first := firstMsg.(*protos.ShortestPathAlgorithmMessage)
	second := secondMsg.(*protos.ShortestPathAlgorithmMessage)

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
