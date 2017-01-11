package algorithm

import (
	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/encoding"
)

type Algorithm interface {
	GetResult(aggregators *aggregator.AggregatorSet) interface{}

	ResultEncoder() encoding.Encoder
}
