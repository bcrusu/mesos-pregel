package algorithm

import "github.com/bcrusu/mesos-pregel/encoding"
import "github.com/bcrusu/mesos-pregel/aggregator"

type Algorithm interface {
	Init(context InitContext) error
	GetResult() interface{}

	BeforeSuperstep(context SuperstepContext) error
	AfterSuperstep(context SuperstepContext) error

	ResultEncoder() encoding.Encoder
}

type InitContext interface {
	Aggregators() *aggregator.AggregatorSet
}

type SuperstepContext interface {
	Superstep() int
	Aggregators() *aggregator.AggregatorSet

	Stop()
}
