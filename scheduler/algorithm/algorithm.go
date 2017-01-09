package algorithm

import "github.com/bcrusu/mesos-pregel/encoding"
import "github.com/bcrusu/mesos-pregel/aggregator"

type Algorithm interface {
	GetResult() interface{}

	BeforeSuperstep(context SuperstepContext) error //TODO move to handlers interface
	AfterSuperstep(context SuperstepContext) error  //TODO move to handlers interface

	ResultEncoder() encoding.Encoder
}

type SuperstepContext interface {
	Superstep() int
	Aggregators() *aggregator.AggregatorSet

	IsCompleted() bool
	Stop()
}
