package job

import "github.com/bcrusu/mesos-pregel/aggregator"

type superstepContext struct {
	superstep   int
	aggregators *aggregator.AggregatorSet
	isCompleted bool
	stopped     bool
}

func (ctx *superstepContext) Superstep() int {
	return ctx.superstep
}

func (ctx *superstepContext) Aggregators() *aggregator.AggregatorSet {
	return ctx.aggregators
}

func (ctx *superstepContext) Stop() {
	ctx.stopped = true
}

func (ctx *superstepContext) IsCompleted() bool {
	return ctx.isCompleted
}
