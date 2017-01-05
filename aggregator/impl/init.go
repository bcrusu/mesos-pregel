package impl

import (
	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/aggregator/impl/logical"
	"github.com/bcrusu/mesos-pregel/aggregator/impl/math"
)

func init() {
	aggregator.Register(aggregator.And, logical.NewAndAgg)
	aggregator.Register(aggregator.Or, logical.NewOrAgg)
	aggregator.Register(aggregator.MinInt, math.NewMinIntAgg)
	aggregator.Register(aggregator.MaxInt, math.NewMaxIntAgg)
	aggregator.Register(aggregator.SumInt, math.NewSumIntAgg)
}
