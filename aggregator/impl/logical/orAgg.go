package logical

import (
	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/encoding"
)

type orAgg struct {
	value bool
}

func NewOrAgg() aggregator.Aggregator {
	return new(orAgg)
}

func (agg *orAgg) Get() interface{} {
	return agg.value
}

func (agg *orAgg) Set(value interface{}) {
	other := value.(bool)
	agg.value = agg.value || other
}

func (agg *orAgg) Combine(other aggregator.Aggregator) aggregator.Aggregator {
	otherAgg := other.(*orAgg)
	value := agg.value || otherAgg.value
	return &orAgg{value}
}

func (agg *orAgg) Encoder() encoding.Encoder {
	return encoding.BoolValueEncoder()
}
