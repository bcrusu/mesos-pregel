package logical

import (
	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/encoding"
)

type andAgg struct {
	value bool
}

func NewAndAgg() aggregator.Aggregator {
	return new(andAgg)
}

func (agg *andAgg) Get() interface{} {
	return agg.value
}

func (agg *andAgg) Set(value interface{}) {
	other := value.(bool)
	agg.value = agg.value && other
}

func (agg *andAgg) Combine(other aggregator.Aggregator) aggregator.Aggregator {
	otherAgg := other.(*andAgg)
	value := agg.value && otherAgg.value
	return &andAgg{value}
}

func (agg *andAgg) Encoder() encoding.Encoder {
	return encoding.BoolValueEncoder()
}
