package math

import (
	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/encoding"
)

type minIntAgg struct {
	value int64
}

func NewMinIntAgg() aggregator.Aggregator {
	return new(minIntAgg)
}

func (agg *minIntAgg) Get() interface{} {
	return agg.value
}

func (agg *minIntAgg) Set(value interface{}) {
	other := value.(int64)
	if other < agg.value {
		agg.value = other
	}
}

func (agg *minIntAgg) Combine(other aggregator.Aggregator) aggregator.Aggregator {
	otherAgg := other.(*minIntAgg)
	if otherAgg.value < agg.value {
		return otherAgg
	}

	return agg
}

func (agg *minIntAgg) Encoder() encoding.Encoder {
	return encoding.Int64ValueEncoder()
}
