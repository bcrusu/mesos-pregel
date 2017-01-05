package math

import (
	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/encoding"
)

type maxIntAgg struct {
	value int64
}

func NewMaxIntAgg() aggregator.Aggregator {
	return new(maxIntAgg)
}

func (agg *maxIntAgg) Get() interface{} {
	return agg.value
}

func (agg *maxIntAgg) Set(value interface{}) {
	other := value.(int64)
	if other > agg.value {
		agg.value = other
	}
}

func (agg *maxIntAgg) Combine(other aggregator.Aggregator) aggregator.Aggregator {
	otherAgg := other.(*maxIntAgg)
	if otherAgg.value > agg.value {
		return otherAgg
	}

	return agg
}

func (agg *maxIntAgg) Encoder() encoding.Encoder {
	return encoding.Int64ValueEncoder()
}
