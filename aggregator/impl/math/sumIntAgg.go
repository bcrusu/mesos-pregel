package math

import (
	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/encoding"
)

type sumIntAgg struct {
	value int64
}

func NewSumIntAgg() aggregator.Aggregator {
	return new(sumIntAgg)
}

func (agg *sumIntAgg) Get() interface{} {
	return agg.value
}

func (agg *sumIntAgg) Set(value interface{}) {
	other := value.(int64)
	agg.value += other
}

func (agg *sumIntAgg) Converter() aggregator.ValueConverter {
	return aggregator.BoolValueConverter()
}

func (agg *sumIntAgg) MessageEncoder() encoding.Encoder {
	return encoding.Int64ValueEncoder()
}
