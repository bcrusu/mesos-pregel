package logical

import "github.com/bcrusu/mesos-pregel/aggregator"
import "github.com/bcrusu/mesos-pregel/encoding"

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

func (agg *andAgg) Converter() aggregator.ValueConverter {
	return aggregator.BoolValueConverter()
}

func (agg *andAgg) MessageEncoder() encoding.Encoder {
	return encoding.BoolValueEncoder()
}
