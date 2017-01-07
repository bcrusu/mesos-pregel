package aggregator

import (
	"fmt"
	"reflect"

	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
)

type int64Agg struct {
	value int64
}

func (agg *int64Agg) GetBytes() ([]byte, error) {
	return encoding.Int64ValueEncoder().Marshal(&protos.Int64Value{Value: agg.value})
}

func (agg *int64Agg) SetBytes(bytes []byte) error {
	value, err := encoding.Int64ValueEncoder().Unmarshal(bytes)
	if err != nil {
		return err
	}

	agg.value = value.(*protos.Int64Value).Value
	return nil
}

func (agg *int64Agg) convertToInt64(value interface{}) (int64, error) {
	v, ok := value.(int64)
	if ok {
		return v, nil
	}

	proto, ok := value.(*protos.Int64Value)
	if ok {
		return proto.Value, nil
	}

	return 0, fmt.Errorf("invalid value type - expected int64 or Int64Value, got %v", reflect.TypeOf(value))
}

func (agg *int64Agg) clone() *int64Agg {
	return &int64Agg{agg.value}
}

type minIntAgg struct {
	*int64Agg
}

func newMinIntAgg() Aggregator {
	return &minIntAgg{int64Agg: &int64Agg{value: 0}}
}

func (agg *minIntAgg) Name() string {
	return MinInt
}

func (agg *minIntAgg) Get() interface{} {
	return agg.value
}

func (agg *minIntAgg) Set(value interface{}) error {
	other, err := agg.convertToInt64(value)
	if err != nil {
		return err
	}

	if other < agg.value {
		agg.value = other
	}
	return nil
}

func (agg *minIntAgg) Clone() Aggregator {
	return &minIntAgg{agg.int64Agg.clone()}
}

type maxIntAgg struct {
	*int64Agg
}

func newMaxIntAgg() Aggregator {
	return &maxIntAgg{int64Agg: &int64Agg{}}
}

func (agg *maxIntAgg) Name() string {
	return MaxInt
}

func (agg *maxIntAgg) Get() interface{} {
	return agg.value
}

func (agg *maxIntAgg) Set(value interface{}) error {
	other, err := agg.convertToInt64(value)
	if err != nil {
		return err
	}

	if other > agg.value {
		agg.value = other
	}
	return nil
}

func (agg *maxIntAgg) Clone() Aggregator {
	return &maxIntAgg{agg.int64Agg.clone()}
}

type sumIntAgg struct {
	*int64Agg
}

func newSumIntAgg() Aggregator {
	return &sumIntAgg{int64Agg: &int64Agg{}}
}

func (agg *sumIntAgg) Name() string {
	return SumInt
}

func (agg *sumIntAgg) Get() interface{} {
	return agg.value
}

func (agg *sumIntAgg) Set(value interface{}) error {
	other, err := agg.convertToInt64(value)
	if err != nil {
		return err
	}

	agg.value += other
	return nil
}

func (agg *sumIntAgg) Clone() Aggregator {
	return &sumIntAgg{agg.int64Agg.clone()}
}
