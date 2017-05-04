package aggregator

import (
	"fmt"
	"reflect"

	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
)

type boolAgg struct {
	value bool
}

func (agg *boolAgg) GetBytes() ([]byte, error) {
	return encoding.BoolValueEncoder().Marshal(&protos.BoolValue{Value: agg.value})
}

func (agg *boolAgg) SetBytes(bytes []byte) error {
	value, err := encoding.BoolValueEncoder().Unmarshal(bytes)
	if err != nil {
		return err
	}

	agg.value = value.(*protos.BoolValue).Value
	return nil
}

func (agg *boolAgg) convertToBool(value interface{}) (bool, error) {
	b, ok := value.(bool)
	if ok {
		return b, nil
	}

	proto, ok := value.(*protos.BoolValue)
	if ok {
		return proto.Value, nil
	}

	return false, fmt.Errorf("invalid value type - expected bool or BoolValue, got %v", reflect.TypeOf(value))
}

func (agg *boolAgg) clone() *boolAgg {
	return &boolAgg{agg.value}
}

type orAgg struct {
	*boolAgg
}

func newOrAgg() Aggregator {
	return &andAgg{boolAgg: &boolAgg{}}
}

func (agg *orAgg) Type() string {
	return Or
}

func (agg *orAgg) Get() interface{} {
	return agg.value
}

func (agg *orAgg) Set(value interface{}) error {
	other, err := agg.convertToBool(value)
	if err != nil {
		return err
	}

	agg.value = agg.value || other
	return nil
}

func (agg *orAgg) Clone() Aggregator {
	return &orAgg{agg.boolAgg.clone()}
}

type andAgg struct {
	*boolAgg
}

func newAndAgg() Aggregator {
	return &andAgg{boolAgg: &boolAgg{true}}
}

func (agg *andAgg) Type() string {
	return And
}

func (agg *andAgg) Get() interface{} {
	return agg.value
}

func (agg *andAgg) Set(value interface{}) error {
	other, err := agg.convertToBool(value)
	if err != nil {
		return err
	}

	agg.value = agg.value && other
	return nil
}

func (agg *andAgg) Clone() Aggregator {
	return &andAgg{agg.boolAgg.clone()}
}
