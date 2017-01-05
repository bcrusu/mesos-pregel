package aggregator

import (
	"fmt"
	"reflect"

	"github.com/bcrusu/mesos-pregel/protos"
)

var (
	boolValueConverterInstance  = &boolValueConverter{}
	int64ValueConverterInstance = &int64ValueConverter{}
)

func BoolValueConverter() ValueConverter {
	return boolValueConverterInstance
}

func Int64ValueConverter() ValueConverter {
	return int64ValueConverterInstance
}

type boolValueConverter struct{}

func (c *boolValueConverter) ToMessage(value interface{}) (interface{}, error) {
	val, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("cannot convert from type %v", reflect.TypeOf(value))
	}
	return &protos.BoolValue{Value: val}, nil
}

func (c *boolValueConverter) FromMessage(value interface{}) (interface{}, error) {
	val, ok := value.(*protos.BoolValue)
	if !ok {
		return nil, fmt.Errorf("cannot convert from type %v", reflect.TypeOf(value))
	}

	return val.Value, nil
}

type int64ValueConverter struct{}

func (c *int64ValueConverter) ToMessage(value interface{}) (interface{}, error) {
	val, ok := value.(int64)
	if !ok {
		return nil, fmt.Errorf("cannot convert from type %v", reflect.TypeOf(value))
	}
	return &protos.Int64Value{Value: val}, nil
}

func (c *int64ValueConverter) FromMessage(value interface{}) (interface{}, error) {
	val, ok := value.(*protos.Int64Value)
	if !ok {
		return nil, fmt.Errorf("cannot convert from type %v", reflect.TypeOf(value))
	}

	return val.Value, nil
}
