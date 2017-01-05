package aggregator

import "github.com/bcrusu/mesos-pregel/encoding"

type Aggregator interface {
	Get() interface{}
	Set(value interface{})

	Converter() ValueConverter
	MessageEncoder() encoding.Encoder
}

type ValueConverter interface {
	ToMessage(value interface{}) (interface{}, error)
	FromMessage(message interface{}) (interface{}, error)
}
