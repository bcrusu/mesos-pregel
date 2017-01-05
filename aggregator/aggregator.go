package aggregator

import "github.com/bcrusu/mesos-pregel/encoding"

type Aggregator interface {
	Get() interface{}
	Set(value interface{})

	Combine(other Aggregator) Aggregator
	Encoder() encoding.Encoder
}
