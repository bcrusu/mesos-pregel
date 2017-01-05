package algorithm

import "github.com/bcrusu/mesos-pregel/encoding"

type Algorithm interface {
	Init()
	GetResult() interface{}

	//TODO BeforeSuperstep(int superstep) error
	//TODO AfterSuperstep(int superstep) error

	ResultEncoder() encoding.Encoder
}
