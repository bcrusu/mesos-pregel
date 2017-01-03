package algorithm

import "github.com/bcrusu/mesos-pregel/encoding"

type Algorithm interface {
	Init()
	GetResult() interface{}

	ResultEncoder() encoding.Encoder
}
