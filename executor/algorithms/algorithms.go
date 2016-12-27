package algorithms

import "github.com/gogo/protobuf/proto"

type Algorithm interface {
	Combine(first proto.Message, second proto.Message) (proto.Message, error)
}
