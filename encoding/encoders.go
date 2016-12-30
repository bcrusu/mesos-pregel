package encoding

import (
	"github.com/bcrusu/pregel/protos"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func NewProtobufEncoder(msgFactory func() proto.Message) Encoder {
	unmarshaler := func(bytes []byte) (interface{}, error) {
		return protobufUnmarshaler(bytes, msgFactory)
	}

	return Encoder{Marshaler(protobufMarshaler), Unmarshaler(unmarshaler)}
}

func NewInt32ValueEncoder() Encoder {
	return NewProtobufEncoder(func() proto.Message { return new(protos.Int32Value) })
}

func protobufMarshaler(message interface{}) ([]byte, error) {
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, errors.New("invalid argument type; expected Protobuf message")
	}

	bytes, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal Protobuf message")
	}

	return bytes, nil
}

func protobufUnmarshaler(bytes []byte, msgFactory func() proto.Message) (interface{}, error) {
	msg := msgFactory()
	if err := proto.Unmarshal(bytes, msg); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal Protobuf message")
	}

	return msg, nil
}
