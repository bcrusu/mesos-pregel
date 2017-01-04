package encoding

import (
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func NewProtobufEncoder(msgFactory func() proto.Message) Encoder {
	unmarshaler := func(bytes []byte) (interface{}, error) {
		return protobufUnmarshaler(bytes, msgFactory)
	}

	return Encoder{Marshaler(ProtobufMarshaler), Unmarshaler(unmarshaler)}
}

func NewInt32ValueEncoder() Encoder {
	return NewProtobufEncoder(func() proto.Message { return new(protos.Int32Value) })
}

func NewNullEncoder() Encoder {
	marshaler := func(msg interface{}) ([]byte, error) {
		return nil, nil
	}

	unmarshaler := func(bytes []byte) (interface{}, error) {
		return nil, nil
	}

	return Encoder{Marshaler(marshaler), Unmarshaler(unmarshaler)}
}

//TODO: make private
func ProtobufMarshaler(message interface{}) ([]byte, error) {
	if message == nil {
		return nil, nil
	}

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
	if bytes == nil {
		return nil, nil
	}

	msg := msgFactory()
	if err := proto.Unmarshal(bytes, msg); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal Protobuf message")
	}

	return msg, nil
}
