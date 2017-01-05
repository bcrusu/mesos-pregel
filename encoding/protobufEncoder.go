package encoding

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func NewProtobufEncoder(msgFactory func() proto.Message) Encoder {
	unmarshaler := func(bytes []byte) (interface{}, error) {
		return protobufUnmarshaler(bytes, msgFactory)
	}

	return Encoder{Marshaler(protobufMarshaler), Unmarshaler(unmarshaler)}
}

func protobufMarshaler(message interface{}) ([]byte, error) {
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
