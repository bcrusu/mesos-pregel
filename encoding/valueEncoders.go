package encoding

import (
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
)

var (
	boolValueEncoder       = NewProtobufEncoder(func() proto.Message { return new(protos.BoolValue) })
	int32ValueEncoder      = NewProtobufEncoder(func() proto.Message { return new(protos.Int32Value) })
	int64ValueEncoder      = NewProtobufEncoder(func() proto.Message { return new(protos.Int64Value) })
	string64ValueEncoder   = NewProtobufEncoder(func() proto.Message { return new(protos.StringValue) })
	stringListValueEncoder = NewProtobufEncoder(func() proto.Message { return new(protos.StringListValue) })
)

func BoolValueEncoder() Encoder {
	return int32ValueEncoder
}

func Int32ValueEncoder() Encoder {
	return int32ValueEncoder
}

func Int64ValueEncoder() Encoder {
	return int32ValueEncoder
}

func StringValueEncoder() Encoder {
	return string64ValueEncoder
}

func StringListValueEncoder() Encoder {
	return stringListValueEncoder
}

func NullEncoder() Encoder {
	marshaler := func(msg interface{}) ([]byte, error) {
		return nil, nil
	}

	unmarshaler := func(bytes []byte) (interface{}, error) {
		return nil, nil
	}

	return Encoder{Marshaler(marshaler), Unmarshaler(unmarshaler)}
}
