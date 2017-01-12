package encoding

import (
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
)

var (
	boolValueEncoder         = NewProtobufEncoder(func() proto.Message { return new(protos.BoolValue) })
	int32ValueEncoder        = NewProtobufEncoder(func() proto.Message { return new(protos.Int32Value) })
	int64ValueEncoder        = NewProtobufEncoder(func() proto.Message { return new(protos.Int64Value) })
	stringValueEncoder       = NewProtobufEncoder(func() proto.Message { return new(protos.StringValue) })
	stringListValueEncoder   = NewProtobufEncoder(func() proto.Message { return new(protos.StringListValue) })
	coordinate2DValueEncoder = NewProtobufEncoder(func() proto.Message { return new(protos.Coordinate2DValue) })
)

func BoolValueEncoder() Encoder {
	return boolValueEncoder
}

func Int32ValueEncoder() Encoder {
	return int32ValueEncoder
}

func Int64ValueEncoder() Encoder {
	return int64ValueEncoder
}

func StringValueEncoder() Encoder {
	return stringValueEncoder
}

func StringListValueEncoder() Encoder {
	return stringListValueEncoder
}

func Coordinate2DValueEncoder() Encoder {
	return coordinate2DValueEncoder
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
