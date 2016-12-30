package encoding

type Encoder struct {
	Marshaler   Marshaler
	Unmarshaler Unmarshaler
}

type Unmarshaler func([]byte) (interface{}, error)
type Marshaler func(interface{}) ([]byte, error)
