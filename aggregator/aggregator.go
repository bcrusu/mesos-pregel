package aggregator

type Aggregator interface {
	Name() string

	Get() interface{}
	Set(value interface{}) error

	GetBytes() ([]byte, error)
	SetBytes(bytes []byte) error

	Clone() Aggregator
}
