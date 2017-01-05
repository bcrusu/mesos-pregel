package aggregator

import "fmt"

type AggregatorFactory func() Aggregator

var factories = make(map[string]AggregatorFactory)

func Register(name string, factory AggregatorFactory) {
	factories[name] = factory
}

func New(name string) (Aggregator, error) {
	factory, ok := factories[name]
	if !ok {
		return nil, fmt.Errorf("unknown aggregator %s", name)
	}

	return factory(), nil
}

func NewFromMessage(name string, messageBytes []byte) (Aggregator, error) {
	agg, err := New(name)
	if err != nil {
		return nil, err
	}

	message, err := agg.MessageEncoder().Unmarshal(messageBytes)
	if err != nil {
		return nil, err
	}

	value, err := agg.Converter().FromMessage(message)
	if err != nil {
		return nil, err
	}

	agg.Set(value)
	return agg, nil
}

func NewFromValue(name string, value interface{}) (Aggregator, error) {
	agg, err := New(name)
	if err != nil {
		return nil, err
	}

	agg.Set(value)
	return agg, nil
}
