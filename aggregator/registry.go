package aggregator

import "fmt"

type AggregatorFactory func() Aggregator

var factories = make(map[string]AggregatorFactory)

func Register(aggType string, factory AggregatorFactory) {
	factories[aggType] = factory
}

func newAggregator(aggType string) (Aggregator, error) {
	factory, ok := factories[aggType]
	if !ok {
		return nil, fmt.Errorf("unknown aggregator type %s", aggType)
	}

	return factory(), nil
}
