package aggregator

import "fmt"

type AggregatorFactory func() Aggregator

var factories = make(map[string]AggregatorFactory)

func Register(name string, factory AggregatorFactory) {
	factories[name] = factory
}

func newAggregator(name string) (Aggregator, error) {
	factory, ok := factories[name]
	if !ok {
		return nil, fmt.Errorf("unknown aggregator %s", name)
	}

	return factory(), nil
}
