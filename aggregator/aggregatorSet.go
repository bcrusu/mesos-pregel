package aggregator

import (
	"fmt"
	"sync"

	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/pkg/errors"
)

type AggregatorSet struct {
	mutex sync.RWMutex
	aggs  map[string]Aggregator
}

func NewSet() *AggregatorSet {
	return &AggregatorSet{
		aggs: make(map[string]Aggregator),
	}
}

func NewSetFromMessages(messages []*protos.Aggregator) (*AggregatorSet, error) {
	result := &AggregatorSet{aggs: make(map[string]Aggregator)}
	if messages == nil {
		return result, nil
	}

	for _, proto := range messages {
		id := proto.Id

		_, ok := result.aggs[id]
		if ok {
			return nil, fmt.Errorf("duplicate aggregator id: %s", id)
		}

		agg, err := newAggregator(proto.Name)
		if err != nil {
			return nil, err
		}

		err = agg.SetBytes(proto.Value)
		if err != nil {
			return nil, err
		}

		result.aggs[id] = agg
	}

	return result, nil
}

func (set *AggregatorSet) GetValue(id string) (interface{}, bool) {
	set.mutex.RLock()
	defer set.mutex.RUnlock()

	agg, ok := set.aggs[id]
	if !ok {
		return nil, false
	}

	return agg.Get(), true
}

func (set *AggregatorSet) SetValue(id string, value interface{}) error {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	agg, ok := set.aggs[id]
	if !ok {
		return fmt.Errorf("aggregator not found: %s", id)
	}

	err := agg.Set(value)
	if err != nil {
		return err
	}

	return nil
}

func (set *AggregatorSet) Add(aggName string, id string, value interface{}) error {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	_, ok := set.aggs[id]
	if ok {
		return fmt.Errorf("duplicate aggregator id: %s", id)
	}

	agg, err := newAggregator(aggName)
	if err != nil {
		return err
	}

	err = agg.Set(value)
	if err != nil {
		return err
	}

	set.aggs[id] = agg
	return nil
}

func (set *AggregatorSet) Remove(id string) error {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	_, ok := set.aggs[id]
	if ok {
		return fmt.Errorf("aggregator not found: %s", id)
	}

	delete(set.aggs, id)
	return nil
}

func (set *AggregatorSet) Clear() {
	set.mutex.Lock()
	defer set.mutex.Unlock()

	set.aggs = make(map[string]Aggregator)
}

func (set *AggregatorSet) Contains(id string) bool {
	set.mutex.RLock()
	defer set.mutex.RUnlock()
	_, ok := set.aggs[id]
	return ok
}

func UnionSets(first AggregatorSet, second AggregatorSet) (*AggregatorSet, error) {
	first.mutex.RLock()
	defer first.mutex.RUnlock()
	second.mutex.RLock()
	defer second.mutex.RUnlock()

	// clone the first set
	result := make(map[string]Aggregator, len(first.aggs))
	for k, agg := range first.aggs {
		result[k] = agg.Clone()
	}

	// merge second set into the clone
	for id, secondAgg := range second.aggs {
		firstAgg, ok := result[id]
		if ok {
			if firstAgg.Name() != secondAgg.Name() {
				// found incompatible aggregators
				return nil, fmt.Errorf("union failed - sets contain different aggregator types for id: %s", id)
			}

			// aggregate values
			firstAgg.Set(secondAgg.Get())
		} else {
			result[id] = secondAgg.Clone()
		}
	}

	return &AggregatorSet{aggs: result}, nil
}

func ConvertSetToProto(set *AggregatorSet) ([]*protos.Aggregator, error) {
	set.mutex.RLock()
	defer set.mutex.RUnlock()

	result := []*protos.Aggregator{}

	for id, agg := range set.aggs {
		bytes, err := agg.GetBytes()
		if err != nil {
			return nil, errors.Wrapf(err, "aggregator id: %s", id)
		}

		result = append(result, &protos.Aggregator{Name: agg.Name(), Id: id, Value: bytes})
	}

	return result, nil
}
