package aggregator

type ImmutableAggregatorSet struct {
	aggs map[string]Aggregator
}

func (set *ImmutableAggregatorSet) GetValue(id string) (interface{}, bool) {
	agg, ok := set.aggs[id]
	if !ok {
		return nil, false
	}

	return agg.Get(), true
}
func (set *ImmutableAggregatorSet) Contains(id string) bool {
	_, ok := set.aggs[id]
	return ok
}

func (set *ImmutableAggregatorSet) AsMutable() *AggregatorSet {
	aggs := make(map[string]Aggregator)

	for id, agg := range set.aggs {
		aggs[id] = agg.Clone()
	}

	return &AggregatorSet{aggs: aggs}
}
