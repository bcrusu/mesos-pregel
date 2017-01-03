package cassandra

import (
	"fmt"
)

type TokenRangeSplitter interface {
	SplitTokenRanges(ranges []*TokenRange, itemsCountPerRange int, totalItemsCount int) []*TokenRange
}

type murmur3TokenRangeSplitter struct {
}

type randomTokenRangeSplitter struct {
}

func NewTokenRangeSplitter(partitioner Partitioner) (TokenRangeSplitter, error) {
	if partitioner == nil {
		return nil, fmt.Errorf("nil partitioner provided", partitioner.Name())
	}

	_, ok := partitioner.(murmur3Partitioner)
	if ok {
		return &murmur3TokenRangeSplitter{}, nil
	}

	_, ok = partitioner.(randomPartitioner)
	if ok {
		return &randomTokenRangeSplitter{}, nil
	}

	return nil, fmt.Errorf("unrecognized partitioner %s", partitioner.Name())
}

func (splitter *murmur3TokenRangeSplitter) SplitTokenRanges(ranges []*TokenRange, itemsCountPerRange int, totalItemsCount int) []*TokenRange {
	//TODO
	return ranges
}

func (splitter *randomTokenRangeSplitter) SplitTokenRanges(ranges []*TokenRange, itemsCountPerRange int, totalItemsCount int) []*TokenRange {
	//TODO
	return ranges
}
