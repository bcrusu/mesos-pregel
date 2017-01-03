package cassandra

type tokenRangeSplitter interface {
	splitTokenRanges(ranges []*TokenRange, verticesPerRange int) ([]*TokenRange, error)
}

type murmur3TokenRangeSplitter struct {
}

type randomTokenRangeSplitter struct {
}

func splitTokenRanges(ranges []*TokenRange, verticesPerRange int) ([]*TokenRange, error) {
	//TODO
	return ranges, nil
}
