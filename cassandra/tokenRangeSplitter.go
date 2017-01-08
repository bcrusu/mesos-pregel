package cassandra

import (
	"math/big"

	"github.com/bcrusu/mesos-pregel/util"
)

// split assumes that data is spread out evenly
// similar approach to datastax/spark-cassandra-connector (https://github.com/datastax/spark-cassandra-connector/tree/master/spark-cassandra-connector/src/main/scala/com/datastax/spark/connector/rdd/partitioner)
func splitTokenRanges(partitioner Partitioner, tokenRanges []*TokenRange, rowCount int, rowsPerSplit int) []*TokenRange {
	rowCountFloat := float64(rowCount)
	rowsPerSplitFloat := float64(rowsPerSplit)

	min := partitioner.MinToken().toBigInt()
	max := partitioner.MaxToken().toBigInt()
	totalTokens := (&big.Int{}).Sub(max, min)
	totalTokensFloat := (&big.Float{}).SetInt(totalTokens)

	tokensInRange := func(start *big.Int, end *big.Int) *big.Int {
		count := (&big.Int{}).Sub(end, start)
		if start == min {
			count.Add(count, big.NewInt(1))
		}

		return count
	}

	ringFraction := func(tokenCount *big.Int) float64 {
		fraction := (&big.Float{}).Quo((&big.Float{}).SetInt(tokenCount), totalTokensFloat)
		result, _ := fraction.Float64()
		return result
	}

	type splitPair struct {
		start *big.Int
		end   *big.Int
	}

	splitRange := func(start *big.Int, end *big.Int) []*splitPair {
		tokensInRange := tokensInRange(start, end)
		ringFraction := ringFraction(tokensInRange)

		rowsInRange := rowCountFloat * ringFraction
		splitsCount := util.Round(rowsInRange / rowsPerSplitFloat)

		// too small to be split
		if splitsCount <= 1 {
			return []*splitPair{&splitPair{start, end}}
		}

		tokensPerSplit := (&big.Int{}).Div(tokensInRange, big.NewInt(int64(splitsCount)))
		result := []*splitPair{}

		prev := start
		for i := 1; i < splitsCount; i++ {
			next := (&big.Int{}).Add(prev, tokensPerSplit)
			result = append(result, &splitPair{prev, next})
			prev = next
		}
		result = append(result, &splitPair{prev, end})

		return result
	}

	result := []*TokenRange{}

	for _, trange := range tokenRanges {
		var splits []*splitPair

		start := trange.Start.toBigInt()
		end := trange.End.toBigInt()

		if trange.Start.Less(trange.End) {
			splits = splitRange(start, end)
		} else {
			splits = splitRange(start, max)
			splits = append(splits, splitRange(min, end)...)
		}

		for _, split := range splits {
			result = append(result, &TokenRange{
				Start:    partitioner.newToken(split.start),
				End:      partitioner.newToken(split.end),
				Replicas: trange.Replicas,
			})
		}
	}

	return result
}
