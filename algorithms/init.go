package algorithms

import "github.com/bcrusu/mesos-pregel/algorithm"

const (
	ShortestPath = "ShortestPath"
	Count        = "Count"
)

func init() {
	algorithm.Register(ShortestPath, &shortestPathAlgorithmFactory{})
	algorithm.Register(Count, &countAlgorithmFactory{})
}
