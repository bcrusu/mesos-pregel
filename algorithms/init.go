package algorithms

import "github.com/bcrusu/mesos-pregel/algorithm"

func init() {
	algorithm.Register("ShortestPath", &shortestPathAlgorithmFactory{})
}
