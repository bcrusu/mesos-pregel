package algorithmImpl

import "github.com/bcrusu/mesos-pregel/executor/algorithm"

func init() {
	algorithm.RegisterAlgorithm("ShortestPath", &shortestPathAlgorithmFactory{})
}
