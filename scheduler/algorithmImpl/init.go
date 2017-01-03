package algorithmImpl

import "github.com/bcrusu/mesos-pregel/scheduler/algorithm"

func init() {
	algorithm.RegisterAlgorithm("ShortestPath", &shortestPathAlgorithmFactory{})
}
