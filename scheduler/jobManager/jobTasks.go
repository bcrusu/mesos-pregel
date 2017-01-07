package jobManager

import (
	"sync"
	"time"

	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/store"
)

const (
	defaultTaskTimeout = 30 * time.Second
	defaultTaskCPU     = 0.1
	defaultTaskMEM     = 32
)

//TODO: rename
type jobTasks struct {
	jobID            string
	mutex            sync.Mutex
	currentSuperstep int
	completed        map[int]*task
	running          map[int]*task
	waiting          map[int]*task
	aggregators      *aggregator.AggregatorSet
}

type task struct {
	ID             int
	preferredHosts []string
	vertexRange    []byte
	startTime      time.Time
	completedTime  time.Time
}

func newJobTasks(jobID string, vertexRanges []*store.VertexRangeHosts) *jobTasks {
	taskMap := make(map[int]*task)
	for i, item := range vertexRanges {
		taskID := i + 1
		taskMap[taskID] = &task{
			ID:             taskID,
			vertexRange:    item.Range,
			preferredHosts: item.Hosts,
		}
	}

	return &jobTasks{jobID: jobID, waiting: taskMap}
}

func newJobTasksFromCheckpoint(checkpoint *protos.JobCheckpoint) *jobTasks {
	//TODO
	return nil
}

func (jt *jobTasks) getCheckpoint() *protos.JobCheckpoint {
	//TODO
	return nil
}

func (jt *jobTasks) getPercentDone() int {
	//TODO
	return 0
}

func (jt *jobTasks) GetTasksToExecute() []*protos.ExecTaskParams {
	//TODO
	return nil
}
