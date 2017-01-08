package jobManager

import (
	"sync"
	"time"

	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/bcrusu/mesos-pregel/util"
)

const (
	defaultTaskTimeout = 30 * time.Second
	defaultTaskCPU     = 0.1
	defaultTaskMEM     = 32
	noHostID           = 0
)

type jobTasks struct {
	taskTimeout         time.Duration
	taskMaxRetryCount   int
	mutex               sync.Mutex // guards all below
	currentSuperstep    int
	currentAggregators  *aggregator.AggregatorSet
	previousAggregators *aggregator.AggregatorSet
	ranges              map[int]*rangeInfo // map[RANGE_ID]value
	hosts               *hostSet
	waitingByHost       map[int]*util.IntSet // map[HOST_ID]Set<RANGE_ID>
	lastTaskID          int
	runningTasks        map[int]*runningTask   // map[TASK_ID]value
	completedTasks      map[int]*completedTask // map[TASK_ID]value
}

type rangeInfo struct {
	vertexRange store.VertexRange
	hosts       []int // List<HOST_ID>
	retryCount  int
}

type runningTask struct {
	rangeID   int //TODO: group multiple small ranges into groups to be scheduled together in the same task
	startTime time.Time
}

type completedTask struct {
}

type startTaskResult struct {
	taskID       int
	superstep    int
	vertexRanges []store.VertexRange
	aggregators  *aggregator.AggregatorSet
}

func newJobTasks(vertexRanges []*store.VertexRangeHosts, taskTimeout int, taskMaxRetryCount int) *jobTasks {
	hosts := &hostSet{}
	ranges := make(map[int]*rangeInfo)

	for i, item := range vertexRanges {
		rangeID := i + 1
		rangeHosts := []int{}

		for _, host := range item.Hosts {
			rangeHosts = append(rangeHosts, hosts.Add(host))
		}

		ranges[rangeID] = &rangeInfo{
			vertexRange: item.Range,
			hosts:       rangeHosts,
		}
	}

	return &jobTasks{
		taskTimeout:         time.Duration(taskTimeout),
		taskMaxRetryCount:   taskMaxRetryCount,
		currentSuperstep:    1,
		currentAggregators:  aggregator.NewSet(),
		previousAggregators: aggregator.NewSet(),
		ranges:              ranges,
		hosts:               hosts,
		waitingByHost:       groupByHost(ranges),
		runningTasks:        make(map[int]*runningTask),
		completedTasks:      make(map[int]*completedTask),
	}
}

func (jt *jobTasks) StartTaskForHost(host string) (*startTaskResult, error) {
	jt.mutex.Lock()
	defer jt.mutex.Unlock()

	if err := jt.cancelTimedOutTasks(); err != nil {
		return nil, err
	}

	hostID, ok := jt.hosts.GetID(host)
	if !ok {
		return nil, nil
	}

	return jt.startTaskForHostID(hostID)
}

func (jt *jobTasks) StartTask() (*startTaskResult, error) {
	jt.mutex.Lock()
	defer jt.mutex.Unlock()

	if err := jt.cancelTimedOutTasks(); err != nil {
		return nil, err
	}

	for hostID := range jt.waitingByHost {
		result, err := jt.startTaskForHostID(hostID)
		if result != nil || err != nil {
			return result, err
		}
	}

	return nil, nil
}

func (jt *jobTasks) startTaskForHostID(hostID int) (*startTaskResult, error) {
	waiting, ok := jt.waitingByHost[hostID]
	if !ok {
		return nil, nil
	}

	//TODO: select the next by taking into account the no. of failures in the past - exponential back off?
	rangeID, ok := waiting.RemoveFirst()
	if !ok {
		return nil, nil
	}

	rangeInfo := jt.ranges[rangeID]
	taskID := jt.getNextTaskID()

	jt.runningTasks[taskID] = &runningTask{
		rangeID:   rangeID,
		startTime: time.Now(),
	}

	return &startTaskResult{
		taskID:       taskID,
		superstep:    jt.currentSuperstep,
		aggregators:  jt.previousAggregators,
		vertexRanges: []store.VertexRange{rangeInfo.vertexRange},
	}, nil
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

func (jt *jobTasks) getNextTaskID() int {
	jt.lastTaskID++
	return jt.lastTaskID
}

func (jt *jobTasks) cancelTimedOutTasks() error {
	now := time.Now()
	cancelled := []int{}

	for taskID, runningTask := range jt.runningTasks {
		deadline := runningTask.startTime.Add(jt.taskTimeout)
		if deadline.After(now) {
			continue
		}

		rangeID := runningTask.rangeID
		rangeInfo := jt.ranges[rangeID]
		rangeInfo.retryCount++

		if rangeInfo.retryCount == jt.taskMaxRetryCount {
			return nil //TODO: propagate error and stop the job
		}

		// add back to waiting pool
		for _, hostID := range rangeInfo.hosts {
			jt.waitingByHost[hostID].Add(rangeID)
		}

		cancelled = append(cancelled, taskID)
	}

	for _, taskID := range cancelled {
		delete(jt.runningTasks, taskID)
	}

	return nil
}

func groupByHost(ranges map[int]*rangeInfo) map[int]*util.IntSet {
	result := make(map[int]*util.IntSet)

	getForHost := func(hostID int) *util.IntSet {
		set, ok := result[hostID]
		if !ok {
			set = &util.IntSet{}
			result[hostID] = set
		}

		return set
	}

	for rangeID, rangeInfo := range ranges {
		if len(rangeInfo.hosts) == 0 {
			getForHost(noHostID).Add(rangeID)
			continue
		}

		for _, hostID := range rangeInfo.hosts {
			getForHost(hostID).Add(rangeID)
		}
	}

	return result
}
