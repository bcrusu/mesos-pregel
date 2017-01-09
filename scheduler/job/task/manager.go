package task

import (
	"fmt"
	"sync"
	"time"

	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/bcrusu/mesos-pregel/util"
	"github.com/golang/glog"
)

const (
	defaultTaskTimeout = 30 * time.Second //TODO:
	noHostID           = 0
)

type Manager struct {
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

type StartTaskResult struct {
	TaskID       int
	Superstep    int
	VertexRanges []store.VertexRange
	Aggregators  *aggregator.AggregatorSet
}

type SetTaskFinishedRequest struct {
	TaskID      int
	Aggregators *aggregator.AggregatorSet

	TotalDuration     time.Duration
	ComputedCount     int
	ComputeDuration   time.Duration
	SentMessagesCount int
	HaltedCount       int
	InactiveCount     int
}

type rangeInfo struct {
	vertexRange store.VertexRange
	hosts       []int // List<HOST_ID>
	retryCount  int
}

type runningTask struct {
	rangeID   int //TODO: group multiple small ranges into groups to be scheduled together in the same task
	hostID    int
	startTime time.Time
}

type completedTask struct {
}

func New(vertexRanges []*store.VertexRangeHosts, taskTimeout int, taskMaxRetryCount int) *Manager {
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

	return &Manager{
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

func NewFromCheckpoint(checkpoint *protos.JobCheckpoint) *Manager {
	//TODO
	return nil
}

func (m *Manager) Superstep() int {
	return m.currentSuperstep
}

func (m *Manager) StartTaskForHost(host string) (*StartTaskResult, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if err := m.cancelTimedOutTasks(); err != nil {
		return nil, err
	}

	hostID, ok := m.hosts.GetID(host)
	if !ok {
		return nil, nil
	}

	return m.startTaskForHostID(hostID)
}

func (m *Manager) StartTask() (*StartTaskResult, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if err := m.cancelTimedOutTasks(); err != nil {
		return nil, err
	}

	for hostID := range m.waitingByHost {
		result, err := m.startTaskForHostID(hostID)
		if result != nil || err != nil {
			return result, err
		}
	}

	return nil, nil
}

func (m *Manager) SetTaskFinished(request *SetTaskFinishedRequest) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	//TODO
}

func (m *Manager) GetCheckpoint() *protos.JobCheckpoint {
	//TODO
	return nil
}

func (m *Manager) startTaskForHostID(hostID int) (*StartTaskResult, error) {
	waiting, ok := m.waitingByHost[hostID]
	if !ok {
		return nil, nil
	}

	//TODO: select the next by taking into account the no. of failures in the past - exponential back off?
	rangeID, ok := waiting.RemoveOne()
	if !ok {
		return nil, nil
	}

	rangeInfo := m.ranges[rangeID]
	taskID := m.getNextTaskID()

	m.runningTasks[taskID] = &runningTask{
		rangeID:   rangeID,
		startTime: time.Now(),
	}

	return &StartTaskResult{
		TaskID:       taskID,
		Superstep:    m.currentSuperstep,
		Aggregators:  m.previousAggregators,
		VertexRanges: []store.VertexRange{rangeInfo.vertexRange},
	}, nil
}

func (m *Manager) PercentDone() int {
	//TODO
	return 0
}

func (m *Manager) getNextTaskID() int {
	m.lastTaskID++
	return m.lastTaskID
}

func (m *Manager) cancelTimedOutTasks() error {
	now := time.Now()
	cancelled := []int{}

	for taskID, runningTask := range m.runningTasks {
		deadline := runningTask.startTime.Add(m.taskTimeout)
		if deadline.After(now) {
			continue
		}

		hostname, _ := m.hosts.GetHostname(runningTask.hostID)
		glog.Warningf("task %d timed out on host %s", taskID, hostname)

		rangeID := runningTask.rangeID
		rangeInfo := m.ranges[rangeID]
		rangeInfo.retryCount++

		if rangeInfo.retryCount == m.taskMaxRetryCount {
			return fmt.Errorf("failed to execute task %d - exceeded max retry count", taskID)
		}

		// add back to waiting pool
		for _, hostID := range rangeInfo.hosts {
			m.waitingByHost[hostID].Add(rangeID)
		}

		cancelled = append(cancelled, taskID)
	}

	for _, taskID := range cancelled {
		delete(m.runningTasks, taskID)
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
