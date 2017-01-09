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
	"github.com/pkg/errors"
)

const (
	defaultTaskTimeout = 30 * time.Second
	noHostID           = 0
)

type Manager struct {
	jobID               string
	taskTimeout         time.Duration
	taskMaxRetryCount   int
	mutex               sync.Mutex // guards all below
	totalStats          *Stats
	currentSuperstep    int
	currentAggregators  *aggregator.AggregatorSet
	currentStats        *Stats
	previousAggregators []*protos.Aggregator
	ranges              map[int]*rangeInfo // map[RANGE_ID]value
	hosts               *hostSet
	waitingByHost       map[int]*util.IntSet // map[HOST_ID]Set<RANGE_ID>
	lastTaskID          int
	runningTasks        map[int]*runningTaskInfo // map[TASK_ID]value
}

type StartTaskResult struct {
	TaskID          int
	SuperstepParams *protos.ExecSuperstepParams
}

type rangeInfo struct {
	vertexRange store.VertexRange
	hosts       []int // List<HOST_ID>
	retryCount  int
}

type runningTaskInfo struct {
	rangeID   int //TODO(nice to have): group multiple small ranges into groups to be scheduled together in the same task
	hostID    int
	startTime time.Time
}

func NewManager(jobID string, vertexRanges []*store.VertexRangeHosts, initialAggregators *aggregator.AggregatorSet,
	taskTimeout time.Duration, taskMaxRetryCount int) (*Manager, error) {
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

	aggProto, err := aggregator.ConvertSetToProto(initialAggregators)
	if err != nil {
		return nil, errors.Wrap(err, "faild to marshal initial aggregators")
	}

	if taskTimeout == 0 {
		taskTimeout = defaultTaskTimeout
	}

	return &Manager{
		jobID:               jobID,
		taskTimeout:         time.Duration(taskTimeout),
		taskMaxRetryCount:   taskMaxRetryCount,
		totalStats:          &Stats{},
		currentSuperstep:    1,
		currentAggregators:  initialAggregators,
		currentStats:        &Stats{},
		previousAggregators: aggProto,
		ranges:              ranges,
		hosts:               hosts,
		waitingByHost:       groupByHost(ranges),
		runningTasks:        make(map[int]*runningTaskInfo),
	}, nil
}

func NewManagerFromCheckpoint(checkpoint *protos.JobCheckpoint) *Manager {
	//TODO
	return nil
}

func (m *Manager) Superstep() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
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

func (m *Manager) SetTaskCompleted(taskID int, result *protos.ExecSuperstepResult) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	aggregators, err := aggregator.NewSetFromMessages(result.Aggregators)
	if err != nil {
		return errors.Wrapf(err, "faild to unmarshal aggregators for task %d", taskID)
	}

	if err = m.currentAggregators.UnionWith(aggregators); err != nil {
		return errors.Wrapf(err, "faild to merge aggregators for task %d", taskID)
	}

	m.totalStats.add(result.Stats)
	m.currentStats.add(result.Stats)

	//TODO: advance to next superstep if current finished
	return nil
}

func (m *Manager) SetTaskFailed(taskID int) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	task, ok := m.runningTasks[taskID]
	if !ok {
		glog.Warningf("job %s - ignoring unknown task %d", m.jobID, taskID)
		return nil
	}

	if hostname, ok := m.hosts.GetHostname(task.hostID); ok {
		glog.Warningf("job %s - task %d failed on host %s", m.jobID, taskID, hostname)
	} else {
		glog.Warningf("job %s - task %d failed", m.jobID, taskID)
	}

	return m.retryTask(taskID, task)
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

	m.runningTasks[taskID] = &runningTaskInfo{
		rangeID:   rangeID,
		startTime: time.Now(),
		hostID:    hostID,
	}

	return &StartTaskResult{
		TaskID: taskID,
		SuperstepParams: &protos.ExecSuperstepParams{
			Superstep:    int32(m.currentSuperstep),
			VertexRanges: convertVertexRangesToProto([]store.VertexRange{rangeInfo.vertexRange}),
			Aggregators:  m.previousAggregators,
		},
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

	for taskID, task := range m.runningTasks {
		deadline := task.startTime.Add(m.taskTimeout)
		if deadline.After(now) {
			continue
		}

		if hostname, ok := m.hosts.GetHostname(task.hostID); ok {
			glog.Warningf("job %s - task %d timed out on host %s", m.jobID, taskID, hostname)
		} else {
			glog.Warningf("job %s - task %d timed out", m.jobID, taskID)
		}

		err := m.retryTask(taskID, task)
		if err != nil {
			return err
		}

		cancelled = append(cancelled, taskID)
	}

	for _, taskID := range cancelled {
		delete(m.runningTasks, taskID)
	}

	return nil
}

func (m *Manager) retryTask(taskID int, task *runningTaskInfo) error {
	rangeID := task.rangeID
	rangeInfo := m.ranges[rangeID]
	rangeInfo.retryCount++

	if rangeInfo.retryCount == m.taskMaxRetryCount {
		return fmt.Errorf("job %s - task %d exceeded the maximum number of retries allowed: %d", m.jobID, taskID, m.taskMaxRetryCount)
	}

	// add back to waiting pool
	for _, hostID := range rangeInfo.hosts {
		m.waitingByHost[hostID].Add(rangeID)
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

func convertVertexRangesToProto(vranges []store.VertexRange) [][]byte {
	result := make([][]byte, len(vranges))
	for i, vrange := range vranges {
		result[i] = []byte(vrange)
	}
	return result
}
