package task

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/bcrusu/mesos-pregel/util"
	"github.com/golang/glog"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
)

const (
	noHostID = 0
)

type Manager struct {
	jobID       string
	ranges      map[int]*rangeInfo // map[RANGE_ID]value
	hosts       *hostSet
	taskTimeout time.Duration

	mutex               sync.Mutex // guards all below
	jobCompleted        bool
	totalStats          *Stats
	superstep           *superstepInfo
	previousAggregators []*protos.Aggregator
	waitingByHost       map[int]*util.IntSet // map[HOST_ID]Set<RANGE_ID>
	tasks               map[string]*taskInfo // map[TASK_ID]value
}

type StartTaskResult struct {
	TaskID          string
	SuperstepParams *protos.ExecSuperstepParams
}

type rangeInfo struct {
	vertexRange store.VertexRange
	hosts       []int // List<HOST_ID>
	//TODO: record last host the range completed on
}

type taskInfo struct {
	rangeID   int
	hostID    int
	startTime time.Time
}

type superstepInfo struct {
	number      int
	aggregators *aggregator.AggregatorSet
	stats       *Stats
	completed   *util.IntSet // Set<RANGE_ID>
}

func NewManager(jobID string, vertexRanges []*store.VertexRangeHosts, taskTimeout time.Duration) *Manager {
	ranges, hosts := processVertexRanges(vertexRanges)

	aggregators := aggregator.NewSet()
	aggregatorsProto, _ := aggregator.ConvertSetToProto(aggregators)

	return &Manager{
		jobID:               jobID,
		taskTimeout:         time.Duration(taskTimeout),
		totalStats:          &Stats{},
		superstep:           newSuperstepInfo(1, aggregators),
		previousAggregators: aggregatorsProto,
		ranges:              ranges,
		hosts:               hosts,
		waitingByHost:       groupRangesByHost(ranges),
		tasks:               make(map[string]*taskInfo),
	}
}

func NewManagerFromCheckpoint(job *pregel.Job, checkpoint *protos.JobCheckpoint) *Manager {
	completedRanges := &util.IntSet{}
	for _, rangeID := range checkpoint.Superstep.Completed {
		completedRanges.Add(int(rangeID))
	}

	allranges := make(map[int]*rangeInfo)
	waitingRanges := make(map[int]*rangeInfo)
	for _, r := range checkpoint.Ranges {
		rangeID := int(r.Id)
		info := &rangeInfo{
			vertexRange: r.VertexRange,
			hosts:       util.ConvertSliceFromInt32ToInt(r.Hosts),
		}

		allranges[rangeID] = info

		if !completedRanges.Contains(rangeID) {
			waitingRanges[rangeID] = info
		}
	}

	tasks := make(map[string]*taskInfo)
	for _, task := range checkpoint.Tasks {
		tasks[task.Id] = &taskInfo{
			rangeID:   int(task.RangeId),
			hostID:    int(task.HostId),
			startTime: time.Unix(task.StartTime, 0),
		}
	}

	aggregators, _ := aggregator.NewSetFromMessages(checkpoint.Superstep.Aggregators)

	return &Manager{
		jobID:       job.ID,
		taskTimeout: time.Duration(job.TaskTimeout),
		totalStats:  (&Stats{}).fromProto(checkpoint.TotalStats),
		superstep: &superstepInfo{
			number:      int(checkpoint.Superstep.Number),
			aggregators: aggregators,
			completed:   completedRanges,
			stats:       (&Stats{}).fromProto(checkpoint.Superstep.Stats),
		},
		previousAggregators: checkpoint.PreviousAggregators,
		ranges:              allranges,
		hosts:               (&hostSet{}).fromProto(checkpoint.Hosts),
		waitingByHost:       groupRangesByHost(waitingRanges),
		tasks:               tasks,
	}
}

func (m *Manager) JobID() string {
	return m.jobID
}

func (m *Manager) Superstep() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.superstep.number
}

func (m *Manager) Aggregators() *aggregator.AggregatorSet {
	return m.superstep.aggregators
}

func (m *Manager) StartTask(hostname string, onlyLocal bool) *StartTaskResult {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.jobCompleted {
		return nil
	}

	m.checkTimedOutTasks()

	hostID, ok := m.hosts.GetID(hostname)
	if !ok {
		hostID = m.hosts.Add(hostname)
	}

	if onlyLocal {
		return m.startTaskForHostID(hostID)
	}

	//TODO(nice to have): order the hosts by taking into account the no. of failures AND past performance drawn from stats
	for hostID := range m.waitingByHost {
		if result := m.startTaskForHostID(hostID); result != nil {
			return result
		}
	}

	return nil
}

func (m *Manager) SetTaskCompleted(taskID string, result *protos.ExecSuperstepResult) (jobCompleted bool, err error) {
	// defensive implementation - Mesos delivers status updates 'at least once'
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.jobCompleted {
		return true, nil
	}

	_, rangeID, ok := m.validateTaskID(taskID)
	if !ok {
		// ignore results from invalid tasks
		return false, nil
	}

	if m.superstep.completed.Contains(rangeID) {
		// ignore duplicate results
		return false, nil
	}

	aggregators, err := aggregator.NewSetFromMessages(result.Aggregators)
	if err != nil {
		return false, errors.Wrapf(err, "faild to unmarshal aggregators for task %d", taskID)
	}

	if err = m.superstep.aggregators.UnionWith(aggregators); err != nil {
		return false, errors.Wrapf(err, "faild to merge aggregators for task %d", taskID)
	}

	m.superstep.stats.add(result.Stats)
	m.superstep.completed.Add(rangeID)
	m.totalStats.add(result.Stats)

	m.removeFromWaiting(rangeID)
	delete(m.tasks, taskID)

	m.advanceSuperstep()
	return m.jobCompleted, nil
}

func (m *Manager) SetTaskFailed(taskID string) {
	// defensive implementation - Mesos delivers status updates 'at least once'
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, rangeID, ok := m.validateTaskID(taskID)
	if !ok {
		// ignore results from invalid tasks
		return
	}

	glog.Infof("job %s - task %d failed", m.jobID, taskID)

	m.addToWaiting(rangeID)

	task, ok := m.tasks[taskID]
	if !ok {
		return
	}

	delete(m.tasks, taskID)

	if host, ok := m.hosts.Get(task.hostID); ok {
		host.failCount++
	}
}

func (m *Manager) GetCheckpoint() *protos.JobCheckpoint {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	ranges := []*protos.JobCheckpoint_Range{}
	for rangeID, rangeInfo := range m.ranges {
		ranges = append(ranges, &protos.JobCheckpoint_Range{
			Id:          int32(rangeID),
			VertexRange: rangeInfo.vertexRange,
			Hosts:       util.ConvertSliceFromIntToInt32(rangeInfo.hosts),
		})
	}

	tasks := []*protos.JobCheckpoint_Task{}
	for taskID, task := range m.tasks {
		tasks = append(tasks, &protos.JobCheckpoint_Task{
			Id:        taskID,
			RangeId:   int32(task.rangeID),
			HostId:    int32(task.hostID),
			StartTime: task.startTime.Unix(),
		})
	}

	aggregatorsProto, _ := aggregator.ConvertSetToProto(m.superstep.aggregators)

	return &protos.JobCheckpoint{
		Ranges:              ranges,
		Hosts:               m.hosts.toProto(),
		Tasks:               tasks,
		PreviousAggregators: m.previousAggregators,
		TotalStats:          m.totalStats.toProto(),
		Superstep: &protos.JobCheckpoint_Superstep{
			Number:      int32(m.superstep.number),
			Aggregators: aggregatorsProto,
			Completed:   util.ConvertSliceFromIntToInt32(m.superstep.completed.ToArray()),
			Stats:       m.superstep.stats.toProto(),
		},
	}
}

func (m *Manager) startTaskForHostID(hostID int) *StartTaskResult {
	waiting, ok := m.waitingByHost[hostID]
	if !ok {
		return nil
	}

	rangeID, ok := waiting.RemoveOne()
	if !ok {
		return nil
	}

	rangeInfo := m.ranges[rangeID]
	taskID := getTaskID(m.superstep.number, rangeID)

	m.tasks[taskID] = &taskInfo{
		rangeID:   rangeID,
		startTime: time.Now(),
		hostID:    hostID,
	}

	return &StartTaskResult{
		TaskID: taskID,
		SuperstepParams: &protos.ExecSuperstepParams{
			Superstep:   int32(m.superstep.number),
			VertexRange: []byte(rangeInfo.vertexRange),
			Aggregators: m.previousAggregators,
		},
	}
}

func (m *Manager) checkTimedOutTasks() {
	timedOut := make(map[string]*taskInfo)
	now := time.Now()

	for taskID, task := range m.tasks {
		deadline := task.startTime.Add(m.taskTimeout)
		if deadline.After(now) {
			continue
		}

		if hostname, ok := m.hosts.GetHostname(task.hostID); ok {
			glog.Infof("job %s - task %d timed out on host %s", m.jobID, taskID, hostname)
		} else {
			glog.Infof("job %s - task %d timed out", m.jobID, taskID)
		}

		timedOut[taskID] = task
	}

	for taskID, task := range timedOut {
		m.addToWaiting(task.rangeID)
		delete(m.tasks, taskID)

		if host, ok := m.hosts.Get(task.hostID); ok {
			host.failCount++
		}
	}
}

func (m *Manager) advanceSuperstep() (jobCompleted bool) {
	if m.jobCompleted {
		return true
	}

	current := m.superstep
	if current.completed.Count() != len(m.ranges) {
		return false
	}

	// mark as completed when all computed vertices in the current superstep voted to halt
	if current.stats.ComputedCount == current.stats.HaltedCount {
		m.jobCompleted = true
		return true
	}

	m.superstep = &superstepInfo{
		number:      current.number + 1,
		completed:   &util.IntSet{},
		stats:       &Stats{},
		aggregators: current.aggregators,
	}

	aggregatorsProto, _ := aggregator.ConvertSetToProto(current.aggregators)
	m.previousAggregators = aggregatorsProto

	// add all ranges to waiting list
	m.waitingByHost = groupRangesByHost(m.ranges)
	// invalidate tasks from past superstep
	m.tasks = make(map[string]*taskInfo)

	return false
}

func (m *Manager) removeFromWaiting(rangeID int) {
	rangeInfo := m.ranges[rangeID]
	for _, host := range rangeInfo.hosts {
		m.waitingByHost[host].Remove(rangeID)
	}
}

func (m *Manager) addToWaiting(rangeID int) {
	rangeInfo := m.ranges[rangeID]
	for _, hostID := range rangeInfo.hosts {
		m.waitingByHost[hostID].Add(rangeID)
	}
}

func (m *Manager) validateTaskID(taskID string) (superstep int, rangeID int, ok bool) {
	superstep, rangeID, ok = parseTaskID(taskID)
	if !ok {
		glog.Warningf("job %s - ignoring malformed task %d", m.jobID, taskID)
		return 0, 0, false
	}

	if superstep != m.superstep.number {
		glog.Warningf("job %s, superstep %d - ignoring task %d from past superstep %d", m.jobID, m.superstep.number, taskID, superstep)
		return 0, 0, false
	}

	if _, ok := m.ranges[rangeID]; !ok {
		glog.Warningf("job %s - ignoring task %d for unknown range %d", m.jobID, taskID, rangeID)
		return 0, 0, false
	}

	return superstep, rangeID, true
}

func groupRangesByHost(ranges map[int]*rangeInfo) map[int]*util.IntSet {
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

func getTaskID(superstep int, rangeID int) string {
	r := uuid.NewRandom().String() // add random token to detect duplicate Mesos status updates
	return fmt.Sprintf("%d/%d/%s", superstep, rangeID, r)
}

func parseTaskID(taskID string) (int, int, bool) {
	splits := strings.Split(taskID, "/")
	if len(splits) != 3 {
		return 0, 0, false
	}

	superstep, err := strconv.Atoi(splits[0])
	if err != nil {
		return 0, 0, false
	}

	rangeID, err := strconv.Atoi(splits[1])
	if err != nil {
		return 0, 0, false
	}

	return superstep, rangeID, true
}

func processVertexRanges(vertexRanges []*store.VertexRangeHosts) (map[int]*rangeInfo, *hostSet) {
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

	return ranges, hosts
}

func newSuperstepInfo(superstep int, aggregators *aggregator.AggregatorSet) *superstepInfo {
	return &superstepInfo{
		number:      superstep,
		stats:       &Stats{},
		completed:   &util.IntSet{},
		aggregators: aggregators,
	}
}
