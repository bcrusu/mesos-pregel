package job

import (
	"sync"
	"time"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/algorithm"
	"github.com/bcrusu/mesos-pregel/scheduler/job/task"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
)

const (
	maxRunningJobs      = 10
	defaultTaskCPU      = 0.1
	defaultTaskMEM      = 32.0
	defaultTaskVertices = 10000
	minTaskVertices     = 1000
)

type Manager struct {
	jobStore          store.JobStore
	checkpointEncoder encoding.Encoder
	mutex             sync.RWMutex
	jobs              map[string]*pregel.Job
	waitingQueue      []string
	runningQueue      []string
	taskManagers      map[string]*task.Manager
}

type Resources interface {
	CanSubtract(cpus float64, mems float64) bool
	Subtract(cpus float64, mems float64) bool
}

func NewManager(jobStore store.JobStore) (*Manager, error) {
	result := &Manager{
		jobStore:          jobStore,
		jobs:              make(map[string]*pregel.Job),
		waitingQueue:      []string{},
		runningQueue:      []string{},
		taskManagers:      make(map[string]*task.Manager),
		checkpointEncoder: encoding.NewProtobufEncoder(func() proto.Message { return new(protos.JobCheckpoint) }),
	}

	err := result.initFromStore()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (m *Manager) CreateJob(request *protos.CreateJobRequest) *protos.CreateJobReply {
	id := uuid.NewRandom().String()
	job := &pregel.Job{
		ID:                id,
		Label:             request.Label,
		CreationTime:      time.Now(),
		Status:            pregel.JobCreated,
		Store:             request.Store,
		StoreParams:       request.StoreParams,
		Algorithm:         request.Algorithm,
		AlgorithmParams:   request.AlgorithmParams,
		TaskCPU:           request.TaskCPU,
		TaskMEM:           request.TaskMEM,
		TaskVertices:      int(request.TaskVertices),
		TaskTimeout:       int(request.TaskTimeout),
		TaskMaxRetryCount: int(request.TaskMaxRetryCount),
	}

	err := m.jobStore.Save(job)
	if err != nil {
		return &protos.CreateJobReply{Status: protos.CallStatus_INTERNAL_ERROR}
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.jobs[id] = job
	m.waitingQueue = append(m.waitingQueue, id)

	return &protos.CreateJobReply{JobId: id, Status: protos.CallStatus_OK}
}

func (m *Manager) GetJobStatus(request *protos.JobIdRequest) *protos.GetJobStatusReply {
	id := request.JobId

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	job, ok := m.jobs[id]
	if !ok {
		return &protos.GetJobStatusReply{Status: protos.CallStatus_ERROR_INVALID_JOB}
	}

	var superstep int
	var percentDone int
	if taskManager, ok := m.taskManagers[id]; ok {
		superstep = taskManager.Superstep()
		percentDone = taskManager.PercentDone()
	}

	return &protos.GetJobStatusReply{
		Status:      protos.CallStatus_OK,
		JobStatus:   convertJobStatusToProto(job.Status),
		Superstep:   int32(superstep),
		PercentDone: int32(percentDone),
	}
}

func (m *Manager) GetJobResult(request *protos.JobIdRequest) *protos.GetJobResultReply {
	//TODO
	return nil
}

func (m *Manager) GetTasksToExecute(host string, resources Resources) []*protos.ExecTaskParams {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	startTasks := func(jobID string, onlyLocalTasks bool) ([]*protos.ExecTaskParams, error) {
		job := m.jobs[jobID]
		taskManager := m.taskManagers[jobID]

		result := []*protos.ExecTaskParams{}
		for resources.CanSubtract(job.TaskCPU, job.TaskMEM) {
			var task *task.StartTaskResult
			var err error

			if onlyLocalTasks {
				task, err = taskManager.StartTaskForHost(host)
			} else {
				task, err = taskManager.StartTask()
			}

			if err != nil {
				return nil, errors.Wrapf(err, "faild to start task for job %s", jobID)
			}

			// stop if no more tasks to start
			if task == nil {
				break
			}

			aggregators, err := aggregator.ConvertSetToProto(task.Aggregators)
			if err != nil {
				return nil, errors.Wrapf(err, "faild to marshal aggregator values for job %s", jobID)
			}

			result = append(result, &protos.ExecTaskParams{
				JobId:           jobID,
				TaskId:          int32(task.TaskID),
				Store:           job.Store,
				StoreParams:     job.StoreParams,
				Algorithm:       job.Algorithm,
				AlgorithmParams: job.AlgorithmParams,
				SuperstepParams: &protos.ExecSuperstepParams{
					Superstep:    int32(task.Superstep),
					VertexRanges: convertVertexRangesToProto(task.VertexRanges),
					Aggregators:  aggregators,
				},
			})

			resources.Subtract(job.TaskCPU, job.TaskMEM)
		}

		return result, nil
	}

	result := []*protos.ExecTaskParams{}

	// first iteration: data locality has priority
	for _, jobID := range m.runningQueue {
		tasks, err := startTasks(jobID, true)
		if err != nil {
			//TODO: stop job on error
		}

		result = append(result, tasks...)
	}

	// second iteration: distribute the remaining resources disregarding data locality
	for _, jobID := range m.runningQueue {
		tasks, err := startTasks(jobID, false)
		if err != nil {
			//TODO: stop job on error
		}

		result = append(result, tasks...)
	}

	return result
}

func (m *Manager) CancelJob(request *protos.JobIdRequest) *protos.SimpleCallReply {
	id := request.JobId

	// verify that the task exists and that it can be cancelled (read lock)
	m.mutex.RLock()
	job, ok := m.jobs[id]
	if !ok || !job.CanCancel() {
		m.mutex.RUnlock()
		return &protos.SimpleCallReply{Status: protos.CallStatus_ERROR_INVALID_JOB}
	}
	m.mutex.RUnlock()

	// update store (no lock)
	err := m.jobStore.SetStatus(id, pregel.JobCancelled)
	if err != nil {
		return &protos.SimpleCallReply{Status: protos.CallStatus_INTERNAL_ERROR}
	}

	// update state if job was not changed (write lock)
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if job.CanCancel() {
		switch job.Status {
		case pregel.JobCreated:
			m.waitingQueue = removeFromQueue(m.waitingQueue, id)
		case pregel.JobRunning:
			m.runningQueue = removeFromQueue(m.runningQueue, id)
			delete(m.taskManagers, id)

			//TODO(optional): call Scheduler.KillTask
		}

		job.Status = pregel.JobCancelled
	}

	return &protos.SimpleCallReply{Status: protos.CallStatus_OK}
}

func (m *Manager) initFromStore() error {
	jobs, err := m.jobStore.LoadAll()
	if err != nil {
		return err
	}

	sortJobsByCreationTime(jobs)

	for _, job := range jobs {
		id := job.ID
		m.jobs[id] = job

		switch job.Status {
		case pregel.JobCreated:
			m.waitingQueue = append(m.waitingQueue, id)
		case pregel.JobRunning:
			checkpoint, err := m.loadCheckpoint(id)
			if err != nil {
				glog.Warningf("failed to load checkpoint for job %s", id)
				continue
			}

			if checkpoint == nil {
				m.waitingQueue = append(m.waitingQueue, id)
				continue
			}

			m.runningQueue = append(m.runningQueue, id)
			m.taskManagers[id] = task.NewFromCheckpoint(checkpoint)
		}
	}

	return nil
}

func (m *Manager) loadCheckpoint(jobID string) (*protos.JobCheckpoint, error) {
	bytes, err := m.jobStore.LoadCheckpoint(jobID)
	if err != nil {
		return nil, err
	}

	if bytes == nil {
		return nil, nil
	}

	checkpoint, err := m.checkpointEncoder.Unmarshal(bytes)
	if err != nil {
		return nil, err
	}

	return checkpoint.(*protos.JobCheckpoint), nil
}

func (m *Manager) saveCheckpoint(jobID string, checkpoint *protos.JobCheckpoint) error {
	bytes, err := m.checkpointEncoder.Marshal(checkpoint)
	if err != nil {
		return err
	}

	return m.jobStore.SaveCheckpoint(jobID, bytes)
}

func connectGraphStore(name string, params []byte) (store.GraphStore, error) {
	graphStore, err := store.New(name, params)
	if err != nil {
		return nil, err
	}

	err = graphStore.Connect()
	if err != nil {
		return nil, err
	}

	err = graphStore.Init()
	if err != nil {
		return nil, err
	}

	return graphStore, nil
}

func getVertexRanges(graphStore store.GraphStore, taskVertices int) ([]*store.VertexRangeHosts, error) {
	if taskVertices == 0 {
		taskVertices = defaultTaskVertices
	} else if taskVertices < minTaskVertices {
		taskVertices = minTaskVertices
	}

	return graphStore.GetVertexRanges(taskVertices)
}

func getAlgorithm(name string, params []byte) (algorithm.Algorithm, error) {
	algorithm, err := algorithm.NewAlgorithm(name, params)
	if err != nil {
		return nil, err
	}

	//TODO algorithm.Init()

	return algorithm, nil
}

func startJob(jobID string) error {
	// graphStore, err := connectGraphStore(request.Store, request.StoreParams)
	// if err != nil {
	// 	return &protos.CreateJobReply{Status: protos.CallStatus_ERROR_INVALID_STORE}
	// }
	// defer graphStore.Close()

	// vertexRanges, err := getVertexRanges(graphStore, int(request.VerticesPerTask))
	// if err != nil {
	// 	return &protos.CreateJobReply{Status: protos.CallStatus_INTERNAL_ERROR}
	// }

	// algorithm, err = getAlgorithm(request.Algorithm, request.AlgorithmParams)
	// if err != nil {
	// 	return &protos.CreateJobReply{Status: protos.CallStatus_ERROR_INVALID_ALGORITHM}
	// }

	return nil
}

func removeFromQueue(queue []string, jobID string) []string {
	var index *int
	for i, id := range queue {
		if jobID == id {
			index = &i
			break
		}
	}

	if index == nil {
		glog.Warningf("queue does not contain job %s", jobID)
		return queue
	}

	return append(queue[:*index], queue[*index+1:]...)
}

func convertJobStatusToProto(status pregel.JobStatus) protos.JobStatus {
	switch status {
	case pregel.JobCreated:
		return protos.JobStatus_Created
	case pregel.JobRunning:
		return protos.JobStatus_Running
	case pregel.JobCompleted:
		return protos.JobStatus_Completed
	case pregel.JobCancelled:
		return protos.JobStatus_Cancelled
	case pregel.JobFailed:
		return protos.JobStatus_Failed
	default:
		panic("unknown job status")
	}
}

func convertVertexRangesToProto(vranges []store.VertexRange) [][]byte {
	result := make([][]byte, len(vranges))
	for i, vrange := range vranges {
		result[i] = []byte(vrange)
	}
	return result
}
