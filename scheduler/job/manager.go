package job

import (
	"sync"
	"time"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/algorithm"
	"github.com/bcrusu/mesos-pregel/scheduler/task"
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
	waitingQueue      *jobQueue
	runningQueue      *jobQueue
	taskManagers      map[string]*task.Manager
}

type Resources interface {
	CanSubtract(cpus float64, mems float64) bool
	Subtract(cpus float64, mems float64) bool
}

type TaskToExecute struct {
	Parmas *protos.ExecTaskParams
	CPU    float64
	MEM    float64
}

func NewManager(jobStore store.JobStore) (*Manager, error) {
	result := &Manager{
		jobStore:          jobStore,
		jobs:              make(map[string]*pregel.Job),
		waitingQueue:      &jobQueue{},
		runningQueue:      &jobQueue{},
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
	job := coerceJobParams(&pregel.Job{
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
	})

	err := m.jobStore.Save(job)
	if err != nil {
		return &protos.CreateJobReply{Status: protos.CallStatus_INTERNAL_ERROR}
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.jobs[id] = job
	m.waitingQueue.Add(id)

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

func (m *Manager) GetTasksToExecute(host string, resources Resources) []*TaskToExecute {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.startWaitingJobs()

	startTasks := func(jobID string, onlyLocalTasks bool) ([]*TaskToExecute, error) {
		job := m.jobs[jobID]
		taskManager := m.taskManagers[jobID]

		result := []*TaskToExecute{}
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

			result = append(result, &TaskToExecute{
				Parmas: &protos.ExecTaskParams{
					JobId:           jobID,
					TaskId:          int32(task.TaskID),
					Store:           job.Store,
					StoreParams:     job.StoreParams,
					Algorithm:       job.Algorithm,
					AlgorithmParams: job.AlgorithmParams,
					SuperstepParams: task.SuperstepParams,
				},
				CPU: job.TaskCPU,
				MEM: job.TaskMEM,
			})

			resources.Subtract(job.TaskCPU, job.TaskMEM)
		}

		return result, nil
	}

	result := []*TaskToExecute{}
	var jobID string

	// first iteration: data locality has priority
	iter := m.runningQueue.Iter(&jobID)
	for iter.Next() {
		tasks, err := startTasks(jobID, true)
		if err != nil {
			//TODO: stop job on error
		}

		result = append(result, tasks...)
	}

	// second iteration: distribute the remaining resources disregarding data locality
	iter = m.runningQueue.Iter(&jobID)
	for iter.Next() {
		tasks, err := startTasks(jobID, false)
		if err != nil {
			//TODO: stop job on error
		}

		result = append(result, tasks...)
	}

	return result
}

func (m *Manager) SetTaskCompleted(result *protos.ExecTaskResult) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	taskManager, ok := m.taskManagers[result.JobId]
	if !ok {
		return
	}

	err := taskManager.SetTaskCompleted(int(result.TaskId), result.SuperstepResult)
	if err != nil {
		//TODO: stop the job
	}

	//TODO: check if the next superstep started and fire Before/After Superstep handlers
}

func (m *Manager) SetTaskFailed(jobID string, taskID int) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	taskManager, ok := m.taskManagers[jobID]
	if !ok {
		return
	}

	err := taskManager.SetTaskFailed(taskID)
	if err != nil {
		//TODO: stop the job
	}
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
			m.waitingQueue.Remove(id)
		case pregel.JobRunning:
			m.runningQueue.Remove(id)
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
			m.waitingQueue.Add(id)
		case pregel.JobRunning:
			checkpoint, err := m.loadCheckpoint(id)
			if checkpoint == nil || err != nil {
				if err != nil {
					glog.Warningf("failed to load checkpoint for job %s; error: %v", id, err)
				}

				m.waitingQueue.Add(id)
				continue
			}

			m.taskManagers[id] = task.NewManagerFromCheckpoint(checkpoint)
			m.runningQueue.Add(id)
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

func (m *Manager) startWaitingJobs() {
	var jobID string
	var started []string

	iter := m.waitingQueue.Iter(&jobID)
	for iter.Next() {
		if m.runningQueue.Count() >= maxRunningJobs {
			break
		}

		if err := m.startJob(jobID); err != nil {
			//TODO mark job as failed
		}

		started = append(started, jobID)
	}

	for _, jobID = range started {
		m.waitingQueue.Remove(jobID)
	}
}

func (m *Manager) startJob(jobID string) error {
	job := m.jobs[jobID]

	graphStore, err := connectGraphStore(job.Store, job.StoreParams)
	if err != nil {
		return err
	}
	defer graphStore.Close()

	vertexRanges, err := graphStore.GetVertexRanges(job.TaskVertices)
	if err != nil {
		return err
	}

	algorithm, err := algorithm.NewAlgorithm(job.Algorithm, job.AlgorithmParams)
	if err != nil {
		return err
	}

	context := &superstepContext{
		superstep:   1,
		aggregators: aggregator.NewSet(),
	}

	err = algorithm.BeforeSuperstep(context)
	if err != nil {
		return err
	}

	if context.stopped {
		return nil //TODO: mark job as finished
	}

	jobManager := task.NewManager(vertexRanges, context.aggregators, job.TaskTimeout, job.TaskMaxRetryCount)
	m.taskManagers[jobID] = jobManager

	return nil
}

func (m *Manager) finishJob(job *pregel.Job) {
	//TODO
}

func coerceJobParams(job *pregel.Job) *pregel.Job {
	//TODO
	// if taskVertices == 0 {
	// 	taskVertices = defaultTaskVertices
	// } else if taskVertices < minTaskVertices {
	// 	taskVertices = minTaskVertices
	// }

	return job
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
