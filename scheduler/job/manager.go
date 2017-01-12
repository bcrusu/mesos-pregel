package job

import (
	"sync"
	"time"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/task"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/golang/glog"
	"github.com/pborman/uuid"
)

const (
	maxRunningJobs = 10
)

type Manager struct {
	jobStore            store.JobStore
	stopChan            chan struct{}
	jobStarter          *jobStarter
	jobStopperChan      chan *jobStopperParams
	mutex               sync.RWMutex
	jobs                map[string]*pregel.Job
	waiting             *jobQueue
	running             *jobQueue
	runningTaskManagers map[string]*task.Manager
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
		stopChan:            make(chan struct{}),
		jobStore:            jobStore,
		jobs:                make(map[string]*pregel.Job),
		running:             &jobQueue{},
		runningTaskManagers: make(map[string]*task.Manager),
		jobStarter:          newJobStarter(jobStore),
	}

	go func() {
		for taskManager := range result.jobStarter.output {
			result.mutex.Lock()

			jobID := taskManager.JobID()
			result.runningTaskManagers[jobID] = taskManager
			result.running.Add(jobID)

			result.mutex.Unlock()
		}
	}()

	result.jobStopperChan = createJobStopper(jobStore)

	err := result.initFromStore()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (m *Manager) CreateJob(request *protos.CreateJobRequest) *protos.CreateJobReply {
	if ok := validateCreateJobRequest(request); !ok {
		return &protos.CreateJobReply{Status: protos.CallStatus_ERROR_INVALID_REQUEST}
	}

	id := uuid.NewRandom().String()
	job := &pregel.Job{
		ID:              id,
		Label:           request.Label,
		CreationTime:    time.Now(),
		Status:          pregel.JobCreated,
		Store:           request.Store,
		StoreParams:     request.StoreParams,
		Algorithm:       request.Algorithm,
		AlgorithmParams: request.AlgorithmParams,
		TaskCPU:         request.TaskCPU,
		TaskMEM:         request.TaskMEM,
		TaskVertices:    int(request.TaskVertices),
		TaskTimeout:     int(request.TaskTimeout),
	}

	err := m.jobStore.Save(job)
	if err != nil {
		return &protos.CreateJobReply{Status: protos.CallStatus_INTERNAL_ERROR}
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.jobs[id] = job
	m.waiting.Add(id)

	return &protos.CreateJobReply{JobId: id, Status: protos.CallStatus_OK}
}

func (m *Manager) GetJobStatus(request *protos.JobIdRequest) *protos.GetJobStatusReply {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	jobID := request.JobId
	job, ok := m.jobs[jobID]
	if !ok {
		return &protos.GetJobStatusReply{Status: protos.CallStatus_ERROR_INVALID_JOB}
	}

	var superstep int
	var percentDone int
	if taskManager, ok := m.runningTaskManagers[jobID]; ok {
		superstep = taskManager.Superstep()
		percentDone = -1 //TODO: add stats values
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

	startTasks := func(jobID string, onlyLocalTasks bool) []*TaskToExecute {
		job := m.jobs[jobID]
		taskManager := m.runningTaskManagers[jobID]

		result := []*TaskToExecute{}
		for resources.CanSubtract(job.TaskCPU, job.TaskMEM) {
			task := taskManager.StartTask(host, onlyLocalTasks)

			// stop if no more tasks to start
			if task == nil {
				break
			}

			result = append(result, &TaskToExecute{
				Parmas: &protos.ExecTaskParams{
					JobId:           jobID,
					TaskId:          task.TaskID,
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

		return result
	}

	var result []*TaskToExecute
	var jobID string

	// first iteration: data locality has priority
	for iter := m.running.Iter(&jobID); iter.Next(); {
		tasks := startTasks(jobID, true)
		result = append(result, tasks...)
	}

	// second iteration: distribute the remaining resources disregarding data locality
	for iter := m.running.Iter(&jobID); iter.Next(); {
		tasks := startTasks(jobID, false)
		result = append(result, tasks...)
	}

	return result
}

func (m *Manager) SetTaskCompleted(result *protos.ExecTaskResult) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	job, ok := m.jobs[result.JobId]
	if !ok {
		glog.Infof("ignoring unknown job %s", job.ID)
		return
	}

	taskID := result.TaskId
	taskManager, ok := m.runningTaskManagers[job.ID]
	if !ok {
		glog.Infof("job %s - ignoring orphaned task %d", job.ID, taskID)
		return
	}

	jobCompleted, err := taskManager.SetTaskCompleted(taskID, result.SuperstepResult)
	if err != nil {
		m.jobStopperChan <- &jobStopperParams{
			job:       job,
			newStatus: pregel.JobFailed,
		}
		m.removeRunning(job.ID)
	}

	if jobCompleted {
		m.jobStopperChan <- &jobStopperParams{
			job:       job,
			newStatus: pregel.JobCompleted,
		}
		m.removeRunning(job.ID)
	}
}

func (m *Manager) SetTaskFailed(jobID string, taskID string) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	_, ok := m.jobs[jobID]
	if !ok {
		glog.Infof("ignoring unknown job %s", jobID)
		return
	}

	taskManager, ok := m.runningTaskManagers[jobID]
	if !ok {
		glog.Infof("job %s - ignoring orphaned failed task %d", jobID, taskID)
		return
	}

	taskManager.SetTaskFailed(taskID)
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

	oldStatus := job.Status
	params := &jobStopperParams{
		job:       job,
		newStatus: pregel.JobCancelled,
		doneChan:  make(chan struct{}),
		errorChan: make(chan error),
	}

	m.jobStopperChan <- params

	select {
	case <-params.errorChan:
		return &protos.SimpleCallReply{Status: protos.CallStatus_INTERNAL_ERROR}
	case <-params.doneChan:
	}

	// update state if job was not changed (write lock)
	m.mutex.Lock()
	defer m.mutex.Unlock()

	switch oldStatus {
	case pregel.JobCreated:
		m.waiting.Remove(id)
	case pregel.JobRunning:
		m.removeRunning(id)
	}

	return &protos.SimpleCallReply{Status: protos.CallStatus_OK}
}

func (m *Manager) startWaitingJobsWatcher(stopChan chan struct{}) {
	sendToStarter := func() {
		m.mutex.Lock()
		defer m.mutex.Unlock()

		started := []string{}
		count := maxRunningJobs - m.running.Count()

		var jobID string
		for iter := m.waiting.Iter(&jobID); count > 0 && iter.Next(); count-- {
			m.jobStarter.input <- m.jobs[jobID]
			started = append(started, jobID)
		}

		for _, jobID := range started {
			m.waiting.Remove(jobID)
		}
	}

	go func() {
		for {
			select {
			case <-time.After(2 * time.Second):
				sendToStarter()
			case <-stopChan:
				return
			}
		}
	}()
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
			m.waiting.Add(id)
		case pregel.JobRunning:
			m.jobStarter.input <- job
		}
	}

	m.jobStarter.wait()
	return nil
}

func (m *Manager) removeRunning(jobID string) {
	m.running.Remove(jobID)
	delete(m.runningTaskManagers, jobID)
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
