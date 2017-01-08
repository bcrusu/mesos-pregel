package jobManager

import (
	"sync"
	"time"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/algorithm"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/pborman/uuid"
)

const (
	maxRunningJobs         = 10
	minVerticesPerTask     = 1000
	defaultVerticesPerTask = 10000
)

type JobManager struct {
	jobStore          store.JobStore
	checkpointEncoder encoding.Encoder
	mutex             sync.RWMutex
	all               map[string]*pregel.Job
	waitingQueue      []string
	runningQueue      []string
	runningJobTasks   map[string]*jobTasks
}

type Resources interface {
	Subtract(cpus float64, mems float64) bool
}

func New(jobStore store.JobStore) (*JobManager, error) {
	result := &JobManager{
		jobStore:          jobStore,
		all:               make(map[string]*pregel.Job),
		waitingQueue:      []string{},
		runningQueue:      []string{},
		runningJobTasks:   make(map[string]*jobTasks),
		checkpointEncoder: encoding.NewProtobufEncoder(func() proto.Message { return new(protos.JobCheckpoint) }),
	}

	err := result.initFromStore()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (manager *JobManager) CreateJob(request *protos.CreateJobRequest) *protos.CreateJobReply {
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
		TaskCPU:           int(request.TaskCPU),
		TaskMEM:           int(request.TaskMEM),
		TaskVertices:      int(request.TaskVertices),
		TaskTimeout:       int(request.TaskTimeout),
		TaskMaxRetryCount: int(request.TaskMaxRetryCount),
	}

	err := manager.jobStore.Save(job)
	if err != nil {
		return &protos.CreateJobReply{Status: protos.CallStatus_INTERNAL_ERROR}
	}

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	manager.all[id] = job
	manager.waitingQueue = append(manager.waitingQueue, id)

	return &protos.CreateJobReply{JobId: id, Status: protos.CallStatus_OK}
}

func (manager *JobManager) GetJobStatus(request *protos.JobIdRequest) *protos.GetJobStatusReply {
	id := request.JobId

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	job, ok := manager.all[id]
	if !ok {
		return &protos.GetJobStatusReply{Status: protos.CallStatus_ERROR_INVALID_JOB}
	}

	var superstep int
	var percentDone int
	if job.Status == pregel.JobRunning {
		jobTasks := manager.runningJobTasks[id]
		superstep = jobTasks.currentSuperstep
		percentDone = jobTasks.getPercentDone()
	}

	return &protos.GetJobStatusReply{
		Status:      protos.CallStatus_OK,
		JobStatus:   toJobStatusProto(job.Status),
		Superstep:   int32(superstep),
		PercentDone: int32(percentDone),
	}
}

func (manager *JobManager) GetJobResult(request *protos.JobIdRequest) *protos.GetJobResultReply {
	//TODO
	return nil
}

func (manager *JobManager) GetTasksToExecute(host string, resources Resources) []*protos.ExecTaskParams {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	result := []*protos.ExecTaskParams{}

	// first iteration: data locality has priority
	// for _, jobID := range manager.runningQueue {
	// 	jobTasks := manager.runningJobTasks[jobID]
	// 	//tasks := jobTasks.GetTasksToExecuteForHost(host, resources)
	// }

	// second iteration: distribute the remaining resources disregarding data locality
	// for _, jobID := range manager.runningQueue {

	// }
	//TODO
	return result
}

func (manager *JobManager) CancelJob(request *protos.JobIdRequest) *protos.SimpleCallReply {
	id := request.JobId

	// verify that the task exists and that it can be cancelled (read lock)
	manager.mutex.RLock()
	job, ok := manager.all[id]
	if !ok || !job.CanCancel() {
		manager.mutex.RUnlock()
		return &protos.SimpleCallReply{Status: protos.CallStatus_ERROR_INVALID_JOB}
	}
	manager.mutex.RUnlock()

	// update store (no lock)
	err := manager.jobStore.SetStatus(id, pregel.JobCancelled)
	if err != nil {
		return &protos.SimpleCallReply{Status: protos.CallStatus_INTERNAL_ERROR}
	}

	// update state if job was not changed (write lock)
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if job.CanCancel() {
		switch job.Status {
		case pregel.JobCreated:
			manager.waitingQueue = removeFromQueue(manager.waitingQueue, id)
		case pregel.JobRunning:
			manager.runningQueue = removeFromQueue(manager.runningQueue, id)
			delete(manager.runningJobTasks, id)

			//TODO(optional): call Scheduler.KillTask
		}

		job.Status = pregel.JobCancelled
	}

	return &protos.SimpleCallReply{Status: protos.CallStatus_OK}
}

func (manager *JobManager) initFromStore() error {
	jobs, err := manager.jobStore.LoadAll()
	if err != nil {
		return err
	}

	sortJobsByCreationTime(jobs)

	for _, job := range jobs {
		id := job.ID
		manager.all[id] = job

		switch job.Status {
		case pregel.JobCreated:
			manager.waitingQueue = append(manager.waitingQueue, id)
		case pregel.JobRunning:
			checkpoint, err := manager.loadCheckpoint(id)
			if err != nil {
				glog.Warningf("failed to load checkpoint for job %s", id)
				continue
			}

			if checkpoint == nil {
				manager.waitingQueue = append(manager.waitingQueue, id)
				continue
			}

			manager.runningQueue = append(manager.runningQueue, id)
			manager.runningJobTasks[id] = newJobTasksFromCheckpoint(checkpoint)
		}
	}

	return nil
}

func (manager *JobManager) loadCheckpoint(jobID string) (*protos.JobCheckpoint, error) {
	bytes, err := manager.jobStore.LoadCheckpoint(jobID)
	if err != nil {
		return nil, err
	}

	if bytes == nil {
		return nil, nil
	}

	checkpoint, err := manager.checkpointEncoder.Unmarshal(bytes)
	if err != nil {
		return nil, err
	}

	return checkpoint.(*protos.JobCheckpoint), nil
}

func (manager *JobManager) saveCheckpoint(jobID string, checkpoint *protos.JobCheckpoint) error {
	bytes, err := manager.checkpointEncoder.Marshal(checkpoint)
	if err != nil {
		return err
	}

	return manager.jobStore.SaveCheckpoint(jobID, bytes)
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

func getVertexRanges(graphStore store.GraphStore, verticesPerTask int) ([]*store.VertexRangeHosts, error) {
	if verticesPerTask == 0 {
		verticesPerTask = defaultVerticesPerTask
	} else if verticesPerTask < minVerticesPerTask {
		verticesPerTask = minVerticesPerTask
	}

	return graphStore.GetVertexRanges(verticesPerTask)
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

func toJobStatusProto(status pregel.JobStatus) protos.JobStatus {
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
