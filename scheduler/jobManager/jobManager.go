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
		ID:              id,
		Label:           request.Label,
		CreationTime:    time.Now(),
		Status:          pregel.JobCreated,
		Superstep:       0,
		Store:           request.Store,
		StoreParams:     request.StoreParams,
		Algorithm:       request.Algorithm,
		AlgorithmParams: request.AlgorithmParams,
		VerticesPerTask: int(request.VerticesPerTask),
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
	//TODO
	return nil
}

func (manager *JobManager) GetJobResult(request *protos.JobIdRequest) *protos.GetJobResultReply {
	//TODO
	return nil
}

func (manager *JobManager) CancelJob(request *protos.JobIdRequest) *protos.SimpleCallReply {
	//TODO
	return nil
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
	graphStore, err := store.New(name, params, nil)
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

func getVertexRanges(graphStore store.GraphStore, verticesPerTask int) ([]*store.VertexRange, error) {
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

	algorithm.Init()

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
