package jobManager

import (
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/store"
)

type JobManager struct {
}

func New() *JobManager {
	return &JobManager{}
}

func (manager *JobManager) StartJob(request *protos.StartJobRequest) *protos.StartJobReply {
	store, err := store.NewJobStore()
	request.Store
}

func (manager *JobManager) GetJobStatus(request *protos.GetJobStatusRequest) *protos.GetJobStatusReply {
	//TODO
	return nil
}

func (manager *JobManager) GetJobResult(request *protos.GetJobResultRequest) *protos.GetJobResultReply {
	//TODO
	return nil
}
