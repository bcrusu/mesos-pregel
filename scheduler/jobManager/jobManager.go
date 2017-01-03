package jobManager

import "github.com/bcrusu/mesos-pregel/protos"

type JobManager struct {
}

func New() *JobManager {
	return &JobManager{}
}

func (manager *JobManager) StartJob(request *protos.StartJobRequest) *protos.StartJobReply {
	//TODO
	return nil
}

func (manager *JobManager) GetJobStatus(request *protos.GetJobStatusRequest) *protos.GetJobStatusReply {
	//TODO
	return nil
}

func (manager *JobManager) GetJobResult(request *protos.GetJobResultRequest) *protos.GetJobResultReply {
	//TODO
	return nil
}
