package api

import (
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/jobManager"
	"golang.org/x/net/context"
)

type service struct {
	jobManager *jobManager.JobManager
}

func newServiceServer(jobManager *jobManager.JobManager) protos.ServiceServer {
	return &service{jobManager}
}

func (s *service) CreateJob(context context.Context, request *protos.CreateJobRequest) (*protos.CreateJobReply, error) {
	return s.jobManager.CreateJob(request), nil
}

func (s *service) CancelJob(context context.Context, request *protos.JobIdRequest) (*protos.SimpleCallReply, error) {
	return s.jobManager.CancelJob(request), nil
}

func (s *service) GetJobStatus(context context.Context, request *protos.JobIdRequest) (*protos.GetJobStatusReply, error) {
	return s.jobManager.GetJobStatus(request), nil
}

func (s *service) GetJobResult(context context.Context, request *protos.JobIdRequest) (*protos.GetJobResultReply, error) {
	return s.jobManager.GetJobResult(request), nil
}
