package api

import (
	"fmt"
	"net"

	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/jobManager"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type APIServer struct {
	jobManager *jobManager.JobManager
	server     *grpc.Server
}

func NewAPIServer(jobManager *jobManager.JobManager) *APIServer {
	return &APIServer{jobManager: jobManager}
}

func (s *APIServer) ServeAsync(port int) error {
	address := getListenAddress(port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return errors.Wrapf(err, "failed to create TCP listener on address %s", address)
	}

	s.server = s.getGrpcServer()
	go s.server.Serve(listener)

	return nil
}

func (s *APIServer) Stop() {
	s.server.Stop()
}

func (s *APIServer) getGrpcServer() *grpc.Server {
	result := grpc.NewServer()
	service := newServiceServer(s.jobManager)
	protos.RegisterServiceServer(result, service)
	return result
}

func getListenAddress(port int) string {
	return fmt.Sprintf(":%d", port)
}
