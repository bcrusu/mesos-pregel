package api

import (
	"fmt"
	"net"

	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/job"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type APIServer struct {
	jobManager *job.Manager
	server     *grpc.Server
}

func NewAPIServer(jobManager *job.Manager) *APIServer {
	return &APIServer{jobManager: jobManager}
}

func (s *APIServer) ServeAsync(port int) error {
	address := getListenAddress(port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return errors.Wrapf(err, "failed to create TCP listener on address %s", address)
	}

	s.server = s.getGrpcServer()
	go func() {
		if err := s.server.Serve(listener); err != nil {
			fmt.Printf("stopped serving API; error=%v\n", err)
		}
	}()

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
