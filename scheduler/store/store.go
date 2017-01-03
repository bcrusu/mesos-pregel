package store

import "github.com/bcrusu/mesos-pregel"

type JobStore interface {
	LoadJobs() ([]*pregel.Job, error)
	SaveJob(*pregel.Job) error

	LoadJobResult(jobID string) ([]byte, error)
	SaveJobResult(jobID string, value []byte) error

	Connect() error
	Init() error
	Close()
}

type EntityStore interface {
	GetVertexRanges(verticesPerRange int) ([]*VertexRange, error)

	Connect() error
	Init() error
	Close()
}

type VertexRange struct {
	PreferredHosts []string
	Range          []interface{}
}
