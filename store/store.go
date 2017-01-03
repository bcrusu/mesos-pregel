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

type GraphStore interface {
	GetVertexRanges(verticesPerRange int) ([]*VertexRange, error)

	LoadVertices() ([]*pregel.Vertex, error)
	LoadEdges() ([]*pregel.Edge, error)

	LoadVertexMessages(jobID string, superstep int) ([]*pregel.VertexMessage, error)
	SaveVertexMessages(messages []*pregel.VertexMessage) error

	LoadVertexOperations(jobID string, superstep int) ([]*pregel.VertexOperation, error)
	SaveVertexOperations(operations []*pregel.VertexOperation) error

	LoadHaltedVertices(jobID string, superstep int) ([]string, error)
	SaveHaltedVertices(halted []*pregel.VertexHalted) error

	LoadEdgeOperations(jobID string, superstep int) ([]*pregel.EdgeOperation, error)
	SaveEdgeOperations(operations []*pregel.EdgeOperation) error

	Connect() error
	Init() error
	Close()
}

type VertexRange struct {
	PreferredHosts []string
	Range          []interface{}
}
