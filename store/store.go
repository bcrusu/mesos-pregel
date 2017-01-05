package store

import "github.com/bcrusu/mesos-pregel"

type JobStore interface {
	LoadAll() ([]*pregel.Job, error)

	Save(*pregel.Job) error
	SetStatus(jobID string, status pregel.JobStatus) error

	LoadResult(jobID string) ([]byte, error)
	SaveResult(jobID string, value []byte) error

	LoadCheckpoint(jobID string) ([]byte, error)
	SaveCheckpoint(jobID string, value []byte) error

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
	Range          []byte
}
