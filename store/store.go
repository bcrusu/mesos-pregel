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
	GetVertexRanges(verticesPerRange int) ([]*VertexRangeHosts, error)

	SaveVertices(vertices []*pregel.Vertex) error
	LoadVertices(vrange VertexRange) ([]*pregel.Vertex, error)

	SaveEdges(edges []*pregel.Edge) error
	LoadEdges(vrange VertexRange) ([]*pregel.Edge, error)

	LoadVertexMessages(jobID string, superstep int, vrange VertexRange) ([]*pregel.VertexMessage, error)
	SaveVertexMessages(messages []*pregel.VertexMessage) error

	LoadVertexOperations(jobID string, superstep int, vrange VertexRange) ([]*pregel.VertexOperation, error)
	SaveVertexOperations(operations []*pregel.VertexOperation) error

	LoadHaltedVertices(jobID string, superstep int, vrange VertexRange) ([]string, error)
	SaveHaltedVertices(halted []*pregel.VertexHalted) error

	LoadEdgeOperations(jobID string, superstep int, vrange VertexRange) ([]*pregel.EdgeOperation, error)
	SaveEdgeOperations(operations []*pregel.EdgeOperation) error

	Connect() error
	Init() error
	Close()
}

type VertexRange []byte

type VertexRangeHosts struct {
	Hosts []string
	Range VertexRange
}
