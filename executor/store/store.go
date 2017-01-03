package store

import "github.com/bcrusu/mesos-pregel"

type Store interface {
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
	Close()
}
