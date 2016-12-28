package stores

import (
	"fmt"

	"github.com/bcrusu/pregel"
	"github.com/bcrusu/pregel/protos"
	"github.com/gogo/protobuf/proto"
)

type Store interface {
	LoadVertices() ([]*pregel.Vertex, error)
	LoadEdges() ([]*pregel.Edge, error)

	LoadVertexOperations(jobID string) ([]*pregel.VertexOperation, error)
	LoadVertexOperationsForSuperstep(jobID string, superstep int) ([]*pregel.VertexOperation, error)
	SaveVertexOperations(operations []*pregel.VertexOperation) error

	LoadEdgeOperations(jobID string) ([]*pregel.EdgeOperation, error)
	LoadEdgeOperationsForSuperstep(jobID string, superstep int) ([]*pregel.EdgeOperation, error)
	SaveEdgeOperations(operations []*pregel.EdgeOperation) error

	Connect() error
	Close()
}

func NewStore(storeType protos.StoreType, params []byte) (Store, error) {
	switch storeType {
	case protos.StoreType_Cassandra:
		paramsMsg := new(protos.CassandraStoreParams)
		if err := proto.Unmarshal(params, paramsMsg); err != nil {
			return nil, err
		}

		return NewCassandraStore(*paramsMsg), nil
	default:
		return nil, fmt.Errorf("Invalid store type '%s'", storeType)
	}
}
