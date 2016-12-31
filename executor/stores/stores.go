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

func NewStore(storeType protos.StoreType, params []byte, entityRange []byte) (Store, error) {
	switch storeType {
	case protos.StoreType_Cassandra:
		paramsMsg := new(protos.CassandraStoreParams)
		if err := proto.Unmarshal(params, paramsMsg); err != nil {
			return nil, err
		}

		entityRangeMsg := new(protos.CassandraEntityRange)
		if err := proto.Unmarshal(entityRange, entityRangeMsg); err != nil {
			return nil, err
		}

		return NewCassandraStore(*paramsMsg, *entityRangeMsg), nil
	default:
		return nil, fmt.Errorf("Invalid store type '%s'", storeType)
	}
}
