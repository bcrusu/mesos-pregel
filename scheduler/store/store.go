package store

import (
	"fmt"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
)

type Store interface {
	LoadJob(id string) (*pregel.Job, error)
	LoadAllJobs() ([]*pregel.Job, error)
	SaveJob(*pregel.Job) error

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

		//return NewCassandraStore(*paramsMsg, *entityRangeMsg), nil
		return nil, nil
	default:
		return nil, fmt.Errorf("Invalid store type '%s'", storeType)
	}
}
