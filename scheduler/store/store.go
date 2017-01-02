package store

import (
	"fmt"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
)

type Store interface {
	LoadJobs() ([]*pregel.Job, error)
	SaveJob(*pregel.Job) error

	Connect() error
	Init() error
	Close()
}

func NewStore(store string, params []byte) (Store, error) {
	switch store {
	case "cassandra":
		paramsMsg := new(protos.CassandraStoreParams)
		if err := proto.Unmarshal(params, paramsMsg); err != nil {
			return nil, err
		}

		//return NewCassandraStore(*paramsMsg, *entityRangeMsg), nil
		return nil, nil
	default:
		return nil, fmt.Errorf("Invalid store type '%s'", store)
	}
}
