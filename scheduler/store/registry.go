package store

import (
	"fmt"

	"github.com/bcrusu/mesos-pregel/encoding"
)

type JobStoreFactory interface {
	CreateStore(params interface{}) JobStore
	ParamsEncoder() encoding.Encoder
}

type EntityStoreFactory interface {
	CreateStore(params interface{}) EntityStore
	ParamsEncoder() encoding.Encoder
}

var jobStoreFactories = make(map[string]JobStoreFactory)
var entityStoreFactories = make(map[string]EntityStoreFactory)

func RegisterJobStore(store string, factory JobStoreFactory) {
	jobStoreFactories[store] = factory
}

func RegisterEntityStore(store string, factory EntityStoreFactory) {
	entityStoreFactories[store] = factory
}

func NewJobStore(store string, params []byte) (JobStore, error) {
	factory, ok := jobStoreFactories[store]
	if !ok {
		return nil, fmt.Errorf("unknown job store %s", store)
	}

	p, err := factory.ParamsEncoder().Unmarshal(params)
	if err != nil {
		return nil, err
	}

	return factory.CreateStore(p), nil
}

func NewEntityStore(store string, params []byte) (EntityStore, error) {
	factory, ok := entityStoreFactories[store]
	if !ok {
		return nil, fmt.Errorf("unknown entity store %s", store)
	}

	p, err := factory.ParamsEncoder().Unmarshal(params)
	if err != nil {
		return nil, err
	}

	return factory.CreateStore(p), nil
}
