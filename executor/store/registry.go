package store

import (
	"fmt"

	"github.com/bcrusu/mesos-pregel/encoding"
)

type StoreFactory interface {
	CreateStore(params interface{}, vertexRange interface{}) (Store, error)

	ParamsEncoder() encoding.Encoder
	VertexRangeEncoder() encoding.Encoder
}

var storeFactories = make(map[string]StoreFactory)

func RegisterStore(store string, factory StoreFactory) {
	storeFactories[store] = factory
}

func NewStore(store string, params []byte, vertexRange []byte) (Store, error) {
	factory, ok := storeFactories[store]
	if !ok {
		return nil, fmt.Errorf("unknown store %s", store)
	}

	p, err := factory.ParamsEncoder().Unmarshal(params)
	if err != nil {
		return nil, err
	}

	vr, err := factory.VertexRangeEncoder().Unmarshal(vertexRange)
	if err != nil {
		return nil, err
	}

	return factory.CreateStore(p, vr)
}
