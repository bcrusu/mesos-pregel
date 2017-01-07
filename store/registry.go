package store

import (
	"fmt"

	"github.com/bcrusu/mesos-pregel/encoding"
)

type GraphStoreFactory interface {
	Create(params interface{}) (GraphStore, error)
	ParamsEncoder() encoding.Encoder
}

var factoryMap = make(map[string]GraphStoreFactory)

func Register(store string, factory GraphStoreFactory) {
	factoryMap[store] = factory
}

func New(store string, params []byte) (GraphStore, error) {
	factory, ok := factoryMap[store]
	if !ok {
		return nil, fmt.Errorf("unknown store %s", store)
	}

	p, err := factory.ParamsEncoder().Unmarshal(params)
	if err != nil {
		return nil, err
	}

	return factory.Create(p)
}
