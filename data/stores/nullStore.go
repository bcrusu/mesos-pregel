package stores

import "github.com/bcrusu/pregel/data/graph"

type NullStore struct {
}

func NewNullStore() *NullStore {
	return new(NullStore)
}

func (store *NullStore) Connect() error {
	return nil
}

func (store *NullStore) Close() {
}

func (store *NullStore) Write(edge []*graph.Edge) error {
	return nil
}
