package store

import "github.com/bcrusu/mesos-pregel"

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

func (store *NullStore) Write(edge []*pregel.Edge) error {
	return nil
}