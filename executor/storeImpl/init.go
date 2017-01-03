package storeImpl

import "github.com/bcrusu/mesos-pregel/executor/store"

func init() {
	store.RegisterStore("Cassandra", &cassandraStoreFactory{})
}
