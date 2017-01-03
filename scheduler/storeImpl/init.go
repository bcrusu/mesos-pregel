package storeImpl

import "github.com/bcrusu/mesos-pregel/scheduler/store"

func init() {
	store.RegisterJobStore("Cassandra", &cassandraJobStoreFactory{})
	store.RegisterEntityStore("Cassandra", &cassandraEntityStoreFactory{})
}
