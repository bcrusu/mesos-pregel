package cassandra

import "github.com/bcrusu/mesos-pregel/store"

func init() {
	store.Register("Cassandra", &cassandraGraphStoreFactory{})
}
