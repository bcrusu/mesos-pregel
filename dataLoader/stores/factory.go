package stores

import (
	"errors"
	"flag"
	"fmt"
	"strings"
)

var (
	storeType          string
	cassandraHosts     string
	cassandraKeyspace  string
	cassandraTableName string
)

func init() {
	flag.StringVar(&storeType, "store", "cassandra", "Store type. Available stores: cassandra")
	flag.StringVar(&cassandraHosts, "cassandra.hosts", "127.0.100.1,127.0.100.2", "Cassandra hosts separated by ','")
	flag.StringVar(&cassandraKeyspace, "cassandra.keyspace", "pregel", "Cassandra keyspace")
	flag.StringVar(&cassandraTableName, "cassandra.table", "", "Cassandra table name")
}

func NewStore() (Store, error) {
	var store Store
	var err error

	switch storeType {
	case "cassandra":
		store, err = createCassandraStore()
	case "null":
		store, err = NewNullStore(), nil
	default:
		store, err = nil, fmt.Errorf("Invalid store type '%s'", storeType)
	}

	return store, err
}

func createCassandraStore() (Store, error) {
	if len(cassandraHosts) == 0 || len(cassandraKeyspace) == 0 || len(cassandraTableName) == 0 {
		return nil, errors.New("invalid CassandraStore arguments")
	}

	hosts := strings.Split(cassandraHosts, ",")

	store := NewCassandraStore(hosts, cassandraKeyspace, cassandraTableName)
	return store, nil
}
