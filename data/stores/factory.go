package stores

import (
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/bcrusu/pregel/data"
)

var (
	storeType          *string
	cassandraHosts     *string
	cassandraKeyspace  *string
	cassandraTableName *string
)

func init() {
	flag.StringVar(storeType, "store", "cassandra", "Store type. Available stores values: cassandra")
	flag.StringVar(cassandraHosts, "cassandra.hosts", "127.0.100.1,127.0.100.2", "Cassandra hosts separated by ','")
	flag.StringVar(cassandraKeyspace, "cassandra.keyspace", "pregel", "Cassandra keyspace")
	flag.StringVar(cassandraTableName, "cassandra.table", "", "Cassandra table name")
}

func NewStore() (data.Store, error) {
	var store data.Store
	var err error

	switch *storeType {
	case "cassandra":
		store, err = createCassandraStore()
	default:
		store, err = nil, fmt.Errorf("Invalid store type '%s'", storeType)
	}

	return store, err
}

func createCassandraStore() (data.Store, error) {
	if cassandraHosts == nil || cassandraKeyspace == nil || cassandraTableName == nil {
		return nil, errors.New("invalid CassandraStore arguments")
	}

	hosts := strings.Split(*cassandraHosts, ",")

	store := NewCassandraStore(hosts, *cassandraKeyspace, *cassandraTableName)
	return store, nil
}
