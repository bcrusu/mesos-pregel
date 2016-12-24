package stores

import (
	"fmt"

	"github.com/bcrusu/pregel/data/graph"
	"github.com/gocql/gocql"
)

type CassandraStore struct {
	hosts     []string
	tableName string
	keyspace  string
	cluster   *gocql.ClusterConfig
	session   *gocql.Session
}

func NewCassandraStore(hosts []string, keyspace string, tableName string) *CassandraStore {
	result := new(CassandraStore)
	result.hosts = hosts
	result.keyspace = keyspace
	result.tableName = tableName
	return result
}

func (store *CassandraStore) Connect() error {
	cluster := gocql.NewCluster(store.hosts...)
	cluster.Keyspace = store.keyspace
	cluster.Consistency = gocql.Quorum

	session, err := store.cluster.CreateSession()
	if err != nil {
		return err
	}

	if err = store.ensureKeyspace(); err != nil {
		session.Close()
		return err
	}

	if err = store.ensureTable(); err != nil {
		session.Close()
		return err
	}

	store.cluster = cluster
	store.session = session
	return nil
}

func (store *CassandraStore) Close() {
	if store.session != nil {
		store.session.Close()
		store.cluster = nil
		store.session = nil
	}
}

func (store *CassandraStore) Write(edge *graph.Edge) {
	//TODO
}

func (store *CassandraStore) ensureTable() error {
	tableExists, err := store.tableExists()
	if err != nil {
		return err
	}

	if tableExists {
		return fmt.Errorf("cannot use existing table '%s.%s'", store.keyspace, store.tableName)
	}

	cql := fmt.Sprintf("CREATE TABLE %s.%s(fromnode text, tonode text, weight int, PRIMARY KEY(fromnode));", store.keyspace, store.tableName)
	return execCql(store.session, cql)
}

func (store *CassandraStore) tableExists() (bool, error) {
	query := store.session.Query("SELECT count(1) FROM system_schema.tables WHERE keyspace_name=? and table_name=?;", store.keyspace, store.tableName)

	var count int
	if err := query.Scan(&count); err != nil {
		return false, err
	}

	return count == 1, nil
}

func (store *CassandraStore) ensureKeyspace() error {
	cql := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", store.keyspace)
	return execCql(store.session, cql)
}

func execCql(session *gocql.Session, cql string) error {
	query := session.Query(cql)

	if err := query.Exec(); err != nil {
		return err
	}

	return nil
}
