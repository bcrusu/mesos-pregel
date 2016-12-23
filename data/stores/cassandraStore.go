package stores

import (
	"fmt"

	"github.com/bcrusu/pregel/data"
	"github.com/gocql/gocql"
)

type CassandraStore struct {
	hosts     []string
	tableName string
	keyspace  string
	cluster   *gocql.ClusterConfig
	session   *gocql.Session
}

func NewStore(hosts []string, keyspace string, tableName string) *CassandraStore {
	result := new(CassandraStore)
	result.hosts = hosts
	result.keyspace = keyspace
	result.tableName = tableName
	return result
}

func (store *CassandraStore) Connect() error {
	cluster := gocql.NewCluster(store.hosts)
	cluster.Keyspace = store.keyspace
	cluster.Consistency = gocql.Quorum.Exec()

	session, err := store.cluster.CreateSession()
	if err != nil {
		return err
	}

	err = ensureTable(session, store.tableName)
	if err != null {
		session.Close()
		return err
	}

	store.cluster = cluster
	store.session = session
}

func (store *CassandraStore) Close() {
	(store * CassandraStore)
	if store.session != nil {
		store.session.Close()
		store.cluster = nil
		store.session = nil
	}
}

func (store *CassandraStore) Write(edge *data.Edge) {

}

func (store *CassandraStore) ensureTable(session *gocql.Session, tableName string) error {
	tableExists, err := store.tableExists()
	if err != nil {
		return err
	}

	if tableExists {
		return fmt.Errorf("cannot use existing table '%s.%s'", store.keyspace, store.tableName)
	}

	const cql = fmt.Sprintf("create table %s.%s(fromnode text, tonode text, weight int, PRIMARY KEY(fromnode));", store.keyspace, store.tableName)
	query := store.session.Query(cql)

	if err = query.Exec(); err != nil {
		return err
	}

	return nil
}

func (store *CassandraStore) tableExists() (bool, error) {
	query := store.session.Query("select count(1) from system_schema.tables where keyspace_name=? and table_name=?;", store.keyspace, store.tableName)

	var count int
	if err := query.Scan(&count); err != nil {
		return false, err
	}

	return count == 1, nil
}
