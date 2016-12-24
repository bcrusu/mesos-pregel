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
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}

	if err = ensureKeyspace(session, store.keyspace); err != nil {
		session.Close()
		return err
	}

	if err = ensureTable(session, store.keyspace, store.tableName); err != nil {
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
	//log.Println(edge.FromNode)
	//TODO
}

func ensureTable(session *gocql.Session, keyspace string, tableName string) error {
	tableExists, err := tableExists(session, keyspace, tableName)
	if err != nil {
		return err
	}

	if tableExists {
		return fmt.Errorf("cannot use existing table '%s.%s'", keyspace, tableName)
	}

	cql := fmt.Sprintf("CREATE TABLE \"%s\".\"%s\"(\"fromNode\" text, \"toNode\" text, \"weight\" int, PRIMARY KEY(\"fromNode\"));", keyspace, tableName)
	return execCql(session, cql, true)
}

func tableExists(session *gocql.Session, keyspace string, tableName string) (bool, error) {
	query := session.Query("SELECT count(1) FROM system_schema.tables WHERE keyspace_name=? and table_name=?;", keyspace, tableName)

	var count int
	if err := query.Scan(&count); err != nil {
		return false, err
	}

	return count == 1, nil
}

func ensureKeyspace(session *gocql.Session, keyspace string) error {
	cql := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", keyspace)
	return execCql(session, cql, false)
}

func execCql(session *gocql.Session, cql string, ignoreTimeout bool) error {
	query := session.Query(cql)

	if err := query.Exec(); err != nil {
		if ignoreTimeout && err.Error() == gocql.ErrTimeoutNoResponse.Error() {
			return nil
		}

		return err
	}

	return nil
}
