package stores

import (
	"fmt"
	"time"

	"github.com/bcrusu/mesos-pregel"
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
	cluster.Timeout = 3 * time.Second

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

func (store *CassandraStore) Write(edges []*pregel.Edge) error {
	batch := store.session.NewBatch(gocql.UnloggedBatch)
	batch.Cons = gocql.One

	cql := fmt.Sprintf(`INSERT INTO "%s"."%s" ("from", "to", value) VALUES(?, ?, ?);`, store.keyspace, store.tableName)

	for _, edge := range edges {
		var entry gocql.BatchEntry
		entry.Stmt = cql
		entry.Args = []interface{}{edge.From, edge.To, edge.Value}
		batch.Entries = append(batch.Entries, entry)
	}

	return store.session.ExecuteBatch(batch)
}

func ensureTable(session *gocql.Session, keyspace string, tableName string) error {
	tableExists, err := tableExists(session, keyspace, tableName)
	if err != nil {
		return err
	}

	if tableExists {
		return fmt.Errorf("cannot use existing table '%s.%s'", keyspace, tableName)
	}

	cql := fmt.Sprintf(`CREATE TABLE "%s"."%s"("from" text, "to" text, value blob, PRIMARY KEY("from", "to"));`, keyspace, tableName)
	return execCql(session, cql)
}

func tableExists(session *gocql.Session, keyspace string, tableName string) (bool, error) {
	query := session.Query(`SELECT count(1) FROM system_schema.tables WHERE keyspace_name=? and table_name=?;`, keyspace, tableName).Consistency(gocql.Quorum)

	var count int
	if err := query.Scan(&count); err != nil {
		return false, err
	}

	return count == 1, nil
}

func ensureKeyspace(session *gocql.Session, keyspace string) error {
	cql := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS "%s" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`, keyspace)
	return execCql(session, cql)
}

func execCql(session *gocql.Session, cql string) error {
	query := session.Query(cql).Consistency(gocql.All)

	if err := query.Exec(); err != nil {
		return err
	}

	return nil
}
