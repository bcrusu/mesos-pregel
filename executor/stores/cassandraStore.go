package stores

import (
	"fmt"
	"time"

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
	cluster.Timeout = 3 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
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

func (store *CassandraStore) Write(edges []*graph.Edge) error {
	batch := store.session.NewBatch(gocql.UnloggedBatch)
	batch.Cons = gocql.One

	cql := fmt.Sprintf("INSERT INTO \"%s\".\"%s\" (\"fromNode\", \"toNode\", \"weight\") VALUES(?, ?, ?);", store.keyspace, store.tableName)

	for _, edge := range edges {
		var entry gocql.BatchEntry
		entry.Stmt = cql
		entry.Args = []interface{}{edge.FromNode, edge.ToNode, edge.Weight}
		batch.Entries = append(batch.Entries, entry)
	}

	return store.session.ExecuteBatch(batch)
}

func execCql(session *gocql.Session, cql string) error {
	query := session.Query(cql).Consistency(gocql.All)

	if err := query.Exec(); err != nil {
		return err
	}

	return nil
}
