package store

import (
	"fmt"
	"time"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

const (
	vertexMessagesTableName   = "vertexMessages"
	vertexOperationsTableName = "vertexOperations"
	haltedVerticesTableName   = "haltedVertices"
	edgeOperationsTableName   = "edgeOperations"
)

type CassandraStore struct {
	params  protos.CassandraStoreParams
	cluster *gocql.ClusterConfig
	session *gocql.Session
}

func NewCassandraStore(params protos.CassandraStoreParams) Store {
	return &CassandraStore{params: params}
}

func (store *CassandraStore) Connect() error {
	cluster := gocql.NewCluster(store.params.Hosts...)
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

func (store *CassandraStore) Init() error {
	if err := ensureTables(store.session, store.params.Keyspace); err != nil {
		return err
	}

	return nil
}

func (store *CassandraStore) LoadJobs() ([]*pregel.Job, error) {
	//TODO
	return nil, nil
}

func (store *CassandraStore) SaveJob(*pregel.Job) error {
	//TODO
	return nil
}

func (store *CassandraStore) fullTableName(table string) string {
	return fullTableName(store.params.Keyspace, table)
}

func fullTableName(keyspace string, table string) string {
	return fmt.Sprintf(`"%s"."%s"`, keyspace, table)
}

func ensureTables(session *gocql.Session, keyspace string) error {
	cql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s("to" text, job_id text, superstep int, unique_id timeuuid, value blob, PRIMARY KEY(("to"), job_id, superstep, unique_id));
CREATE TABLE IF NOT EXISTS %s(id text, job_id text, superstep int, unique_id timeuuid, type int, value blob, PRIMARY KEY((id), job_id, superstep, unique_id));
CREATE TABLE IF NOT EXISTS %s(id text, job_id text, superstep int, PRIMARY KEY((id), job_id, superstep));
CREATE TABLE IF NOT EXISTS %s("from" text, "to" text, job_id text, superstep int, unique_id timeuuid, type int, value blob, PRIMARY KEY(("from"), "to", job_id, superstep, unique_id));`,
		fullTableName(keyspace, vertexMessagesTableName), fullTableName(keyspace, vertexOperationsTableName),
		fullTableName(keyspace, haltedVerticesTableName), fullTableName(keyspace, edgeOperationsTableName))

	if err := session.Query(cql).Exec(); err != nil {
		return errors.Wrap(err, "error creating vertex/edge operations tables")
	}

	return nil
}
