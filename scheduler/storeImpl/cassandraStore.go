package storeImpl

import (
	"fmt"
	"time"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/common/cassandra"
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/store"
	"github.com/gocql/gocql"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func init() {
	store.RegisterJobStore("Cassandra", &jobStoreFactory{})
	store.RegisterEntityStore("Cassandra", &entityStoreFactory{})
}

const (
	vertexMessagesTableName   = "vertexMessages"
	vertexOperationsTableName = "vertexOperations"
	haltedVerticesTableName   = "haltedVertices"
	edgeOperationsTableName   = "edgeOperations"
)

type cassandraStore struct {
	params  protos.CassandraStoreParams
	cluster *gocql.ClusterConfig
	session *gocql.Session
}

func (store *cassandraStore) Connect() error {
	cluster := gocql.NewCluster(store.params.Hosts...)
	cluster.Timeout = time.Duration(store.params.Timeout)

	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}

	store.cluster = cluster
	store.session = session
	return nil
}

func (store *cassandraStore) Close() {
	if store.session != nil {
		store.session.Close()
		store.cluster = nil
		store.session = nil
	}
}

func (store *cassandraStore) Init() error {
	if err := ensureTables(store.session, store.params.Keyspace); err != nil {
		return err
	}

	return nil
}

func (store *cassandraStore) GetVertexRanges(verticesPerRange int) ([]*store.VertexRange, error) {
	partitioner, tokenRanges, err := cassandra.BuildTokenRanges(store.params.Hosts, store.params.Keyspace)
	if err != nil {
		return nil, err
	}

	splitter, err := cassandra.NewTokenRangeSplitter(partitioner)
	if err != nil {
		return nil, err
	}

	tokenRanges = splitter.SplitTokenRanges(tokenRanges, verticesPerRange)

	//TODO:
	return nil, nil
}

func (store *cassandraStore) LoadJobs() ([]*pregel.Job, error) {
	//TODO
	return nil, nil
}

func (store *cassandraStore) SaveJob(job *pregel.Job) error {
	//TODO
	return nil
}

func (store *cassandraStore) LoadJobResult(jobID string) ([]byte, error) {
	//TODO
	return nil, nil
}

func (store *cassandraStore) SaveJobResult(jobID string, value []byte) error {
	//TODO
	return nil
}

func (store *cassandraStore) fullTableName(table string) string {
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

// factories

type jobStoreFactory struct{}

func (store *jobStoreFactory) CreateStore(params interface{}) store.JobStore {
	return &cassandraStore{params: params.(protos.CassandraStoreParams)}
}

func (store *jobStoreFactory) ParamsEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.CassandraStoreParams) })
}

type entityStoreFactory struct{}

func (store *entityStoreFactory) CreateStore(params interface{}) store.EntityStore {
	return &cassandraStore{params: params.(protos.CassandraStoreParams)}
}

func (store *entityStoreFactory) ParamsEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.CassandraStoreParams) })
}
