package cassandra

import (
	"fmt"
	"time"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

const (
	jobsTableName = "_jobs"
)

type cassandraJobStore struct {
	hosts    []string
	keyspace string
	cluster  *gocql.ClusterConfig
	session  *gocql.Session
}

func NewJobStore(hosts []string, keyspace string) store.JobStore {
	return &cassandraJobStore{hosts: hosts, keyspace: keyspace}
}

func (store *cassandraJobStore) Connect() error {
	cluster := gocql.NewCluster(store.hosts...)
	cluster.Timeout = 5 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}

	store.cluster = cluster
	store.session = session
	return nil
}

func (store *cassandraJobStore) Close() {
	if store.session != nil {
		store.session.Close()
		store.cluster = nil
		store.session = nil
	}
}

func (store *cassandraJobStore) Init() error {
	if err := store.ensureTables(); err != nil {
		return err
	}

	return nil
}

func (store *cassandraJobStore) LoadJobs() ([]*pregel.Job, error) {
	cql := fmt.Sprintf(`SELECT id, status, superstep, store, store_params, algorithm, algorithm_params, vertices_per_task FROM %s;`, store.fullTableName(jobsTableName))

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(int), new(int), new(string), new([]byte), new(string), new([]byte), new(int)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.Job{ID: dest[0].(string), Status: dest[1].(pregel.JobStatus), Superstep: dest[2].(int), Store: dest[3].(string),
			StoreParams: dest[4].([]byte), Algorithm: dest[5].(string), AlgorithmParams: dest[6].([]byte), VerticesPerTask: dest[7].(int)}
	}

	entities, err := ExecuteSelect(store.session, cql, createScanDest, createEntityFunc)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.Job, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.Job)
	}

	return result, nil
}

func (store *cassandraJobStore) SaveJob(job *pregel.Job) error {
	cql := fmt.Sprintf(`INSERT INTO %s (id, status, superstep, store, store_params, algorithm, algorithm_params, vertices_per_task) VALUES(?, ?, ?, ?, ?, ?, ?, ?);`, store.fullTableName(jobsTableName))
	args := []interface{}{job.ID, job.Status, job.Superstep, job.Store, job.StoreParams, job.Algorithm, job.AlgorithmParams, job.VerticesPerTask}
	query := store.session.Query(cql, args...)

	return query.Exec()
}

func (store *cassandraJobStore) LoadJobResult(jobID string) ([]byte, error) {
	cql := fmt.Sprintf(`SELECT result FROM %s WHERE id = ?;`, store.fullTableName(jobsTableName))

	var result []byte
	err := ExecuteScalar(store.session, cql, &result, jobID)

	return result, err
}

func (store *cassandraJobStore) SaveJobResult(jobID string, value []byte) error {
	cql := fmt.Sprintf(`UPDATE %s SET result = ? WHERE id = ?;`, store.fullTableName(jobsTableName))
	args := []interface{}{value, jobID}
	query := store.session.Query(cql, args...)

	return query.Exec()
}

func (store *cassandraJobStore) fullTableName(table string) string {
	return GetFullTableName(store.keyspace, table)
}

func (store *cassandraJobStore) ensureTables() error {
	cql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(id text, status int, superstep int, store text, store_params blob, algorithm text, algorithm_params blob, vertices_per_task int, result blob, PRIMARY KEY(id));`,
		store.fullTableName(jobsTableName))

	if err := store.session.Query(cql).Exec(); err != nil {
		return errors.Wrap(err, "error creating job table")
	}

	return nil
}
