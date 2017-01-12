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
	hosts             []string
	keyspace          string
	replicationFactor int
	cluster           *gocql.ClusterConfig
	session           *gocql.Session
}

func NewJobStore(hosts []string, keyspace string, replicationFactor int) store.JobStore {
	return &cassandraJobStore{
		hosts:             hosts,
		keyspace:          keyspace,
		replicationFactor: replicationFactor,
	}
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
	if err := store.ensureSchema(); err != nil {
		return err
	}

	return nil
}

func (store *cassandraJobStore) LoadAll() ([]*pregel.Job, error) {
	cql := fmt.Sprintf(`SELECT id, status, store, store_params, algorithm, algorithm_params, label, creationTime,
task_cpu, task_mem, task_vertices, task_timeout FROM %s;`, store.fullTableName(jobsTableName))

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(int), new(string), new([]byte), new(string), new([]byte), new(string),
			new(time.Time), new(float64), new(float64), new(int), new(int)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.Job{
			ID:              dest[0].(string),
			Status:          dest[1].(pregel.JobStatus),
			Store:           dest[2].(string),
			StoreParams:     dest[3].([]byte),
			Algorithm:       dest[4].(string),
			AlgorithmParams: dest[5].([]byte),
			Label:           dest[6].(string),
			CreationTime:    dest[7].(time.Time),
			TaskCPU:         dest[8].(float64),
			TaskMEM:         dest[9].(float64),
			TaskVertices:    dest[10].(int),
			TaskTimeout:     dest[11].(int),
		}
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

func (store *cassandraJobStore) Save(job *pregel.Job) error {
	cql := fmt.Sprintf(`INSERT INTO %s (id, label, creationTime, status, store, store_params, algorithm, algorithm_params, 
task_cpu, task_mem, task_vertices, task_timeout) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`, store.fullTableName(jobsTableName))
	args := []interface{}{job.ID, job.Label, job.CreationTime, job.Status, job.Store, job.StoreParams, job.Algorithm, job.AlgorithmParams,
		job.TaskCPU, job.TaskMEM, job.TaskVertices, job.TaskTimeout}
	query := store.session.Query(cql, args...)

	return query.Exec()
}

func (store *cassandraJobStore) SaveStatus(jobID string, status pregel.JobStatus, result []byte, checkpoint []byte) error {
	cql := fmt.Sprintf(`UPDATE %s SET status = ?, result = ?, checkpoint = ? WHERE id = ?;`, store.fullTableName(jobsTableName))
	args := []interface{}{status, result, checkpoint, jobID}
	query := store.session.Query(cql, args...)

	return query.Exec()
}

func (store *cassandraJobStore) SetStatus(jobID string, status pregel.JobStatus) error {
	cql := fmt.Sprintf(`UPDATE %s SET status = ? WHERE id = ?;`, store.fullTableName(jobsTableName))
	args := []interface{}{status, jobID}
	query := store.session.Query(cql, args...)

	return query.Exec()
}

func (store *cassandraJobStore) LoadResult(jobID string) ([]byte, error) {
	cql := fmt.Sprintf(`SELECT result FROM %s WHERE id = ?;`, store.fullTableName(jobsTableName))

	var result []byte
	err := ExecuteScalar(store.session, cql, &result, jobID)

	return result, err
}

func (store *cassandraJobStore) LoadCheckpoint(jobID string) ([]byte, error) {
	cql := fmt.Sprintf(`SELECT checkpoint FROM %s WHERE id = ?;`, store.fullTableName(jobsTableName))

	var checkpoint []byte
	err := ExecuteScalar(store.session, cql, &checkpoint, jobID)

	return checkpoint, err
}

func (store *cassandraJobStore) fullTableName(table string) string {
	return GetFullTableName(store.keyspace, table)
}

func (store *cassandraJobStore) ensureSchema() error {
	statements := []string{
		fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS "%s" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }`, store.keyspace, store.replicationFactor),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(id text, label text, creationTime timestamp, status int, store text, 
store_params blob, algorithm text, algorithm_params blob, task_cpu double, task_mem double, task_vertices int, 
task_timeout int, result blob, checkpoint blob, PRIMARY KEY(id));`, store.fullTableName(jobsTableName)),
	}

	for _, cql := range statements {
		if err := store.session.Query(cql).Exec(); err != nil {
			return errors.Wrap(err, "error creating schema")
		}
	}

	return nil
}
