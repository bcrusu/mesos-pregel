package stores

import (
	"fmt"
	"time"

	"github.com/bcrusu/pregel"
	"github.com/bcrusu/pregel/protos"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
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

func (store *CassandraStore) LoadVertices() ([]*pregel.Vertex, error) {
	cql := fmt.Sprintf("SELECT id, value FROM %s WHERE token(id) >= ? AND token(id) <= ?;", store.fullVerticesTableName())
	tokenRange := store.params.TokenRange
	iter := store.session.Query(cql, tokenRange.Start, tokenRange.End).Iter()

	result := make([]*pregel.Vertex, 0, 1000)

	var id string
	var value []byte
	for iter.Scan(&id, &value) {
		vertex := &pregel.Vertex{ID: id, Value: value}
		result = append(result, vertex)
	}

	if err := iter.Close(); err != nil {
		return nil, errors.Wrap(err, "failed to load graph vertices")
	}

	return result, nil
}

func (store *CassandraStore) LoadEdges() ([]*pregel.Edge, error) {
	cql := fmt.Sprintf("SELECT \"from\", \"to\", weight FROM %s WHERE token(\"from\") >= ? AND token(\"from\") <= ?;", store.fullEdgesTableName())
	tokenRange := store.params.TokenRange
	iter := store.session.Query(cql, tokenRange.Start, tokenRange.End).Iter()

	result := make([]*pregel.Edge, 0, 1000)

	var from string
	var to string
	var weight int
	for iter.Scan(&from, &to, &weight) {
		edge := &pregel.Edge{From: from, To: to, Weight: weight}
		result = append(result, edge)
	}

	if err := iter.Close(); err != nil {
		return nil, errors.Wrap(err, "failed to load graph edges")
	}

	return result, nil
}

func (store *CassandraStore) LoadVertexOperations(jobID string) ([]*pregel.VertexOperation, error) {
	//TODO
	return nil, nil
}

func (store *CassandraStore) LoadVertexOperationsForSuperstep(jobID string, superstep int) ([]*pregel.VertexOperation, error) {
	//TODO
	return nil, nil
}

func (store *CassandraStore) SaveVertexOperations(operations []*pregel.VertexOperation) error {
	cql := fmt.Sprintf("INSERT INTO %s (id, \"jobId\", superstep, \"performedBy\", type, value) VALUES(?, ?, ?, ?, ?, ?);", store.fullTableName("vertexOperations"))

	items := make([]interface{}, len(operations))
	for i, v := range operations {
		items[i] = v
	}

	getItemSize := func(item interface{}) int {
		return vertexOperationSize(item.(*pregel.VertexOperation))
	}

	getItemArgs := func(item interface{}) []interface{} {
		op := item.(*pregel.VertexOperation)
		return []interface{}{op.ID, op.JobID, op.Superstep, op.PerformedBy, op.Type, op.Value}
	}

	batches := store.createBatches(cql, items, getItemSize, getItemArgs)
	return store.executeBatches(batches)
}

func (store *CassandraStore) LoadEdgeOperations(jobID string) ([]*pregel.EdgeOperation, error) {
	//TODO
	return nil, nil
}
func (store *CassandraStore) LoadEdgeOperationsForSuperstep(jobID string, superstep int) ([]*pregel.EdgeOperation, error) {
	//TODO
	return nil, nil
}

func (store *CassandraStore) SaveEdgeOperations(operations []*pregel.EdgeOperation) error {
	cql := fmt.Sprintf("INSERT INTO %s (\"from\", \"to\", \"jobId\", superstep, \"performedBy\", type, value) VALUES(?, ?, ?, ?, ?, ?, ?);", store.fullTableName("edgeOperations"))

	items := make([]interface{}, len(operations))
	for i, v := range operations {
		items[i] = v
	}

	getItemSize := func(item interface{}) int {
		return edgeOperationSize(item.(*pregel.EdgeOperation))
	}

	getItemArgs := func(item interface{}) []interface{} {
		op := item.(*pregel.EdgeOperation)
		return []interface{}{op.From, op.To, op.JobID, op.Superstep, op.PerformedBy, op.Type, op.Value}
	}

	batches := store.createBatches(cql, items, getItemSize, getItemArgs)
	return store.executeBatches(batches)
}

func (store *CassandraStore) fullVerticesTableName() string {
	return store.fullTableName(store.params.VerticesTable)
}

func (store *CassandraStore) fullEdgesTableName() string {
	return store.fullTableName(store.params.EdgesTable)
}

func (store *CassandraStore) fullTableName(table string) string {
	return fmt.Sprintf("\"%s\".\"%s\"", store.params.Keyspace, table)
}

type itemSizeFunc func(interface{}) int
type itemArgsFunc func(interface{}) []interface{}

func (store *CassandraStore) createBatches(cql string, items []interface{}, getItemSize itemSizeFunc, getItemArgs itemArgsFunc) []*gocql.Batch {
	batchMaxSize := int(store.params.BatchOptions.MaxSize)
	batchMaxBytes := int(store.params.BatchOptions.MaxBytes)

	result := make([]*gocql.Batch, 0, 100)

	currentBatch := store.session.NewBatch(gocql.LoggedBatch)
	var currentBatchBytes int
	for _, item := range items {
		itemBytes := getItemSize(item)
		if currentBatch.Size() == batchMaxSize || currentBatchBytes+itemBytes >= batchMaxBytes {
			result = append(result, currentBatch)
			currentBatch = store.session.NewBatch(gocql.LoggedBatch)
			currentBatchBytes = 0
		}

		var entry gocql.BatchEntry
		entry.Stmt = cql
		entry.Args = getItemArgs(item)
		currentBatch.Entries = append(currentBatch.Entries, entry)
	}

	if currentBatch.Size() > 0 {
		result = append(result, currentBatch)
	}

	return result
}

func (store *CassandraStore) executeBatches(batches []*gocql.Batch) error {
	for i, batch := range batches {
		if err := store.session.ExecuteBatch(batch); err != nil {
			return errors.Wrapf(err, "failed to execute batch no. %d of %d", i+1, len(batches))
		}
	}

	return nil
}

func vertexOperationSize(operation *pregel.VertexOperation) int {
	result := 8
	result += len(operation.ID)
	result += len(operation.JobID)
	result += len(operation.PerformedBy)
	result += len(operation.Value)
	return result
}

func edgeOperationSize(operation *pregel.EdgeOperation) int {
	result := 8
	result += len(operation.From)
	result += len(operation.To)
	result += len(operation.JobID)
	result += len(operation.PerformedBy)
	result += len(operation.Value)
	return result
}
