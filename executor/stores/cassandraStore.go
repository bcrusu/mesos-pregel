package stores

import (
	"fmt"
	"time"

	"github.com/bcrusu/pregel"
	"github.com/bcrusu/pregel/protos"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

const (
	vertexOperationsTableName = "vertexOperations"
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

	if err := ensureOperationsTables(session, store.params.Keyspace); err != nil {
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
	cql := fmt.Sprintf(`SELECT id, value FROM %s WHERE token(id) >= ? AND token(id) <= ?;`, store.fullTableName(store.params.VerticesTable))
	params := []interface{}{store.params.TokenRange.Start, store.params.TokenRange.End}

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new([]byte)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.Vertex{ID: dest[0].(string), Value: dest[1].([]byte)}
	}

	entities, err := store.executeSelect(cql, params, createScanDest, createEntityFunc)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.Vertex, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.Vertex)
	}

	return result, nil
}

func (store *CassandraStore) LoadEdges() ([]*pregel.Edge, error) {
	cql := fmt.Sprintf(`SELECT "from", "to", weight FROM %s WHERE token("from") >= ? AND token("from") <= ?;`, store.fullTableName(store.params.EdgesTable))
	params := []interface{}{store.params.TokenRange.Start, store.params.TokenRange.End}

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new(int)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.Edge{From: dest[0].(string), To: dest[1].(string), Weight: dest[2].(int)}
	}

	entities, err := store.executeSelect(cql, params, createScanDest, createEntityFunc)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.Edge, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.Edge)
	}

	return result, nil
}

func (store *CassandraStore) LoadVertexOperations(jobID string, superstep int) ([]*pregel.VertexOperation, error) {
	cql := fmt.Sprintf(`SELECT id, job_id, superstep, performed_by, type, value FROM %s WHERE token(id) >= ? AND token(id) <= ? AND job_id=? AND superstep=?;`,
		store.fullTableName(vertexOperationsTableName))
	params := []interface{}{store.params.TokenRange.Start, store.params.TokenRange.End, jobID, superstep}

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new(int), new(string), new(pregel.VertexOperationType), new([]byte)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.VertexOperation{ID: dest[0].(string), JobID: dest[1].(string), Superstep: dest[2].(int),
			PerformedBy: dest[3].(string), Type: dest[4].(pregel.VertexOperationType), Value: dest[5].([]byte)}
	}

	entities, err := store.executeSelect(cql, params, createScanDest, createEntityFunc)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.VertexOperation, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.VertexOperation)
	}

	return result, nil
}

func (store *CassandraStore) SaveVertexOperations(operations []*pregel.VertexOperation) error {
	cql := fmt.Sprintf(`INSERT INTO %s (id, job_id, superstep, performed_by, type, value) VALUES(?, ?, ?, ?, ?, ?);`, store.fullTableName(vertexOperationsTableName))

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

func (store *CassandraStore) LoadEdgeOperations(jobID string, superstep int) ([]*pregel.EdgeOperation, error) {
	cql := fmt.Sprintf(`SELECT "from", "to", job_id, superstep, performed_by, type, value FROM %s WHERE token("from") >= ? AND token("from") <= ? AND job_id=? AND superstep=?;`,
		store.fullTableName(edgeOperationsTableName))
	params := []interface{}{store.params.TokenRange.Start, store.params.TokenRange.End, jobID, superstep}

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new(string), new(int), new(string), new(pregel.EdgeOperationType), new([]byte)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.EdgeOperation{From: dest[0].(string), To: dest[1].(string), JobID: dest[2].(string), Superstep: dest[3].(int),
			PerformedBy: dest[4].(string), Type: dest[5].(pregel.EdgeOperationType), Value: dest[6].([]byte)}
	}

	entities, err := store.executeSelect(cql, params, createScanDest, createEntityFunc)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.EdgeOperation, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.EdgeOperation)
	}

	return result, nil
}

func (store *CassandraStore) SaveEdgeOperations(operations []*pregel.EdgeOperation) error {
	cql := fmt.Sprintf(`INSERT INTO %s ("from", "to", job_id, superstep, performed_by, type, value) VALUES(?, ?, ?, ?, ?, ?, ?);`, store.fullTableName(edgeOperationsTableName))

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

func (store *CassandraStore) fullTableName(table string) string {
	return fullTableName(store.params.Keyspace, table)
}

func fullTableName(keyspace string, table string) string {
	return fmt.Sprintf(`"%s"."%s"`, keyspace, table)
}

type createScanDestFunc func() []interface{}
type createEntityFunc func(dest []interface{}) interface{}

func (store *CassandraStore) executeSelect(cql string, params interface{}, createScanDest createScanDestFunc, createEntity createEntityFunc) ([]interface{}, error) {
	iter := store.session.Query(cql, params).Iter()

	result := make([]interface{}, 0, 1000)

	dest := createScanDest()
	for iter.Scan(dest) {
		entity := createEntity(dest)
		result = append(result, entity)
	}

	if err := iter.Close(); err != nil {
		return nil, errors.Wrap(err, "failed to execute select query")
	}

	return result, nil
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

func ensureOperationsTables(session *gocql.Session, keyspace string) error {
	cql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(id text, job_id text, superstep int, performed_by text, type int, value blob, PRIMARY KEY((id), job_id, superstep, performed_by));
CREATE TABLE IF NOT EXISTS %s("from" text, "to" text, job_id text, superstep int, performed_by text, type int, value blob, PRIMARY KEY(("from"), "to", job_id, superstep, performed_by));`,
		fullTableName(keyspace, vertexOperationsTableName), fullTableName(keyspace, edgeOperationsTableName))

	if err := session.Query(cql).Exec(); err != nil {
		return errors.Wrap(err, "error creating vertex/edge operations tables")
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
