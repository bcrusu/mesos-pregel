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
	vertexMessagesTableName   = "vertexMessages"
	vertexOperationsTableName = "vertexOperations"
	haltedVerticesTableName   = "haltedVertices"
	edgeOperationsTableName   = "edgeOperations"
)

type CassandraStore struct {
	params      protos.CassandraStoreParams
	entityRange protos.CassandraEntityRange
	cluster     *gocql.ClusterConfig
	session     *gocql.Session
}

func NewCassandraStore(params protos.CassandraStoreParams, entityRange protos.CassandraEntityRange) Store {
	return &CassandraStore{params: params, entityRange: entityRange}
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
	if len(store.params.VerticesTable) == 0 {
		return make([]*pregel.Vertex, 0), nil
	}

	cql := fmt.Sprintf(`SELECT id, value FROM %s WHERE token(id) >= ? AND token(id) <= ?;`, store.fullTableName(store.params.VerticesTable))
	params := []interface{}{store.entityRange.StartToken, store.entityRange.EndToken}

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
	if len(store.params.EdgesTable) == 0 {
		return make([]*pregel.Edge, 0), nil
	}

	cql := fmt.Sprintf(`SELECT "from", "to", value FROM %s WHERE token("from") >= ? AND token("from") <= ?;`, store.fullTableName(store.params.EdgesTable))
	params := []interface{}{store.entityRange.StartToken, store.entityRange.EndToken}

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new([]byte)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.Edge{From: dest[0].(string), To: dest[1].(string), Value: dest[2].([]byte)}
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

func (store *CassandraStore) LoadVertexMessages(jobID string, superstep int) ([]*pregel.VertexMessage, error) {
	cql := fmt.Sprintf(`SELECT "to", "from", value FROM %s WHERE token(id) >= ? AND token(id) <= ? AND job_id=? AND superstep=?;`,
		store.fullTableName(vertexMessagesTableName))
	params := []interface{}{store.entityRange.StartToken, store.entityRange.EndToken, jobID, superstep}

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new([]byte)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.VertexMessage{To: dest[0].(string), From: dest[1].(string), Value: dest[2].([]byte), JobID: jobID, Superstep: superstep}
	}

	entities, err := store.executeSelect(cql, params, createScanDest, createEntityFunc)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.VertexMessage, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.VertexMessage)
	}

	return result, nil
}

func (store *CassandraStore) SaveVertexMessages(messages []*pregel.VertexMessage) error {
	cql := fmt.Sprintf(`INSERT INTO %s ("to", "from", job_id, superstep, value) VALUES(?, ?, ?, ?, ?);`, store.fullTableName(vertexMessagesTableName))

	items := make([]interface{}, len(messages))
	for i, v := range messages {
		items[i] = v
	}

	getItemSize := func(item interface{}) int {
		return vertexMessageSize(item.(*pregel.VertexMessage))
	}

	getItemArgs := func(item interface{}) []interface{} {
		msg := item.(*pregel.VertexMessage)
		return []interface{}{msg.To, msg.From, msg.JobID, msg.Superstep, msg.Value}
	}

	batches := store.createBatches(cql, items, getItemSize, getItemArgs)
	return store.executeBatches(batches)
}

func (store *CassandraStore) LoadVertexOperations(jobID string, superstep int) ([]*pregel.VertexOperation, error) {
	cql := fmt.Sprintf(`SELECT id, performed_by, type, value FROM %s WHERE token(id) >= ? AND token(id) <= ? AND job_id=? AND superstep=?;`,
		store.fullTableName(vertexOperationsTableName))
	params := []interface{}{store.entityRange.StartToken, store.entityRange.EndToken, jobID, superstep}

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new(pregel.VertexOperationType), new([]byte)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.VertexOperation{ID: dest[0].(string), JobID: jobID, Superstep: superstep,
			PerformedBy: dest[1].(string), Type: dest[1].(pregel.VertexOperationType), Value: dest[3].([]byte)}
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

func (store *CassandraStore) LoadHaltedVertices(jobID string, superstep int) ([]string, error) {
	cql := fmt.Sprintf(`SELECT id FROM %s WHERE token(id) >= ? AND token(id) <= ? AND job_id=? AND superstep=?;`,
		store.fullTableName(haltedVerticesTableName))
	params := []interface{}{store.entityRange.StartToken, store.entityRange.EndToken, jobID, superstep}

	createScanDest := func() []interface{} {
		return []interface{}{new(string)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return dest[0].(string)
	}

	entities, err := store.executeSelect(cql, params, createScanDest, createEntityFunc)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(string)
	}

	return result, nil
}

func (store *CassandraStore) SaveHaltedVertices(halted []*pregel.VertexHalted) error {
	cql := fmt.Sprintf(`INSERT INTO %s (id, job_id, superstep) VALUES(?, ?, ?);`, store.fullTableName(vertexMessagesTableName))

	items := make([]interface{}, len(halted))
	for i, v := range halted {
		items[i] = v
	}

	getItemSize := func(item interface{}) int {
		return vertexHaltedSize(item.(*pregel.VertexHalted))
	}

	getItemArgs := func(item interface{}) []interface{} {
		h := item.(*pregel.VertexHalted)
		return []interface{}{h.ID, h.JobID, h.Superstep}
	}

	batches := store.createBatches(cql, items, getItemSize, getItemArgs)
	return store.executeBatches(batches)
}

func (store *CassandraStore) LoadEdgeOperations(jobID string, superstep int) ([]*pregel.EdgeOperation, error) {
	cql := fmt.Sprintf(`SELECT "from", "to", performed_by, type, value FROM %s WHERE token("from") >= ? AND token("from") <= ? AND job_id=? AND superstep=?;`,
		store.fullTableName(edgeOperationsTableName))
	params := []interface{}{store.entityRange.StartToken, store.entityRange.EndToken, jobID, superstep}

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new(string), new(pregel.EdgeOperationType), new([]byte)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.EdgeOperation{From: dest[0].(string), To: dest[1].(string), JobID: jobID, Superstep: superstep,
			PerformedBy: dest[2].(string), Type: dest[3].(pregel.EdgeOperationType), Value: dest[4].([]byte)}
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
	cql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s("to" text, "from" text, job_id text, superstep int, value blob, PRIMARY KEY(("to"), job_id, superstep, "from"));
CREATE TABLE IF NOT EXISTS %s(id text, job_id text, superstep int, performed_by text, type int, value blob, PRIMARY KEY((id), job_id, superstep, performed_by));
CREATE TABLE IF NOT EXISTS %s(id text, job_id text, superstep int, PRIMARY KEY((id), job_id, superstep));
CREATE TABLE IF NOT EXISTS %s("from" text, "to" text, job_id text, superstep int, performed_by text, type int, value blob, PRIMARY KEY(("from"), "to", job_id, superstep, performed_by));`,
		fullTableName(keyspace, vertexMessagesTableName), fullTableName(keyspace, vertexOperationsTableName),
		fullTableName(keyspace, haltedVerticesTableName), fullTableName(keyspace, edgeOperationsTableName))

	if err := session.Query(cql).Exec(); err != nil {
		return errors.Wrap(err, "error creating vertex/edge operations tables")
	}

	return nil
}

func vertexHaltedSize(halted *pregel.VertexHalted) int {
	result := 4
	result += len(halted.ID)
	result += len(halted.JobID)
	return result
}

func vertexMessageSize(message *pregel.VertexMessage) int {
	result := 4
	result += len(message.From)
	result += len(message.To)
	result += len(message.JobID)
	result += len(message.Value)
	return result
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
