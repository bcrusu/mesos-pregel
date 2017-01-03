package store

import (
	"fmt"
	"time"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/common/cassandra"
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/executor/store"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gocql/gocql"
	"github.com/gogo/protobuf/proto"
)

const (
	vertexMessagesTableName   = "vertexMessages"
	vertexOperationsTableName = "vertexOperations"
	haltedVerticesTableName   = "haltedVertices"
	edgeOperationsTableName   = "edgeOperations"
)

type CassandraStore struct {
	params           protos.CassandraStoreParams
	tokenRangeParams []interface{}
	cluster          *gocql.ClusterConfig
	session          *gocql.Session
	batchExecutor    *cassandra.BatchExecutor
}

func NewCassandraStore(params protos.CassandraStoreParams, tokenRange protos.CassandraTokenRange) (Store, error) {

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
	if len(store.params.VerticesTable) == 0 {
		return make([]*pregel.Vertex, 0), nil
	}

	cql := fmt.Sprintf(`SELECT id, value FROM %s WHERE token(id) >= ? AND token(id) <= ?;`, store.fullTableName(store.params.VerticesTable))
	params := store.tokenRangeParams

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new([]byte)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.Vertex{ID: dest[0].(string), Value: dest[1].([]byte)}
	}

	entities, err := cassandra.ExecuteSelect(store.session, cql, params, createScanDest, createEntityFunc)
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
	params := store.tokenRangeParams

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new([]byte)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.Edge{From: dest[0].(string), To: dest[1].(string), Value: dest[2].([]byte)}
	}

	entities, err := cassandra.ExecuteSelect(store.session, cql, params, createScanDest, createEntityFunc)
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
	cql := fmt.Sprintf(`SELECT "to", value FROM %s WHERE token(id) >= ? AND token(id) <= ? AND job_id=? AND superstep=?;`,
		store.fullTableName(vertexMessagesTableName))
	params := append(store.tokenRangeParams, jobID, superstep)

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new([]byte)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.VertexMessage{To: dest[0].(string), Value: dest[1].([]byte), JobID: jobID, Superstep: superstep}
	}

	entities, err := cassandra.ExecuteSelect(store.session, cql, params, createScanDest, createEntityFunc)
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
	cql := fmt.Sprintf(`INSERT INTO %s ("to", job_id, superstep, unique_id, value) VALUES(?, ?, ?, ?, ?);`, store.fullTableName(vertexMessagesTableName))

	items := make([]interface{}, len(messages))
	for i, v := range messages {
		items[i] = v
	}

	getItemSize := func(item interface{}) int {
		message := item.(*pregel.VertexMessage)
		result := 20
		result += len(message.To)
		result += len(message.JobID)
		result += len(message.Value)
		return result
	}

	getItemArgs := func(item interface{}) []interface{} {
		msg := item.(*pregel.VertexMessage)
		return []interface{}{msg.To, msg.JobID, msg.Superstep, gocql.TimeUUID(), msg.Value}
	}

	return store.batchExecutor.Execute(cql, items, getItemSize, getItemArgs)
}

func (store *CassandraStore) LoadVertexOperations(jobID string, superstep int) ([]*pregel.VertexOperation, error) {
	cql := fmt.Sprintf(`SELECT id, type, value FROM %s WHERE token(id) >= ? AND token(id) <= ? AND job_id=? AND superstep=?;`,
		store.fullTableName(vertexOperationsTableName))
	params := append(store.tokenRangeParams, jobID, superstep)

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new(pregel.VertexOperationType), new([]byte)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.VertexOperation{ID: dest[0].(string), JobID: jobID, Superstep: superstep,
			Type: dest[1].(pregel.VertexOperationType), Value: dest[2].([]byte)}
	}

	entities, err := cassandra.ExecuteSelect(store.session, cql, params, createScanDest, createEntityFunc)
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
	cql := fmt.Sprintf(`INSERT INTO %s (id, job_id, superstep, unique_id, type, value) VALUES(?, ?, ?, ?, ?, ?);`, store.fullTableName(vertexOperationsTableName))

	items := make([]interface{}, len(operations))
	for i, v := range operations {
		items[i] = v
	}

	getItemSize := func(item interface{}) int {
		operation := item.(*pregel.VertexOperation)
		result := 24
		result += len(operation.ID)
		result += len(operation.JobID)
		result += len(operation.Value)
		return result
	}

	getItemArgs := func(item interface{}) []interface{} {
		op := item.(*pregel.VertexOperation)
		return []interface{}{op.ID, op.JobID, op.Superstep, gocql.TimeUUID(), op.Type, op.Value}
	}

	return store.batchExecutor.Execute(cql, items, getItemSize, getItemArgs)
}

func (store *CassandraStore) LoadHaltedVertices(jobID string, superstep int) ([]string, error) {
	cql := fmt.Sprintf(`SELECT id FROM %s WHERE token(id) >= ? AND token(id) <= ? AND job_id=? AND superstep=?;`,
		store.fullTableName(haltedVerticesTableName))
	params := append(store.tokenRangeParams, jobID, superstep)

	createScanDest := func() []interface{} {
		return []interface{}{new(string)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return dest[0].(string)
	}

	entities, err := cassandra.ExecuteSelect(store.session, cql, params, createScanDest, createEntityFunc)
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
		halted := item.(*pregel.VertexHalted)
		result := 4
		result += len(halted.ID)
		result += len(halted.JobID)
		return result
	}

	getItemArgs := func(item interface{}) []interface{} {
		h := item.(*pregel.VertexHalted)
		return []interface{}{h.ID, h.JobID, h.Superstep}
	}

	return store.batchExecutor.Execute(cql, items, getItemSize, getItemArgs)
}

func (store *CassandraStore) LoadEdgeOperations(jobID string, superstep int) ([]*pregel.EdgeOperation, error) {
	cql := fmt.Sprintf(`SELECT "from", "to", type, value FROM %s WHERE token("from") >= ? AND token("from") <= ? AND job_id=? AND superstep=?;`,
		store.fullTableName(edgeOperationsTableName))
	params := append(store.tokenRangeParams, jobID, superstep)

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new(pregel.EdgeOperationType), new([]byte)}
	}

	createEntityFunc := func(dest []interface{}) interface{} {
		return &pregel.EdgeOperation{From: dest[0].(string), To: dest[1].(string), JobID: jobID, Superstep: superstep,
			Type: dest[2].(pregel.EdgeOperationType), Value: dest[3].([]byte)}
	}

	entities, err := cassandra.ExecuteSelect(store.session, cql, params, createScanDest, createEntityFunc)
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
	cql := fmt.Sprintf(`INSERT INTO %s ("from", "to", job_id, superstep, unique_id, type, value) VALUES(?, ?, ?, ?, ?, ?, ?);`, store.fullTableName(edgeOperationsTableName))

	items := make([]interface{}, len(operations))
	for i, v := range operations {
		items[i] = v
	}

	getItemSize := func(item interface{}) int {
		operation := item.(*pregel.EdgeOperation)

		result := 24
		result += len(operation.From)
		result += len(operation.To)
		result += len(operation.JobID)
		result += len(operation.Value)
		return result
	}

	getItemArgs := func(item interface{}) []interface{} {
		op := item.(*pregel.EdgeOperation)
		return []interface{}{op.From, op.To, op.JobID, op.Superstep, gocql.TimeUUID(), op.Type, op.Value}
	}

	return store.batchExecutor.Execute(cql, items, getItemSize, getItemArgs)
}

func (store *CassandraStore) fullTableName(table string) string {
	return cassandra.GetFullTableName(store.params.Keyspace, table)
}

func getTokenRangeParams(tokenRange protos.CassandraTokenRange) ([]interface{}, error) {
	partitioner, err := cassandra.NewPartitioner(tokenRange.Partitioner)
	if err != nil {
		return nil, err
	}

	startToken := partitioner.ParseString(tokenRange.StartToken)
	endToken := partitioner.ParseString(tokenRange.EndToken)

	return []interface{}{startToken, endToken}, nil
}

type cassandraStoreFactory struct{}

func (store *cassandraStoreFactory) CreateStore(params interface{}, vertexRange interface{}) (store.Store, error) {
	tokenRange := vertexRange.(*protos.CassandraTokenRange)
	tokenRangeParams, err := getTokenRangeParams(tokenRange)
	if err != nil {
		return nil, err
	}

	return &CassandraStore{params: params.(*protos.CassandraStoreParams), tokenRangeParams: tokenRangeParams}, nil
}

func (store *cassandraStoreFactory) ParamsEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.CassandraStoreParams) })
}

func (store *cassandraStoreFactory) VertexRangeEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.CassandraTokenRange) })
}
