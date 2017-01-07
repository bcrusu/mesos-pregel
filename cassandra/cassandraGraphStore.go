package cassandra

import (
	"fmt"
	"strings"
	"time"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/gocql/gocql"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

const (
	vertexMessagesTableName   = "_vertexMessages"
	vertexOperationsTableName = "_vertexOperations"
	haltedVerticesTableName   = "_haltedVertices"
	edgeOperationsTableName   = "_edgeOperations"
	tokenFilterPlaceholder    = "<TOKEN_FILTER>"
)

type cassandraGraphStore struct {
	params            protos.CassandraStoreParams
	tokenRangeEncoder encoding.Encoder
	cluster           *gocql.ClusterConfig
	session           *gocql.Session
	batchExecutor     *BatchExecutor
}

type tokenFilter struct {
	cql    string
	params []interface{}
}

func (store *cassandraGraphStore) Connect() error {
	cluster := gocql.NewCluster(store.params.Hosts...)
	cluster.Timeout = time.Duration(store.params.Timeout)

	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}

	store.cluster = cluster
	store.session = session

	batchOptions := store.params.BatchOptions
	store.batchExecutor = NewBatchExecutor(session, int(batchOptions.MaxSize), int(batchOptions.MaxBytes))

	return nil
}

func (store *cassandraGraphStore) Close() {
	if store.session != nil {
		store.session.Close()
		store.cluster = nil
		store.session = nil
	}
}

func (store *cassandraGraphStore) Init() error {
	if err := store.ensureSchema(); err != nil {
		return err
	}

	return nil
}

func (store *cassandraGraphStore) GetVertexRanges(verticesPerRange int) ([]*store.VertexRangeHosts, error) {
	// tokenRanges, partitioner, err := BuildTokenRanges(store.params.Hosts, store.params.Keyspace)
	// if err != nil {
	// 	return nil, err
	// }

	// splitter, err := NewTokenRangeSplitter(partitioner)
	// if err != nil {
	// 	return nil, err
	// }

	// tokenRanges = splitter.SplitTokenRanges(tokenRanges, verticesPerRange)

	//TODO:
	return nil, nil
}

func (store *cassandraGraphStore) LoadVertices(vrange store.VertexRange) ([]*pregel.Vertex, error) {
	cql := fmt.Sprintf(`SELECT id, value FROM %s WHERE %s;`, store.fullTableName(store.params.VerticesTable), tokenFilterPlaceholder)

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new([]byte)}
	}

	createEntity := func(dest []interface{}) interface{} {
		return &pregel.Vertex{ID: dest[0].(string), Value: dest[1].([]byte)}
	}

	entities, err := store.executeSelectWithTokenRange(cql, vrange, `id`, createScanDest, createEntity)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.Vertex, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.Vertex)
	}

	return result, nil
}

func (store *cassandraGraphStore) SaveVertices(vertices []*pregel.Vertex) error {
	cql := fmt.Sprintf(`INSERT INTO %s (id, value) VALUES(?, ?);`, store.fullTableName(store.params.VerticesTable))

	items := make([]interface{}, len(vertices))
	for i, v := range vertices {
		items[i] = v
	}

	getItemSize := func(item interface{}) int {
		vertex := item.(*pregel.Vertex)
		result := len(vertex.ID)
		result += len(vertex.Value)
		return result
	}

	getItemArgs := func(item interface{}) []interface{} {
		vertex := item.(*pregel.Vertex)
		return []interface{}{vertex.ID, vertex.Value}
	}

	return store.batchExecutor.Execute(cql, items, getItemSize, getItemArgs)
}

func (store *cassandraGraphStore) LoadEdges(vrange store.VertexRange) ([]*pregel.Edge, error) {
	cql := fmt.Sprintf(`SELECT "from", "to", value FROM %s WHERE %s;`, store.fullTableName(store.params.EdgesTable), tokenFilterPlaceholder)

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new([]byte)}
	}

	createEntity := func(dest []interface{}) interface{} {
		return &pregel.Edge{From: dest[0].(string), To: dest[1].(string), Value: dest[2].([]byte)}
	}

	entities, err := store.executeSelectWithTokenRange(cql, vrange, `"from"`, createScanDest, createEntity)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.Edge, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.Edge)
	}

	return result, nil
}

func (store *cassandraGraphStore) SaveEdges(edges []*pregel.Edge) error {
	cql := fmt.Sprintf(`INSERT INTO %s ("from", "to", value) VALUES(?, ?, ?);`, store.fullTableName(store.params.EdgesTable))

	items := make([]interface{}, len(edges))
	for i, v := range edges {
		items[i] = v
	}

	getItemSize := func(item interface{}) int {
		edge := item.(*pregel.Edge)
		result := len(edge.From)
		result += len(edge.To)
		result += len(edge.Value)
		return result
	}

	getItemArgs := func(item interface{}) []interface{} {
		edge := item.(*pregel.Edge)
		return []interface{}{edge.From, edge.To, edge.Value}
	}

	return store.batchExecutor.Execute(cql, items, getItemSize, getItemArgs)
}

func (store *cassandraGraphStore) LoadVertexMessages(jobID string, superstep int, vrange store.VertexRange) ([]*pregel.VertexMessage, error) {
	cql := fmt.Sprintf(`SELECT "to", value FROM %s WHERE job_id=? AND superstep=? AND %s;`, store.fullTableName(vertexMessagesTableName), tokenFilterPlaceholder)
	params := []interface{}{jobID, superstep}

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new([]byte)}
	}

	createEntity := func(dest []interface{}) interface{} {
		return &pregel.VertexMessage{To: dest[0].(string), Value: dest[1].([]byte), JobID: jobID, Superstep: superstep}
	}

	entities, err := store.executeSelectWithTokenRange(cql, vrange, `id`, createScanDest, createEntity, params...)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.VertexMessage, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.VertexMessage)
	}

	return result, nil
}

func (store *cassandraGraphStore) SaveVertexMessages(messages []*pregel.VertexMessage) error {
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

func (store *cassandraGraphStore) LoadVertexOperations(jobID string, superstep int, vrange store.VertexRange) ([]*pregel.VertexOperation, error) {
	cql := fmt.Sprintf(`SELECT id, type, value FROM %s WHERE job_id=? AND superstep=? AND %s;`, store.fullTableName(vertexOperationsTableName), tokenFilterPlaceholder)
	params := []interface{}{jobID, superstep}

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new(pregel.VertexOperationType), new([]byte)}
	}

	createEntity := func(dest []interface{}) interface{} {
		return &pregel.VertexOperation{ID: dest[0].(string), JobID: jobID, Superstep: superstep,
			Type: dest[1].(pregel.VertexOperationType), Value: dest[2].([]byte)}
	}

	entities, err := store.executeSelectWithTokenRange(cql, vrange, `id`, createScanDest, createEntity, params...)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.VertexOperation, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.VertexOperation)
	}

	return result, nil
}

func (store *cassandraGraphStore) SaveVertexOperations(operations []*pregel.VertexOperation) error {
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

func (store *cassandraGraphStore) LoadHaltedVertices(jobID string, superstep int, vrange store.VertexRange) ([]string, error) {
	cql := fmt.Sprintf(`SELECT id FROM %s WHERE job_id=? AND superstep=? AND %s;`, store.fullTableName(haltedVerticesTableName), tokenFilterPlaceholder)
	params := []interface{}{jobID, superstep}

	createScanDest := func() []interface{} {
		return []interface{}{new(string)}
	}

	createEntity := func(dest []interface{}) interface{} {
		return dest[0].(string)
	}

	entities, err := store.executeSelectWithTokenRange(cql, vrange, `id`, createScanDest, createEntity, params...)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(string)
	}

	return result, nil
}

func (store *cassandraGraphStore) SaveHaltedVertices(halted []*pregel.VertexHalted) error {
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

func (store *cassandraGraphStore) LoadEdgeOperations(jobID string, superstep int, vrange store.VertexRange) ([]*pregel.EdgeOperation, error) {
	cql := fmt.Sprintf(`SELECT "from", "to", type, value FROM %s WHERE job_id=? AND superstep=? AND %s;`, store.fullTableName(edgeOperationsTableName), tokenFilterPlaceholder)
	params := []interface{}{jobID, superstep}

	createScanDest := func() []interface{} {
		return []interface{}{new(string), new(string), new(pregel.EdgeOperationType), new([]byte)}
	}

	createEntity := func(dest []interface{}) interface{} {
		return &pregel.EdgeOperation{From: dest[0].(string), To: dest[1].(string), JobID: jobID, Superstep: superstep,
			Type: dest[2].(pregel.EdgeOperationType), Value: dest[3].([]byte)}
	}

	entities, err := store.executeSelectWithTokenRange(cql, vrange, `"from"`, createScanDest, createEntity, params...)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.EdgeOperation, 0, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.EdgeOperation)
	}

	return result, nil
}

func (store *cassandraGraphStore) SaveEdgeOperations(operations []*pregel.EdgeOperation) error {
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

func (store *cassandraGraphStore) fullTableName(table string) string {
	return GetFullTableName(store.params.Keyspace, table)
}

func (store *cassandraGraphStore) parseTokenRange(vrange store.VertexRange, primaryKey string) ([]*tokenFilter, error) {
	proto, err := store.tokenRangeEncoder.Unmarshal(vrange)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal token range")
	}

	tokenRange := proto.(*protos.CassandraTokenRange)

	partitioner, err := NewPartitioner(tokenRange.Partitioner)
	if err != nil {
		return nil, err
	}

	min := partitioner.MinToken()
	start := partitioner.ParseString(tokenRange.StartToken)
	end := partitioner.ParseString(tokenRange.EndToken)

	// the ring contains only one token (see BuildTokenRanges):
	if start == min && end == min {
		return []*tokenFilter{&tokenFilter{
			cql:    fmt.Sprintf("token(%s) >= ?", primaryKey),
			params: []interface{}{min},
		}}, nil
	}

	// split into two token ranges: (start, MAX_TOKEN] + [MIN_TOKEN, end]
	if end.Less(start) {
		return []*tokenFilter{&tokenFilter{
			cql:    fmt.Sprintf("token(%s) > ?", primaryKey),
			params: []interface{}{start},
		}, &tokenFilter{
			cql:    fmt.Sprintf("token(%s) <= ?", primaryKey),
			params: []interface{}{end},
		}}, nil
	}

	// the token range interval is: (start, end]
	return []*tokenFilter{&tokenFilter{
		cql:    fmt.Sprintf("token(%s) > ? AND token(%s) <= ?", primaryKey, primaryKey),
		params: []interface{}{start, end},
	}}, nil
}

func (store *cassandraGraphStore) executeSelectWithTokenRange(cql string, vrange store.VertexRange, primaryKey string,
	createScanDest CreateScanDestFunc, createEntity CreateEntityFunc, params ...interface{}) ([]interface{}, error) {
	tokenFilters, err := store.parseTokenRange(vrange, primaryKey)
	if err != nil {
		return nil, err
	}

	result := []interface{}{}

	for _, tokenFilter := range tokenFilters {
		finalCql := strings.Replace(cql, tokenFilterPlaceholder, tokenFilter.cql, 1)
		finalParams := append(params, tokenFilter.params...)

		entities, err := ExecuteSelect(store.session, finalCql, createScanDest, createEntity, finalParams...)
		if err != nil {
			return nil, err
		}

		result = append(result, entities...)
	}

	return result, nil
}

func (store *cassandraGraphStore) ensureSchema() error {
	statements := []string{
		fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }`, store.params.Keyspace, store.params.ReplicationFactor),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(id text, value blob, PRIMARY KEY(id))`, store.fullTableName(store.params.VerticesTable)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s("from" text, "to" text, value blob, PRIMARY KEY(("from"), "to"))`, store.fullTableName(store.params.EdgesTable)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s("to" text, job_id text, superstep int, unique_id timeuuid, value blob, PRIMARY KEY(("to"), job_id, superstep, unique_id))`, store.fullTableName(vertexMessagesTableName)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(id text, job_id text, superstep int, unique_id timeuuid, type int, value blob, PRIMARY KEY((id), job_id, superstep, unique_id))`, store.fullTableName(vertexOperationsTableName)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(id text, job_id text, superstep int, PRIMARY KEY((id), job_id, superstep))`, store.fullTableName(haltedVerticesTableName)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s("from" text, "to" text, job_id text, superstep int, unique_id timeuuid, type int, value blob, PRIMARY KEY(("from"), "to", job_id, superstep, unique_id))`, store.fullTableName(edgeOperationsTableName)),
	}

	for _, cql := range statements {
		if err := store.session.Query(cql).Exec(); err != nil {
			return errors.Wrap(err, "error creating schema")
		}
	}

	return nil
}

type cassandraGraphStoreFactory struct{}

func (store *cassandraGraphStoreFactory) Create(params interface{}) (store.GraphStore, error) {
	return &cassandraGraphStore{
		params:            *params.(*protos.CassandraStoreParams),
		tokenRangeEncoder: encoding.NewProtobufEncoder(func() proto.Message { return new(protos.CassandraTokenRange) }),
	}, nil
}

func (store *cassandraGraphStoreFactory) ParamsEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.CassandraStoreParams) })
}
