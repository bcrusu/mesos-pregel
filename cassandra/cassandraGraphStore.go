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

func (cstore *cassandraGraphStore) Connect() error {
	cluster := gocql.NewCluster(cstore.params.Hosts...)
	cluster.Timeout = time.Duration(cstore.params.TimeoutSec) * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}

	cstore.cluster = cluster
	cstore.session = session

	batchOptions := cstore.params.BatchOptions
	cstore.batchExecutor = NewBatchExecutor(session, int(batchOptions.MaxSize), int(batchOptions.MaxBytes))

	return nil
}

func (cstore *cassandraGraphStore) Close() {
	if cstore.session != nil {
		cstore.session.Close()
		cstore.cluster = nil
		cstore.session = nil
	}
}

func (cstore *cassandraGraphStore) Init() error {
	if err := cstore.ensureSchema(); err != nil {
		return err
	}

	return nil
}

func (cstore *cassandraGraphStore) GetVertexRanges(verticesPerRange int) ([]*store.VertexRangeHosts, error) {
	tokenRanges, partitioner, err := BuildTokenRanges(cstore.params.Hosts, cstore.params.Keyspace)
	if err != nil {
		return nil, err
	}

	verticesCount, err := GetRowCount(cstore.session, cstore.params.Keyspace, cstore.params.VerticesTable)
	if err != nil {
		return nil, err
	}

	tokenRanges = splitTokenRanges(partitioner, tokenRanges, verticesCount, verticesPerRange)

	// convert
	result := []*store.VertexRangeHosts{}
	for _, tokenRange := range tokenRanges {
		proto := &protos.CassandraTokenRange{
			Partitioner: partitioner.Name(),
			StartToken:  tokenRange.Start.String(),
			EndToken:    tokenRange.End.String(),
		}

		bytes, err := cstore.tokenRangeEncoder.Marshal(proto)
		if err != nil {
			return nil, err
		}

		result = append(result, &store.VertexRangeHosts{
			Hosts: tokenRange.Replicas,
			Range: bytes,
		})
	}

	return result, nil
}

func (cstore *cassandraGraphStore) LoadVertices(vrange store.VertexRange) ([]*pregel.Vertex, error) {
	cql := fmt.Sprintf(`SELECT id, value FROM %s WHERE %s;`, cstore.fullTableName(cstore.params.VerticesTable), tokenFilterPlaceholder)

	createEntity := func() (interface{}, []interface{}) {
		e := &pregel.Vertex{}
		dest := []interface{}{&e.ID, &e.Value}
		return e, dest
	}

	entities, err := cstore.executeSelectForTokenRange(cql, vrange, `id`, createEntity)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.Vertex, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.Vertex)
	}

	return result, nil
}

func (cstore *cassandraGraphStore) SaveVertices(vertices []*pregel.Vertex) error {
	cql := fmt.Sprintf(`INSERT INTO %s (id, value) VALUES(?, ?);`, cstore.fullTableName(cstore.params.VerticesTable))

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

	return cstore.batchExecutor.Execute(cql, items, getItemSize, getItemArgs)
}

func (cstore *cassandraGraphStore) LoadEdges(vrange store.VertexRange) ([]*pregel.Edge, error) {
	cql := fmt.Sprintf(`SELECT "from", "to", value FROM %s WHERE %s;`, cstore.fullTableName(cstore.params.EdgesTable), tokenFilterPlaceholder)

	createEntity := func() (interface{}, []interface{}) {
		e := &pregel.Edge{}
		dest := []interface{}{&e.From, &e.To, &e.Value}
		return e, dest
	}

	entities, err := cstore.executeSelectForTokenRange(cql, vrange, `"from"`, createEntity)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.Edge, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.Edge)
	}

	return result, nil
}

func (cstore *cassandraGraphStore) SaveEdges(edges []*pregel.Edge) error {
	cql := fmt.Sprintf(`INSERT INTO %s ("from", "to", value) VALUES(?, ?, ?);`, cstore.fullTableName(cstore.params.EdgesTable))

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

	return cstore.batchExecutor.Execute(cql, items, getItemSize, getItemArgs)
}

func (cstore *cassandraGraphStore) LoadVertexMessages(jobID string, superstep int, vrange store.VertexRange) ([]*pregel.VertexMessage, error) {
	cql := fmt.Sprintf(`SELECT "to", value FROM %s WHERE job_id=? AND superstep=? AND %s ALLOW FILTERING;`, cstore.fullTableName(vertexMessagesTableName), tokenFilterPlaceholder)
	params := []interface{}{jobID, superstep}

	createEntity := func() (interface{}, []interface{}) {
		e := &pregel.VertexMessage{JobID: jobID, Superstep: superstep}
		dest := []interface{}{&e.To, &e.Value}
		return e, dest
	}

	entities, err := cstore.executeSelectForTokenRange(cql, vrange, `"to"`, createEntity, params...)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.VertexMessage, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.VertexMessage)
	}

	return result, nil
}

func (cstore *cassandraGraphStore) SaveVertexMessages(messages []*pregel.VertexMessage) error {
	cql := fmt.Sprintf(`INSERT INTO %s ("to", job_id, superstep, unique_id, value) VALUES(?, ?, ?, ?, ?);`, cstore.fullTableName(vertexMessagesTableName))

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

	return cstore.batchExecutor.Execute(cql, items, getItemSize, getItemArgs)
}

func (cstore *cassandraGraphStore) LoadVertexOperations(jobID string, superstep int, vrange store.VertexRange) ([]*pregel.VertexOperation, error) {
	cql := fmt.Sprintf(`SELECT id, type, value FROM %s WHERE job_id=? AND superstep=? AND %s ALLOW FILTERING;`, cstore.fullTableName(vertexOperationsTableName), tokenFilterPlaceholder)
	params := []interface{}{jobID, superstep}

	createEntity := func() (interface{}, []interface{}) {
		e := &pregel.VertexOperation{JobID: jobID, Superstep: superstep}
		dest := []interface{}{&e.ID, &e.Type, &e.Value}
		return e, dest
	}

	entities, err := cstore.executeSelectForTokenRange(cql, vrange, `id`, createEntity, params...)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.VertexOperation, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.VertexOperation)
	}

	return result, nil
}

func (cstore *cassandraGraphStore) SaveVertexOperations(operations []*pregel.VertexOperation) error {
	cql := fmt.Sprintf(`INSERT INTO %s (id, job_id, superstep, unique_id, type, value) VALUES(?, ?, ?, ?, ?, ?);`, cstore.fullTableName(vertexOperationsTableName))

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

	return cstore.batchExecutor.Execute(cql, items, getItemSize, getItemArgs)
}

func (cstore *cassandraGraphStore) LoadHaltedVertices(jobID string, superstep int, vrange store.VertexRange) ([]string, error) {
	cql := fmt.Sprintf(`SELECT id FROM %s WHERE job_id=? AND superstep=? AND %s ALLOW FILTERING;`, cstore.fullTableName(haltedVerticesTableName), tokenFilterPlaceholder)
	params := []interface{}{jobID, superstep}

	createEntity := func() (interface{}, []interface{}) {
		var e string
		dest := []interface{}{&e}
		return e, dest
	}

	entities, err := cstore.executeSelectForTokenRange(cql, vrange, `id`, createEntity, params...)
	if err != nil {
		return nil, err
	}

	result := make([]string, len(entities))
	for i, e := range entities {
		result[i] = e.(string)
	}

	return result, nil
}

func (cstore *cassandraGraphStore) SaveHaltedVertices(halted []*pregel.VertexHalted) error {
	cql := fmt.Sprintf(`INSERT INTO %s (id, job_id, superstep) VALUES(?, ?, ?);`, cstore.fullTableName(haltedVerticesTableName))

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

	return cstore.batchExecutor.Execute(cql, items, getItemSize, getItemArgs)
}

func (cstore *cassandraGraphStore) LoadEdgeOperations(jobID string, superstep int, vrange store.VertexRange) ([]*pregel.EdgeOperation, error) {
	cql := fmt.Sprintf(`SELECT "from", "to", type, value FROM %s WHERE job_id=? AND superstep=? AND %s ALLOW FILTERING;`, cstore.fullTableName(edgeOperationsTableName), tokenFilterPlaceholder)
	params := []interface{}{jobID, superstep}

	createEntity := func() (interface{}, []interface{}) {
		e := &pregel.EdgeOperation{JobID: jobID, Superstep: superstep}
		dest := []interface{}{&e.From, &e.To, &e.Type, &e.Value}
		return e, dest
	}

	entities, err := cstore.executeSelectForTokenRange(cql, vrange, `"from"`, createEntity, params...)
	if err != nil {
		return nil, err
	}

	result := make([]*pregel.EdgeOperation, len(entities))
	for i, e := range entities {
		result[i] = e.(*pregel.EdgeOperation)
	}

	return result, nil
}

func (cstore *cassandraGraphStore) SaveEdgeOperations(operations []*pregel.EdgeOperation) error {
	cql := fmt.Sprintf(`INSERT INTO %s ("from", "to", job_id, superstep, unique_id, type, value) VALUES(?, ?, ?, ?, ?, ?, ?);`, cstore.fullTableName(edgeOperationsTableName))

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

	return cstore.batchExecutor.Execute(cql, items, getItemSize, getItemArgs)
}

func (cstore *cassandraGraphStore) fullTableName(table string) string {
	return GetFullTableName(cstore.params.Keyspace, table)
}

func (cstore *cassandraGraphStore) parseTokenRange(vrange store.VertexRange, primaryKey string) ([]*tokenFilter, error) {
	proto, err := cstore.tokenRangeEncoder.Unmarshal(vrange)
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

	// the token range interval includes the min token: [min, end]
	if start == min {
		return []*tokenFilter{&tokenFilter{
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

func (cstore *cassandraGraphStore) executeSelectForTokenRange(cql string, vrange store.VertexRange, primaryKey string,
	createEntity CreateEntityFunc, params ...interface{}) ([]interface{}, error) {
	result := []interface{}{}

	tokenFilters, err := cstore.parseTokenRange(vrange, primaryKey)
	if err != nil {
		return nil, err
	}

	for _, tokenFilter := range tokenFilters {
		finalCql := strings.Replace(cql, tokenFilterPlaceholder, tokenFilter.cql, 1)
		finalParams := append(params, tokenFilter.params...)

		entities, err := ExecuteSelect(cstore.session, finalCql, createEntity, finalParams...)
		if err != nil {
			return nil, err
		}

		result = append(result, entities...)
	}

	return result, nil
}

func (cstore *cassandraGraphStore) ensureSchema() error {
	statements := []string{
		fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }`, cstore.params.Keyspace, cstore.params.ReplicationFactor),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(id text, value blob, PRIMARY KEY(id))`, cstore.fullTableName(cstore.params.VerticesTable)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s("from" text, "to" text, value blob, PRIMARY KEY(("from"), "to"))`, cstore.fullTableName(cstore.params.EdgesTable)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s("to" text, job_id text, superstep int, unique_id timeuuid, value blob, PRIMARY KEY(("to"), job_id, superstep, unique_id))`, cstore.fullTableName(vertexMessagesTableName)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(id text, job_id text, superstep int, unique_id timeuuid, type int, value blob, PRIMARY KEY((id), job_id, superstep, unique_id))`, cstore.fullTableName(vertexOperationsTableName)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(id text, job_id text, superstep int, PRIMARY KEY((id), job_id, superstep))`, cstore.fullTableName(haltedVerticesTableName)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s("from" text, "to" text, job_id text, superstep int, unique_id timeuuid, type int, value blob, PRIMARY KEY(("from"), "to", job_id, superstep, unique_id))`, cstore.fullTableName(edgeOperationsTableName)),
	}

	for _, cql := range statements {
		if err := cstore.session.Query(cql).Exec(); err != nil {
			return errors.Wrap(err, "error creating schema")
		}
	}

	return nil
}

type cassandraGraphStoreFactory struct{}

func (cstore *cassandraGraphStoreFactory) Create(params interface{}) (store.GraphStore, error) {
	return &cassandraGraphStore{
		params:            *params.(*protos.CassandraStoreParams),
		tokenRangeEncoder: encoding.NewProtobufEncoder(func() proto.Message { return new(protos.CassandraTokenRange) }),
	}, nil
}

func (cstore *cassandraGraphStoreFactory) ParamsEncoder() encoding.Encoder {
	return encoding.NewProtobufEncoder(func() proto.Message { return new(protos.CassandraStoreParams) })
}
