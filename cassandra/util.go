package cassandra

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

func GetRowCount(session *gocql.Session, keyspace string, table string) (int, error) {
	cql := fmt.Sprintf(`select count(*) from %s`, GetFullTableName(keyspace, table))
	var count int
	err := ExecuteScalar(session, cql, &count)
	return count, err
}

func GetFullTableName(keyspace string, table string) string {
	return fmt.Sprintf(`"%s"."%s"`, keyspace, table)
}

type CreateScanDestFunc func() []interface{}
type CreateEntityFunc func(dest []interface{}) interface{}

func ExecuteSelect(session *gocql.Session, cql string, createScanDest CreateScanDestFunc, createEntity CreateEntityFunc, params ...interface{}) ([]interface{}, error) {
	iter := session.Query(cql, params...).Iter()

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

func ExecuteScalar(session *gocql.Session, cql string, dest interface{}, params ...interface{}) error {
	return session.Query(cql, params...).Scan(dest)
}

func TableExists(session *gocql.Session, keyspace string, tableName string) (bool, error) {
	query := session.Query(`SELECT count(1) FROM system_schema.tables WHERE keyspace_name=? and table_name=?;`, keyspace, tableName)

	var count int
	if err := query.Scan(&count); err != nil {
		return false, err
	}

	return count == 1, nil
}
