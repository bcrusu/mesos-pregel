package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type BatchExecutor struct {
	session  *gocql.Session
	maxSize  int
	maxBytes int
}

type ItemSizeFunc func(interface{}) int
type ItemArgsFunc func(interface{}) []interface{}

func NewBatchExecutor(session *gocql.Session, maxSize int, maxBytes int) *BatchExecutor {
	return &BatchExecutor{session, maxSize, maxBytes}
}

func (exe *BatchExecutor) Execute(cql string, items []interface{}, getItemSize ItemSizeFunc, getItemArgs ItemArgsFunc) error {
	batches := exe.createBatches(cql, items, getItemSize, getItemArgs)
	return exe.executeBatches(batches)
}

func (exe *BatchExecutor) createBatches(cql string, items []interface{}, getItemSize ItemSizeFunc, getItemArgs ItemArgsFunc) []*gocql.Batch {
	result := make([]*gocql.Batch, 0, 100)

	currentBatch := exe.session.NewBatch(gocql.LoggedBatch)
	var currentBatchBytes int
	for _, item := range items {
		itemBytes := getItemSize(item)
		if currentBatch.Size() == exe.maxSize || currentBatchBytes+itemBytes >= exe.maxBytes {
			result = append(result, currentBatch)
			currentBatch = exe.session.NewBatch(gocql.LoggedBatch)
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

func (exe *BatchExecutor) executeBatches(batches []*gocql.Batch) error {
	for i, batch := range batches {
		if err := exe.session.ExecuteBatch(batch); err != nil {
			return errors.Wrapf(err, "failed to execute batch no. %d of %d", i+1, len(batches))
		}
	}

	return nil
}
