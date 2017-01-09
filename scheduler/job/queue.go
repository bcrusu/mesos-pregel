package job

import "github.com/golang/glog"

// atm. implemented as a simple FIFO queue
type jobQueue struct {
	items []string
}

type jobQueueIterator interface {
	Next() bool
}

type jobQueueIteratorFunc func() bool

func (q *jobQueue) Add(jobID string) {
	if q.Contains(jobID) {
		return
	}

	q.items = append(q.items, jobID)
}

func (q *jobQueue) Contains(jobID string) bool {
	_, ok := q.getIndex(jobID)
	return ok
}

func (q *jobQueue) Remove(jobID string) {
	index, ok := q.getIndex(jobID)
	if !ok {
		glog.Warningf("queue does not contain job %s", jobID)
		return
	}

	q.items = append(q.items[:index], q.items[index+1:]...)
}

func (q *jobQueue) Count() int {
	return len(q.items)
}

func (q *jobQueue) IsEmpty() bool {
	return len(q.items) == 0
}

func (q *jobQueue) Iter(destJobID *string) jobQueueIterator {
	i := 0
	return jobQueueIteratorFunc(func() bool {
		if i == len(q.items) {
			return false
		}

		*destJobID = q.items[i]
		i++

		return true
	})
}

func (i jobQueueIteratorFunc) Next() bool {
	return i()
}

func (q *jobQueue) getIndex(jobID string) (int, bool) {
	for i, id := range q.items {
		if jobID == id {
			return i, true
		}
	}

	return 0, false
}
