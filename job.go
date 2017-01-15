package pregel

import "time"

type Job struct {
	ID              string
	Label           string
	CreationTime    time.Time
	Status          JobStatus
	Store           string
	StoreParams     []byte
	Algorithm       string
	AlgorithmParams []byte
	TaskCPU         float64
	TaskMEM         float64
	TaskVertices    int
	TaskTimeoutSec  int
}

type JobStatus int

const (
	_ JobStatus = iota
	JobCreated
	JobRunning
	JobCompleted
	JobCancelled
	JobFailed
)

func (j Job) CanCancel() bool {
	return j.Status == JobCreated || j.Status == JobRunning
}
