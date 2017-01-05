package pregel

import "time"

type Job struct {
	ID              string
	Label           string    //TODO: persist
	CreationTime    time.Time //TODO: persist
	Status          JobStatus
	Store           string
	StoreParams     []byte
	Algorithm       string
	AlgorithmParams []byte
	VerticesPerTask int
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