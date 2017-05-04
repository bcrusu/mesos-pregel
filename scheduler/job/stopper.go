package job

import (
	"fmt"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/algorithm"
	"github.com/bcrusu/mesos-pregel/scheduler/task"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/golang/glog"
)

type jobStopperParams struct {
	job         *pregel.Job
	taskManager *task.Manager
	newStatus   pregel.JobStatus

	doneChan  chan struct{}
	errorChan chan error
}

func createJobStopper(jobStore store.JobStore) chan *jobStopperParams {
	inputChan := make(chan *jobStopperParams)

	processCompleted := func(job *pregel.Job, taskManager *task.Manager) error {
		algorithm, err := algorithm.New(job.Algorithm, job.AlgorithmParams)
		if err != nil {
			return err
		}

		aggregators := taskManager.Aggregators()
		result := algorithm.GetResult(aggregators.AsImmutable())

		bytes, err := algorithm.ResultEncoder().Marshal(result)
		if err != nil {
			return err
		}

		return jobStore.SaveStatus(job.ID, pregel.JobCompleted, bytes, nil)
	}

	processFailed := func(job *pregel.Job) error {
		return jobStore.SaveStatus(job.ID, pregel.JobFailed, nil, nil)
	}

	processCancelled := func(job *pregel.Job) error {
		return jobStore.SaveStatus(job.ID, pregel.JobCancelled, nil, nil)
	}

	process := func(params *jobStopperParams) {
		job := params.job
		var err error

		switch params.newStatus {
		case pregel.JobCompleted:
			err = processCompleted(job, params.taskManager)
		case pregel.JobFailed:
			err = processFailed(job)
		case pregel.JobCancelled:
			err = processCancelled(job)
		default:
			if params.errorChan != nil {
				params.errorChan <- fmt.Errorf("job %s - already in a terminal state", job.ID)
			} else {
				glog.Error("job %s - already in a terminal state %d", job.ID, params.newStatus)
			}
			return
		}

		if err != nil {
			if params.errorChan != nil {
				params.errorChan <- err
			} else {
				glog.Error("job %s - failed to transition to terminal state %d; error: %v", job.ID, params.newStatus, err)
			}
			return
		}

		params.job.Status = params.newStatus

		if params.doneChan != nil {
			close(params.doneChan)
		}
	}

	go func() {
		for params := range inputChan {
			go process(params)
		}
	}()

	return inputChan
}
