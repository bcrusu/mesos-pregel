package job

import (
	"sync"
	"time"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/task"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
)

type jobStarter struct {
	input  chan *pregel.Job
	output chan *task.Manager
	wg     *sync.WaitGroup
}

func newJobStarter(jobStore store.JobStore) *jobStarter {
	wg := &sync.WaitGroup{}
	inputChan := make(chan *pregel.Job)
	outputChan := make(chan *task.Manager)

	checkpointEncoder := encoding.NewProtobufEncoder(func() proto.Message { return new(protos.JobCheckpoint) })

	connectGraphStore := func(name string, params []byte) (store.GraphStore, error) {
		graphStore, err := store.New(name, params)
		if err != nil {
			return nil, err
		}

		err = graphStore.Connect()
		if err != nil {
			return nil, err
		}

		err = graphStore.Init()
		if err != nil {
			return nil, err
		}

		return graphStore, nil
	}

	loadCheckpoint := func(jobID string) (*protos.JobCheckpoint, error) {
		bytes, err := jobStore.LoadCheckpoint(jobID)
		if err != nil {
			return nil, err
		}

		if bytes == nil {
			return nil, nil
		}

		checkpoint, err := checkpointEncoder.Unmarshal(bytes)
		if err != nil {
			return nil, err
		}

		return checkpoint.(*protos.JobCheckpoint), nil
	}

	saveStatus := func(jobID string, status pregel.JobStatus, checkpoint *protos.JobCheckpoint) error {
		var checkpointBytes []byte

		if checkpoint != nil {
			var err error
			checkpointBytes, err = checkpointEncoder.Marshal(checkpoint)
			if err != nil {
				return err
			}
		}

		return jobStore.SaveStatus(jobID, status, nil, checkpointBytes)
	}

	startJob := func(job *pregel.Job) (*task.Manager, error) {
		graphStore, err := connectGraphStore(job.Store, job.StoreParams)
		if err != nil {
			return nil, err
		}
		defer graphStore.Close()

		vertexRanges, err := graphStore.GetVertexRanges(job.TaskVertices)
		if err != nil {
			return nil, err
		}

		manager, err := task.NewManager(job.ID, vertexRanges, time.Duration(job.TaskTimeout))
		if err != nil {
			return nil, err
		}

		return manager, nil
	}

	processJob := func(job *pregel.Job) {
		wg.Add(1)
		defer wg.Done()

		checkpoint, err := loadCheckpoint(job.ID)
		if err != nil {
			glog.Errorf("job %s - failed to load checkpoint; error: %v", job.ID, err)
			return
		}

		var taskManager *task.Manager

		if checkpoint != nil {
			taskManager, err = task.NewManagerFromCheckpoint(checkpoint)
		} else {
			taskManager, err = startJob(job)
			if err != nil {
				checkpoint = taskManager.GetCheckpoint()
			}
		}

		if err != nil {
			err = saveStatus(job.ID, pregel.JobFailed, nil)
			if err != nil {
				glog.Warningf("job %s - failed to persist job status 'Failed'; error: %v", job.ID, err)
			} else {
				job.Status = pregel.JobFailed
			}
			return
		}

		if job.Status != pregel.JobRunning {
			err = saveStatus(job.ID, pregel.JobRunning, checkpoint)
			if err != nil {
				glog.Warningf("job %s - failed to persist job status 'Running'; error: %v", job.ID, err)
			}

			job.Status = pregel.JobRunning
		}

		outputChan <- taskManager
	}

	go func() {
		for job := range inputChan {
			go processJob(job)
		}
		close(outputChan)
	}()

	return &jobStarter{inputChan, outputChan, wg}
}

func (s *jobStarter) wait() {
	s.wg.Wait()
}
