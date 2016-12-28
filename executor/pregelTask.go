package executor

import (
	"sync"

	"github.com/bcrusu/pregel/executor/algorithms"
	"github.com/bcrusu/pregel/executor/stores"
	"github.com/bcrusu/pregel/protos"
	"github.com/pkg/errors"
)

type PregelTask struct {
	params           protos.PregelTaskParams
	store            stores.Store
	algorithm        algorithms.Algorithm
	mutex            sync.Mutex
	currentSuperstep int
}

func NewPregelTask(params protos.PregelTaskParams) (*PregelTask, error) {
	store, err := stores.NewStore(params.StoreType, params.StoreParams, params.EntityRange)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize store: %v", params.StoreType)
	}

	algorithm, err := algorithms.NewAlgorithm(params.AlgorithmType, params.AlgorithmParams)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize algorithm: %v", params.AlgorithmType)
	}

	return &PregelTask{params: params, store: store, algorithm: algorithm}, nil
}

func (task *PregelTask) ExecSuperstep() error {
	task.mutex.Lock()
	defer task.mutex.Unlock()

	//TODO
	return nil
}

func (task *PregelTask) loadEntities() error {
	return nil
}

func (task *PregelTask) fastForwardToSuperstep(superstep int) error {
	return nil
}
