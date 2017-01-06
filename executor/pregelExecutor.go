package main

import (
	"strconv"
	"sync"
	"time"

	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	exec "github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/patrickmn/go-cache"
)

const (
	taskCacheExpiration      = 10 * time.Minute
	taskCacheCleanupInterval = 7 * time.Minute
)

type PregelExecutor struct {
	taskParamsEncoder encoding.Encoder
	taskStatusEncoder encoding.Encoder
	mutex             sync.Mutex
	taskCache         *cache.Cache
}

func NewPregelExecutor() *PregelExecutor {
	return &PregelExecutor{
		taskCache:         cache.New(taskCacheExpiration, taskCacheCleanupInterval),
		taskParamsEncoder: encoding.NewProtobufEncoder(func() proto.Message { return new(protos.PregelTaskParams) }),
		taskStatusEncoder: encoding.NewProtobufEncoder(func() proto.Message { return new(protos.PregelTaskStatus) }),
	}
}

func (executor *PregelExecutor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	glog.Infof("registered Executor on slave %s", slaveInfo.GetHostname())
}

func (executor *PregelExecutor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	glog.Infof("Re-registered Executor on slave %s", slaveInfo.GetHostname())
}

func (executor *PregelExecutor) Disconnected(driver exec.ExecutorDriver) {
	glog.Infof("Executor disconnected.")
}

func (executor *PregelExecutor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	go executor.processLaunchTask(driver, taskInfo)
}

func (executor *PregelExecutor) KillTask(driver exec.ExecutorDriver, taskID *mesos.TaskID) {
	glog.Infof("Kill task")
}

func (executor *PregelExecutor) FrameworkMessage(driver exec.ExecutorDriver, msg string) {
	glog.Infof("Got framework message: %s", msg)
}

func (executor *PregelExecutor) Shutdown(driver exec.ExecutorDriver) {
	glog.Infof("Shutting down the executor")
}

func (executor *PregelExecutor) Error(driver exec.ExecutorDriver, err string) {
	glog.Infof("Got error message: %s", err)
}

func (executor *PregelExecutor) processLaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	glog.Infof("Launching task %s", taskInfo.GetName())

	sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_RUNNING, nil)

	var taskParams *protos.PregelTaskParams
	if params, err := executor.taskParamsEncoder.Unmarshal(taskInfo.Data); err != nil {
		glog.Errorf("Failed to unmarshal task params: %v", err)
		sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED, nil)
		return
	} else {
		taskParams = params.(*protos.PregelTaskParams)
	}

	task, err := executor.getPregelTask(*taskParams)
	if err != nil {
		glog.Errorf("Failed to initialize task: %d", taskParams.TaskId)
		sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED, nil)
		return
	}

	aggregators, err := aggregator.NewSetFromMessages(taskParams.Aggregators)
	if err != nil {
		glog.Errorf("Failed to create aggregators for tas: %d", taskParams.TaskId)
		sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED, nil)
		return
	}

	taskStatus, err := task.ExecSuperstep(int(taskParams.Superstep), aggregators)
	if err != nil {
		glog.Errorf("Failed to execute superstep %d for job %s. Error %v", taskParams.Superstep, taskParams.JobId, err)
		sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED, nil)
		return
	}

	taskStatusBytes, err := executor.taskStatusEncoder.Marshal(taskStatus)
	if err != nil {
		panic("failed to marshal task status")
	}

	sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FINISHED, taskStatusBytes)
}

func sendStatusUpdate(driver exec.ExecutorDriver, taskId *mesos.TaskID, state mesos.TaskState, data []byte) {
	status := &mesos.TaskStatus{
		TaskId: taskId,
		State:  state.Enum(),
		Data:   data,
	}

	if _, err := driver.SendStatusUpdate(status); err != nil {
		glog.Errorf("error sending status update: %s", err)
	}
}

func (executor *PregelExecutor) getPregelTask(params protos.PregelTaskParams) (*PregelTask, error) {
	executor.mutex.Lock()
	defer executor.mutex.Unlock()

	cacheKey := params.JobId + ":" + strconv.Itoa(int(params.TaskId))

	if task, found := executor.taskCache.Get(cacheKey); found {
		return task.(*PregelTask), nil
	}

	task, err := NewPregelTask(params)
	if err != nil {
		return nil, err
	}

	executor.taskCache.Set(cacheKey, task, cache.DefaultExpiration)
	return task, nil
}
