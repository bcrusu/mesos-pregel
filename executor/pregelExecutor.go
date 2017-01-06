package main

import (
	"strconv"
	"sync"
	"time"

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
	execTaskParamsEncoder encoding.Encoder
	execTaskResultEncoder encoding.Encoder
	mutex                 sync.Mutex
	taskCache             *cache.Cache
}

func NewPregelExecutor() *PregelExecutor {
	return &PregelExecutor{
		taskCache:             cache.New(taskCacheExpiration, taskCacheCleanupInterval),
		execTaskParamsEncoder: encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ExecTaskParams) }),
		execTaskResultEncoder: encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ExecTaskResult) }),
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

	executor.sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_RUNNING, nil)

	var taskParams *protos.ExecTaskParams
	if params, err := executor.execTaskParamsEncoder.Unmarshal(taskInfo.Data); err != nil {
		glog.Errorf("mesos task %s - failed to unmarshal task params; error %v", *taskInfo.TaskId.Value, err)
		executor.sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED, nil)
		return
	} else {
		taskParams = params.(*protos.ExecTaskParams)
	}

	jobID := taskParams.JobId
	taskID := taskParams.TaskId
	superstep := taskParams.SuperstepParams.Superstep

	task, err := executor.getPregelTask(*taskParams)
	if err != nil {
		glog.Errorf("job %s - failed to initialize task %d; error: %v", jobID, taskID, err)
		executor.sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED, nil)
		return
	}

	superstepResult, err := task.ExecSuperstep(taskParams.SuperstepParams)
	if err != nil {
		glog.Errorf("job %s - failed to execute superstep %d for task %d; error %v", jobID, superstep, taskID, err)
		executor.sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED, nil)
		return
	}

	taskResult := &protos.ExecTaskResult{
		JobId:           jobID,
		TaskId:          taskID,
		SuperstepResult: superstepResult,
	}

	executor.sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FINISHED, taskResult)
}

func (executor *PregelExecutor) sendStatusUpdate(driver exec.ExecutorDriver, taskId *mesos.TaskID,
	state mesos.TaskState, taskResult *protos.ExecTaskResult) {
	status := &mesos.TaskStatus{
		TaskId: taskId,
		State:  state.Enum(),
	}

	if taskResult != nil {
		data, err := executor.execTaskResultEncoder.Marshal(taskResult)
		if err != nil {
			glog.Errorf("job %s - failed to marshal result for task %d", taskResult.JobId, taskResult.TaskId)
			return
		}

		status.Data = data
	}

	if _, err := driver.SendStatusUpdate(status); err != nil {
		glog.Errorf("mesos task %s - error sending status update; error %v", *taskId.Value, err)
	}
}

func (executor *PregelExecutor) getPregelTask(params protos.ExecTaskParams) (*PregelTask, error) {
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
