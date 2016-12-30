package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/bcrusu/pregel/protos"
	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	exec "github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/patrickmn/go-cache"
)

type PregelExecutor struct {
	mutex     sync.Mutex
	taskCache *cache.Cache
}

func NewPregelExecutor() *PregelExecutor {
	var result = new(PregelExecutor)
	result.taskCache = cache.New(15*time.Minute, 30*time.Second)
	result.taskCache.OnEvicted(taskCache_OnEvicted)
	return result
}

func (executor *PregelExecutor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Registered Executor on slave ", slaveInfo.GetHostname())
}

func (executor *PregelExecutor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Re-registered Executor on slave ", slaveInfo.GetHostname())
}

func (executor *PregelExecutor) Disconnected(driver exec.ExecutorDriver) {
	fmt.Println("Executor disconnected.")
}

func (executor *PregelExecutor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	go executor.processLaunchTask(driver, taskInfo)
}

func (executor *PregelExecutor) KillTask(driver exec.ExecutorDriver, taskID *mesos.TaskID) {
	fmt.Println("Kill task")
}

func (executor *PregelExecutor) FrameworkMessage(driver exec.ExecutorDriver, msg string) {
	fmt.Println("Got framework message: ", msg)
}

func (executor *PregelExecutor) Shutdown(driver exec.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
}

func (executor *PregelExecutor) Error(driver exec.ExecutorDriver, err string) {
	fmt.Println("Got error message:", err)
}

func (executor *PregelExecutor) processLaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	log.Infof("Launching task %s", taskInfo.GetName())

	sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_RUNNING)

	taskParams := new(protos.PregelTaskParams)
	if err := proto.Unmarshal(taskInfo.Data, taskParams); err != nil {
		log.Errorf("Failed to unmarshal task params: %v", err)
		sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED)
		return
	}

	task, err := executor.getPregelTask(*taskParams)
	if err != nil {
		log.Errorf("Failed to initialize task: %v", err)
		sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED)
		return
	}

	err = task.ExecSuperstep()
	if err != nil {
		log.Errorf("Failed to execute superstep %d for job %s. Error %v", taskParams.Superstep, taskParams.JobId, err)
		sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED)
		return
	}

	sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FINISHED)
}

func sendStatusUpdate(driver exec.ExecutorDriver, taskId *mesos.TaskID, state mesos.TaskState) {
	status := &mesos.TaskStatus{
		TaskId: taskId,
		State:  state.Enum(),
	}

	if _, err := driver.SendStatusUpdate(status); err != nil {
		log.Errorf("error sending status update: %s", err)
	}
}

func (executor *PregelExecutor) getPregelTask(params protos.PregelTaskParams) (*PregelTask, error) {
	executor.mutex.Lock()
	defer executor.mutex.Unlock()

	cacheKey := "" //TODO

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

func taskCache_OnEvicted(string, interface{}) {
	//TODO:
}
