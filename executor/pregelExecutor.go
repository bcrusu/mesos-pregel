package executor

import (
	"fmt"

	log "github.com/golang/glog"
	exec "github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

type PregelExecutor struct {
	tasksLaunched int
}

func NewPregelExecutor() *PregelExecutor {
	var result = new(PregelExecutor)
	result.tasksLaunched = 0
	return result
}

func (this *PregelExecutor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Registered Executor on slave ", slaveInfo.GetHostname())
}

func (this *PregelExecutor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Re-registered Executor on slave ", slaveInfo.GetHostname())
}

func (this *PregelExecutor) Disconnected(driver exec.ExecutorDriver) {
	fmt.Println("Executor disconnected.")
}

func (this *PregelExecutor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	go this.processLaunchTask(driver, taskInfo)
}

func (this *PregelExecutor) KillTask(driver exec.ExecutorDriver, taskID *mesos.TaskID) {
	fmt.Println("Kill task")
}

func (this *PregelExecutor) FrameworkMessage(driver exec.ExecutorDriver, msg string) {
	fmt.Println("Got framework message: ", msg)
}

func (this *PregelExecutor) Shutdown(driver exec.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
}

func (this *PregelExecutor) Error(driver exec.ExecutorDriver, err string) {
	fmt.Println("Got error message:", err)
}

func (this *PregelExecutor) processLaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	fmt.Println("Launching task", taskInfo.GetName())

	if err := sendStatusUpdate(driver, taskInfo.GetTaskId(), mesos.TaskState_TASK_RUNNING); err != nil {
		log.Errorf("error sending status update: %s", err)
		return
	}

	taskInfo.Data

	//TODO
}

func sendStatusUpdate(driver exec.ExecutorDriver, taskId *mesos.TaskID, state mesos.TaskState) error {
	status := &mesos.TaskStatus{
		TaskId: taskId,
		State:  state.Enum(),
	}

	if _, err := driver.SendStatusUpdate(status); err != nil {
		return err
	}

	return nil
}
