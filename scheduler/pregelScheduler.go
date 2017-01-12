package main

import (
	"fmt"
	"strings"

	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/job"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
)

const (
	resourceNameCPU     = "cpus"
	resourceNameMEM     = "mem"
	executorOverheadCPU = 0.01 // executor CPU overhead
	executorOverheadMEM = 32.0 // executor MEM overhead
)

type PregelScheduler struct {
	executorInfo          *mesos.ExecutorInfo
	jobManager            *job.Manager
	execTaskParamsEncoder encoding.Encoder
	execTaskResultEncoder encoding.Encoder
}

func NewPregelScheduler(jobManager *job.Manager) *PregelScheduler {
	return &PregelScheduler{
		executorInfo:          getExecutorInfo(),
		jobManager:            jobManager,
		execTaskParamsEncoder: encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ExecTaskParams) }),
		execTaskResultEncoder: encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ExecTaskResult) })}
}

func (scheduler *PregelScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	glog.Infoln("framework Registered with Master ", masterInfo)
}

func (scheduler *PregelScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	glog.Infoln("framework Re-Registered with Master ", masterInfo)
	_, err := driver.ReconcileTasks([]*mesos.TaskStatus{})
	if err != nil {
		glog.Errorf("failed to request task reconciliation: %v", err)
	}
}

func (scheduler *PregelScheduler) Disconnected(sched.SchedulerDriver) {
	glog.Warningf("disconnected from master")
}

func (scheduler *PregelScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	offersBySlave := groupOffersBySlave(offers)

	for slaveID, slaveOffers := range offersBySlave {
		resourceOffer := newResourcePool(slaveOffers)
		host := *slaveOffers[0].Hostname
		tasksToExecute := scheduler.jobManager.GetTasksToExecute(host, resourceOffer)

		// create task info protos
		var tasks []*mesos.TaskInfo
		for _, task := range tasksToExecute {
			mesosTaskID := getMesosTaskID(task.Parmas.JobId, task.Parmas.TaskId)

			task, err := scheduler.getTaskInfo(slaveID, mesosTaskID, task)
			if err != nil {
				glog.Errorf("failed to create Mesos task info %s; error: %v", mesosTaskID, err)
				continue
			}

			tasks = append(tasks, task)
		}

		// launch tasks
		offerIDs := getOfferIDs(slaveOffers)

		if len(tasks) > 0 {
			driver.LaunchTasks(offerIDs, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(5)})
		} else {
			// decline offers
			driver.LaunchTasks(offerIDs, []*mesos.TaskInfo{}, &mesos.Filters{RefuseSeconds: proto.Float64(60)})
		}
	}
}

func (scheduler *PregelScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	mesosTaskID := status.TaskId.GetValue()
	jobID, taskID, ok := parseMesosTaskID(mesosTaskID)

	// ignore task status having invalid task id
	if !ok {
		return
	}

	switch status.GetState() {
	case mesos.TaskState_TASK_FINISHED:
		taskResult, err := scheduler.execTaskParamsEncoder.Unmarshal(status.Data)
		if err != nil {
			glog.Errorf("mesos task %s - failed to unmarshal task result; error %v", mesosTaskID, err)
			scheduler.jobManager.SetTaskFailed(jobID, taskID)
			return
		}

		scheduler.jobManager.SetTaskCompleted(taskResult.(*protos.ExecTaskResult))
	case mesos.TaskState_TASK_FAILED:
	case mesos.TaskState_TASK_LOST:
	case mesos.TaskState_TASK_KILLED:
	case mesos.TaskState_TASK_ERROR:
		scheduler.jobManager.SetTaskFailed(jobID, taskID)
	}
}

func (scheduler *PregelScheduler) OfferRescinded(_ sched.SchedulerDriver, oid *mesos.OfferID) {
	glog.Errorf("offer rescinded: %v", oid)
}

func (scheduler *PregelScheduler) FrameworkMessage(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, msg string) {
	glog.Errorf("framework message from executor %q slave %q: %q", eid, sid, msg)
}

func (scheduler *PregelScheduler) SlaveLost(_ sched.SchedulerDriver, sid *mesos.SlaveID) {
	glog.Errorf("slave lost: %v", sid)
}

func (scheduler *PregelScheduler) ExecutorLost(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, code int) {
	glog.Errorf("executor %q lost on slave %q code %d", eid, sid, code)
}

func (scheduler *PregelScheduler) Error(_ sched.SchedulerDriver, err string) {
	glog.Errorf("scheduler received error: %v", err)
}

func (scheduler *PregelScheduler) getTaskInfo(slaveID string, taskID string, task *job.TaskToExecute) (*mesos.TaskInfo, error) {
	params := task.Parmas
	data, err := scheduler.execTaskParamsEncoder.Marshal(params)
	if err != nil {
		glog.Errorf("job %s - failed to marshal params for task %d", params.JobId, params.TaskId)
		return nil, err
	}

	return &mesos.TaskInfo{
		Name:     proto.String(taskID),
		TaskId:   &mesos.TaskID{Value: proto.String(taskID)},
		Data:     data,
		SlaveId:  &mesos.SlaveID{Value: proto.String(slaveID)},
		Executor: scheduler.executorInfo,
		Resources: []*mesos.Resource{
			util.NewScalarResource(resourceNameCPU, task.CPU),
			util.NewScalarResource(resourceNameMEM, task.MEM),
		},
	}, nil
}

func getExecutorInfo() *mesos.ExecutorInfo {
	executorCommand := "pregelExe"

	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("pregel"),
		Name:       proto.String("Pregel Executor"),
		Command: &mesos.CommandInfo{
			Value: proto.String(executorCommand),
		},
		Resources: []*mesos.Resource{
			util.NewScalarResource(resourceNameCPU, executorOverheadCPU),
			util.NewScalarResource(resourceNameMEM, executorOverheadMEM),
		},
	}
}

func groupOffersBySlave(offers []*mesos.Offer) map[string][]*mesos.Offer {
	result := make(map[string][]*mesos.Offer)

	for _, offer := range offers {
		slaveID := offer.GetSlaveId().GetValue()

		list, ok := result[slaveID]
		if !ok {
			list = []*mesos.Offer{}
		}

		result[slaveID] = append(list, offer)
	}

	return result
}

func getOfferIDs(offers []*mesos.Offer) []*mesos.OfferID {
	result := make([]*mesos.OfferID, len(offers))
	for i, offer := range offers {
		result[i] = offer.Id
	}

	return result
}

func getMesosTaskID(jobID string, pregelTaskID string) string {
	return fmt.Sprintf("%s:%s", jobID, pregelTaskID)
}

func parseMesosTaskID(taskID string) (jobID string, pregelTaskID string, success bool) {
	splits := strings.Split(taskID, ":")
	if len(splits) != 2 {
		return "", "", false
	}

	return splits[0], splits[1], true
}
