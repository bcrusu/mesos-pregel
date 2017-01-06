package main

import (
	"fmt"

	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/scheduler/jobManager"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
)

const (
	resourceNameCPU     = "cpus"
	resourceNameMEM     = "mems"
	executorOverheadCPU = 0.01 // executor CPU overhead
	executorOverheadMEM = 32.0 // executor MEM overhead
)

type PregelScheduler struct {
	executorInfo          *mesos.ExecutorInfo
	jobManager            *jobManager.JobManager
	execTaskParamsEncoder encoding.Encoder
	execTaskResultEncoder encoding.Encoder
}

func NewPregelScheduler(jobManager *jobManager.JobManager) *PregelScheduler {
	return &PregelScheduler{
		executorInfo:          getExecutorInfo(),
		jobManager:            jobManager,
		execTaskParamsEncoder: encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ExecTaskParams) }),
		execTaskResultEncoder: encoding.NewProtobufEncoder(func() proto.Message { return new(protos.ExecTaskResult) })}
}

func (scheduler *PregelScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	glog.Infoln("Framework Registered with Master ", masterInfo)
}

func (scheduler *PregelScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	glog.Infoln("Framework Re-Registered with Master ", masterInfo)
	_, err := driver.ReconcileTasks([]*mesos.TaskStatus{})
	if err != nil {
		glog.Errorf("failed to request task reconciliation: %v", err)
	}
}

func (scheduler *PregelScheduler) Disconnected(sched.SchedulerDriver) {
	glog.Warningf("disconnected from master")
}

func (scheduler *PregelScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	offersBySlave := getOffersBySlave(offers)

	for slaveID, slaveOffers := range offersBySlave {
		resourceOffer := newResourceOffer(slaveOffers)
		taskParamList := scheduler.jobManager.GetTasksToExecute(resourceOffer)

		// create task info protos
		var tasks []*mesos.TaskInfo
		for _, params := range taskParamList {
			taskID := fmt.Sprintf("Pregel_job=%s:task=%d:superstep=%d", params.JobId, params.TaskId, params.SuperstepParams.Superstep)

			task, err := scheduler.getTaskInfo(slaveID, taskID, params)
			if err != nil {
				glog.Errorf("failed to create task info %s; error: %v", taskID, err)
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
	glog.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())

	// switch *status.State {
	// case mesos.TaskState_TASK_FINISHED:
	// 	var taskResult *protos.ExecTaskResult
	// 	if result, err := scheduler.execTaskParamsEncoder.Unmarshal(status.Data); err != nil {
	// 		glog.Errorf("mesos task %s - failed to unmarshal task result; error %v", *status.TaskId.Value, err)
	// 		//TODO
	// 		return
	// 	} else {
	// 		taskResult = result.(*protos.ExecTaskResult)
	// 	}
	// case mesos.TaskState_TASK_FAILED:
	// case mesos.TaskState_TASK_LOST:

	// }
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
	glog.Errorf("Scheduler received error: %v", err)
}

func (scheduler *PregelScheduler) getTaskInfo(slaveID string, taskID string, params *protos.ExecTaskParams) (*mesos.TaskInfo, error) {
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
			util.NewScalarResource(resourceNameCPU, 0.2), //TODO: pass in the correct values for cpu and mem
			util.NewScalarResource(resourceNameMEM, 32),
		},
	}, nil
}

func getExecutorInfo() *mesos.ExecutorInfo {
	//TODO: executor cmd & command uri
	executorCommand := ""

	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("pregel"),
		Name:       proto.String("Pregel Executor"),
		Command: &mesos.CommandInfo{
			Value: proto.String(executorCommand),
			Uris:  nil,
		},
		Resources: []*mesos.Resource{
			util.NewScalarResource(resourceNameCPU, executorOverheadCPU),
			util.NewScalarResource(resourceNameMEM, executorOverheadMEM),
		},
	}
}

func getOffersBySlave(offers []*mesos.Offer) map[string][]*mesos.Offer {
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
