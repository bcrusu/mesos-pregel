package main

import (
	"github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
)

type PregelScheduler struct {
	executorInfo  *mesos.ExecutorInfo
	tasksLaunched int
}

func NewPregelScheduler(executorInfo *mesos.ExecutorInfo) *PregelScheduler {
	return &PregelScheduler{executorInfo: executorInfo}
}

func (this *PregelScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	glog.Infoln("Framework Registered with Master ", masterInfo)
}

func (this *PregelScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	glog.Infoln("Framework Re-Registered with Master ", masterInfo)
	_, err := driver.ReconcileTasks([]*mesos.TaskStatus{})
	if err != nil {
		glog.Errorf("failed to request task reconciliation: %v", err)
	}
}

func (this *PregelScheduler) Disconnected(sched.SchedulerDriver) {
	glog.Warningf("disconnected from master")
}

func (this *PregelScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	//TODO
}

func (this *PregelScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	glog.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
}

func (this *PregelScheduler) OfferRescinded(_ sched.SchedulerDriver, oid *mesos.OfferID) {
	glog.Errorf("offer rescinded: %v", oid)
}

func (this *PregelScheduler) FrameworkMessage(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, msg string) {
	glog.Errorf("framework message from executor %q slave %q: %q", eid, sid, msg)
}

func (this *PregelScheduler) SlaveLost(_ sched.SchedulerDriver, sid *mesos.SlaveID) {
	glog.Errorf("slave lost: %v", sid)
}

func (this *PregelScheduler) ExecutorLost(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, code int) {
	glog.Errorf("executor %q lost on slave %q code %d", eid, sid, code)
}

func (this *PregelScheduler) Error(_ sched.SchedulerDriver, err string) {
	glog.Errorf("Scheduler received error: %v", err)
}
