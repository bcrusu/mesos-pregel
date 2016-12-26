package scheduler

import (
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
)

type PregelScheduler struct {
	tasksLaunched int
}

func NewPregelScheduler(exec *mesos.ExecutorInfo) *PregelScheduler {
	var result = new(PregelScheduler)
	result.tasksLaunched = 0
	return result
}

func (this *PregelScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Registered with Master ", masterInfo)
}

func (this *PregelScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Re-Registered with Master ", masterInfo)
	_, err := driver.ReconcileTasks([]*mesos.TaskStatus{})
	if err != nil {
		log.Errorf("failed to request task reconciliation: %v", err)
	}
}

func (this *PregelScheduler) Disconnected(sched.SchedulerDriver) {
	log.Warningf("disconnected from master")
}

func (this *PregelScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	//TODO
}

func (this *PregelScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
}

func (this *PregelScheduler) OfferRescinded(_ sched.SchedulerDriver, oid *mesos.OfferID) {
	log.Errorf("offer rescinded: %v", oid)
}
func (this *PregelScheduler) FrameworkMessage(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, msg string) {
	log.Errorf("framework message from executor %q slave %q: %q", eid, sid, msg)
}
func (this *PregelScheduler) SlaveLost(_ sched.SchedulerDriver, sid *mesos.SlaveID) {
	log.Errorf("slave lost: %v", sid)
}
func (this *PregelScheduler) ExecutorLost(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, code int) {
	log.Errorf("executor %q lost on slave %q code %d", eid, sid, code)
}
func (this *PregelScheduler) Error(_ sched.SchedulerDriver, err string) {
	log.Errorf("Scheduler received error: %v", err)
}
