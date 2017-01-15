package job

import (
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/golang/glog"
)

const (
	minTaskCPU        = 0.05
	minTaskMEM        = 32.0
	minTaskVertices   = 1000
	minTaskTimeoutSec = 10
	maxTaskTimeoutSec = 10 * 60 // 10 min
)

func validateCreateJobRequest(r *protos.CreateJobRequest) bool {
	if r.Label == "" {
		glog.Warning("CreateJobRequest - invalid label")
		return false
	}

	if r.Store == "" || r.StoreParams == nil {
		glog.Warningf("CreateJobRequest - job %s - invalid store parameters", r.Label)
		return false
	}

	if r.Algorithm == "" {
		glog.Warningf("CreateJobRequest - job %s - invalid algorithm", r.Label)
		return false
	}

	if r.TaskCPU < minTaskCPU {
		glog.Warningf("CreateJobRequest - job %s - invalid taks CPU value", r.Label)
		return false
	}

	if r.TaskMEM < minTaskMEM {
		glog.Warningf("CreateJobRequest - job %s - invalid taks MEM value", r.Label)
		return false
	}

	if r.TaskVertices < minTaskVertices {
		glog.Warningf("CreateJobRequest - job %s - invalid taks vertices value", r.Label)
		return false
	}

	if r.TaskTimeoutSec < minTaskTimeoutSec || r.TaskTimeoutSec > maxTaskTimeoutSec {
		glog.Warningf("CreateJobRequest - job %s - invalid taks timeout value", r.Label)
		return false
	}

	return true
}
