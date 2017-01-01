package main

import (
	"flag"
	"os"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"github.com/pkg/errors"
)

const (
	CPUS_PER_EXECUTOR = 1
	MEM_PER_EXECUTOR  = 128
)

var (
	mesosMaster  = flag.String("mesos.master", "127.0.100.1:5050", "Master address <ip:port>")
	executorPath = flag.String("executorPath", "./executor", "Path to Pregel executor")
)

func main() {
	flag.Parse()

	glog.Info("Running...")

	if err := run(); err != nil {
		glog.Errorf("Unexpected error: %s", err)
		time.Sleep(2 * time.Second)
		os.Exit(1)
	}

	glog.Info("Done.")
}

func run() error {
	executorInfo := getExecutorInfo()
	config := sched.DriverConfig{
		Scheduler: NewPregelScheduler(executorInfo),
		Framework: &mesos.FrameworkInfo{
			User: proto.String(""),
			Name: proto.String("Pregel"),
		},
		Master: *mesosMaster,
	}

	driver, err := sched.NewMesosSchedulerDriver(config)
	if err != nil {
		return errors.Wrap(err, "unable to create a SchedulerDriver")
	}

	if status, err := driver.Run(); err != nil {
		return errors.Wrapf(err, "framework stopped with status %s", status.String())
	}

	return nil
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
			util.NewScalarResource("cpus", CPUS_PER_EXECUTOR),
			util.NewScalarResource("mem", MEM_PER_EXECUTOR),
		},
	}
}
