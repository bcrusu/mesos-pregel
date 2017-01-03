package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/bcrusu/mesos-pregel/cassandra"
	_ "github.com/bcrusu/mesos-pregel/scheduler/algorithmImpl" // register default algorithms
	"github.com/bcrusu/mesos-pregel/store"
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
	jobStore, err := getJobStore()
	if err != nil {
		return err
	}

	err = jobStore.Connect()
	if err != nil {
		return err
	}
	defer jobStore.Close()

	err = jobStore.Init()
	if err != nil {
		return err
	}

	return runDriver(jobStore)
}

func runDriver(jobStore store.JobStore) error {
	executorInfo := getExecutorInfo()
	config := sched.DriverConfig{
		Scheduler: NewPregelScheduler(executorInfo, jobStore),
		Framework: &mesos.FrameworkInfo{
			User: proto.String(""),
			Name: proto.String("Pregel"),
		},
		Master: *MesosMaster,
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

func getJobStore() (store.JobStore, error) {
	switch *JobStore {
	case "cassandra":
		hosts := strings.Split(*CassandraHosts, ",")
		return cassandra.NewJobStore(hosts, *CassandraKeyspace), nil
	default:
		return nil, fmt.Errorf("unknown job store '%s'", *JobStore)
	}
}
