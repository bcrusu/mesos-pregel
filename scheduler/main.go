package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/bcrusu/mesos-pregel/aggregator" // register default aggregators
	_ "github.com/bcrusu/mesos-pregel/algorithms" // register default algorithms
	"github.com/bcrusu/mesos-pregel/cassandra"
	"github.com/bcrusu/mesos-pregel/scheduler/api"
	"github.com/bcrusu/mesos-pregel/scheduler/job"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
	"github.com/pkg/errors"
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

	jobManager, err := job.NewManager(jobStore)
	if err != nil {
		return err
	}

	err = startAPIServer(jobManager)
	if err != nil {
		return err
	}

	return runDriver(jobManager)
}

func runDriver(jobManager *job.Manager) error {
	config := sched.DriverConfig{
		Scheduler: NewPregelScheduler(jobManager),
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

func getJobStore() (store.JobStore, error) {
	switch *JobStore {
	case "cassandra":
		hosts := strings.Split(*CassandraHosts, ",")
		return cassandra.NewJobStore(hosts, *CassandraKeyspace, *CassandraReplicationFactor), nil
	default:
		return nil, fmt.Errorf("unknown job store '%s'", *JobStore)
	}
}

func startAPIServer(jobManager *job.Manager) error {
	server := api.NewAPIServer(jobManager)
	return server.ServeAsync(*APIPort)
}
