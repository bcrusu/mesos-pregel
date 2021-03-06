package main

import (
	"flag"
	"os"

	"github.com/golang/glog"
	exec "github.com/mesos/mesos-go/api/v0/executor"
	"github.com/pkg/errors"

	_ "github.com/bcrusu/mesos-pregel/aggregator" // register default aggregators
	_ "github.com/bcrusu/mesos-pregel/algorithms" // register default algorithms
	_ "github.com/bcrusu/mesos-pregel/cassandra"  // register Cassandra store
)

func main() {
	flag.Parse()

	glog.Infof("Executor args: %v", os.Args)
	glog.Info("Running...")

	if err := run(); err != nil {
		glog.Errorf("Unexpected error: %s", err)
		os.Exit(1)
	}

	glog.Info("Done.")
}

func run() error {
	dconfig := exec.DriverConfig{
		Executor: NewPregelExecutor(),
	}

	driver, err := exec.NewMesosExecutorDriver(dconfig)
	if err != nil {
		return errors.Wrap(err, "unable to create a ExecutorDriver")
	}

	if status, err := driver.Start(); err != nil {
		return errors.Wrapf(err, "unexpected error on driver start; status %s", status.String())
	}

	glog.Info("executor process has started and running.")

	if _, err = driver.Join(); err != nil {
		return errors.Wrap(err, "unexpected error on driver join")
	}

	return nil
}
