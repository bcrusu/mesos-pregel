package main

import (
	"fmt"
	"log"

	exec "github.com/mesos/mesos-go/executor"
	"github.com/pkg/errors"
)

func main() {
	log.Println("Running...")

	if err := run(); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	log.Println("Done.")
}

func run() error {
	dconfig := exec.DriverConfig{
		Executor: NewPregelExecutor(),
	}

	driver, err := exec.NewMesosExecutorDriver(dconfig)
	if err != nil {
		return errors.Wrap(err, "unable to create a ExecutorDriver")
	}

	_, err = driver.Start()
	if err != nil {
		return errors.Wrap(err, "unexpected error on driver start")
	}

	fmt.Println("Executor process has started and running.")

	_, err = driver.Join()
	if err != nil {
		return errors.Wrap(err, "unexpected error on driver join")
	}

	return nil
}
