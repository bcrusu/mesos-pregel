package main

import (
	"log"

	"github.com/bcrusu/mesos-pregel/cli/cmd"
)

func main() {
	if err := cmd.PregelRootCmd.Execute(); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}
}
