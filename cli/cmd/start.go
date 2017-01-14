package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/bcrusu/mesos-pregel/cli/api"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	jobLabel     = startCmd.PersistentFlags().StringP("label", "l", "", "Job label")
	taskCPU      = startCmd.PersistentFlags().Float64P("taskCPU", "cpu", 0.1, "Total CPU used by a single Pregel task")
	taskMEM      = startCmd.PersistentFlags().Float64P("taskMEM", "mem", 64, "Total memory used by a single Pregel task")
	taskVertices = startCmd.PersistentFlags().Int32P("taskVertices", "vertices", 10000, "The number of graph vertices processed by a single Pregel task")
	taskTimeout  = startCmd.PersistentFlags().DurationP("taskTimeout", "timeout", 30*time.Second, "The maximum duration to wait for a Pregel task to complete before retrying to execute it")

	shortestPathFrom = startShortestPathCmd.Flags().StringP("from", "f", "", "Source Vertex ID")
	shortestPathTo   = startShortestPathCmd.Flags().StringP("to", "t", "", "Destination Vertex ID")
)

func init() {
	startCmd.AddCommand(startShortestPathCmd)
	startShortestPathCmd.RunE = startShortestPathCmdRunE // break dependency loop
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts a new job that executes the selected algorithm.",
}

var startShortestPathCmd = &cobra.Command{
	Use:   "shortestPath",
	Short: "Finds the shortest path from source to destination vertices.",
}

func createJob(algorithm string, algorithmParams []byte) error {
	store, storeParams, err := processStoreFlags()
	if err != nil {
		return err
	}

	request := &protos.CreateJobRequest{
		Label:           *jobLabel,
		Algorithm:       algorithm,
		AlgorithmParams: algorithmParams,
		Store:           store,
		StoreParams:     storeParams,
		TaskCPU:         *taskCPU,
		TaskMEM:         *taskMEM,
		TaskVertices:    *taskVertices,
		TaskTimeout:     (*taskTimeout).Nanoseconds(),
	}

	return useAPI(func(client *api.Client) error {
		reply, err := client.CreateJob(context.Background(), request)
		if err != nil {
			return errors.Wrapf(err, "failed to start job")
		}

		switch reply.Status {
		case protos.CallStatus_OK:
			fmt.Printf("Job created successfully. ID: %s\n", reply.JobId)
			return nil
		default:
			return errors.Errorf("API call failed with status %d", reply.Status)
		}
	})
}

func startShortestPathCmdRunE(cmd *cobra.Command, args []string) error {
	params := &protos.ShortestPathAlgorithmParams{
		From: *shortestPathFrom,
		To:   *shortestPathTo,
	}

	bytes, err := proto.Marshal(params)
	if err != nil {
		return err
	}

	return createJob("ShortestPath", bytes)
}