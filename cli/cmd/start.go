package cmd

import (
	"context"
	"fmt"

	"github.com/bcrusu/mesos-pregel/algorithms"
	"github.com/bcrusu/mesos-pregel/cli/api"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	jobLabel     = startCmd.PersistentFlags().StringP("label", "l", "", "Job label")
	taskCPU      = startCmd.PersistentFlags().Float64P("taskCPU", "c", 0.25, "Total CPU used by a single Pregel task")
	taskMEM      = startCmd.PersistentFlags().Float64P("taskMEM", "m", 64, "Total memory used by a single Pregel task")
	taskVertices = startCmd.PersistentFlags().Int32P("taskVertices", "v", 10000, "The number of graph vertices processed by a single Pregel task")
	taskTimeout  = startCmd.PersistentFlags().Int32P("taskTimeout", "t", 30, "The maximum duration in seconds to wait for a Pregel task to complete before retrying to execute it")

	shortestPathFrom = startShortestPathAlgorithmCmd.Flags().StringP("from", "f", "", "Source Vertex ID")
	shortestPathTo   = startShortestPathAlgorithmCmd.Flags().StringP("to", "t", "", "Destination Vertex ID")
)

func init() {
	startCmd.AddCommand(startShortestPathAlgorithmCmd)
	startShortestPathAlgorithmCmd.RunE = startShortestPathAlgorithmCmdRunE // break dependency loop

	startCmd.AddCommand(startCountAlgorithmCmd)
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts a new job that executes the selected algorithm.",
}

var startShortestPathAlgorithmCmd = &cobra.Command{
	Use:   "shortestPath",
	Short: "Finds the shortest path from source to destination vertices.",
}

var startCountAlgorithmCmd = &cobra.Command{
	Use:   "count",
	Short: "Counts the number of vertices and edges in the graph.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return createJob(algorithms.Count, nil)
	},
}

func createJob(algorithm string, algorithmParams proto.Message) error {
	var algorithmParamsBytes []byte

	if algorithmParams != nil {
		var err error
		if algorithmParamsBytes, err = proto.Marshal(algorithmParams); err != nil {
			return err
		}
	}

	store, storeParams, err := processStoreFlags()
	if err != nil {
		return err
	}

	request := &protos.CreateJobRequest{
		Label:           *jobLabel,
		Algorithm:       algorithm,
		AlgorithmParams: algorithmParamsBytes,
		Store:           store,
		StoreParams:     storeParams,
		TaskCPU:         *taskCPU,
		TaskMEM:         *taskMEM,
		TaskVertices:    *taskVertices,
		TaskTimeoutSec:  *taskTimeout,
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
			return errors.Errorf("API call failed with status %s", getCallStatusName(reply.Status))
		}
	})
}

func startShortestPathAlgorithmCmdRunE(cmd *cobra.Command, args []string) error {
	return createJob(algorithms.ShortestPath, &protos.ShortestPathAlgorithmParams{
		From: *shortestPathFrom,
		To:   *shortestPathTo,
	})
}
