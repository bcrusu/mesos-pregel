package cmd

import (
	"context"
	"fmt"

	"github.com/bcrusu/mesos-pregel/cli/api"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var resultCmd = &cobra.Command{
	Use:   "result",
	Short: "Displayes the result for a completed job.",
	RunE: func(cmd *cobra.Command, args []string) error {
		request := &protos.JobIdRequest{JobId: *jobID}

		return useAPI(func(client *api.Client) error {
			reply, err := client.GetJobResult(context.Background(), request)
			if err != nil {
				return errors.Wrapf(err, "failed to fetch job result")
			}

			switch reply.Status {
			case protos.CallStatus_OK:
				printJobResult(reply)
				return nil
			default:
				return errors.Errorf("API call failed with status %d", getCallStatusName(reply.Status))
			}
		})
	},
}

func printJobResult(reply *protos.GetJobResultReply) {
	fmt.Printf("Job: %s\n", reply.JobId)
	//TODO: format and print actual result
}
