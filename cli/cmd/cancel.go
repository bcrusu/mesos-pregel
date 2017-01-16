package cmd

import (
	"context"
	"fmt"

	"github.com/bcrusu/mesos-pregel/cli/api"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var cancelCmd = &cobra.Command{
	Use:   "cancel",
	Short: "Cancels a running job.",
	RunE: func(cmd *cobra.Command, args []string) error {
		request := &protos.JobIdRequest{JobId: *jobID}

		return useAPI(func(client *api.Client) error {
			reply, err := client.CancelJob(context.Background(), request)
			if err != nil {
				return errors.Wrapf(err, "failed to cancel job")
			}

			switch reply.Status {
			case protos.CallStatus_OK:
				fmt.Printf("Job %s was cancelled.\n", *jobID)
				return nil
			default:
				return errors.Errorf("API call failed with status %s", getCallStatusName(reply.Status))
			}
		})
	},
}
