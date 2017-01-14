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
		jobID := *jobID

		client := api.New()
		err := client.Dial(*apiAddress)
		if err != nil {
			return err
		}
		defer client.Close()

		request := &protos.JobIdRequest{JobId: jobID}
		reply, err := client.CancelJob(context.Background(), request)
		if err != nil {
			return errors.Wrapf(err, "API call failed")
		}

		switch reply.Status {
		case protos.CallStatus_OK:
			fmt.Printf("Job %s was cancelled.\n", jobID)
		default:
			return errors.Errorf("API call failed with status %d", reply.Status)
		}

		return nil
	},
}
