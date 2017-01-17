package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/bcrusu/mesos-pregel/cli/api"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Displays statistics for a running job.",
	RunE: func(cmd *cobra.Command, args []string) error {
		request := &protos.JobIdRequest{JobId: *jobID}

		return useAPI(func(client *api.Client) error {
			reply, err := client.GetJobStats(context.Background(), request)
			if err != nil {
				return errors.Wrapf(err, "failed to fetch job status")
			}

			switch reply.Status {
			case protos.CallStatus_OK:
				printJobStats(reply)
				return nil
			default:
				return errors.Errorf("API call failed with status %s", getCallStatusName(reply.Status))
			}
		})
	},
}

func printJobStats(reply *protos.GetJobStatsReply) {
	job := reply.Job

	fmt.Printf("Job: %s - %s\n", job.Label, job.Id)
	fmt.Printf("Status: %s\n", getJobStatusName(job.Status))

	if job.Status != protos.JobStatus_Running {
		return
	}

	fmt.Printf("Superstep: %d\n", reply.Superstep)
	fmt.Println()

	totalDuration := time.Duration(reply.CurrentStats.TotalDuration)
	computeDuration := time.Duration(reply.CurrentStats.ComputeDuration)

	fmt.Printf(`Current superstep:
TotalDuration=%.2f (sec)
ComputedCount=%d
ComputeDuration=%.2f (sec)
SentMessagesCount=%d
HaltedCount=%d
InactiveCount=%d
`, totalDuration.Seconds(), reply.CurrentStats.ComputedCount, computeDuration.Seconds(),
		reply.CurrentStats.SentMessagesCount, reply.CurrentStats.HaltedCount, reply.CurrentStats.InactiveCount)

	totalDuration = time.Duration(reply.TotalStats.TotalDuration)
	computeDuration = time.Duration(reply.TotalStats.ComputeDuration)

	fmt.Println()

	fmt.Printf(`Total:
TotalDuration=%.2f (sec)
ComputedCount=%d
ComputeDuration=%.2f (sec)
SentMessagesCount=%d
`, totalDuration.Seconds(), reply.TotalStats.ComputedCount, computeDuration.Seconds(), reply.TotalStats.SentMessagesCount)
}
