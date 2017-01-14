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

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists all jobs.",
	RunE: func(cmd *cobra.Command, args []string) error {
		request := &protos.EmptyRequest{}

		return useAPI(func(client *api.Client) error {
			reply, err := client.GetJobs(context.Background(), request)
			if err != nil {
				return errors.Wrapf(err, "failed to fetch jobs")
			}

			switch reply.Status {
			case protos.CallStatus_OK:
				printJobs(reply)
				return nil
			default:
				return errors.Errorf("API call failed with status %d", reply.Status)
			}
		})
	},
}

func printJobs(reply *protos.GetJobsReply) {
	if len(reply.Jobs) == 0 {
		fmt.Println(`No jobs found. Use the "job start" command to create a new job.`)
		return
	}

	layout := "%s\t\t\t%s\t\t%s\t\t%s\n"

	fmt.Printf(layout, "Job", "Status", "Creation time", "ID")

	for _, job := range reply.Jobs {
		status := getJobStatusName(job.Status)
		creationTime := time.Unix(job.CreationTime, 0)
		creationTimeStr := creationTime.Format("yyyy-MM-dd HH:mm:ss")

		fmt.Printf(layout, job.Label, status, creationTimeStr, job.Id)
	}
}

func getJobStatusName(status protos.JobStatus) string {
	name, ok := protos.JobStatus_name[int32(status)]
	if !ok {
		return "unknown"
	}

	return name
}
