package cmd

import (
	"context"
	"fmt"
	"sort"
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
	jobs := reply.Jobs
	if len(jobs) == 0 {
		fmt.Println(`No jobs found. Use the "start" command to create a new job.`)
		return
	}

	sortJobsByCreationTime(jobs)

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

func sortJobsByCreationTime(jobs []*protos.Job) {
	sorter := &jobSorterByCreationTime{jobs}
	sort.Sort(sorter)
}

type jobSorterByCreationTime struct {
	jobs []*protos.Job
}

func (s *jobSorterByCreationTime) Len() int {
	return len(s.jobs)
}

func (s *jobSorterByCreationTime) Less(i, j int) bool {
	return s.jobs[i].CreationTime < s.jobs[j].CreationTime
}

func (s *jobSorterByCreationTime) Swap(i, j int) {
	s.jobs[i], s.jobs[j] = s.jobs[j], s.jobs[i]
}
