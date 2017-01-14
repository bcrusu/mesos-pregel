package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Displayes the status and statistics for a job.",
	RunE: func(cmd *cobra.Command, args []string) error {
		jobID := *jobID

		//TODO
		fmt.Printf("Job %s status....\n", jobID)

		return nil
	},
}
