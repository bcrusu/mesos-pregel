package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var resultCmd = &cobra.Command{
	Use:   "result",
	Short: "Displayes the result for a completed job.",
	RunE: func(cmd *cobra.Command, args []string) error {
		jobID := *jobID

		//TODO
		fmt.Printf("Job %s result....\n", jobID)

		return nil
	},
}
