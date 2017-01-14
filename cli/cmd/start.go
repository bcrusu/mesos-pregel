package cmd

import "github.com/spf13/cobra"

var (
	shortestPathFrom = startShortestPathCmd.Flags().StringP("from", "f", "", "Source Vertex ID")
	shortestPathTo   = startShortestPathCmd.Flags().StringP("to", "t", "", "Destination Vertex ID")
)

func init() {
	startCmd.AddCommand(startShortestPathCmd)
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts a new job that executes the selected algorithm.",
}

var startShortestPathCmd = &cobra.Command{
	Use:   "shortestPath",
	Short: "Finds the shortest path from source to destination vertices.",
	RunE: func(cmd *cobra.Command, args []string) error {
		//TODO
		return nil
	},
}
