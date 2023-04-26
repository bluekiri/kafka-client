/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
    version = "dev"
    commit  = "none"
    date    = "unknown"
)

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Prints the version.",
	Long: `All software has versions. This is mine.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("%s v%s (commit %s) compiled at %s", rootCmd.Name(), version, commit, date)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
