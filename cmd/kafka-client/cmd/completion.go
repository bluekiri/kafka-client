/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package cmd

import (
	"io"
	"os"
	"strings"

	"github.com/bluekiri/kafka-client/internal/formatters"
	"github.com/bluekiri/kafka-client/internal/sliceutils"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const completionLong = `
Generate autocompletions script for kafka-client for the specified shell (bash or zsh).

This command can generate shell autocompletions. e.g.

    $ kafka-client completion bash

Can be sourced as such

    $ source <(kafka-client completion bash)
`

var (
	completionShells = map[string]func(cmd *cobra.Command, out io.Writer) error{
		"bash":       runCompletionBash,
		"zsh":        runCompletionZsh,
		"powershell": runCompletionPowerShell,
	}

	notInternalTopics = sliceutils.NotEqual("__consumer_offsets")
)

func init() {
	rootCmd.AddCommand(newCompletionCmd())
}

func newCompletionCmd() *cobra.Command {
	shells := make([]string, 0, len(completionShells))
	for shell := range completionShells {
		shells = append(shells, shell)
	}

	cmd := &cobra.Command{
		Use:       "completion SHELL",
		Short:     "generate autocompletions script for the specified shell (bash or zsh)",
		Long:      completionLong,
		ValidArgs: shells,
		Args:      cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		RunE:      func(cmd *cobra.Command, args []string) error { return completionShells[args[0]](cmd, os.Stdout) },
	}

	return cmd
}

func runCompletionBash(cmd *cobra.Command, out io.Writer) error {
	return cmd.Root().GenBashCompletionV2(out, true)
}

func runCompletionZsh(cmd *cobra.Command, out io.Writer) error {
	return cmd.Root().GenZshCompletion(out)
}

func runCompletionPowerShell(cmd *cobra.Command, out io.Writer) error {
	return cmd.Root().GenPowerShellCompletionWithDesc(out)
}

func colonWorkarround(args []string) []string {
	if len(args) < 2 {
		return args
	}

	var normArgs []string
	currArg := args[0]
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if arg == ":" || strings.HasSuffix(currArg, ":") {
			currArg += arg
			continue
		}
		normArgs = append(normArgs, currArg)
		currArg = arg
	}
	normArgs = append(normArgs, currArg)

	return normArgs
}

func completeProtoFile(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	// Find all regular files in import-path directories
	files := make([]string, 0)
	for _, importPath := range viper.GetStringSlice(importPath) {
		dirEntries, _ := os.ReadDir(importPath)
		for _, dirEntry := range dirEntries {
			if dirEntry.Type().IsRegular() {
				files = append(files, dirEntry.Name())
			}
		}
	}
	return sliceutils.FilterSlice(files, sliceutils.SuffixFilter(".proto").And(sliceutils.PrefixFilter(toComplete))), cobra.ShellCompDirectiveNoFileComp
}

func completeProto(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	protoFile := viper.GetStringSlice(protoFile)

	protoMessageTypes, err := formatters.ProtoMessageTypes(cmd.Context(), protoFile, viper.GetStringSlice(importPath))
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}
	return sliceutils.FilterSlice(protoMessageTypes, sliceutils.PrefixFilter(toComplete)), cobra.ShellCompDirectiveDefault
}

func completeClustersAndTopic(nArgs int) func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		normArgs := colonWorkarround(args)

		if len(normArgs) >= nArgs {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}

		if len(normArgs)%2 == 0 {
			return completeClusters(cmd, normArgs, toComplete)
		}
		return completeTopic(cmd, normArgs, toComplete)
	}
}

func completeClusters(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	configuredClusters := viper.GetStringMap(clusters)
	clusters := make([]string, 0, len(configuredClusters))
	for cluster := range configuredClusters {
		if strings.HasPrefix(cluster, toComplete) {
			clusters = append(clusters, cluster)
		}
	}
	return clusters, cobra.ShellCompDirectiveDefault
}

func completeTopic(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	// Get the Kafka client
	clientID := viper.GetString(clientID)
	config := sarama.NewConfig()
	config.ClientID = clientID

	cobra.CompDebugln("Connecting to Kafka cluster", true)
	client, err := sarama.NewClient(
		strings.Split(resolveCluster(args[len(args)-1]), ","),
		config,
	)
	if err != nil {
		cobra.CompErrorln(err.Error())
		return nil, cobra.ShellCompDirectiveError
	}
	defer client.Close()

	cobra.CompDebugln("Getting topics", true)
	// Get the available topics
	availableTopics, err := client.Topics()
	if err != nil {
		cobra.CompErrorln(err.Error())
		return nil, cobra.ShellCompDirectiveError
	}

	cobra.CompDebugln("Filtering topics topics", true)
	// Filter topics
	filter := notInternalTopics
	if toComplete != "" {
		filter = filter.And(sliceutils.PrefixFilter(toComplete))
	}

	return sliceutils.FilterSlice(availableTopics, filter), cobra.ShellCompDirectiveDefault
}

func resolveCluster(cluster string) string {
	if hosts, ok := viper.GetStringMapString(clusters)[cluster]; ok && hosts != "" {
		return hosts
	}
	return cluster
}
