/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/bluekiri/kafka-client/internal/formatters"
	"github.com/bluekiri/kafka-client/internal/sliceutils"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	notInternalTopics = sliceutils.NotEqual("__consumer_offsets")
)

type CompleteFunc func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective)

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

func wrapCompletion(completion CompleteFunc, wrappers ...func(cmd *cobra.Command) error) CompleteFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		for _, wrapper := range wrappers {
			if err := wrapper(cmd); err != nil {
				cobra.CompErrorln(err.Error())
				return nil, cobra.ShellCompDirectiveError
			}
		}
		return completion(cmd, args, toComplete)
	}
}

func completeProtoFile(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	importPath := viper.GetStringSlice(importPath)
	cobra.CompDebugln(fmt.Sprintf("using import-path: %s", strings.Join(importPath, ", ")), true)

	// Find all regular files in import-path directories
	files := make([]string, 0)
	for _, importPath := range importPath {
		dirEntries, _ := os.ReadDir(importPath)
		for _, dirEntry := range dirEntries {
			if dirEntry.Type().IsRegular() && strings.HasSuffix(dirEntry.Name(), ".proto") {
				files = append(files, dirEntry.Name())
			}
		}
	}
	cobra.CompDebugln(fmt.Sprintf("found protobuffer files: %s", strings.Join(files, ", ")), true)
	return sliceutils.FilterSlice(files, sliceutils.PrefixFilter(toComplete)), cobra.ShellCompDirectiveNoFileComp
}

func completeProto(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	viper.BindPFlag(importPath, cmd.Flags().Lookup(importPath))
	viper.BindPFlag(protoFile, cmd.Flags().Lookup(protoFile))

	importPath := viper.GetStringSlice(importPath)
	cobra.CompDebugln(fmt.Sprintf("using import-path: %s", strings.Join(importPath, ", ")), true)

	protoFile := viper.GetStringSlice(protoFile)
	cobra.CompDebugln(fmt.Sprintf("using proto-file: %s", strings.Join(protoFile, ", ")), true)

	protoMessageTypes, err := formatters.ProtoMessageTypes(cmd.Context(), protoFile, importPath)
	if err != nil {
		cobra.CompErrorln(err.Error())
		return nil, cobra.ShellCompDirectiveError
	}
	cobra.CompDebugln(fmt.Sprintf("found protobuffer messages: %s", strings.Join(protoMessageTypes, ", ")), true)

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
	config.Net.DialTimeout = 500 * time.Millisecond
	config.Net.ReadTimeout = 500 * time.Millisecond
	config.Net.WriteTimeout = 500 * time.Millisecond
	config.Metadata.Timeout = 500 * time.Millisecond
	config.Metadata.Retry.Max = 0
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
	availableTopics, err := client.Topics()
	if err != nil {
		cobra.CompErrorln(err.Error())
		return nil, cobra.ShellCompDirectiveError
	}

	cobra.CompDebugln("Filtering topics", true)
	filter := notInternalTopics.And(sliceutils.PrefixFilter(toComplete))
	return sliceutils.FilterSlice(availableTopics, filter), cobra.ShellCompDirectiveDefault
}

func resolveCluster(cluster string) string {
	if hosts, ok := viper.GetStringMapString(clusters)[cluster]; ok && hosts != "" {
		return hosts
	}
	return cluster
}
