/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/bluekiri/kafka-client/internal/handlers"
	"github.com/bluekiri/kafka-client/internal/ioutils"
	"github.com/bluekiri/kafka-client/internal/sliceutils"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

const (
	consumeExample = "kafka-client consume localhost:9092 my_topic"
	consumeShort   = "Consumes messages from a Kafka topic."
	consumeLong    = `consume command uses bootstrap_servers to get the brokers of the Kafka cluster
and consume messages from the indicated topic printing them to stdout unless a
filename is provided by the --output flag.`
)

// consumeCmd represents the consume command
var consumeCmd = &cobra.Command{
	Use:               "consume bootstrap_servers topic",
	Short:             consumeShort,
	Long:              consumeLong,
	Example:           consumeExample,
	Args:              cobra.ExactArgs(2),
	ValidArgsFunction: completeClustersAndTopic(2),
	RunE:              consume,
}

func init() {
	rootCmd.AddCommand(consumeCmd)

	consumeCmd.Flags().StringP(output, "o", "", "write to file instead of stdout.")
	consumeCmd.Flags().BoolP(formatRaw, "r", false, "write the message as raw bytes (default true if an output file is given).")
	consumeCmd.Flags().BoolP(formatText, "t", false, "write the message as text (default true if no output file is given).")
	consumeCmd.Flags().String(formatProto, "", "write the message as JSON using the given protobuf message type.")

	consumeCmd.Flags().StringSlice(importPath, []string{"."}, "directory from which proto sources can be imported.")
	consumeCmd.Flags().StringSlice(protoFile, []string{"*.proto"}, "the name of a proto source file. Imports will be resolved using the given --import-path flags. Multiple proto files can be specified by specifying multiple --proto-file flags.")

	consumeCmd.MarkFlagDirname(importPath)
	consumeCmd.RegisterFlagCompletionFunc(protoFile, completeProtoFile)
	consumeCmd.RegisterFlagCompletionFunc(formatProto, completeProto)
	consumeCmd.MarkFlagFilename(output)

	viper.BindPFlag(importPath, consumeCmd.Flags().Lookup(importPath))
	viper.BindPFlag(protoFile, consumeCmd.Flags().Lookup(protoFile))
}

func consume(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true

	// Get the command arguments and flags
	kafkaBrokers := strings.Split(resolveCluster(args[0]), ",")
	kafkaTopic := args[1]
	kafkaClientID := viper.GetString(clientID)
	outputFilename, _ := cmd.Flags().GetString(output)
	duration := viper.GetDuration(duration)
	reportingPeriod := time.Duration(1) * time.Second
	if viper.GetBool(quiet) {
		reportingPeriod = -1
	}

	// Get the formatter
	formatter, err := getFormatter(cmd, outputFilename)
	if err != nil {
		return err
	}

	// Get the writer (sink of messages)
	writer, err := ioutils.Create(outputFilename)
	if err != nil {
		return err
	}
	defer writer.Close()

	// Kafka configuration
	config := sarama.NewConfig()
	config.ClientID = kafkaClientID
	config.Consumer.Return.Errors = true

	// Get the Kafka client
	client, err := sarama.NewClient(kafkaBrokers, config)
	if err != nil {
		return err
	}
	defer client.Close()

	// Check topic exists
	if topics, err := client.Topics(); err != nil {
		return err
	} else if !sliceutils.Contains(topics, kafkaTopic) {
		return fmt.Errorf("kafka: topic %s does not exist", kafkaTopic)
	}

	// Create the handlers
	inputHandler, err := handlers.NewKafkaInputHandler(client, kafkaTopic)
	if err != nil {
		return nil
	}
	outputHandler, err := handlers.NewFileOutputHandler(inputHandler.Messages(), formatter.NewWriter(writer))
	if err != nil {
		return nil
	}
	reportingHandler := handlers.NewReportingHandler(logger, reportingPeriod)

	// Get the interruptable context
	ctx := interruptableContext(cmd.Context(), duration)

	// Create the error group
	g, ctx := errgroup.WithContext(ctx)

	// Start reporting goroutine
	g.Go(reportingHandler.Start(inputHandler.Progress(), outputHandler.Progress()))

	// Start the output goroutine
	g.Go(outputHandler.Run)

	// Start the input goroutine
	g.Go(inputHandler.Start(ctx))

	// Log start
	logOutput := ""
	if outputFilename != "" {
		logOutput = fmt.Sprintf(" to '%s'", outputFilename)
	}
	logger.Printf(
		"consuming messages from cluster %s topic '%s'%s",
		strings.Join(kafkaBrokers, ","), kafkaTopic,
		logOutput,
	)

	// Return the error group error
	return adaptError(g.Wait())
}
