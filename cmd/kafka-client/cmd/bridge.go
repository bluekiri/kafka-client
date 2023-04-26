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
	"github.com/bluekiri/kafka-client/internal/sliceutils"
	"github.com/bluekiri/kafka-client/internal/timeutils"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

const (
	bridgeExample = "kafka-client bridge localhost:9092 from_topic localhost:9092 to_topic"
	bridgeShort   = "Bridges messages from a Kafka topic to another Kafka topic."
	bridgeLong    = `bridge command consumes messages from from_topic, using from_bootstrap_servers
to get the brokers of the source Kafka cluster, and produces those messages to
to_topic, using to_bootstrap_servers to get the brokers of the destination
Kafka cluster.`
)

// bridgeCmd represents the bridge command
var bridgeCmd = &cobra.Command{
	Use:               "bridge source_bootstrap_servers source_topic destination_bootstrap_servers destination_topic",
	Short:             bridgeShort,
	Long:              bridgeLong,
	Example:           bridgeExample,
	Args:              cobra.ExactArgs(4),
	ValidArgsFunction: completeClustersAndTopic(4),
	RunE:              bridge,
}

func init() {
	rootCmd.AddCommand(bridgeCmd)

	bridgeCmd.Flags().DurationP(period, "p", 0, "time to wait between producing two messages.")
	viper.BindPFlag(period, bridgeCmd.Flags().Lookup(period))
}

func bridge(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true

	// Get the command arguments and flags
	inputKafkaBrokers := strings.Split(resolveCluster(args[0]), ",")
	inputKafkaTopic := args[1]
	outputKafkaBrokers := strings.Split(resolveCluster(args[2]), ",")
	outputKafkaTopic := args[3]
	kafkaClientID := viper.GetString(clientID)
	pacerPeriod := viper.GetDuration(period)
	duration := viper.GetDuration(duration)
	reportingPeriod := time.Duration(1) * time.Second
	if viper.GetBool(quiet) {
		reportingPeriod = -1
	}
	// input Kafka configuration
	inputConfig := sarama.NewConfig()
	inputConfig.ClientID = kafkaClientID
	inputConfig.Consumer.Return.Errors = true

	// Get the input Kafka client
	inputClient, err := sarama.NewClient(inputKafkaBrokers, inputConfig)
	if err != nil {
		return err
	}
	defer inputClient.Close()

	// Check input topic exists
	if topics, err := inputClient.Topics(); err != nil {
		return err
	} else if !sliceutils.Contains(topics, inputKafkaTopic) {
		return fmt.Errorf("kafka: topic %s does not exist in source cluster", inputKafkaTopic)
	}

	// input Kafka configuration
	outputConfig := sarama.NewConfig()
	outputConfig.ClientID = kafkaClientID
	outputConfig.Producer.Return.Successes = true
	outputConfig.Producer.Return.Errors = true

	// Get the input Kafka client
	outputClient, err := sarama.NewClient(outputKafkaBrokers, outputConfig)
	if err != nil {
		return err
	}
	defer outputClient.Close()

	// Check input topic exists
	if topics, err := outputClient.Topics(); err != nil {
		return err
	} else if !sliceutils.Contains(topics, outputKafkaTopic) {
		return fmt.Errorf("kafka: topic %s does not exist in destination cluster", outputKafkaTopic)
	}

	// Create the pacer
	pacer, stopPacer := timeutils.NewPacer(pacerPeriod)
	defer stopPacer()

	// Create the handlers
	inputHandler, err := handlers.NewKafkaInputHandler(inputClient, inputKafkaTopic)
	if err != nil {
		return nil
	}
	outputHandler, err := handlers.NewKafkaOutputHandler(inputHandler.Messages(), pacer, outputClient, outputKafkaTopic)
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
	logEvery := ""
	if pacerPeriod != 0 {
		logEvery = fmt.Sprintf(" every %v", pacerPeriod)
	}
	logger.Printf(
		"bridging messages from cluster %s topic %s to cluster %s topic %s%s",
		strings.Join(inputKafkaBrokers, ","), inputKafkaTopic,
		strings.Join(outputKafkaBrokers, ","), outputKafkaTopic,
		logEvery,
	)
	logger.Printf("press ctrl-c to exit")

	// Return the error group error
	return adaptError(g.Wait())
}
