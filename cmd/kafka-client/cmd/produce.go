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
	"github.com/bluekiri/kafka-client/internal/timeutils"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

const (
	produceExample = "kafka-client produce localhost:9092 my_topic"
	produceShort   = "Produces messages to a Kafka topic."
	produceLong    = `produce command uses bootstrap_servers to get the brokers of the Kafka cluster
and produces messages to the indicated topic reading them from stdin unless a
filename is provided by the --input flag.`
)

// produceCmd represents the produce command
var produceCmd = &cobra.Command{
	Use:               "produce bootstrap_servers topic",
	Short:             produceShort,
	Long:              produceLong,
	Example:           produceExample,
	Args:              cobra.ExactArgs(2),
	ValidArgsFunction: completeClustersAndTopic(2),
	RunE:              produce,
}

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().StringP(input, "i", "", "read from file instead of stdin.")
	produceCmd.MarkFlagFilename(input)

	produceCmd.Flags().DurationP(period, "p", 0, "time to wait between producing two messages.")
	viper.BindPFlag(period, produceCmd.Flags().Lookup(period))

	addFormatFlags(produceCmd)
}

func produce(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true

	// Get the command arguments and flags
	kafkaBrokers := strings.Split(resolveCluster(args[0]), ",")
	kafkaTopic := args[1]
	kafkaClientID := viper.GetString(clientID)
	inputFilename, _ := cmd.Flags().GetString(input)
	pacerPeriod := viper.GetDuration(period)
	duration := viper.GetDuration(duration)
	reportingPeriod := time.Duration(1) * time.Second
	if viper.GetBool(quiet) {
		reportingPeriod = -1
	}

	// Get the formatter
	formatter, err := getFormatter(cmd, inputFilename)
	if err != nil {
		return err
	}

	// Get the reader (source of messages)
	reader, err := ioutils.Open(inputFilename)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Kafka configuration
	config := sarama.NewConfig()
	config.ClientID = kafkaClientID
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

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

	// Create the pacer
	pacer, stopPacer := timeutils.NewPacer(pacerPeriod)
	defer stopPacer()

	// Create the handlers
	inputHandler, err := handlers.NewFileInputHandler(formatter.NewReader(reader))
	if err != nil {
		return nil
	}
	outputHandler, err := handlers.NewKafkaOutputHandler(inputHandler.Messages(), pacer, client, kafkaTopic)
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
	logInput := ""
	if inputFilename != "" {
		logInput = fmt.Sprintf(" from '%s'", inputFilename)
	}
	logEvery := ""
	if pacerPeriod != 0 {
		logEvery = fmt.Sprintf(" every %v", pacerPeriod)
	}
	logger.Printf(
		"producing messages%s to cluster %s topic %s%s",
		logInput,
		strings.Join(kafkaBrokers, ","), kafkaTopic,
		logEvery,
	)

	// Return the error group error
	return adaptError(g.Wait())
}
