/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	logger  = log.New(os.Stderr, "", log.LstdFlags)
)

const (
	rootShort = "Command line client for Kafka"
	rootLong  = `Command line Kafka consumer and producer which can be used to save raw messages to
a file or display protobuf messages in JSON representation.`
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:           "kafka-client",
	Short:         rootShort,
	Long:          rootLong,
	SilenceErrors: true,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, config, "", "config file (default is $HOME/.kafka-client.yaml)")
	rootCmd.PersistentFlags().StringP(clientID, "c", "kafka-client", "client ID to sent to Kafka")
	rootCmd.PersistentFlags().DurationP(duration, "d", 0, "time to wait before exiting")
	rootCmd.PersistentFlags().BoolP(quiet, "q", false, "enable quiet mode")

	viper.BindPFlag(clientID, rootCmd.PersistentFlags().Lookup(clientID))
	viper.BindPFlag(duration, rootCmd.PersistentFlags().Lookup(duration))
	viper.BindPFlag(quiet, rootCmd.PersistentFlags().Lookup(quiet))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".kafka-client" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".kafka-client")
	}

	viper.SetEnvPrefix("kafka-client")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	viper.ReadInConfig()
}

func interruptableContext(parent context.Context, timeout time.Duration) context.Context {
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, timeout)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}

	// Listen for interrupt signals
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

		// Wait until user interrupts execution and then cancel the context
		<-interrupt
		cancel()
	}()

	return ctx
}

func adaptError(err error) error {
	if errors.Is(err, context.Canceled) {
		return errors.New("interrupted by user")
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return nil
	}

	return err
}
