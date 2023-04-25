/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package cmd

import (
	"fmt"

	"github.com/bluekiri/kafka-client/internal/formatters"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func bindFlags(cmd *cobra.Command) error {
	if err := viper.BindPFlag(importPath, cmd.Flags().Lookup(importPath)); err != nil {
		return err
	}
	return viper.BindPFlag(protoFile, cmd.Flags().Lookup(protoFile))
}

func addFormatFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP(formatRaw, "r", false, "write the message as raw bytes (default true if an output file is given).")
	cmd.Flags().BoolP(formatText, "t", false, "write the message as text (default true if no output file is given).")
	cmd.Flags().String(formatProto, "", "write the message as JSON using the given protobuf message type.")

	cmd.Flags().StringSlice(importPath, []string{"."}, "directory from which proto sources can be imported.")
	cmd.Flags().StringSlice(protoFile, []string{"*.proto"}, "the name of a proto source file. Imports will be resolved using the given --import-path flags. Multiple proto files can be specified by specifying multiple --proto-file flags.")

	cmd.MarkFlagDirname(importPath)
	cmd.RegisterFlagCompletionFunc(protoFile, wrapCompletion(completeProtoFile, bindFlags))
	cmd.RegisterFlagCompletionFunc(formatProto, wrapCompletion(completeProto, bindFlags))

	bindFlags(cmd)
}

func getFormatter(cmd *cobra.Command, filename string) (formatters.Formatter, error) {
	raw, _ := cmd.Flags().GetBool(formatRaw)
	text, _ := cmd.Flags().GetBool(formatText)
	messageFullName, _ := cmd.Flags().GetString(formatProto)

	// Ensure only one format is given
	nFormats := 0
	if raw {
		nFormats++
	}
	if text {
		nFormats++
	}
	if len(messageFullName) > 0 {
		nFormats++
	}
	if nFormats > 1 {
		return nil, fmt.Errorf("too many formats, expected only one format: raw, text or proto")
	}

	// Return the requested Formatter
	if raw {
		return formatters.NewRawFormatter(), nil
	}

	if text {
		return formatters.NewTextFormatter(), nil
	}

	if len(messageFullName) > 0 {
		return formatters.NewProtoFormatter(
			cmd.Context(),
			messageFullName,
			viper.GetStringSlice(protoFile),
			viper.GetStringSlice(importPath))
	}

	// If no formatter is requested return raw if filename is given or text otherwise
	if len(filename) > 0 {
		return formatters.NewRawFormatter(), nil
	}

	return formatters.NewTextFormatter(), nil
}
