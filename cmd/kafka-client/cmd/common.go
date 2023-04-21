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
