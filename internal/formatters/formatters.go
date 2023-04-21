/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package formatters

import (
	"io"

	"github.com/bluekiri/kafka-client/internal/dto"
)

type Reader interface {
	Read() (*dto.KafkaMessage, error)
}

type Writer interface {
	Write(*dto.KafkaMessage) error
}

type Formatter interface {
	NewReader(io.Reader) Reader
	NewWriter(io.Writer) Writer
}
