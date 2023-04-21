/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package formatters

import (
	"bufio"
	"fmt"
	"io"

	"github.com/bluekiri/kafka-client/internal/dto"
)

func NewTextFormatter() Formatter {
	return &textFactory{}
}

type textFactory struct{}

func (factory *textFactory) NewReader(reader io.Reader) Reader {
	return &textReader{bufio.NewReader(reader)}
}

func (factory *textFactory) NewWriter(writer io.Writer) Writer {
	return &textWriter{writer}
}

type textReader struct {
	reader *bufio.Reader
}

func (reader *textReader) Read() (*dto.KafkaMessage, error) {
	line, err := reader.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	message := &dto.KafkaMessage{
		Value: line[:len(line)-1],
	}
	return message, nil
}

type textWriter struct {
	writer io.Writer
}

func (writer *textWriter) Write(message *dto.KafkaMessage) error {
	_, err := fmt.Fprintln(writer.writer, string(message.Value))
	return err
}
