/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package handlers

import (
	"github.com/bluekiri/kafka-client/internal/dto"
	"github.com/bluekiri/kafka-client/internal/formatters"
)

func NewFileOutputHandler(input <-chan *dto.KafkaMessage, writer formatters.Writer) (OutputHandler, error) {
	handler := &fileOutputHandler{
		outputHandler: &outputHandler{
			input:    input,
			progress: make(chan error),
		},
		writer: writer,
	}
	return handler, nil
}

type fileOutputHandler struct {
	*outputHandler
	writer formatters.Writer
}

func (handler *fileOutputHandler) Run() error {
	defer handler.close()

	// Read next message from the input channel
	for message := range handler.input {
		// Write the message
		err := handler.writer.Write(message)

		// Notify the progress
		handler.progress <- err

		// If we got an error, return the error
		if err != nil {
			return err
		}
	}
	return nil
}
