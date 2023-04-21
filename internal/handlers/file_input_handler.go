/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package handlers

import (
	"context"

	"github.com/bluekiri/kafka-client/internal/dto"
	"github.com/bluekiri/kafka-client/internal/errorutils"
	"github.com/bluekiri/kafka-client/internal/formatters"
)

func NewFileInputHandler(reader formatters.Reader) (InputHandler, error) {
	handler := &fileInputHandler{
		inputHandler: &inputHandler{
			messages: make(chan *dto.KafkaMessage),
			progress: make(chan error),
		},
		reader: reader,
	}

	return handler, nil
}

type fileInputHandler struct {
	*inputHandler
	reader formatters.Reader
}

func (handler *fileInputHandler) Start(ctx context.Context) func() error {
	handler.ctx = ctx
	return handler.run
}

func (handler *fileInputHandler) run() error {
	defer handler.close()

	done := make(chan struct{})

	// Start the read loop goroutine
	go errorutils.RecoverWith(errorutils.NoPanic,
		func() {
			// Signal the read loop is done when exiting
			defer close(done)

			for {
				// Read the message
				message, err := handler.reader.Read()
				if err != nil {
					handler.progress <- err
					return
				}

				// Send the message to the channel
				select {
				case handler.messages <- message:
				case <-handler.ctx.Done():
					return
				}
			}
		})

	// Wait for the read loop to finish or the context is done
	select {
	case <-done:
	case <-handler.ctx.Done():
	}
	return handler.ctx.Err()
}
