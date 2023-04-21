/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package handlers

import (
	"context"

	"github.com/bluekiri/kafka-client/internal/dto"
)

type InputHandler interface {
	Start(context.Context) func() error
	Messages() <-chan *dto.KafkaMessage
	Progress() ProgressSource
}

type inputHandler struct {
	messages chan *dto.KafkaMessage
	progress chan error
	ctx      context.Context
}

func (handler *inputHandler) Messages() <-chan *dto.KafkaMessage {
	return handler.messages
}

func (handler *inputHandler) Progress() ProgressSource {
	return handler.progress
}

func (handler *inputHandler) close() {
	close(handler.messages)
	close(handler.progress)
}

type OutputHandler interface {
	Run() error
	Progress() ProgressSource
}

type outputHandler struct {
	input    <-chan *dto.KafkaMessage
	progress chan error
}

func (handler *outputHandler) Progress() ProgressSource {
	return handler.progress
}

func (handler *outputHandler) close() {
	close(handler.progress)
}
