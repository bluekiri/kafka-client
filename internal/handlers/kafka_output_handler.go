/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package handlers

import (
	"time"

	"github.com/bluekiri/kafka-client/internal/dto"

	"github.com/Shopify/sarama"
)

func NewKafkaOutputHandler(input <-chan *dto.KafkaMessage, pacer <-chan time.Time, client sarama.Client, topic string) (OutputHandler, error) {
	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	handler := &kafkaOutputHandler{
		outputHandler: &outputHandler{
			input:    input,
			progress: make(chan error),
		},
		pacer:    pacer,
		producer: producer,
		topic:    topic,
	}

	return handler, nil
}

type kafkaOutputHandler struct {
	*outputHandler
	pacer    <-chan time.Time
	producer sarama.AsyncProducer
	topic    string
}

func (handler *kafkaOutputHandler) Run() error {
	defer handler.close()
	go handler.produce()
	handler.notifyProgress()
	return nil
}

func (handler *kafkaOutputHandler) produce() {
	defer handler.producer.AsyncClose()

	// Read next message from the input channel
	for message := range handler.input {
		producerMessage := &sarama.ProducerMessage{
			Topic: handler.topic,
			Value: sarama.ByteEncoder(message.Value),
		}
		// If we have no key, don't set the key
		if len(message.Key) > 0 {
			producerMessage.Key = sarama.ByteEncoder(message.Key)
		}

		// Wait for the timer (or cancelation)
		<-handler.pacer

		// Produce the message to Kafka
		handler.producer.Input() <- producerMessage
	}
}

func (handler *kafkaOutputHandler) notifyProgress() {
	successes, errors := handler.producer.Successes(), handler.producer.Errors()
	for successes != nil || errors != nil {
		select {
		case _, ok := <-successes:
			if !ok {
				successes = nil
				continue
			}
			handler.progress <- nil
		case err, ok := <-errors:
			if !ok {
				errors = nil
				continue
			}
			handler.progress <- err
		}
	}
}
