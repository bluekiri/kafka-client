/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package handlers

import (
	"context"
	"sync"

	"github.com/bluekiri/kafka-client/internal/dto"

	"github.com/IBM/sarama"
	"golang.org/x/sync/errgroup"
)

func NewKafkaInputHandler(client sarama.Client, topic string) (InputHandler, error) {
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	// Read the partitions of the topic
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return nil, err
	}

	handler := &kafkaInputHandler{
		inputHandler: &inputHandler{
			messages: make(chan *dto.KafkaMessage, len(partitions)),
			progress: make(chan error, len(partitions)),
		},
		consumer:   consumer,
		topic:      topic,
		partitions: partitions,
	}

	return handler, nil
}

type kafkaInputHandler struct {
	*inputHandler
	consumer   sarama.Consumer
	topic      string
	partitions []int32
}

func (handler *kafkaInputHandler) Start(ctx context.Context) func() error {
	handler.ctx = ctx
	return handler.run
}

func (handler *kafkaInputHandler) run() error {
	defer handler.close()
	defer handler.consumer.Close()

	g, ctx := errgroup.WithContext(handler.ctx)

	// For every partition, start a consumer goroutine
	for _, partition := range handler.partitions {
		consumePartition := partition
		g.Go(func() error {
			partitionConsumer, err := handler.consumer.ConsumePartition(handler.topic, consumePartition, sarama.OffsetNewest)
			if err != nil {
				return err
			}

			var wg sync.WaitGroup
			wg.Add(2)
			// Read messages from partition consumer and send them downstream
			go func() {
				defer wg.Done()
				for consumerMessage := range partitionConsumer.Messages() {
					message := &dto.KafkaMessage{
						Key:   consumerMessage.Key,
						Value: consumerMessage.Value,
					}
					handler.messages <- message
				}
			}()

			// Notify errors from partition consumer
			go func() {
				defer wg.Done()
				for err := range partitionConsumer.Errors() {
					handler.progress <- err
				}
			}()

			// Wait until the context is done, close the partition consumer
			// and wait until the consuming goroutines are done
			<-ctx.Done()
			partitionConsumer.AsyncClose()
			wg.Wait()
			return ctx.Err()
		})
	}

	return g.Wait()
}
