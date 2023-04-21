/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package handlers

import (
	"log"
	"sync"
	"time"
)

type ProgressSource <-chan error

type ReportingHandler interface {
	Start(ProgressSource, ...ProgressSource) func() error
}

func NewReportingHandler(logger *log.Logger, period time.Duration) ReportingHandler {
	return &reportingHandler{
		logger:  logger,
		sources: []ProgressSource{},
		period:  period,
	}
}

type reportingHandler struct {
	logger     *log.Logger
	sources    []ProgressSource
	aggregated chan error
	period     time.Duration
}

func (handler *reportingHandler) Start(source ProgressSource, sources ...ProgressSource) func() error {
	handler.sources = append(handler.sources, source)
	handler.sources = append(handler.sources, sources...)
	handler.aggregated = make(chan error, len(handler.sources))

	return handler.run
}

func (handler *reportingHandler) run() error {
	// Channel to signal logging gorountine finished
	done := make(chan struct{})

	// Start the logging goroutine
	go func() {
		// Signal logging goroutine finished
		defer close(done)

		// Get the reporting ticker
		var tickerChan <-chan time.Time;
		if handler.period > 0 {
			ticker := time.NewTicker(handler.period)
			defer ticker.Stop()
			tickerChan = ticker.C
		}

		// Accounting and logging loog
		successes, errors, prevSuccesses := 0, 0, 0
	loop:
		for {
			select {
			case err, ok := <-handler.aggregated:
				if !ok {
					break loop
				}
				if err != nil {
					handler.logger.Printf("error processing message: %v\n", err)
					errors++
				} else {
					successes++
				}
			case <-tickerChan:
				handler.logger.Printf("messages processed: %d (total: %d | errors: %d)\n", successes-prevSuccesses, successes, errors)
				prevSuccesses = successes
			}
		}
		handler.logger.Printf("total processed messages: %d\n", successes)
	}()

	// Start the aggregating goroutines
	var g sync.WaitGroup
	g.Add(len(handler.sources))
	for _, source := range handler.sources {
		go func(source ProgressSource) {
			defer g.Done()
			for err := range source {
				handler.aggregated <- err
			}
		}(source)
	}

	// Wait for the aggregating gorountines to exit and then close the
	// aggregated channel to signal the logging goroutine to exit
	g.Wait()
	close(handler.aggregated)

	// Wait for the logging goroutine to exit
	<-done

	return nil
}
