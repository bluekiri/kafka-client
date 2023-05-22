package handlers_test

import (
	"bufio"
	"errors"
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bluekiri/kafka-client/internal/handlers"
)

func TestReportingHandler(t *testing.T) {
	tickDuration := time.Duration(100) * time.Millisecond

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal("could not create the pipe")
	}

	handler := handlers.NewReportingHandler(
		log.New(writer, "", 0),
		tickDuration,
	)

	// Create the single source
	source := make(chan error)
	destination := make(chan string)

	// Start the handler in a goroutine
	go handler.Start(source)()

	// Start the goroutine that reads the logged lines
	go func() {
		bufferedReader := bufio.NewReader(reader)
		for {
			line, err := bufferedReader.ReadBytes('\n')
			if err == io.EOF {
				close(destination)
				return
			}
			if err != nil {
				t.Error("error reading logged lines")
				return
			}
			
			destination <- string(line)
		}
	}()

	var actual, expected string
	var timer *time.Timer

	// Write a nil error to the source, then wait for a logged line or 1 millisecond
	expected = ""
	source <- nil
	timer = time.NewTimer(1 * time.Millisecond)
	select {
	case actual = <- destination:
		if actual != expected {
			t.Errorf("expected '%v' but got '%v'", expected, actual)
		}
	case <- timer.C:
		// this is the expected path
		timer.Stop()
	}

	// Write an error to the source, then wait for a logged line or 1 millisecond
	testError := errors.New("test error")
	expected = testError.Error()
	source <- testError
	timer = time.NewTimer(1 * time.Millisecond)
	select {
	case actual = <- destination:
		// this is the expected path
		if !strings.Contains(actual, expected) {
			t.Errorf("expected string containing '%v' but got '%v'", expected, actual)
		}
	case <- timer.C:
		timer.Stop()
		t.Errorf("expected string containing '%v' but got nothing", expected)
	}

	// Wait for summary
	expected = "messages processed: 1 (total: 1 | errors: 1)"
	actual = <- destination
	if !strings.Contains(actual, expected) {
		t.Errorf("expected '%v' but got '%v'", expected, actual)
	}

	close(source)
	writer.Close()
	reader.Close()
}