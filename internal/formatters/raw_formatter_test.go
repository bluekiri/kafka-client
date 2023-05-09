package formatters_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/bluekiri/kafka-client/internal/dto"
	"github.com/bluekiri/kafka-client/internal/formatters"
)

var expectedRawMessage = dto.KafkaMessage {
	Key: []byte("this is a key"),
	Value: []byte("this is a message"),
}

func TestRawFormatter(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}

	formatter := formatters.NewRawFormatter()

	// If we write a messsage to the formater and then read it we should
	// get a message that is equal to the written
	err = formatter.NewWriter(writer).Write(&expectedRawMessage)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	actual, err := formatter.NewReader(reader).Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(actual.Key, expectedRawMessage.Key) {
		t.Errorf("Expected key '%v' but got '%v'", expectedRawMessage.Key, actual.Key)
	}

	if !bytes.Equal(actual.Value, expectedRawMessage.Value) {
		t.Errorf("Expected value '%v' but got '%v'", expectedRawMessage.Value, actual.Value)
	}
}

func TestRawFormatterWriteError(t *testing.T) {
	_, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}

	formatter := formatters.NewRawFormatter()

	// Write should fail when trying to write to a closed writer
	writer.Close()
	err = formatter.NewWriter(writer).Write(&expectedRawMessage)
	if err == nil {
		t.Fatal("Write should have failed")
	}
}

func TestRawFormatterReadError(t *testing.T) {
	// Get a message in raw format
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}

	formatter := formatters.NewRawFormatter()

	err = formatter.NewWriter(writer).Write(&expectedRawMessage)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	buff := make([]byte, 1024)
	n, err := reader.Read(buff) 
	if err != nil {
		t.Fatalf("Read from pipe failed: %v", err)
	}

	raw := buff[:n]

	testCases := []int{
		3,
		4 + len(expectedRawMessage.Key) - 1,
		4 + len(expectedRawMessage.Key) + 3,
		4 + len(expectedRawMessage.Key) + 4 + len(expectedRawMessage.Value) - 1,
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d", tc), func(t *testing.T) {
			reader := bytes.NewReader(raw[:tc])
			_, err = formatter.NewReader(reader).Read()
			if err == nil {
				t.Fatal("Read should have failed")
			}		
		})
	}
}
