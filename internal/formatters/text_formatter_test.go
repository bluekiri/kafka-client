package formatters_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/bluekiri/kafka-client/internal/dto"
	"github.com/bluekiri/kafka-client/internal/formatters"
)

var expectedTextMessage = dto.KafkaMessage {
	Key: []byte("this is a key"),
	Value: []byte("this is a message"),
}

func TestTextFormatterWrite(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}

	formatter := formatters.NewTextFormatter()

	// TextFormatter Writer only writes the value followed by newline
	err = formatter.NewWriter(writer).Write(&expectedTextMessage)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	buff := make([]byte, 1024)
	n, err := reader.Read(buff)
	if err != nil {
		t.Fatalf("Read from pipe failed: %v", err)
	}

	actual, hasNewLine := bytes.CutSuffix(buff[:n], []byte("\n"))
	if !bytes.Equal(actual, expectedTextMessage.Value) {
		t.Errorf("Expected value '%v' but got '%v'", expectedTextMessage.Value, actual)
	}
	if !hasNewLine {
		t.Error("The writer must add a new line")
	}
}

func TestTextFormatterWriteError(t *testing.T) {
	_, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}

	formatter := formatters.NewTextFormatter()

	// TextFormatter Writer should fail when trying to write to a closed writer
	writer.Close()
	err = formatter.NewWriter(writer).Write(&expectedTextMessage)
	if err == nil {
		t.Fatal("Write should have failed")
	}
}

func TestTextFormatterRead(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}

	formatter := formatters.NewTextFormatter()

	// Write the value + new line
	_, err = fmt.Fprintln(writer, string(expectedTextMessage.Value))
	if err != nil {
		t.Fatalf("fmt.Fprintln failed: %v", err)
	}

	// TextFormatter Reader reads the value
	actual, err := formatter.NewReader(reader).Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(actual.Key, []byte{}) {
		t.Errorf("Expected empty key but got '%v'", actual.Key)
	}

	if !bytes.Equal(actual.Value, expectedTextMessage.Value) {
		t.Errorf("Expected value '%v' but got '%v'", expectedTextMessage.Value, actual.Value)
	}
}

func TestTextFormatterReadError(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}

	formatter := formatters.NewTextFormatter()

	// Write just the value (without new line)
	_, err = fmt.Fprint(writer, string(expectedTextMessage.Value))
	if err != nil {
		t.Fatalf("fmt.Fprint failed: %v", err)
	}
	writer.Close()

	// TextFormatter Reader should fail as the reader has no new line
	_, err = formatter.NewReader(reader).Read()
	if err == nil {
		t.Fatal("Read should have failed")
	}
}
