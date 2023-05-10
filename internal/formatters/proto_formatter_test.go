package formatters_test

import (
	"bytes"
	"context"
	"os"
	"path"
	"testing"

	"github.com/bluekiri/kafka-client/internal/dto"
	"github.com/bluekiri/kafka-client/internal/formatters"
	"github.com/bluekiri/kafka-client/internal/protoutils"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	expectedProtoMessage dto.KafkaMessage
	messageType protoreflect.MessageType
)

func init() {
	// Get the messageType
	workingDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	messageType, err = protoutils.ResolveProtoMessageType(
		context.Background(),
		"test.TestMessage",
		[]string{"test.proto"},
		[]string{path.Join(workingDir, "testdata")},
	)
	if err != nil {
		panic(err)
	}

	// Construct and serialice a message value
	value := messageType.New()
	value.Set(messageType.Descriptor().Fields().ByTextName("value"), protoreflect.ValueOf("this is a proto message"))
	messageValue, err := proto.Marshal(value.Interface())
	if err != nil {
		panic(err)
	}

	// Construct a KafkaMessage
	expectedProtoMessage = dto.KafkaMessage{
		Key: []byte("this is a key"),
		Value: messageValue,
	}
}

func TestProtoFormatter(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}

	formatter := formatters.NewProtoFormatter(messageType)

	// If we write a messsage to the formater and then read it we should
	// get a message that is equal to the written
	err = formatter.NewWriter(writer).Write(&expectedProtoMessage)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	actual, err := formatter.NewReader(reader).Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(actual.Key, []byte{}) {
		t.Errorf("Expected empty key but got '%v'", actual.Key)
	}

	if !bytes.Equal(actual.Value, expectedProtoMessage.Value) {
		t.Errorf("Expected value '%v' but got '%v'", expectedProtoMessage.Value, actual.Value)
	}
}