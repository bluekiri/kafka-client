/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package formatters

import (
	"bufio"
	"fmt"
	"io"

	"github.com/bluekiri/kafka-client/internal/dto"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	jsonMarshalOptions = protojson.MarshalOptions{
		Multiline:       false,
		EmitUnpopulated: true,
		AllowPartial:    false,
	}

	jsonUnmarshalOptions = protojson.UnmarshalOptions{
		DiscardUnknown: true,
		AllowPartial:   false,
	}
)

func NewProtoFormatter(messageType protoreflect.MessageType) Formatter {
	return &protoFactory{messageType}
}

type protoFactory struct {
	messageType protoreflect.MessageType
}

func (factory *protoFactory) NewReader(reader io.Reader) Reader {
	return &protoReader{factory.messageType.New().Interface(), bufio.NewReader(reader)}
}

func (factory *protoFactory) NewWriter(writer io.Writer) Writer {
	return &protoWriter{factory.messageType.New().Interface(), writer}
}

type protoReader struct {
	protoMessage protoreflect.ProtoMessage
	reader       *bufio.Reader
}

func (reader *protoReader) Read() (*dto.KafkaMessage, error) {
	jsonBytes, err := reader.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	pb := reader.protoMessage
	if err := jsonUnmarshalOptions.Unmarshal(jsonBytes, pb); err != nil {
		return nil, err
	}

	bytes, err := proto.Marshal(pb)
	if err != nil {
		return nil, err
	}

	message := &dto.KafkaMessage{
		Value: bytes,
	}

	return message, nil
}

type protoWriter struct {
	protoMessage protoreflect.ProtoMessage
	writer       io.Writer
}

func (writer *protoWriter) Write(message *dto.KafkaMessage) error {
	pb := writer.protoMessage

	// Unmarshal the protobuf object from the message
	err := proto.Unmarshal(message.Value, pb)
	if err != nil {
		return err
	}

	// Marshal the protobuf object to JSON
	jsonBytes, err := jsonMarshalOptions.Marshal(pb)
	if err != nil {
		return err
	}

	// Write the JSON string
	_, err = fmt.Fprintln(writer.writer, string(jsonBytes))
	return err
}
