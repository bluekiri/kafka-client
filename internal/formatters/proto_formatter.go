/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package formatters

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/bluekiri/kafka-client/internal/dto"

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/linker"
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

func NewProtoFormatter(ctx context.Context, messageFullName string, protoFiles []string, importPaths []string) (Formatter, error) {
	resolver, err := compileProtoFiles(ctx, protoFiles, importPaths)
	if err != nil {
		return nil, err
	}

	// Resolve the message descriptor
	messageType, err := resolver.FindMessageByName(protoreflect.FullName(messageFullName))
	if err != nil {
		return nil, err
	}

	return &protoFactory{messageType}, nil
}

func ProtoMessageTypes(ctx context.Context, protoFiles []string, importPaths []string) ([]string, error) {
	resolver, err := compileProtoFiles(ctx, protoFiles, importPaths)
	if err != nil {
		return nil, err
	}

	// Resolve all the message descriptors
	messageTypes := make([]string, 0)
	for _, protoFile := range protoFiles {
		fileDescriptor, err := resolver.FindFileByPath(protoFile)
		if err != nil {
			return nil, err
		}
		messages := fileDescriptor.Messages()
		for i := 0; i < messages.Len(); i++ {
			messageTypes = append(messageTypes, string(messages.Get(i).FullName()))
		}
	}
	return messageTypes, nil
}

func compileProtoFiles(ctx context.Context, protoFiles []string, importPaths []string) (linker.Resolver, error) {
	// Create the protobuf compiler
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: importPaths,
		}),
	}

	// Compile the .proto files
	files, err := compiler.Compile(ctx, protoFiles...)
	if err != nil {
		return nil, err
	}
	return files.AsResolver(), nil
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
