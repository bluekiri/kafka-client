package protoutils

import (
	"context"

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/linker"
	"google.golang.org/protobuf/reflect/protoreflect"
)

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

func ResolveProtoMessageType(ctx context.Context, messageFullName string, protoFiles []string, importPaths []string) (protoreflect.MessageType, error) {
	resolver, err := compileProtoFiles(ctx, protoFiles, importPaths)
	if err != nil {
		return nil, err
	}

	// Resolve the message descriptor
	return resolver.FindMessageByName(protoreflect.FullName(messageFullName))
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