/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package formatters

import (
	"encoding/binary"
	"io"

	"github.com/bluekiri/kafka-client/internal/dto"
)

func NewRawFormatter() Formatter {
	return &rawFactory{}
}

type rawFactory struct{}

func (factory *rawFactory) NewReader(reader io.Reader) Reader {
	return &rawReader{reader}
}

func (factory *rawFactory) NewWriter(writer io.Writer) Writer {
	return &rawWriter{writer}
}

type rawReader struct {
	reader io.Reader
}

func (reader *rawReader) Read() (*dto.KafkaMessage, error) {
	// Read the key
	key, err := reader.readBytes()
	if err != nil {
		return nil, err
	}

	// Read the value
	value, err := reader.readBytes()
	if err != nil {
		return nil, err
	}

	message := &dto.KafkaMessage{
		Key:   key,
		Value: value,
	}

	return message, nil
}

func (reader *rawReader) readBytes() ([]byte, error) {
	// Read the data length
	var length uint32
	if err := binary.Read(reader.reader, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	// Read the data
	data := make([]byte, length)
	if _, err := io.ReadFull(reader.reader, data); err != nil {
		return nil, err
	}
	return data, nil
}

type rawWriter struct {
	writer io.Writer
}

func (writer *rawWriter) Write(message *dto.KafkaMessage) error {
	if err := writer.writeBytes(message.Key); err != nil {
		return err
	}
	return writer.writeBytes(message.Value)
}

func (writer *rawWriter) writeBytes(bytes []byte) error {
	if err := binary.Write(writer.writer, binary.LittleEndian, uint32(len(bytes))); err != nil {
		return err
	}
	_, err := writer.writer.Write(bytes)
	return err
}
