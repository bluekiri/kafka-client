/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package ioutils

import (
	"bufio"
	"io"
	"os"
)

func Create(filename string) (io.WriteCloser, error) {
	if filename == "" {
		return os.Stdout, nil
	}

	// If not Stdout, open the filename and return a buffered WriteCloser
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(file)
	closer := func() error {
		writer.Flush()
		return file.Close()
	}
	writeCloser := NewWriteCloserWrapper(writer, closer)
	return writeCloser, nil
}

func NewWriteCloserWrapper(writer io.Writer, closer func() error) io.WriteCloser {
	return &writeCloserWrapper{
		Writer: writer,
		closer: closer,
	}
}

type writeCloserWrapper struct {
	io.Writer
	closer func() error
}

func (wrapper *writeCloserWrapper) Close() error {
	return wrapper.closer()
}
