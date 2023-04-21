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

func Open(filename string) (io.ReadCloser, error) {
	if filename == "" {
		// Avoid trying to close stdin
		return NewReadCloserWrapper(os.Stdin, func() error { return nil }), nil
	}

	// If not Stdin, open the filename and return a buffered ReadCloser
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	readCloser := NewReadCloserWrapper(bufio.NewReader(file), file.Close)
	return readCloser, nil
}

func NewReadCloserWrapper(reader io.Reader, closer func() error) io.ReadCloser {
	return &readCloserWrapper{
		Reader: reader,
		closer: closer,
	}
}

type readCloserWrapper struct {
	io.Reader
	closer func() error
}

func (wrapper *readCloserWrapper) Close() error {
	return wrapper.closer()
}
