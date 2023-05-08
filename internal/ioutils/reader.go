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
		return io.NopCloser(os.Stdin), nil
	}

	// If not Stdin, open the filename and return a buffered ReadCloser
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return newBufferedReadCloser(file), nil
}

type bufferedReadCloser struct {
	file           *os.File
	bufferedReader *bufio.Reader
}

func newBufferedReadCloser(file *os.File) *bufferedReadCloser {
	return &bufferedReadCloser{
		file: file,
		bufferedReader: bufio.NewReader(file),
	}
}

func (brc *bufferedReadCloser) Read(p []byte) (int, error){
	return brc.bufferedReader.Read(p)
}

func (brc *bufferedReadCloser) Close() error {
	return brc.file.Close()
}
