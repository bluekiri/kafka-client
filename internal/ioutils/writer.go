/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package ioutils

import (
	"bufio"
	"errors"
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
	return newBufferedWriteCloser(file), nil
}

type bufferedWriteCloser struct {
	file           *os.File
	bufferedWriter *bufio.Writer
}

func newBufferedWriteCloser(file *os.File) *bufferedWriteCloser {
	return &bufferedWriteCloser{
		file:           file,
		bufferedWriter: bufio.NewWriter(file),
	}
}

func (brc *bufferedWriteCloser) Write(p []byte) (int, error) {
	return brc.bufferedWriter.Write(p)
}

func (brc *bufferedWriteCloser) Close() error {
	flushErr := brc.bufferedWriter.Flush()
	closeErr := brc.file.Close()
	return errors.Join(flushErr, closeErr)
}
