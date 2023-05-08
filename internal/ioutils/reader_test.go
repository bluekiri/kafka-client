package ioutils_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bluekiri/kafka-client/internal/ioutils"
)

func TestOpenStdin(t *testing.T) {
	const expected = "Hello"

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("error creating the pipe: %v", err)
	}
	prevStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = prevStdin
	}()
	
	readCloser, err := ioutils.Open("")

	if readCloser == nil {
		t.Fatal("readcloser should not be null")
	}

	if err != nil {
		t.Fatalf("ioutils.Open returned the error %v", err)
	}

	// Write something to the pipe
	w.WriteString(expected)

	// Read from the readcloser and close it
	buf := make([]byte, 1024)
	n, err := readCloser.Read(buf)
	if err != nil {
		t.Fatalf("Read returned the error %v", err)
	}
	if err = readCloser.Close(); err != nil {
		t.Fatalf("Close returned the error %v", err)
	}

	actual := string(buf[:n])
	if actual != expected {
		t.Fatalf("read '%s' but '%s' was expected", actual, expected)
	}
}

func TestOpenExistingFile(t *testing.T) {
	const expected = "TestOpenExistingFile"

	var filename = filepath.Join(os.TempDir(), "TestOpenExistingFile.txt")

	// Write some content to the file
	if err := os.WriteFile(filename, []byte(expected), 0600); err != nil {
		t.Fatalf("error creating the file %s: %v", filename, err)
	}
	// Ensure the file will be removed after the test
	defer func() {
		if err := os.Remove(filename); err != nil {
			t.Fatalf("Could not delete file %s: %v", filename, err)
		}
	}()

	readCloser, err := ioutils.Open(filename)

	if readCloser == nil {
		t.Fatal("readcloser should not be null")
	}

	if err != nil {
		t.Fatalf("ioutils.Open returned the error %v", err)
	}

	// Read from the readCloser and close it
	buf := make([]byte, 1024)
	n, err := readCloser.Read(buf)
	if err != nil {
		t.Fatalf("Read returned the error %v", err)
	}
	if err = readCloser.Close(); err != nil {
		t.Fatalf("Close returned the error %v", err)
	}

	actual := string(buf[:n])
	if actual != expected {
		t.Fatalf("read '%s' but '%s' was expected", actual, expected)
	}
}

func TestOpenNonExistingFile(t *testing.T) {
	readcloser, err := ioutils.Open("-1")

	if readcloser != nil {
		t.Fatal("readcloser should be null")
	}

	if err == nil {
		t.Fatal("ioutils.Open should have returned an error")
	}
}
