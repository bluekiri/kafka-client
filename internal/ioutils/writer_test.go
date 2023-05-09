package ioutils_test

import (
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/bluekiri/kafka-client/internal/ioutils"
)

func TestCreateStdout(t *testing.T) {
	const expected = "Hello"

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("error creating the pipe: %v", err)
	}
	prevStdout := os.Stdout
	os.Stdout = w
	defer func() {
		os.Stdout = prevStdout
	}()

	writeCloser, err := ioutils.Create("")

	if writeCloser == nil {
		t.Fatal("writecloser should not be null")
	}

	if err != nil {
		t.Fatalf("ioutils.Create returned the error %v", err)
	}

	// Write something to the writeCloser and close it
	_, err = writeCloser.Write([]byte(expected))
	if err != nil {
		t.Fatalf("Write returned the error %v", err)
	}
	if err = writeCloser.Close(); err != nil {
		t.Fatalf("Close returned the error %v", err)
	}

	// Read from the pipe
	buf := make([]byte, 1024)
	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("Read returned the error %v", err)
	}

	actual := string(buf[:n])
	if actual != expected {
		t.Fatalf("read '%s' but '%s' was expected", actual, expected)
	}
}

func TestCreateExistingFile(t *testing.T) {
	const expected = "TestCreateExistingFile"

	var filename = filepath.Join(os.TempDir(), "TestCreateExistingFile.txt")

	// Write some previous content to the file.
	if err := os.WriteFile(filename, []byte("Previous content"), 0600); err != nil {
		t.Fatalf("error creating the file %s: %v", filename, err)
	}
	// Ensure the file will be removed after the test
	defer func() {
		if err := os.Remove(filename); err != nil {
			t.Fatalf("Could not delete file %s: %v", filename, err)
		}
	}()

	writeCloser, err := ioutils.Create(filename)

	if writeCloser == nil {
		t.Fatal("readcloser should not be null")
	}

	if err != nil {
		t.Fatalf("ioutils.Create returned the error %v", err)
	}

	// Write to the writeCloser and close it
	if _, err = writeCloser.Write([]byte(expected)); err != nil {
		t.Fatalf("Write returned the error %v", err)
	}
	if err = writeCloser.Close(); err != nil {
		t.Fatalf("Close returned the error %v", err)
	}

	// Read the contents of the file
	actual, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("os.ReadFile returned the error %v", err)
	}
	if string(actual) != expected {
		t.Fatalf("read '%s' but '%s' was expected", actual, expected)
	}
}

func TestCreateNonExistingFile(t *testing.T) {
	const expected = "TestCreateNonExistingFile"

	var filename = filepath.Join(os.TempDir(), "TestCreateNonExistingFile.txt")

	writeCloser, err := ioutils.Create(filename)

	if writeCloser == nil {
		t.Fatal("readcloser should not be null")
	}

	if err != nil {
		t.Fatalf("ioutils.Create returned the error %v", err)
	}

	// Ensure the file will be removed after the test
	defer func() {
		if err := os.Remove(filename); err != nil {
			t.Fatalf("Could not delete file %s: %v", filename, err)
		}
	}()

	// Write to the writeCloser and close it
	if _, err = writeCloser.Write([]byte(expected)); err != nil {
		t.Fatalf("Write returned the error %v", err)
	}
	if err = writeCloser.Close(); err != nil {
		t.Fatalf("Close returned the error %v", err)
	}

	// Read the contents of the file
	actual, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("os.ReadFile returned the error %v", err)
	}
	if string(actual) != expected {
		t.Fatalf("read '%s' but '%s' was expected", actual, expected)
	}
}

func TestCreateNotAllowedFile(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "TestCreateNotAllowedFile")
	if err != nil {
		t.Fatalf("os.MkdirTemp returned the error %v", err)
	}

	// Make temporary directory read only
	err = os.Chmod(tmpDir, 0600)
	if err != nil {
		t.Fatalf("os.Chmod returned the error %v", err)
	}

	// Try to create the output file in the readonly directory
	var filename = path.Join(tmpDir, "TestCreateNotAllowedFile.txt")

	writeCloser, err := ioutils.Create(filename)

	if writeCloser != nil {
		if err := writeCloser.Close(); err != nil {
			t.Logf("Close returned the error %v", err)
		}
		if err := os.Remove(filename); err != nil {
			t.Logf("os.Remove returned the error %v", err)
		}
		t.Fatal("writeCloser should be null")
		
	}

	if err == nil {
		t.Fatal("ioutils.Create should have returned an error")
	}
}
