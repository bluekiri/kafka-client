package errorutils_test

import (
	"testing"

	"github.com/bluekiri/kafka-client/internal/errorutils"
)

func TestRecoverWithNoHandler(t *testing.T) {
	const errorMessage = "not handled by RecoverWith"
	defer func() {
		err := recover()
		if err == nil {
			t.Fatal("recover should have returned an error")
		}
		if err.(string) != errorMessage {
			t.Fatalf("expected '%s' but got '%s'", errorMessage, err)
		}
	}()

	errorutils.RecoverWith(nil, func() {
		panic(errorMessage)
	})
}

func TestRecoverWithNoPanicHandler(t *testing.T) {
	const errorMessage = "recovered by NoPanic"
	defer func() {
		err := recover()
		if err != nil {
			t.Fatal("panic should have been handeled by the NoPanic handler")
		}
	}()

	errorutils.RecoverWith(errorutils.NoPanic, func() {
		panic(errorMessage)
	})
}

func TestRecoverWithCustomHandler(t *testing.T) {
	const errorMessage = "recovered by custom handler"
	customHandler := func(err any) {
		if err.(string) != errorMessage {
			t.Fatal("RecoverWith should have passed the panic argument to err")
		}
	}

	defer func() {
		err := recover()
		if err != nil {
			t.Fatal("panic should have been handeled by the custom handler")
		}
	}()

	errorutils.RecoverWith(customHandler, func() {
		panic(errorMessage)
	})
}
