package timeutils_test

import (
	"testing"
	"time"

	"github.com/bluekiri/kafka-client/internal/timeutils"
)

func FuzzNewPacerWithInvalidPeriod(f *testing.F) {
	f.Add(0)
	f.Add(-1)

	f.Fuzz(func(t *testing.T, seconds int) {
		if seconds > 0 {
			t.SkipNow()
		}

		pacer, closer := timeutils.NewPacer(time.Duration(seconds) * time.Second)

		if pacer == nil {
			t.Fatal("channel should not be nil")
		}

		if closer == nil {
			t.Fatal("close function chould not be nil")
		}

		defer closer()

		select {
		case _, ok := <- pacer:
			if ok {
				t.Error("channel should be closed")
			}
		default:
			// this is the wrong path: if channel is closed, reading from it in the previous
			// case should return ok = false
			t.Error("channel should be closed")
		}
	})
}

func FuzzNewPacerWithValidPeriod(f *testing.F) {
	f.Add(1)

	f.Fuzz(func(t *testing.T, seconds int) {
		if seconds <= 0 {
			t.SkipNow()
		}
		
		pacer, closer := timeutils.NewPacer(time.Duration(seconds) * time.Second)

		if pacer == nil {
			t.Fatal("channel should not be nil")
		}

		if closer == nil {
			t.Fatal("close function chould not be nil")
		}

		defer closer()

		select {
		case _, ok := <- pacer:
			if !ok {
				t.Error("channel should be open")
			}
		default:
			// this is the right path: if channel is open, reading from it in the previous
			// case would block and the default case will execute.
		}
	})
}
