/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package errorutils

// PanicHandler is called for recovering from panics in the RecoverWith function.
// Defaults to nil, which means panics are not recovered
type PanicHandler func(interface{})

// NoPanic is a PanicHandler that recovers the panic doing nothing
var NoPanic = func(interface{}) {}

// RecoverWith executes the funcion f and, if f panics, recovers the execution
// using the PanicHandler.
func RecoverWith(handler PanicHandler, f func()) {
	defer func() {
		// If no handler is passed, don't recover panic
		if handler != nil {
			if err := recover(); err != nil {
				handler(err)
			}
		}
	}()

	f()
}
