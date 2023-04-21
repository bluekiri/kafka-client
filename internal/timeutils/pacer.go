/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package timeutils

import "time"

/*
NewPacer is a convenient function that returns a time.Ticker channel if
duration is greater than 0 and a closed channel if its not.
Reading from the closed channel will return inmediately so no pace will
be introduced.

Usage example:

	  pacer, stop := NewPacer(period)
	  defer stop()

	  for {
		<-pacer
		doSomethingEveryPeriod()
	  }
*/
func NewPacer(period time.Duration) (<-chan time.Time, func()) {
	if period > 0 {
		ticker := time.NewTicker(period)
		return ticker.C, ticker.Stop
	}
	
	// If period is <= 0, mock a ticker with a closed channel
	tick := make(chan time.Time)
	close(tick)
	return tick, func() {}
}
