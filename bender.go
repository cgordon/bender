/*
Copyright 2014-2016 Pinterest, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bender

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
	"runtime"
)

type Request interface{}
type Response interface{}

// RequestExecutor takes the current time and a request, sends the request to the service, waits for the response and
// returns the response or an error. This function is used by the load tester to determine the service time, so it
// should do as little else as possible.
type RequestExec func(time.Time, Request) (Response, error)

// TimingEvent records all of the timing information for a single request.
type TimingEvent struct {
	// The amount of time that the main loop waited between the previous request and this one (as returned by the
	// interval generator).
	WaitTime time.Duration

	// The start time of the request, taken before the request executor was called.
	StartTime time.Time

	// The end time of the request, taken after the request executor returned.
	EndTime time.Time

	// The amount of "overage" time that had accumulated at the time of this request. Overage time is a measure of how
	// much slower the load tester is going than it "should" be going, and includes time spent in garbage collection,
	// over-sleeping and waiting for requests to be generated.
	Overage time.Duration

	// The request executed by the executor, as received from the request channel.
	Request Request

	// The response returned by the executor, or nil if the request resulted in an error.
	Response Response

	// The error returned by the executor, or nil if the request returned a response.
	Error error

	// The number of goroutines when this request began
	NumGoroutines int

	// The amount of time it took to spawn the goroutine
	GoroutineDelay time.Duration
}

// LoadTestResults records global statistics about the entire run of the load tester, to help tune future runs.
type LoadTestResults struct {
	// The number of times the load tester blocked waiting for a request from the request channel, indicating that the
	// code to generate requests is too slow, or the channel does not have enough of a buffer.
	ReqStalls int64

	// The number of times the load tester blocked trying to write a TimingEvent to the recorder channel, indicating that
	// the goroutine reading the channel was too slow, or the channel does not have enough of a buffer.
	RecStalls int64
}

// LoadTestThroughput sends load using the request executor at intervals defined by the interval generator and it reads
// requests to send from the reqs channel and writes the results to the recorder channel. The return result is a LoadTestResults
// struct that contains tuning information for use in setting the sizes of the channels.
func LoadTestThroughput(intervals IntervalGenerator, reqs chan Request, reqExec RequestExec, recorder chan TimingEvent) LoadTestResults {
	var (
		reqStalls int64
		recStalls int64
		wg        sync.WaitGroup
		overage   time.Duration
	)

	for {
		loopStart := time.Now()

		var req Request
		select {
		case req = <-reqs:
		default:
			reqStalls += 1
			req = <-reqs
		}

		if req == nil {
			break
		}

		wait := intervals(loopStart)
		adj := time.Duration(math.Min(float64(wait), float64(overage)))
		wait -= adj
		overage -= adj

		time.Sleep(time.Duration(wait))

		overage += time.Now().Sub(loopStart) - wait

		wg.Add(1)
		beforeGoroutine := time.Now()
		go func(req Request, wait time.Duration, overage time.Duration) {
			defer wg.Done()
			startGoroutineDelay := time.Now().Sub(beforeGoroutine)
			numGoroutines := runtime.NumGoroutine()
			reqStart := time.Now()
			res, err := reqExec(reqStart, req)

			tev := TimingEvent{
				wait,
				reqStart,
				time.Now(),
				overage,
				req,
				res,
				err,
				numGoroutines,
				startGoroutineDelay,
			}

			select {
			case recorder <- tev:
			default:
				atomic.AddInt64(&recStalls, 1)
				recorder <- tev
			}
		}(req, wait, overage)
	}

	wg.Wait()
	close(recorder)

	return LoadTestResults{reqStalls, recStalls}
}
