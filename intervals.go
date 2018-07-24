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
	"math/rand"
	"time"
)

// IntervalGenerator takes the current time and returns a duration until the next request will be sent. The argument is
// usually ignored, but can be be used to build a time-varying interval generator (for example, to follow daily or
// hourly load variations for a web site).
type IntervalGenerator func(time.Time) time.Duration

// ExponentialIntervalGenerator creates an IntervalGenerator that outputs exponentially distributed
// intervals. The resulting arrivals constitute a Poisson process. The rate parameter is the average
// queries per second for the generator, and corresponds to the reciprocal of the lambda parameter
// to an exponential distribution. In English, if you want to generate 30 QPS on average, pass 30
// as the value of rate.
func ExponentialIntervalGenerator(rate float64) IntervalGenerator {
	rate = rate / float64(time.Second)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return func(_ time.Time) time.Duration {
		return time.Duration(r.ExpFloat64() / rate)
	}
}

// UniformIntervalGenerator creates and IntervalGenerator that outputs 1/rate every time it is
// called.
func UniformIntervalGenerator(rate float64) IntervalGenerator {
	var irate int64 = math.MaxInt64
	if rate != 0.0 {
		irate = int64(float64(time.Second) / rate)
	}
	return func(_ time.Time) time.Duration {
		return time.Duration(irate)
	}
}
