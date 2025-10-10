package util

import (
	"math/rand"
	"time"
)

// JitterDuration returns a random duration in [min, max).
func JitterDuration(minDur, maxDur time.Duration) time.Duration {
	if maxDur <= minDur {
		if minDur < 0 {
			return 0
		}
		return minDur
	}
	span := maxDur - minDur
	if span <= 0 {
		if minDur < 0 {
			return 0
		}
		return minDur
	}
	n := rand.Int63n(int64(span))
	return minDur + time.Duration(n)
}
