package util

import (
	"crypto/rand"
	"math/big"
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
	n, err := rand.Int(rand.Reader, big.NewInt(int64(span)))
	if err != nil {
		return maxDur
	}
	return minDur + time.Duration(n.Int64())
}
