package util

import (
	"crypto/rand"
	"math/big"
	"time"
)

// JitterDuration returns a random duration in [min, max).
func JitterDuration(min, max time.Duration) time.Duration {
	if max <= min {
		if min < 0 {
			return 0
		}
		return min
	}
	span := max - min
	if span <= 0 {
		if min < 0 {
			return 0
		}
		return min
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(span)))
	if err != nil {
		return max
	}
	return min + time.Duration(n.Int64())
}
