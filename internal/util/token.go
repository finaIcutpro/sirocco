package util

import "strings"

// MaskToken obfuscates the token for logs while keeping the prefix recognizable.
func MaskToken(token string) string {
	if len(token) <= 4 {
		return token
	}
	return token[:4] + strings.Repeat("*", len(token)-4)
}
