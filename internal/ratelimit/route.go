package ratelimit

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
)

func Normalize(method, path string) string {
	if path == "" {
		path = "/"
	}

	parts := strings.Split(path, "/")
	preserve := false
	for i := 1; i < len(parts); i++ {
		segment := parts[i]
		switch strings.ToLower(segment) {
		case "channels", "guilds", "webhooks":
			preserve = true
		default:
			if preserve {
				preserve = false
				continue
			}
			if snowflake(segment) {
				parts[i] = ":id"
			}
		}
	}

	if method == http.MethodHead {
		method = http.MethodGet
	}
	return strings.ToUpper(method) + " " + strings.Join(parts, "/")
}

func TokenKey(token string) string {
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func MaskToken(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	if len(token) <= 6 {
		return "***"
	}
	return token[:4] + "..." + token[len(token)-2:]
}

func snowflake(value string) bool {
	if len(value) < 5 {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}
