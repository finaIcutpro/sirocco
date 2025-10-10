package util

import "net/http"

// CopyHeaders copies all headers from src to dst, preserving multi-valued headers.
func CopyHeaders(dst, src http.Header) {
	for k, v := range src {
		for _, vv := range v {
			dst.Add(k, vv)
		}
	}
}
