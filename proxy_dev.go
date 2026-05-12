//go:build dev

package main

import "net/http"

// executorHTTPClient returns a plain (non-attested) HTTP client
func executorHTTPClient(domain, repo string) (*http.Client, error) {
	return &http.Client{}, nil
}
